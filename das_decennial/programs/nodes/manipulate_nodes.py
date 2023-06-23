"""
manipulate_nodes.py:  The core functionality for manipulating nodes as part of the top-down algorithm.

"""
# python imports
import logging
import numpy as np
from collections import defaultdict
from typing import Tuple, List, Iterable, Union, Callable
# das-created imports
import das_utils
import das_framework.ctools.clogging as clogging
import programs.sparse as sparse
import programs.queries.constraints_dpqueries as cons_dpq
import programs.utilities.numpy_utils as np_utils
import programs.optimization.sequential_optimizers as sequential_optimizers
from programs.nodes.nodes import GeounitNode
from das_constants import CC

def geoimp_wrapper(*, config, parent_child_node, optimizers, keep_debug_info=False, aian=False):
    """
    This function performs the Post-Processing Step for a generic parent to the Child geography.
    It is called from topdown_engine.py:topdown in a Spark map operation.
    It runs on the CORE and TASK nodes, not on the MASTER.
    So there is no das object!

    Inputs:
        config: configuration object
        parent_child_node: a (k,v) RDD with key being a geocode and
            value being a tuple of GeounitNode objects containing one parent and multiple children
        optimizers: which L2, Rounder and SequentialOptimizer to use,
        keep_debug_info: keep dp_queries ans syn_unrounded in the optimized nodes; delete if False
        aian: if it's AIAN spine, it will keep state total invariance on US -> ({aian_parts_of_states} + {non_aian_parts_of_states}) optimization

    Output:
        children: a list of Node objects for each of the children, after post-processing
        :param optimizers:
    """

    # Make sure that the logger is set up on all the nodes
    clogging.setup(level=logging.INFO, syslog=True,
                   syslog_address=(das_utils.getMasterIp(), CC.SYSLOG_UDP))

    parent, children = findParentChildNodes(parent_child_node)
    parent_hist = parent.getDenseSyn(), parent.getDenseSynHousing()
    parent_geocode = parent.geocode
    parent_shape = tuple(h.shape for h in parent_hist)

    #######
    # under certain circumstances we can skip the gurobi optimization
    #######

    # Only 1 child
    if len(children) == 1:
        children = [children[0].copyParentSyn(parent, keep_debug_info)]
        return constraintsCheck(children, config, parent_geocode)

    # If the parent is empty (NOTE: all histograms should be empty. Also, the sum check obviously works only if values are non-negative)
    if parent.histsAreEmpty():
        children = [child.copyParentSyn(parent, keep_debug_info, zerosyn=True) for child in children]
        print(f"parent geocode {parent_geocode} is empty")
        return constraintsCheck(children, config, parent_geocode)

    #########
    # resume code for gurobi optimization
    ########

    children = [child.unzipNoisy() for child in children]

    children = makeInputsAndRunOptimizer(children, config, parent_hist, parent_shape, parent_geocode, optimizers, keep_debug_info=keep_debug_info,
                                         aian=aian)
    return children


def geoimp_wrapper_root(*, config, parent_shape, root_node: GeounitNode, optimizers, keep_debug_info=False):
    """
    This function performs the Post-Processing Step of Root Geonode (e.g. US or a State) to Root Geonode level.
    It is called from engine_utils.py:topdown in a Spark map operation

    Inputs:
        config: configuration object
        root_node: a GeounitNode object referring to the top/root node of the geographical tree (e.g. US, US+PR or a single state for state-size runs)

    Output:
        root_node: a GeounitNode object referring to the top/root node of the geographical tree (e.g. US, US+PR or a single state for state-size runs)
        :param optimizers:
    """
    # Make sure that the logger is set up on all of the nodes
    clogging.setup(level=logging.INFO, syslog=True,
                   syslog_address=(das_utils.getMasterIp(), CC.SYSLOG_UDP))
    # t_start = time.time()
    parent_hist = [None] * len(parent_shape)

    children = [GeounitNode.fromZipped(root_node).unzipNoisy()]

    children = makeInputsAndRunOptimizer(children, config, parent_hist, parent_shape, "root_to_root", optimizers, keep_debug_info=keep_debug_info)

    return children[0]


def makeInputsAndRunOptimizer(children, config, parent_hist, parent_shape, parent_geocode, optimizers, keep_debug_info=False, aian=False):
    """
    Converts the data from nodes to the inputs taken by optimizer: multiarrays, StackedConstraints, StackedDPQueries etc.,
    creates the optimizer, runs it, and puts the optimized answers back into the nodes

    This is called from:
         * geoimp_wrapper_root().
         * geoimp_wrapper()
    :param optimizers:
    :param children: iterable (list or multiarray) of children noisy histograms (i.e. detailed query measurements, aka noisy counts)
    :param config: DAS config file
    :param parent_hist: optimized histogram of the parent node
    :param parent_shape: shape of the parent histogram (children have the same shape too)
    :param parent_geocode: parent geocode
    :param keep_debug_info: whether to delete DPqueries after optimization (they take a lot of space) and not include unrounded optimized data into the node
    :return: list of optimized children nodes
    """

    if config.getboolean(section=CC.ENGINE, option="reset_dpq_weights", fallback=False):
        variances = []
        for child in children:
            variances.extend(child.getAllVariances())
        min_var = min(variances)
        children = [child.setDPQVar(func=lambda v: v/min_var) for child in children]

    # # This is to make sure that total constraint is not accidentially left on for AIAN and non-AIAN, but really should be taken care of in config
    # # Have to set up the total US population as invariant, and turn of State
    # if aian:
    #     for child in children:
    #         child.removeConstraintByName('total')

    child_groups = makeChildGroups(children) if aian else None

    # # This is to make sure that total constraint is not accidentially left on for AIAN and non-AIAN, but really should be taken care of in config
    # # Have to set up the total US population as invariant, and turn of State
    # if aian:
    #     for child in children:
    #         child.removeConstraintByName('total')

    # Get the stacked detailed dp_queries (if we've taken detailed measurements), as well as their weights. If only one child, just expand.
    noisy_child = np.stack([child.stackDetailedDPAnswers(parent_shape) for child in children], axis=-1) if children[0].dp else None
    noisy_child_weights = [child.detailedWeight() for child in children]
    constraints_comb = stackNodeProperties(children, lambda node: node.cons, cons_dpq.StackedConstraint)

    # A loop over histograms. Each iteration goes over children (stackNodeProperties does that) and gets the dp_queries dict
    # corresponding to that histogram and stacks them:
    dp_queries_comb = [stackNodeProperties(children, lambda node: node.querySets2Stack()[i], cons_dpq.StackedDPquery) for i in range(len(parent_shape))]
    # Do the same for rounder_queries:
    rounder_queries_comb = [stackNodeProperties(children, lambda node: rq, cons_dpq.StackedQuery) for rq in children[0].rounder_queries]

    opt_dict = {
        "Cons": stackNodeProperties(children, lambda node: node.opt_dict["Cons"], cons_dpq.StackedConstraint),
        "npass_info": children[0].opt_dict["npass_info"],
    } if children[0].opt_dict is not None else None

    sequential_optimizers_dict = {
        CC.L2_PLUS_ROUNDER: sequential_optimizers.L2PlusRounder,
        CC.L2_PLUS_ROUNDER_INTERLEAVED: sequential_optimizers.L2PlusRounder_interleaved,
    }

    seq_opt_name, l2_opt, rounder = optimizers
    seq_opt_cls = sequential_optimizers_dict[seq_opt_name]

    try:
        l2c2o = children[0].query_ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING]
    except KeyError:
        l2c2o = None

    if config.getboolean(option=CC.USE_POSTPROCESS_HEURISTICS, section=CC.GUROBI, fallback=False):
        require_feasible_postproc_dim = get_require_feasible_postproc_dim(config, children)
    else:
        require_feasible_postproc_dim = False

    # Create an appropriate sequential optimizer object
    seq_opt = seq_opt_cls(identifier=parent_geocode, child_geolevel=children[0].geolevel,
                          parent=parent_hist, parent_shape=parent_shape,
                          childGeoLen=len(children), constraints=constraints_comb, NoisyChild=noisy_child,
                          noisy_child_weights=noisy_child_weights, DPqueries=dp_queries_comb,
                          rounder_queries=rounder_queries_comb, child_groups=child_groups, opt_dict=opt_dict,
                          L2_DPqueryOrdering=children[0].query_ordering[CC.L2_QUERY_ORDERING],
                          L2_Constrain_to_Ordering=l2c2o,
                          Rounder_DPqueryOrdering=children[0].query_ordering[CC.ROUNDER_QUERY_ORDERING],
                          optimizers=(l2_opt, rounder), das=None, config=config,
                          require_feasible_postproc_dim=require_feasible_postproc_dim)

    l2_answer, int_answer = seq_opt.run()

    # Slice off the combined child solution to make lists of ndarrays, with one element for each child
    int_answer_list = np_utils.sliceArray(int_answer[0])
    unit_int_answer_list = np_utils.sliceArray(int_answer[1])
    l2_answer_list = np_utils.sliceArray(l2_answer[0])

    for i, child in enumerate(children):
        child.syn = int_answer_list[i]
        child.unit_syn = unit_int_answer_list[i]
    constraintsCheck(children, config, constraints=constraints_comb)

    # Convert to sparse arrays for efficiency
    for i, child in enumerate(children):
        child.syn = sparse.multiSparse(int_answer_list[i])
        child.unit_syn = sparse.multiSparse(unit_int_answer_list[i])
        if keep_debug_info:
            child.syn_unrounded = sparse.multiSparse(l2_answer_list[i])
        else:
            child.dp_queries.clear()
    return children


def stackNodeProperties(children: Iterable[GeounitNode], get_prop_func: Callable, stacking_func: Callable):
    """
        This function takes a child node, extracts their individual properties and builds
        a stacked version across children for each unique property (constraint or dp_query)
        Inputs:
            children: list of child nodes
            get_prop_func: function that pulls out the property which will be stacked out of a node (e.g. lambda node: node.cons)
            stacking_func: function that creates the stacked object (StackedConstraint or StackedDPquery)
        Outpus:
            stacked_prop_comb: a list of stacked objects
    """
    # children may have different properties/dp_queries. only combine the ones that match by name.

    # Dictionary: for each constraint/dp_query name as key has list of tuples: (constraint/dp_query, index_of_child_that_has_it)
    stacked_prop_dict = defaultdict(list)
    for i, properties in enumerate(map(get_prop_func, children)):
        if properties is None or not any(properties):
            return []  # None
        for key, prop in properties.items():
            stacked_prop_dict[key].append((prop, i))

    stacked_prop_comb = [stacking_func(prop_ind) for key, prop_ind in stacked_prop_dict.items()]

    return stacked_prop_comb


def constraintsCheck(nodes: Iterable[GeounitNode], config=None, parent_geocode=None, constraints=None):
    """
    This function checks that a set of constraints is met given a solution.
    It will raise an exception if any constraint is not met.
    Inputs:
        node: geounit node with "syn" field to be checked against the node constraints
    """

    constr_names = [constr.name for constr in nodes[0].cons.values()]
    if config is not None:
        use_postprocess_heuristics = config.getboolean(option=CC.USE_POSTPROCESS_HEURISTICS, section=CC.GUROBI, fallback=False)
        postprocess_only_constr_names = get_gurobi_tuple(config, False, option=CC.POSTPROCESS_ONLY_CONSTR_NAMES)
        conditional_constr_names = get_gurobi_tuple(config, False, option=CC.CONDITIONAL_CONSTR_NAMES)
        # We optionally also ensure that the remaining constraints are satisfied:
        require_feasible_secondary_dim = config.getboolean(option=CC.REQUIRE_FEASIBLE_SECONDARY_DIM, section=CC.GUROBI, fallback=True)
        if use_postprocess_heuristics:
            require_feasible_postproc_dim = get_require_feasible_postproc_dim(config, nodes)
        else:
            require_feasible_postproc_dim = False

        if not use_postprocess_heuristics:
            constrs_to_skip = ()
        elif require_feasible_secondary_dim and require_feasible_postproc_dim:
            constrs_to_skip = ()
        elif require_feasible_secondary_dim and not require_feasible_postproc_dim:
            constrs_to_skip = postprocess_only_constr_names
        else:
            assert not require_feasible_secondary_dim and require_feasible_postproc_dim, f"use_postprocess_heuristics is turned on but both require_feasible_secondary_dim and require_feasible_postproc_dim are False."
            constrs_to_skip = tuple(constr.name for constr in constraints if (constr.name not in conditional_constr_names) and (constr.name not in postprocess_only_constr_names))
            #From accuracy_experiments_v2 merge
            #constrs_to_check = postprocess_only_constr_names + conditional_constr_names
    else:
        constrs_to_skip = ()

    for node in nodes:
        if parent_geocode is None:
            parent_geocode = node.parentGeocode
        out = node.checkConstraints(raw=False, return_list=True, constrs_to_skip=constrs_to_skip)
        failed_constrs = [] if type(out) == bool else out
        assert len(failed_constrs) == 0, f"Constraints for parent geocode {parent_geocode} failed. failed_constrs:\n{failed_constrs}\n"
        print(f"Constraints for parent geocode {parent_geocode} are satisfied.")
    return nodes

def findParentChildNodes(parent_child_node: Tuple[str, Union[Iterable[GeounitNode], Tuple[Iterable[GeounitNode]]]]) -> Tuple[GeounitNode, List[GeounitNode]]:
    """
    This function inputs an RDD element containing both a parent and child(ren) nodes,
    figures out which is which, and separates them
    Inputs:
        parent_child_node: an (k,v) rdd, value is a tuple containing both a parent and child(ren) nodes, key is parent geocode
    Outputs:
        parent: parent node
        children: list of children nodes
    """

    # Key of (k,v) pair given as argument
    parent_geocode: str = parent_child_node[0]
    # print("parent geocode is", parent_geocode)

    # Value of (k,v) converted to a list of the node objects.
    if isinstance(parent_child_node[1], tuple):
        # (v would be a tuple of (pyspark.resultiterable.ResultIterable, ) if this function is called from spark rdd map() and rdd was .cogroup-ed)
        list_of_nodelists = [list(node) for node in parent_child_node[1]]
        nodes_list: List[GeounitNode] = list_of_nodelists[0] + list_of_nodelists[1]
    else:
        # (v would be pyspark.resultiterable.ResultIterable if this function is called from spark rdd map() and rdd was .groupByKey-ed)
        nodes_list: List[GeounitNode] = list(parent_child_node[1])

    nodes_list = [GeounitNode.fromZipped(node) for node in nodes_list]

    # calculate the length of each of the geocodes (to determine which is the parent)
    geocode_lens = [len(node.geocode) for node in nodes_list]
    # the parent is the shortest geocode
    parent_ind: int = np.argmin(geocode_lens)

    # Alternatively, parent is where geocode is equal to k of (k,v) pair given as argument, also works
    # parent_ind = [node.geocode for node in nodes_list].index(parent_geocode)

    # Get the parent (it's also removed from list)
    parent = nodes_list.pop(parent_ind)

    # Check the the code found by argmin is the same as the parent geocode taken from key of the pair
    assert parent.geocode == nodes_list[0].parentGeocode

    # sort the children
    children = sorted(nodes_list, key=lambda geocode_data: geocode_data.geocode)

    return parent, children

# def findParentChildNodes(children_parent_joined: Tuple[str, Union[Iterable[GeounitNode], Tuple[Iterable[GeounitNode]]]]) -> Tuple[GeounitNode, List[GeounitNode]]:
#     """
#     This function inputs an RDD element containing both a parent and child(ren) nodes,
#     figures out which is which, and separates them
#     Inputs:
#         children_parent_joined: an (k,v) rdd, value is a tuple containing both a parent and child(ren) nodes, key is parent geocode
#     Outputs:
#         parent: parent node
#         children: list of children nodes
#     """
#
#     nodes_list = list(children_parent_joined[1][0])
#     parent = children_parent_joined[1][1]
#
#     # sort the children
#     children = sorted(nodes_list, key=lambda geocode_data: geocode_data.geocode)
#
#     return parent, children


def makeChildGroups(children):
    """ Aggregate AIAN and non-AIAN part of each state into the full state"""

    # Put indices of children in children array into bystategeo dict, with key being their state geocode
    bystategeo = defaultdict(list)
    for ichild, child in enumerate(children):
        bystategeo[child.geocode[1:]].append(ichild)

    # Create child groups going over the dict
    child_groups = []
    for stcode, child_indices in bystategeo.items():
        # If there's no AIAN area, put the child in group by itself, attach its total
        if len(child_indices) == 1:
            child_groups.append(((child_indices[0],), children[child_indices[0]].invar['tot']))
        # If there is an AIAN area and non-AIAN part of the state, put them in the group together, attach total as the sum of totals
        elif len(child_indices) == 2:
            child_groups.append(((child_indices[0], child_indices[1]), children[child_indices[0]].invar['tot'] + children[child_indices[1]].invar['tot']))
        # Either 1 or 2 children per state, nothing else should happen
        else:
            raise ValueError(f"More than 2 AIAN/non-AIAN elements within state {stcode}")
    return child_groups


def get_gurobi_tuple(config, is_bool, **kwargs):
    # Note that we set section=CC.GUROBI and the fallback="" in call to config.get() but when config.get() resorts to fallback we return ():
    res = tuple(str_k.strip() for str_k in config.get(section=CC.GUROBI, fallback="", **kwargs).split(","))
    res = tuple(str_k for str_k in res if len(str_k) > 0)
    if is_bool:
        is_true = lambda x: x == "True" or x == "1" or x == "yes" or x == "on"
        res = tuple(is_true(str_k) for str_k in res)
    print(f"get_gurobi_tuple will return: {res}")
    return res


def get_require_feasible_postproc_dim(config, nodes):
    require_feasible_postproc_dim = get_gurobi_tuple(config, True, option=CC.REQUIRE_FEASIBLE_POSTPROC_DIM)
    geocode_lens = sorted(nodes[0].geocodeDict.keys())
    geolevel_index = geocode_lens.index(len(nodes[0].geocode))
    return require_feasible_postproc_dim[geolevel_index]
