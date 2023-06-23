import numpy as np
from copy import deepcopy
from das_constants import CC
from collections import OrderedDict
from typing import Tuple
from fractions import *


def group_elements(elements:list, cutoff:int, group_type:str) -> list:
    """
    Groups the elements into lists with maximum length equal to cutoff
    :param elements: a list of the elements that will be grouped
    :param cutoff: the maximum length of each group
    :param group_type: indicates which method to use to group child geounits to form parent geounit
    :return grouped_elements: a list of lists that is equal to elements when concatenated
    """
    if group_type == "orig":
        return group_elements_orig(elements, cutoff)
    assert group_type == "even", group_type
    return group_elements_evenly(elements, cutoff)


def group_elements_orig(elements, cutoff):
    return [elements[k * cutoff:(k + 1) * cutoff] for k in range(int(np.ceil(len(elements) / cutoff)))]


def group_elements_evenly(elements, cutoff):
    # This function groups elements as evenly as possible without decreasing the number of geounits with only one child
    # relative to group_elements_orig()
    n_elms = len(elements)
    # When n_elms <= cutoff, group_elements_orig() returns only one group. When n_elms % cutoff == 0,
    # group_elements_orig() produces evenly sized groups. When n_elms % cutoff == 1, the size of one group in the
    # output of group_elements_orig() is one. For these reasons, return group_elements_orig() in all three of these
    # cases:
    if n_elms <= cutoff or (n_elms % cutoff in [0, 1]):
        return group_elements_orig(elements, cutoff)
    # Note n_groups = int(np.ceil(n_elms/cutoff)):
    n_groups = n_elms // cutoff + 1
    # We will output n_groups groups of only two distinct sizes, which will be defined as group_size_low and
    # group_size_low + 1:
    group_size_low = n_elms // n_groups
    n_groups_high = n_elms - group_size_low * n_groups
    n_groups_low = n_groups - n_groups_high
    group_sizes = [group_size_low] * n_groups_low + [group_size_low + 1] * n_groups_high
    group_inds_endpts = np.cumsum(np.array([0] + group_sizes, dtype=np.int32)).tolist()
    return [elements[ind1:ind2] for ind1,ind2 in zip(group_inds_endpts[:-1], group_inds_endpts[1:])]


def recursion_through_tracts_entity_k(adjacency_dict: dict, levels_dict: dict, entity_k: list) -> Tuple[int, tuple]:
    """
    Performs the first two iterations of the algorithm proposed in the Alternative Geographic Spine document to find
    the off-spine entity distance (OSED), which is the number of geounits that must be added or subtracted from one
    another in order to derive the offspine entity k.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param entity_k: block-level (string) geoids that are in entity k
    :return bound_ck: dictionary providing the OSED of entity_k for each tract.
    :return cks: dictionaries providing the OSED of entity_k and the OSED of the complement of entity_k for each tract.
    """
    c_k_bg = dict()
    c_notk_bg = dict()
    # Initialize recursive formula described in the Alternative Geographic Spine Document at the block-group geolevel.
    for bg in levels_dict[CC.GEOLEVEL_BLOCK_GROUP]:
        total_in_k = sum([block in entity_k for block in adjacency_dict[bg]])
        total_not_in_k = len(adjacency_dict[bg]) - total_in_k
        c_k_bg[bg] = min(total_in_k, total_not_in_k + 1)
        c_notk_bg[bg] = min(total_in_k + 1, total_not_in_k)

    # Perform one more iteration of the recursion to define c_k and c_notk at the tract geolevel.
    c_k = dict()
    c_notk = dict()

    bound_on_tg_c_k = 0
    bound_on_tg_c_notk = 0
    for unit in levels_dict[CC.GEOLEVEL_TRACT]:
        total_in_k = sum([c_k_bg[child] for child in adjacency_dict[unit]])
        total_not_in_k = sum([c_notk_bg[child] for child in adjacency_dict[unit]])
        c_k[unit] = min(total_in_k, total_not_in_k + 1)
        c_notk[unit] = min(total_in_k + 1, total_not_in_k)
        bound_on_tg_c_k += c_k[unit]
        bound_on_tg_c_notk += c_notk[unit]

    # Compute final bound on c_k_county. This is given by OSED value for the case in which the tract-group geolevel
    # is removed entirely, and the parent geounit for all tracts is simply the county:
    bound_county_c_k = min(bound_on_tg_c_k, bound_on_tg_c_notk + 1)

    return bound_county_c_k, (c_k, c_notk)


def dist_of_entity_k(adjacency_dict: dict, levels_dict: dict, c_k: dict, c_notk: dict) -> int:
    """
    Performs the final iterations of the algorithm proposed in the Alternative Geographic Spine document to find
    the "off spine entity distance" (OSED), which is the number of geounits that must be added or subtracted from one
    another in order to derive the offspine entity k.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param c_k: dictionary providing the OSED of entity_k for each tract
    :param c_notk: dictionary providing the OSED of the complement of entity_k for each tract
    :return OSED: the OSED at the county geolevel
    """

    # Perform recursion through county geolevel (see Alternative Geographic Spine document):
    for level in [CC.GEOLEVEL_TRACT_GROUP, CC.GEOLEVEL_COUNTY]:
        for unit in levels_dict[level]:
            total_in_k = sum([c_k[child] for child in adjacency_dict[unit]])
            total_not_in_k = sum([c_notk[child] for child in adjacency_dict[unit]])
            c_k[unit] = min(total_in_k, total_not_in_k + 1)
            c_notk[unit] = min(total_in_k + 1, total_not_in_k)

    return c_k[levels_dict[CC.GEOLEVEL_COUNTY][0]]


def entity_dist_obj_fxn(adjacency_dict: dict, levels_dict: dict, fun, cks: list):
    """
    Finds the off-spine entity distances (OSEDs) at the county geolevel for each off-spine entity (OSE) and then
    performs a reduce operation on this list
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param cks: each tuple element should be defined as the second output of recursion_through_tracts_entity_k()
    :param fun: a function that will be used in the reduce step of this function
    :return obj_fxn: the value of the function fun applied to the list of integer OSED values
    """

    cks = [dist_of_entity_k(adjacency_dict, levels_dict, c_k, c_notk) for c_k, c_notk in cks]
    return fun(cks)


def combine_geounits(units: list, adjacency_dict: dict, levels_dict: dict, parent: int,
                     level: str) -> Tuple[dict, dict]:
    """
    Combines the geounits in units.
    :param units: indices of geounits to be combined
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param parent: integer geoid of the parent of the geounits in units
    :param level: the geolevel of units
    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    """

    for sibling in deepcopy(units[1:]):
        # reassign the children of sibling to the parent units[0]:
        adjacency_dict[units[0]].extend(adjacency_dict[sibling])
        # The next three lines deletes the sibling of units[0]:
        levels_dict[level].remove(sibling)
        adjacency_dict[parent].remove(sibling)
        del adjacency_dict[sibling]

    return adjacency_dict, levels_dict


def lexicographic_gtoet(a: list, b: list) -> bool:
    """
    lexicographic "greater than or equal to" comparison. If the first order statistic for a and b that differ are such
    that this order statistic of a is greater than or equal to that of b, returns True, and otherwise returns False
    :param a: a list of integers
    :param b: a list of integers such that len(a) == len(b)
    :return is_larger_than: True if and only if a >= b in the lexicographic sense
    """

    assert len(a) == len(b)

    a = np.array(a)
    b = np.array(b)
    if np.all(a == b):
        return True
    idx = np.where((a > b) != (a < b))[0][0]
    if a[idx] > b[idx]:
        return True
    return False


def minimize_entity_distance(adjacency_dict: dict, levels_dict: dict, blocks_in_entities: list, entities_in_tract: dict,
                             fanout_cutoff: int, entity_threshold: int, group_type: str = "orig") -> Tuple[dict, dict]:
    """
    Approximates the tract-groups that minimize entity_dist_obj_fxn().
    We do not consider all combinations of adjacency relationships, so this is only an approximation.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param blocks_in_entities: blocks_in_entities[k] is a list of the blocks that are in entity k
    :param entities_in_tract: provides the list of entities in each tract
    :param fanout_cutoff: the fanouts of the block-groups and tract-groups will be no more than
    int(np.sqrt(number_of_tracts)) + fanout_cutoff at the end of the first optimization routine
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param group_type: indicates which method to use to group child geounits to form parent geounit
    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    """

    changed = True
    num_tracts = len(entities_in_tract)
    cutoff = int(np.sqrt(num_tracts) + fanout_cutoff + 1)

    # Since, unlike the psuedocode in the Alternative Geographic Spine document, we do not iterate over block-groups, we
    # can simply fix the level iterator from the pseudocode at the tract-group:
    level = CC.GEOLEVEL_TRACT_GROUP
    # All tract-groups have the same parent because we parallelized over counties.
    parent = levels_dict[CC.GEOLEVEL_COUNTY][0]

    # Initialize tract-groups:
    while changed:
        changed = False
        tracts = list(entities_in_tract.keys())
        for i in range(len(tracts) - 1):
            # Uncommenting the next two lines may improve accuracy in some cases, but it will also take longer.
            # if len(entities_in_tract[tracts[i]]) > 1:
            #     continue
            q = [entities_in_tract[tracts[i]] == entities_in_tract[tracts[k]] for k in range(i + 1, len(tracts))]
            if any(q):
                # create lists of tract-groups to combine and then group them so that no group has more than threshold
                # geounits. Note that the parent of a tract with index tracts[i] is tracts[i] - num_tracts:
                to_combine = [tracts[i] - num_tracts] + [tracts[k] - num_tracts for k in
                                                            range(i + 1, len(tracts)) if q[k - i - 1]]
                combine_lists = group_elements(to_combine, cutoff, group_type)
                for combine in combine_lists:
                    adjacency_dict, levels_dict = combine_geounits(combine, adjacency_dict, levels_dict, parent, level)
                for combine in combine_lists:
                    for tg in combine:
                        # Likewise, the child of a tract-group with index tg is tg + num_tracts:
                        del entities_in_tract[tg + num_tracts]
                changed = True
                break
            else:
                del entities_in_tract[tracts[i]]

    # Ignore entities that will automatically be close to the spine regardless of tract-groups:
    cks_tract = []
    for entity in blocks_in_entities:
        bound_county_c_k, c_k_and_c_notk_tract = recursion_through_tracts_entity_k(adjacency_dict, levels_dict, entity)
        if bound_county_c_k > entity_threshold:
            cks_tract.append(c_k_and_c_notk_tract)

    if len(cks_tract) == 0:
        # Comment out the following four lines to avoid combining TGs further in this case. This increases the number of TGs
        # bypassed (rather than counties) in move_to_pareto_frontier, but can also make fanouts at the county less favorable.
        to_combine = deepcopy(levels_dict[CC.GEOLEVEL_TRACT_GROUP])
        combine_lists = group_elements(to_combine, cutoff, group_type)
        for combine in combine_lists:
            adjacency_dict, levels_dict = combine_geounits(combine, adjacency_dict, levels_dict, parent, level)
        return adjacency_dict, levels_dict

    objective_init = entity_dist_obj_fxn(adjacency_dict, levels_dict, lambda x: x, cks_tract)
    objective_init.sort(reverse=True)
    finalized_units = []

    while True:
        combined_a_pair = False
        if len(levels_dict[level]) == 1:
            break

        # Find a pair (=[child, child's sibling]) such that the objective function is reduced when they are combined:
        siblings = [child for child in adjacency_dict[parent] if child not in finalized_units]
        for i, child in enumerate(siblings[:-1]):
            for sibling in siblings[i + 1:]:
                pair = [child, sibling]
                new_unit_fan_out = len(adjacency_dict[child]) + len(adjacency_dict[sibling])
                if new_unit_fan_out > cutoff:
                    continue
                # Test if combining the pair improves the objective function:
                adjacency_dict2, levels_dict2 = combine_geounits(pair, deepcopy(adjacency_dict),
                                                                 deepcopy(levels_dict), parent, level)
                objective_test = entity_dist_obj_fxn(adjacency_dict2, levels_dict2, lambda x: x, cks_tract)
                objective_test.sort(reverse=True)
                in_better_than_set = lexicographic_gtoet(objective_init, objective_test)
                if in_better_than_set:
                    objective_init = objective_test
                    combined_a_pair = True
                    adjacency_dict, levels_dict = adjacency_dict2, levels_dict2
                    break
            if combined_a_pair:
                break
            else:
                finalized_units.append(child)
        # If we cannot combine any pair of siblings without increasing the objective function:
        if not combined_a_pair:
            break

    return adjacency_dict, levels_dict


def make_plb_dicts(levels_dict: dict, user_plb_dict: dict) -> Tuple[dict, dict]:
    """
    Adds PLB allocation(s) to the values of the dictionary, adjacency_dict. For every geounit above the block-group
    geolevel, the format is {geoid:[children, plb_allocation]}, where plb_allocation is a Fraction. For block-groups,
    the format is, {geoid:[children, plb_allocation_children, plb_allocation]}, where plb_allocation_children is also
    a Fraction.
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param user_plb_dict: the user-specified PLB value for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for each geounit above the block geolevel
    """

    geolevels = list(levels_dict.keys())
    plb_blocks = dict()
    plb_above_blocks = dict()

    for bg in levels_dict[CC.GEOLEVEL_BLOCK_GROUP]:
        plb_blocks[bg] = user_plb_dict[CC.GEOLEVEL_BLOCK]

    for level in geolevels:
        for unit in levels_dict[level]:
            plb_above_blocks[unit] = user_plb_dict[level]

    return plb_blocks, plb_above_blocks


def bypass_geounit(adjacency_dict: dict, levels_dict: dict, plb_blocks: dict, plb_above_blocks: dict, unit_level: str,
                   parent: int, unit: int, highest_geounit: int) -> Tuple[dict, dict, dict, dict, int]:
    """
    Bypasses the geounit, unit. This involves the following operations to preserve uniform depth length:
    Replaces geounit with geoid unit with a number of geounits given by its number of children. Each such geounit
    is the child of parent and the parent of one child of unit. The new PLB of these geounits are the sum of the plb
    allocated to its children and the geounit, unit. The PLB of the children is set to zero.
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param plb_blocks: provides the PLB allocation of the child geounits for each block-group
    :param plb_above_blocks: provides the PLB allocation of the geounit for each geounit above the block geolevel
    :param unit: the geounit index of the geounit to be bypassed
    :param unit_level: the geolevel of unit
    :param parent: the parent geounit index
    :param highest_geounit: the largest geounit index
    :return highest_geounit: the largest geounit index
    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    """

    message = f"geounit {unit} is not in the {unit_level} geolevel of levels_dict."
    assert unit in levels_dict[unit_level], message

    for child in deepcopy(adjacency_dict[unit]):
        # Add new geounit to level of unit with one child:
        levels_dict[unit_level].append(highest_geounit + 1)
        adjacency_dict[highest_geounit + 1] = [child]
        # In each of the next four cases, we reallocate all PLB of child to this newly created geounit in two steps.
        # First, the PLB of parent is redefined, and then the PLB of the child is set to zero. Remark 2 in the
        # Alternative Geographic Spine Document describes how this ensures that the final sensitivity is correct.
        if unit_level == CC.GEOLEVEL_BLOCK_GROUP:
            plb_above_blocks[highest_geounit + 1] = plb_above_blocks[unit] + plb_blocks[unit]
            plb_blocks[highest_geounit + 1] = Fraction(0, 1)
        else:
            plb_above_blocks[highest_geounit + 1] = plb_above_blocks[child] + plb_above_blocks[unit]
            plb_above_blocks[child] = Fraction(0, 1)
        # parent is the parent of the new geounit:
        adjacency_dict[parent].append(highest_geounit + 1)
        highest_geounit += 1

    # Delete old unit:
    if unit_level == CC.GEOLEVEL_BLOCK_GROUP:
        del plb_blocks[unit]
    del adjacency_dict[unit]
    adjacency_dict[parent].remove(unit)
    levels_dict[unit_level].remove(unit)
    del plb_above_blocks[unit]

    return adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, highest_geounit


def bypassing_improves_parent(parent_plb: Fraction, child_plbs: list, epsilon_delta: bool, bypass_geolevels: list,
                              unit_level: str, bypass_cutoff: int, num_siblings: int) -> bool:
    """
    Returns True if and only if bypassing the parent will improve the expected squared error of the OLS estimate for the
    parent. This in turn implies that bypassing will not decrease the expected squared error of the OLS estimate for any
    geounit, as described by Theorem 1 in the Alternative Geographic Spine document.
    :param parent_plb: PLB allocation of parent
    :param child_plbs: PLB allocations of each child
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param bypass_geolevels: the geolevels that should be bypassed
    :param unit_level: the geolevel of the parent geounit
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param num_siblings: the number of sibling geounits of the geounit for which bypassing is being considered
    :return bool: True if and only if bypassing parent will not increase the expected squared error of the OLS
    estimates for all geounits or unit_level is in bypass_geolevels
    """

    if unit_level in bypass_geolevels:
        return True
    if len(child_plbs) == 1:
        return True
    if epsilon_delta:
        return False
    if bypass_cutoff >= num_siblings + len(child_plbs) and min(child_plbs) * 2 >= (len(child_plbs) - 1) * parent_plb:
        return True

    return False


def move_to_pareto_frontier(adjacency_dict: dict, levels_dict: dict, plb_blocks: dict, plb_above_blocks: dict,
                            epsilon_delta: bool, bypass_geolevels: list, bypass_cutoff: int) -> Tuple[dict, dict, dict, dict]:
    """
    The algorithm bypasses over geounits when doing so would not increase the expected squared error of any query in
    any geolevel of the OLS estimator. (See Theorem 1 in the Alternative Geographic Spine document.)
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param plb_blocks: provides the PLB allocation of the child geounits for each block-group
    :param plb_above_blocks: provides the PLB allocation of the geounit for each geounit above the block geolevel
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param bypass_geolevels: the geolevels that should be bypassed
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :return adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :return levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    """

    geolevels = list(levels_dict.keys())
    highest_geounit = max(levels_dict[CC.GEOLEVEL_BLOCK_GROUP])

    # Start from the block-group geolevel and move toward the root:
    for parent_level, unit_level in zip(geolevels[-2::-1], geolevels[-1:0:-1]):
        # Note that bypassing alters the adjacency relationships between three different geounits: the "unit", its
        # children, and its parent, so it is helpful to also have the parent ID available:
        for parent in deepcopy(levels_dict[parent_level]):
            for unit in deepcopy(adjacency_dict[parent]):
                if parent_level != geolevels[-2]:
                    # In this case, geounit parent is the third highest geolevel, so geounit child is a leaf of the spine (ie: a block geounit):
                    child_plbs = [plb_above_blocks[child] for child in adjacency_dict[unit]]
                else:
                    child_plbs = [plb_blocks[unit]] * len(adjacency_dict[unit])
                num_siblings = len(adjacency_dict[parent]) - 1
                if bypassing_improves_parent(plb_above_blocks[unit], child_plbs, epsilon_delta,
                                             bypass_geolevels, unit_level, bypass_cutoff, num_siblings):
                    adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, highest_geounit =\
                        bypass_geounit(adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, unit_level, parent,
                                       unit, highest_geounit)

    # Bypass county geolevel using bypassing decision rule corresponding to case in which epsilon_delta==True:
    county_id = levels_dict[CC.GEOLEVEL_COUNTY][0]
    child_ids = adjacency_dict[county_id]
    if len(child_ids) == 1:
        # Since this decision rule does not change the number of counties, maintain the same county integer ID:
        plb_above_blocks[county_id] = plb_above_blocks[child_ids[0]] + plb_above_blocks[county_id]
        plb_above_blocks[child_ids[0]] = Fraction(0, 1)

    return adjacency_dict, levels_dict, plb_blocks, plb_above_blocks


def check_total_plb(adjacency_dict: dict, levels_dict: dict, plb_blocks: dict, plb_above_blocks: dict,
                    user_plb_dict: dict) -> bool:
    """
    Starts at the root geounit, sums PLBs down to the block geolevel, and then throws an error if sensitivity of the
    corresponding (row-weighted-)strategy matrix is not one
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    :param user_plb_dict: the user-specified PLB value for each geolevel
    :return bool: True if the sensitivity is the same as that of the user-specified PLB values
    """

    # This function is called just before reformatting the outputs and returning them to ensure the sensitivity of
    # the output is correct.

    # This function iteratively moves PLB (raised to either p=1 or p=2) from parents to their children. This ensures
    # that the weighted strategy matrix over all geolevels has the correct sensitivity by the logic outlined in
    # Remark 2 of the Alternative Geographic Spine Document:
    plb_above_blocks = deepcopy(plb_above_blocks)
    geolevels = list(levels_dict.keys())

    # Start by moving PLB down to block-group geolevel:
    for level in geolevels[:-1]:
        for parent in levels_dict[level]:
            parent_plb = plb_above_blocks[parent]
            for child in adjacency_dict[parent]:
                plb_above_blocks[child] = parent_plb + plb_above_blocks[child]

    target_plb = np.sum(np.array([plb_i for plb_i in list(user_plb_dict.values())]))

    for bg in levels_dict[CC.GEOLEVEL_BLOCK_GROUP]:
        sensitivity = plb_above_blocks[bg] + plb_blocks[bg]
        message = f'Sensitivity for blocks in BG #{bg} are: {sensitivity} (!= {target_plb})'
        assert sensitivity == target_plb, message


def reformat_adjacency_dict(state_county: str, adjacency_dict: dict, levels_dict: dict, plb_blocks: dict,
                            plb_above_blocks: dict) -> Tuple[list, list, tuple]:
    """
    Encodes spine in its final format.
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit indicating if next four digits are [0][county] or [AIANNHCE]]
    [4 digit county/AIANNHCE]
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :return plb_blocks: provides the PLB allocation of the child geounits for
    each block-group
    :return plb_above_blocks: provides the PLB allocation of the geounit for
    each geounit above the block geolevel
    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below
    """

    geolevels = list(levels_dict.keys()) + [CC.GEOLEVEL_BLOCK]

    # Rename the root geounit state_county:
    adjacency_dict[state_county] = adjacency_dict.pop(levels_dict[CC.GEOLEVEL_COUNTY][0])
    plb_above_blocks[state_county] = plb_above_blocks.pop(levels_dict[CC.GEOLEVEL_COUNTY][0])
    levels_dict[CC.GEOLEVEL_COUNTY].append(state_county)
    levels_dict[CC.GEOLEVEL_COUNTY].remove(levels_dict[CC.GEOLEVEL_COUNTY][0])

    # state_county is formatted as [3 digit DAS state geocode][5 digit DAS County geocode]:
    widths = [len(state_county) - 3]

    for level_id, level in enumerate(geolevels[:-2]):
        fan_outs = [len(adjacency_dict[parent]) for parent in levels_dict[level]]
        id_lengths = len(str(max(fan_outs)))
        widths.append(id_lengths)
        for parent_num, parent in enumerate(deepcopy(levels_dict[level])):
            # Define geocodes of children as list(range(1, 1 + fan_outs[parent_num])), after left padding with parent
            # geocode as well as the number of zeros required to ensure a fixed width:
            ids = [str(k) for k in range(1, fan_outs[parent_num] + 1)]
            ids = [parent + '0' * (id_lengths - len(idk)) + idk for idk in ids]
            for child, new_id in zip(deepcopy(adjacency_dict[parent]), ids):
                adjacency_dict[new_id] = adjacency_dict.pop(child)
                plb_above_blocks[new_id] = plb_above_blocks.pop(child)
                levels_dict[geolevels[level_id + 1]].append(new_id)
                levels_dict[geolevels[level_id + 1]].remove(child)
                # Note that level is geolevel of parent, and not the geolevel of child:
                if level == geolevels[-3]:
                    plb_blocks[new_id] = plb_blocks.pop(child)
                assert type(plb_above_blocks[new_id]) is Fraction

    geoid_mapping = []
    # we always represent the block id digits as the block id in [geocode16] format to simplify mapping
    # back to geocode16 format after top-down is run:
    block_width= len(adjacency_dict[levels_dict[CC.GEOLEVEL_BLOCK_GROUP][0]][0])
    widths.append(block_width)
    # Add PLB allocations of block geounits to plb_above_blocks to avoid making a new dictionary:
    for parent in levels_dict[CC.GEOLEVEL_BLOCK_GROUP]:
        for child in adjacency_dict[parent]:
            geoid_mapping.append([child, parent + child])
            plb_above_blocks[parent + child] = plb_blocks[parent]
            assert type(plb_blocks[parent]) is Fraction

    # Redefine dictionary as a list of tuples:
    plb_above_blocks = [(k, v) for k, v in plb_above_blocks.items()]

    widths = tuple(widths)

    return plb_above_blocks, geoid_mapping, widths


def optimize_spine(state_county: str, adjacency_dict: dict, levels_dict: dict, blocks_in_entities: list,
                   entities_in_tract: dict, user_plb_dict: dict, fanout_cutoff: int, epsilon_delta: bool,
                   entity_threshold: int, bypass_cutoff: int, includes_tg: bool, prim_spine: bool, group_type: str) -> Tuple[list, list, tuple]:
    """
    Provides an optimized geographic subspine with a county defined as the root geounit.
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit county/AIANNHCE]
    [4 digit county/AIANNHCE]
    :param adjacency_dict: provides a list of child-geounit indices for each parent geounit index
    :param levels_dict: provides a list of geounit indices for each geolevel
    :param blocks_in_entities: blocks_in_entities[k] is a list of the blocks that are in entity k
    :param entities_in_tract: provides the list of entities in each tract
    :param user_plb_dict: the user-specified PLB value for each geolevel
    :param fanout_cutoff: a constant used to derive the maximum number of geounits included in each parent geounit that
    is in an optimized geolevel
    :param epsilon_delta: True if and only if an approximate DP primitive will be used in the engine
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param includes_tg: indicates if the user included tract-groups in the initial spine
    :param prim_spine: whether to create the prim_spine (ie: a spine that includes the prim geolevel)
    :param group_type: indicates which method to use to group child geounits to form parent geounit
    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below
    """

    if includes_tg:
        # In this case, we do not want to include tract-groups in the initial spine, so we do not redefine them by
        # aggregating them together. These tract-group geounits will be bypassed move_to_pareto_frontier().
        adjacency_dict, levels_dict = minimize_entity_distance(adjacency_dict, levels_dict, blocks_in_entities,
                                                               entities_in_tract, fanout_cutoff, entity_threshold, group_type)
    # to free up some memory:
    del blocks_in_entities, entities_in_tract

    plb_blocks, plb_above_blocks = make_plb_dicts(levels_dict, user_plb_dict)

    bypass_geolevels = [] if includes_tg or prim_spine else [CC.GEOLEVEL_TRACT_GROUP]

    # Check PLBs below county to be corresponding to the ones requested in config (before bypassing)
    # TODO: It is checked against user_plb_dict. To be less trivial check, should be checked against budget.geolevel_prop_budgets_dict
    for gl, indices in levels_dict.items():
        gl_plbs = tuple(plb_above_blocks[ind] for ind in indices)
        # s += f"{gl}: User: {user_plb_dict[gl]}, max: {max(gl_plbs)}, min: {min(gl_plbs)}\n"
        assert user_plb_dict[gl] == max(gl_plbs) == min(gl_plbs)
    # s += f"Block: User: {user_plb_dict['Block']}, max: {max(plb_blocks.values())}, min: {min(plb_blocks.values())}\n"
    assert user_plb_dict['Block'] == max(plb_blocks.values()) == min(plb_blocks.values())

    adjacency_dict, levels_dict, plb_blocks, plb_above_blocks = move_to_pareto_frontier(adjacency_dict, levels_dict, plb_blocks, plb_above_blocks,
                                                                                        epsilon_delta, bypass_geolevels, bypass_cutoff)

    check_total_plb(adjacency_dict, levels_dict, plb_blocks, plb_above_blocks, user_plb_dict)
    return reformat_adjacency_dict(state_county, adjacency_dict, levels_dict, plb_blocks, plb_above_blocks)


def initial_geounit_index_ranges_prim_spine(num_prims: int, num_ts: int) -> dict:
    # Geolevels are: County, Prim, TSG, TS, BG, Block
    # Note that the ids of TSG will actually be a proper subset of list(range(num_prims + 1, num_prims + 1 + num_ts)):
    return OrderedDict([(CC.GEOLEVEL_COUNTY, (0, 1)), (CC.GEOLEVEL_PRIM, (1, num_prims + 1)),
                        (CC.GEOLEVEL_TRACT_SUBSET_GROUP, (num_prims + 1, num_prims + 1 + num_ts)),
                        (CC.GEOLEVEL_TRACT_SUBSET, (num_prims + 1 + num_ts, num_prims + 1 + 2 *  num_ts))])


def initial_geounit_index_ranges(num_tracts: int) -> dict:
    return OrderedDict([(CC.GEOLEVEL_COUNTY, (0, 1)), (CC.GEOLEVEL_TRACT_GROUP, (1, num_tracts + 1)),
                        (CC.GEOLEVEL_TRACT, (num_tracts + 1, 2 * num_tracts + 1))])


def call_optimize_non_prim_spine(state_county: str, row: tuple, user_plb_dict: dict, fanout_cutoff: int, epsilon_delta: bool,
                                 entity_threshold: int, bypass_cutoff: int, includes_tg: bool, group_type:str) -> Tuple[list, list, tuple]:
    """
    Calls spine optimization routines for an individual county. Note that block-groups in the input spine may
    conform with the standard Census definition; however, this is not the case for the spine that is output from
    this function. Instead, these optimization routines redefine these geounits by their optimized counterparts,
    or geounits in the geolevel block-group-custom. (The standard Census definition is defined by:
    [2 digit state][3 digit county][6 digit tract][first digit of block ID].)
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit county/AIANNHCE]
    [4 digit county/AIANNHCE]
    :param row: tuple with length given by the number of blocks in the county and format of element i, (tract_i,
    block_i, Place_i/MCD_i, gq_OSE_i)
    :param user_plb_dict: the user-specified PLB value for each geolevel
    :param fanout_cutoff: a constant used to derive the maximum number of blocks included in each block group and
    the maximum number of tracts in each tract group
    :param epsilon_delta: True if and only if the L2 sensitivity of strategy matrix of spine should be held fixed
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param includes_tg: indicates if the user included tract-groups in the initial spine
    :param group_type: indicates which method to use to group child geounits to form parent geounit

    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below.
    """
    # Recall each element of row is formattd as (tract_i, block_i, OSEs_i, gq_OSE_i), where OSEs_i is a tuple containing the geographic codes of
    # off-spine entities to target in the spine optimization routines.

    # Sort by block geocode16 geoid:
    row = tuple(sorted(row, key=lambda d: d[1]))

    adjacency_dict = dict()
    intersect_entities = [''.join(row_k[2] + (row_k[3],)) for row_k in row]

    tracts = np.unique([row_k[0] for row_k in row])
    num_tracts = len(tracts)

    # Initialize county and tract group geolevels:
    init_ranges = initial_geounit_index_ranges(num_tracts)
    levels_dict = OrderedDict((k, list(range(v[0], v[1]))) for k, v in init_ranges.items())
    adjacency_dict[init_ranges[CC.GEOLEVEL_COUNTY][0]] = list(range(*init_ranges[CC.GEOLEVEL_TRACT_GROUP]))
    values = [[k] for k in deepcopy(levels_dict[CC.GEOLEVEL_TRACT])]
    adjacency_dict = {**adjacency_dict, **dict(zip(deepcopy(levels_dict[CC.GEOLEVEL_TRACT_GROUP]), values))}

    # Initialize block-groups so that they group together blocks in a single off-spine entity that are within a tract.
    cur_bg_id = init_ranges[CC.GEOLEVEL_TRACT][1]
    levels_dict[CC.GEOLEVEL_BLOCK_GROUP] = []
    # Define block-groups:
    for k, tract in enumerate(tracts):
        unique_intersect_entities_in_tract = np.unique([intersect_entities[n] for n, row_k in enumerate(row) if row_k[0] == tract])
        tract_id = init_ranges[CC.GEOLEVEL_TRACT][0] + k
        adjacency_dict[tract_id] = []
        for i, entity in enumerate(unique_intersect_entities_in_tract):
            num_blocks_in_tract = sum([int(row_k[0] == tract) for row_k in row])
            cutoff = int(np.sqrt(num_blocks_in_tract) + fanout_cutoff + 1)
            blocks = [row_k[1] for n, row_k in enumerate(row) if intersect_entities[n] == entity and row_k[0] == tract]
            block_groups = group_elements(blocks, cutoff, group_type)
            num_bgs = len(block_groups)
            bg_ids_to_add = list(range(cur_bg_id, cur_bg_id + num_bgs))
            adjacency_dict[tract_id].extend(bg_ids_to_add)
            levels_dict[CC.GEOLEVEL_BLOCK_GROUP].extend(bg_ids_to_add)
            # Note: for the BG geolevel, adjacency_dict maps BG geoid (int) to a list of strings (the geoids
            # of all other geolevels map to a list of ints):
            adjacency_dict.update({bg_id_to_add:blocks_in_bg for bg_id_to_add, blocks_in_bg in zip(bg_ids_to_add, block_groups)})
            cur_bg_id += num_bgs

    # Create blocks_in_entities input for optimize_spine(.):
    blocks_in_entities = []
    n_ose_types = len(row[0][2])
    for ose_type_index in range(n_ose_types):
        unique_oses = np.unique([row_k[2][ose_type_index] for row_k in row if not np.all([xi == "9" for xi in row_k[2][ose_type_index]])])
        for ose in unique_oses:
            blocks_in_entities.append({row_k[1] for row_k in row if row_k[2][ose_type_index] == ose})

    # Create entities_in_tract input for optimize_spine(.):
    entities_in_tract = dict()
    for k, tract in enumerate(tracts):
        tract_id = init_ranges[CC.GEOLEVEL_TRACT][0] + k
        ose_tuples_in_tract = [row_k[2] for row_k in row if row_k[0] == tract]
        unique_entities_in_tract = np.unique([ose_tuple[k] for ose_tuple in ose_tuples_in_tract for k in range(n_ose_types)])
        entities_in_tract[tract_id] = unique_entities_in_tract.tolist()

    return optimize_spine(state_county, adjacency_dict, levels_dict, blocks_in_entities, entities_in_tract, user_plb_dict,
                          fanout_cutoff, epsilon_delta, entity_threshold, bypass_cutoff, includes_tg,
                          prim_spine=False, group_type=group_type)


def call_optimize_prim_spine(state_county: str, row: tuple, user_plb_dict: dict, fanout_cutoff: int, epsilon_delta: bool,
                             entity_threshold: int, bypass_cutoff: int, group_type:str) -> Tuple[list, list, tuple]:
    """
    Calls spine optimization routines for an individual county. Note that block-groups in the input spine may
    conform with the standard Census definition; however, this is not the case for the spine that is output from
    this function. Instead, these optimization routines redefine these geounits by their optimized counterparts,
    or geounits in the geolevel block-group-custom. (The standard Census definition is defined by:
    [2 digit state][3 digit county][6 digit tract][first digit of block ID].)
    :param state_county: a string with format [1 digit AIAN/non-AIAN][2 digit state][1 digit county/AIANNHCE]
    [4 digit county/AIANNHCE]
    :param row: tuple with length given by the number of blocks in the county and format of element i, (tract_i,
    block_i, Place_i/MCD_i, gq_OSE_i, prim_i)
    :param user_plb_dict: the user-specified PLB value for each geolevel.
    :param fanout_cutoff: a constant used to derive the maximum number of blocks included in each block group and
    the maximum number of tract subsets in each tract subset group
    :param epsilon_delta: True if and only if the L2 sensitivity of strategy matrix of spine should be held fixed
    :param entity_threshold: all entities that have an off-spine entity distance that can be bounded above by
    entity_threshold will be ignored when optimizing over the definition of tract-groups
    :param bypass_cutoff: bypassing is not carried out when doing so would result in the parent of the geounit
    having more than bypass_cutoff children
    :param group_type: indicates which method to use to group child geounits to form parent geounit
    :return plb_mapping: elements are given by (geocode_in_optimized_spine, PLB_allocation)
    :return geoid_mapping: elements are given by (block_geocode16_string, block_geocode_in_optimized_spine)
    :return widths: the fixed number of additional digits of DAS_geoid that is required to represent each geolevel at
    the county level and below.
    """

    # Recall each element of row is formattd as (tract_i, block_i, OSEs_i, gq_OSE_i, prim_i), where OSEs_i is a tuple
    # containing the geographic codes of off-spine entities to target in the spine optimization routines.

    # Sort by block geocode16 geoid:
    row = tuple(sorted(row, key=lambda d: d[1]))

    adjacency_dict = dict()
    intersect_entities = [''.join(row_k[2] + (row_k[3],)) for row_k in row]

    tract_subsets = np.unique([''.join((row_k[0], row_k[4])) for row_k in row])
    prims = np.unique([row_k[4] for row_k in row])
    num_tract_subsets = len(tract_subsets)
    num_prims = len(prims)
    init_ranges = initial_geounit_index_ranges_prim_spine(num_prims, num_tract_subsets)

    # Initialize county geolevel s.t. it contains geounit index 0 and all prim geounits are children
    levels_dict = OrderedDict([(CC.GEOLEVEL_COUNTY, [init_ranges[CC.GEOLEVEL_COUNTY][0]]), (CC.GEOLEVEL_PRIM, list(range(*init_ranges[CC.GEOLEVEL_PRIM])))])
    adjacency_dict[init_ranges[CC.GEOLEVEL_COUNTY][0]] = list(range(*init_ranges[CC.GEOLEVEL_PRIM]))

    # Initialize TSGs so that they group together Tract_Subsets
    cur_tsg_id = init_ranges[CC.GEOLEVEL_TRACT_SUBSET_GROUP][0]
    cur_ts_id = init_ranges[CC.GEOLEVEL_TRACT_SUBSET][0]
    cur_bg_id = init_ranges[CC.GEOLEVEL_TRACT_SUBSET][1]
    levels_dict[CC.GEOLEVEL_TRACT_SUBSET_GROUP] = []
    levels_dict[CC.GEOLEVEL_TRACT_SUBSET] = []
    levels_dict[CC.GEOLEVEL_BLOCK_GROUP] = []
    # Define TSGs and continue to assign them TS children as long as there are no more than cutoff children assigned:
    for prim_index, prim in enumerate(prims):
        prim_id = init_ranges[CC.GEOLEVEL_PRIM][0] + prim_index
        adjacency_dict[prim_id] = []
        elements_in_prim = [row_k for row_k in row if row_k[4] == prim]

        tract_subsets_in_prim = np.unique([''.join((row_k[0], prim)) for row_k in elements_in_prim])
        num_tract_subsets_in_prim = len(tract_subsets_in_prim)
        cutoff = int(np.sqrt(num_tract_subsets_in_prim) + fanout_cutoff + 1)
        tract_subset_groups = group_elements(tract_subsets_in_prim, cutoff, group_type)
        num_tsg = len(tract_subset_groups)
        levels_dict[CC.GEOLEVEL_TRACT_SUBSET_GROUP].extend(list(range(cur_tsg_id, cur_tsg_id + num_tsg)))
        adjacency_dict[prim_id] = list(range(cur_tsg_id, cur_tsg_id + num_tsg))
        for tsg_index, tract_subset_group in enumerate(tract_subset_groups):
            num_ts = len(tract_subset_group)
            adjacency_dict[cur_tsg_id + tsg_index] = list(range(cur_ts_id, cur_ts_id + num_ts))
            levels_dict[CC.GEOLEVEL_TRACT_SUBSET].extend(list(range(cur_ts_id, cur_ts_id + num_ts)))
            # Define optimized block groups in this tract subset
            for ts_index, tract_subset in enumerate(tract_subset_group):
                intersect_entities_in_ts = np.unique([intersect_entities[n] for n, row_k in enumerate(row) if ''.join((row_k[0], prim)) == tract_subset])
                adjacency_dict[cur_ts_id + ts_index] = []
                elements_in_ts = [row_k for row_k in elements_in_prim if ''.join((row_k[0], prim)) == tract_subset]
                num_blocks = len(elements_in_ts)
                cutoff = int(np.sqrt(num_blocks) + fanout_cutoff + 1)
                for entity in intersect_entities_in_ts:
                    blocks_in_ts_and_entity = [row_k[1] for row_k in elements_in_ts if entity == ''.join(row_k[2] + (row_k[3],))]
                    block_groups = group_elements(blocks_in_ts_and_entity, cutoff, group_type)
                    num_bgs = len(block_groups)
                    levels_dict[CC.GEOLEVEL_BLOCK_GROUP].extend(list(range(cur_bg_id, cur_bg_id + num_bgs)))
                    adjacency_dict[cur_ts_id + ts_index].extend(list(range(cur_bg_id, cur_bg_id + num_bgs)))
                    # For the BG geolevel, adjacency_dict maps BG geoid (int) to a list of strings, but
                    # adjacency_dict maps the int geoids of all other geolevels to a list of ints:
                    adjacency_dict.update({(cur_bg_id + bg_index):block_group for bg_index, block_group in enumerate(block_groups)})
                    cur_bg_id += num_bgs
            cur_ts_id += num_ts
            assert cur_ts_id <= init_ranges[CC.GEOLEVEL_TRACT_SUBSET][1]
        cur_tsg_id += num_tsg
        assert cur_tsg_id <= init_ranges[CC.GEOLEVEL_TRACT_SUBSET_GROUP][1]
    assert cur_ts_id == init_ranges[CC.GEOLEVEL_TRACT_SUBSET][1]

    entities_in_tract = []
    blocks_in_entities = []
    return optimize_spine(state_county, adjacency_dict, levels_dict, blocks_in_entities, entities_in_tract, user_plb_dict,
                          fanout_cutoff, epsilon_delta, entity_threshold, bypass_cutoff, includes_tg=False,
                          prim_spine=True, group_type=group_type)
