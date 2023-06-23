"""This file implements the following classes:

L2PlusRounder
  A sequentialOptimizer that runs the GeoRound
  optimizer and, if it fails, invokes the failsafe mechanism.

  This optimizer is called from manipulate_nodes:geoimp_wrapper()

"""

import time
from copy import deepcopy
from abc import ABCMeta
from typing import Tuple, Union
import numpy as np
from functools import reduce
from operator import add
# das-created imports
from programs.optimization.optimizer import AbstractOptimizer
from programs.optimization.geo_optimizers import L2GeoOpt
from programs.optimization.simple_rounder import GeoRound
from programs.optimization.multipass_rounder import DataIndependentNpassRound
from programs.optimization.multipass_rounder_decomp import DataIndependentNpassRoundDecomp
from programs.optimization.multipass_query_rounder import DataIndependentNpassQueryRound
from programs.optimization.l2_dataIndep_npass_optimizer import DataIndQueriesL2NPass
from programs.optimization.l2_dataIndep_npass_optimizer_decomp import DataIndQueriesL2NPassDecomp
from programs.optimization.l2_dataIndep_npass_optimizer_improv import DataIndQueriesL2NPassImprov
from programs.optimization.maps import toGRBfromStrStatus, toGRBFromStr
from programs.queries.constraints_dpqueries import Constraint, StackedConstraint
from programs.queries.querybase import MultiHistQuery, StubQuery
# constants file
from das_constants import CC


class SequentialOptimizer(AbstractOptimizer, metaclass=ABCMeta):
    """
    This specifies a class that runs a single or sequence of optimizations.
    Superclass Inputs:
        config: a configuration object
    Creates:
        grb_env should NOT be provided, because this one creates its own.
    """
    def __init__(self, **kwargs):
        assert 'grb_env' not in kwargs
        super().__init__(**kwargs)
        self.gurobiEnvironment()

    def gurobiEnvironment(self):
        """
        This method is a wrapper for a function that creates a new gurobi Environment
        This wrapper exists for a reason. DO NOT try to cut it out in __init__ or getGurobiEnvironment
        """
        self.grb_env     = self.getGurobiEnvironment()



class L2PlusRounder(SequentialOptimizer):
    """
    L2GeoOpt + geoRound w/ degree-of-infeasibility failsafe
        Inputs:
            identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
            parent: a numpy multi-array of the parent histogram (or None if no parent)
            parent_shape: the shape of the parent histogram or would be shape if parent histogram is None
            childGeoLen: int giving the number of child geographies
            constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
            NoisyChild: numpy multidimensional array of noisy measurments of the detailed child cells
            noisy_child_weights: float giving the coefficient for the optimization function for each NoisyChild cell
            DPqueries: a list of StackedDPquery objects (see constraints_dpqueries.py)
            query_weights: a list of floats giving the coefficent for the optimization function for each cell of each query or None
            constraints_schema: list of attributes that
            opt_tol: bool; True by default. Should data-ind npass optimizer use optimized tolerances?
            const_tol_val: float; optional, used by data-ind npass optimizer
            opt_tol_slack: float; optional, used by data-ind npass optimizer
        Superclass Inputs:
            config: a configuration object
    """

    # Key is config option value, value is tuple (OptimizerClass, requires OLS pre-pass)
    l2_optimizers_dict = {
        CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS_PLUS: (DataIndQueriesL2NPassImprov, False),
        CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS: (DataIndQueriesL2NPass, False),
        CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS_DECOMP: (DataIndQueriesL2NPassDecomp, False),
        CC.SINGLE_PASS_REGULAR: (L2GeoOpt, False),
    }

    rounder_optimizers_dict = {
        CC.CELLWISE_ROUNDER: GeoRound,
        CC.MULTIPASS_ROUNDER: DataIndependentNpassRound,
        CC.MULTIPASS_ROUNDER_DECOMP: DataIndependentNpassRoundDecomp,
        CC.MULTIPASS_QUERY_ROUNDER: DataIndependentNpassQueryRound,
    }

    def __init__(self, *, identifier, child_geolevel, parent, parent_shape, childGeoLen, constraints, NoisyChild, noisy_child_weights, DPqueries,
                 rounder_queries, opt_dict, child_groups=None, L2_DPqueryOrdering=None, L2_Constrain_to_Ordering=None, Rounder_DPqueryOrdering=None,
                 optimizers=None, require_feasible_postproc_dim=False, **kwargs):
        """
        :param identifier: the geocode
        :param geolevel: the geolevel. e.g. 'us', 'state', 'county'
        """
        super().__init__(**kwargs)
        self.identifier: str = identifier # geocode
        self.child_geolevel: str = child_geolevel
        self.parent: Tuple[Union[np.ndarray, None], ...] = parent
        self.parent_shape: Tuple[Tuple[int, ...], ...] = parent_shape
        self.childGeoLen: int = childGeoLen
        self.constraints = constraints
        self.NoisyChild: np.ndarray = NoisyChild
        self.noisy_child_weights = noisy_child_weights
        self.DPqueries = DPqueries
        self.rounder_queries = rounder_queries
        self.opt_dict = opt_dict
        self.L2_DPqueryOrdering = L2_DPqueryOrdering
        self.L2_Constrain_to_Ordering = L2_Constrain_to_Ordering
        self.Rounder_DPqueryOrdering = Rounder_DPqueryOrdering
        self.nnls: bool = self.getboolean(CC.NONNEG_LB, default=True)
        self.opt_tol = True
        self.const_tol_val = 5.0
        self.opt_tol_slack = 0.01
        self.optimizers = optimizers
        optimal_only =  {toGRBfromStrStatus('OPTIMAL')}
        self.acceptable_l2_statuses = optimal_only.union(set(map(toGRBfromStrStatus, self.gettuple(CC.L2_ACCEPTABLE_STATUSES, default=()))))
        self.acceptable_rounder_statuses = optimal_only.union(set(map(toGRBfromStrStatus, self.gettuple(CC.ROUNDER_ACCEPTABLE_STATUSES, default=()))))
        self.require_feasible_postproc_dim = require_feasible_postproc_dim

        print(f"NNLS will accept optimization status flags: {self.acceptable_l2_statuses}")
        print(f"Rounder will accept optimization status flags: {self.acceptable_rounder_statuses}")

        self.child_groups = child_groups

    def createL2opt(self, l2opt, nnls, ols_result=None):
        """ Creates L2GeoOpt object"""

        return l2opt(das=self.das, config=self.config, grb_env=self.grb_env,
                     identifier=self.identifier, child_geolevel=self.child_geolevel,
                        parent=self.parent, parent_shape=self.parent_shape,
                        childGeoLen=self.childGeoLen,
                        NoisyChild=self.NoisyChild, noisy_child_weights=self.noisy_child_weights,
                        constraints=self.constraints, DPqueries=self.DPqueries, nnls=nnls, child_groups=self.child_groups, opt_dict=self.opt_dict,
                        dpq_order=self.L2_DPqueryOrdering,
                        const_tol=self.const_tol_val, opt_tol=self.opt_tol, opt_tol_slack=self.opt_tol_slack,
                        ols_result=ols_result, acceptable_l2_statuses=self.acceptable_l2_statuses)

    def createRounder(self, l2opt, rounder_opt_class):
        """ Creates Rounder object based on answer from an L2GeoOpt"""
        return rounder_opt_class(das=self.das, config=self.config, grb_env=self.grb_env, identifier=self.identifier, child_geolevel=self.child_geolevel,
                                 parent=self.parent, parent_shape=self.parent_shape,
                                 constraints=self.constraints,
                                 childGeoLen=self.childGeoLen, child=l2opt.answer, child_groups=self.child_groups,
                                 DPqueries=self.rounder_queries,
                                 dpq_order=self.Rounder_DPqueryOrdering, acceptable_rounder_statuses=self.acceptable_rounder_statuses)

    def run(self):
        """
        Runs on the CORE nodes, not on the master.
        The main method running the chain of optimizations:
        Outputs:
            L2opt.answer: numpy multi-array, the un-rounded solution
            Rounder.answer: numpy multi-array, the rounded solution
        """
        # L2 optimization
        l2_approach, rounder_approach = self.optimizers
        l2_opt_class, requires_ols_prepass = self.l2_optimizers_dict[l2_approach]
        rounder_opt_class = self.rounder_optimizers_dict[rounder_approach]

        if l2_approach in (CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS,
                           CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS_DECOMP,
                           CC.DATA_IND_USER_SPECIFIED_QUERY_NPASS_PLUS):
            tol_type = self.getconfig(CC.DATA_IND_NPASS_TOL_TYPE, default=CC.OPT_TOL)
            if tol_type == CC.CONST_TOL:
                self.const_tol_val = self.getfloat(CC.CONST_TOL_VAL, default=5.0)
                self.opt_tol = False
            elif tol_type == CC.OPT_TOL:
                self.opt_tol_slack = self.getfloat(CC.OPT_TOL_SLACK, default=0.01)
            else:
                raise ValueError(f"Unknown tolerance type for data-independent user-specified-queries multipass.")

        if not requires_ols_prepass:
            L2opt = self.createL2opt(l2opt=l2_opt_class, nnls=self.nnls)
            L2opt.run()
            self.checkModelStatus(L2opt.mstatus, self.acceptable_l2_statuses, "L2")
        else:
            L2opt_ols = self.createL2opt(l2opt=L2GeoOpt, nnls=False)
            L2opt_ols.run()
            self.checkModelStatus(L2opt_ols.mstatus, self.acceptable_l2_statuses, "L2", CC.L2_OLS_PREPASS)
            L2opt = self.createL2opt(l2opt=l2_opt_class, nnls=self.nnls, ols_result=L2opt_ols.answer)
            L2opt.run()
            self.checkModelStatus(L2opt.mstatus, self.acceptable_l2_statuses, "L2")

        self.addGurobiStatistics(L2opt_mstatus=L2opt.mstatus, msg='L2 model END')

        # TODO: Implement accounting for suboptimal solutions
        # L1 optimization (Rounding)
        Rounder = self.createRounder(L2opt, rounder_opt_class=rounder_opt_class)
        Rounder.run()
        self.checkModelStatus(Rounder.mstatus, self.acceptable_rounder_statuses, "Rounder")

        self.sendGurobiStatistics()
        del self.grb_env
        return L2opt.answer, Rounder.answer

    def checkModelStatus(self, m_status, acceptable_statuses, phase, msg=""):
        """
        Checks the status of the model, if not optimal, raises an exception
        Inputs:
            m_status: gurobi model status code
            phase: str indicating the (L2 or Rounder) phase
            msg: str, a message to print if not optimal
            :param acceptable_statuses:
        """

        # TODO: Implement accounting for suboptimal solutions
        if m_status not in acceptable_statuses:
            del self.grb_env
            raise Exception(f"{msg} Failure encountered in geocode {self.identifier} after {phase}. Model status was {m_status}.")


class L2PlusRounder_interleaved(L2PlusRounder):

    def __init__(self, *args, **kwargs):
        """
        Inputs:
            DPqueryOrdering: {str:int} indicating pass in which to target each DPquery
        """
        super().__init__(**kwargs)

        self.L2_revDPqueryOrdering = self.reverseQueryOrdering(self.L2_DPqueryOrdering)
        self.L2_DPqueryOrdering_pairs = self.getDPqueryOrderingAsPairs(self.L2_DPqueryOrdering)
        self.L2_outerPassNums = sorted(self.L2_DPqueryOrdering.keys())

        self.revL2_Constrain_to_Ordering = self.reverseQueryOrdering(self.L2_Constrain_to_Ordering)
        self.L2_Constrain_to_Ordering_pairs = self.getDPqueryOrderingAsPairs(self.L2_Constrain_to_Ordering)
        self.L2_Constrain_to_outerPassNums = sorted(self.L2_Constrain_to_Ordering.keys())

        self.Rounder_revDPqueryOrdering = self.reverseQueryOrdering(self.Rounder_DPqueryOrdering)
        self.Rounder_DPqueryOrdering_pairs = self.getDPqueryOrderingAsPairs(self.Rounder_DPqueryOrdering)
        self.Rounder_outerPassNums = sorted(self.Rounder_DPqueryOrdering.keys())

        assert self.Rounder_outerPassNums == self.L2_outerPassNums, "L2, Rounder must agree on outer passes"
        assert self.L2_Constrain_to_outerPassNums == self.L2_outerPassNums, "Constraint-to, Rounder must agree on outer passes"
        self.outerPassNums = self.L2_outerPassNums

        print(f"dataIndQueriesL2NPass has DPqueryOrdering:")
        for key in self.L2_DPqueryOrdering.keys():
            print(f"{key} -> {self.L2_DPqueryOrdering[key]}")
        print(f"dataIndQueriesL2NPass has revDPqueryOrdering:")
        for key in self.L2_revDPqueryOrdering.keys():
            print(f"{key} -> {self.L2_revDPqueryOrdering[key]}")


    def createL2opt(self, l2opt, nnls, ols_result=None,
                    DPqueryOrdering=None, Constrain_to_Ordering=None, DPqueries=None, single_sub_model=None):
        """ Creates L2GeoOpt object"""
        if DPqueries is None:
            DPqueries = self.DPqueries
        if DPqueryOrdering is None:
            DPqueryOrdering = self.L2_DPqueryOrdering
        if Constrain_to_Ordering is None:
            Constrain_to_Ordering = self.L2_DPqueryOrdering
        return l2opt(das=self.das, config=self.config, grb_env=self.grb_env,
                     identifier=self.identifier, child_geolevel=self.child_geolevel,
                     parent=self.parent, parent_shape=self.parent_shape,
                     childGeoLen=self.childGeoLen, NoisyChild=self.NoisyChild, noisy_child_weights=self.noisy_child_weights,
                     constraints=self.constraints, DPqueries=DPqueries, nnls=nnls,
                     child_groups=self.child_groups, opt_dict=self.opt_dict,
                     dpq_order=DPqueryOrdering, constrain_to_order=Constrain_to_Ordering,
                     const_tol=self.const_tol_val, opt_tol=self.opt_tol, opt_tol_slack=self.opt_tol_slack,
                     ols_result=ols_result, acceptable_l2_statuses=self.acceptable_l2_statuses, single_sub_model=single_sub_model)

    def createRounder(self, l2opt, rounder_opt_class, DPqueryOrdering=None, DPqueries=None, single_sub_model=None):
        """ Creates Rounder object based on answer from an L2GeoOpt"""
        if DPqueries is None:
            DPqueries = self.rounder_queries
        if DPqueryOrdering is None:
            DPqueryOrdering = self.Rounder_DPqueryOrdering
        return rounder_opt_class(das=self.das, config=self.config, grb_env=self.grb_env,
                                 identifier=self.identifier, child_geolevel=self.child_geolevel,
                                 parent=self.parent, parent_shape=self.parent_shape,
                                 constraints=self.constraints, childGeoLen=self.childGeoLen, child=l2opt.answer,
                                 child_groups=self.child_groups, DPqueries=DPqueries, dpq_order=DPqueryOrdering,
                                 acceptable_rounder_statuses=self.acceptable_rounder_statuses, single_sub_model=single_sub_model)

    def run(self):
        """
        :return:
        """
        base_constraints = deepcopy(self.constraints)
        single_sub_model = True if self.child_groups is not None else False
        for outerPassNum in self.outerPassNums:
            L2_answer, rounder_answer = self.interleave_optimizations(outerPassNum=outerPassNum, single_sub_model=single_sub_model)
            single_sub_model = False

        if self.getboolean(CC.USE_POSTPROCESS_HEURISTICS, section=CC.GUROBI, default=False):
            rounder_answer = self.postprocess(rounder_answer, base_constraints)
        return L2_answer, rounder_answer

    def postprocess(self, sol, constraints):
        init_time = time.time()

        # This function adjusts the histogram estimate sol so that the following constraint is satisfied:
        postprocess_only_constr_names = self.gettuple(CC.POSTPROCESS_ONLY_CONSTR_NAMES, section=CC.GUROBI, default=())
        postproc_constrs = [postproc_constr for postproc_constr in constraints if postproc_constr.name in postprocess_only_constr_names]
        # We only move records between histogram cells that are within the same query level of the following constraint:
        conditional_constr_names = self.gettuple(CC.CONDITIONAL_CONSTR_NAMES, section=CC.GUROBI, default=())
        cond_constrs = [constr for constr in constraints if constr.name in conditional_constr_names]

        # This method uses one of three possible approaches for switching the attributes of records. The approach used is dictated by the following boolean and self.require_feasible_postproc_dim.
        # The boolean self.require_feasible_postproc_dim dictates whether to continue switching attributes until each constraint in postproc_constrs are satisfied.
        # Comments describing each approach can be found in the function find_recipient_ind() below.
        # We optionally ensure that the remaining constraints are satisfied (ie: constraints that are not in conditional_constr_names or postprocess_only_constr_names):
        require_feasible_secondary_dim = self.getboolean(CC.REQUIRE_FEASIBLE_SECONDARY_DIM, section=CC.GUROBI, default=True)
        # The boolean self.require_feasible_postproc_dim dictates whether to continue switching attributes until each constrain in postproc_constrs are satisfied.
        remaining_constrs = [constr for constr in constraints if (constr.name not in conditional_constr_names) and (constr.name not in postprocess_only_constr_names)]

        assert require_feasible_secondary_dim or self.require_feasible_postproc_dim, f"Both 'require_feasible_postproc_dim' and 'require_feasible_secondary_dim' are False; however, 'use_postprocess_heuristics' is True."

        # To check the residuals at the end of this function, save the input histogram:
        resid = np.zeros(sol[0].shape)

        # This block of code simply checks to verify all of our assumptions are satisfied and also defines the variables expanded_dim and expanded_dim_kron:
        # We assume all children are included in each constraint:
        for postproc_constr in postproc_constrs:
            assert postproc_constr.indices == tuple(range(self.childGeoLen))
        # We assume cond_constrs only contains one constraint:
        assert len(cond_constrs) == 1
        # We assume constraints in postproc_constrs are not equality constraints, LHS matrices are all identical, all nonzero elements of LHS matrices are 1, all columns of
        # LHS matrix block corresponding to the main histogram contain a single nonzero element, there is only one expanded dimension, and the expanded dimension is in the main histogram:
        kron_facs = postproc_constrs[0].query.kronFactors()
        assert toGRBFromStr()[postproc_constrs[0].sign] != "="
        expanded_dim = None
        expanded_dim_kron = None
        for postproc_constr in postproc_constrs:
            kron_facs_k = postproc_constr.query.kronFactors()
            assert toGRBFromStr()[postproc_constr.sign] != "="
            for hist_ind, (hist_kron_facs, hist_kron_facs_k) in enumerate(zip(kron_facs, kron_facs_k)):
                for dim_ind, (hist_kron_fac, hist_kron_fac_k) in enumerate(zip(hist_kron_facs, hist_kron_facs_k)):
                    if hist_kron_fac is None:
                        assert hist_kron_fac_k is None
                        assert hist_ind == 1
                    else:
                        assert hist_kron_fac.shape == hist_kron_fac_k.shape
                        assert np.all(hist_kron_fac.data == hist_kron_fac_k.data)
                        assert np.all(hist_kron_fac.data == 1)
                        assert len(hist_kron_fac.data) == hist_kron_fac.shape[1]
                        assert np.all(hist_kron_fac.indices == hist_kron_fac_k.indices)
                        assert np.all(hist_kron_fac.indptr == hist_kron_fac_k.indptr)
                        if hist_kron_fac_k.shape[0] > 1:
                            assert hist_ind == 0
                            assert expanded_dim in (None, dim_ind)
                            expanded_dim = dim_ind
                            expanded_dim_kron = hist_kron_fac_k.tocsr()
        # We assume all constraints in remaining_constrs expand at most one dimension other than expanded_dim:
        rem_constrs_expanded_dim = None
        if len(remaining_constrs) > 0:
            for constr in remaining_constrs:
                kron_facs = constr.query.kronFactors()
                for hist_ind, hist_kron_facs in enumerate(kron_facs):
                    for dim_ind, hist_kron_fac in enumerate(hist_kron_facs):
                        if hist_kron_fac is not None:
                            dim_is_expanded = hist_kron_fac.shape[0] > 1 or len(hist_kron_fac.data) < hist_kron_fac.shape[1]
                            assert np.all(hist_kron_fac.data == 1)
                            if dim_is_expanded:
                                if dim_ind != expanded_dim and rem_constrs_expanded_dim is None and hist_ind==0:
                                    rem_constrs_expanded_dim = dim_ind
                                assert dim_ind in (rem_constrs_expanded_dim, expanded_dim)

        find_col_groups = lambda mat: [row.indices for row in mat]

        # Under the assumptions above, each postprocessing constraint constrains the sum over detailed cells that have a value of the expanded_dim attribute within certain groups. These groups are given by:
        postproc_constr_col_groups = find_col_groups(expanded_dim_kron)

        # Define the remaining required Kronecker factor column groups in a similar way; as in previous line, use suffix 'col_groups' for each such variable name:
        all_expanded_dims = np.array([expanded_dim])
        remaining_constr_col_groups = {}
        for constr in remaining_constrs:
            kron_facs = [mat.tocsr() for mat in constr.query.kronFactors()[0]]
            remaining_constrs_expanded_dims = [k for k, mat in enumerate(kron_facs) if mat.shape[0] > 1 or len(mat.data) < mat.shape[1]]
            all_expanded_dims = np.unique(np.concatenate((all_expanded_dims, remaining_constrs_expanded_dims)))
            col_groups =  [find_col_groups(mat) for k, mat in enumerate(kron_facs) if k in remaining_constrs_expanded_dims]  # Format: [[<list of nonzero cols in row 0 of kron_fac[0]>, <list of nonzero cols in row 1 of kron_fac[0]>, ...], ...]
            remaining_constr_col_groups[constr.name] = dict(zip(remaining_constrs_expanded_dims, col_groups))

        cond_constr_kron_facs = [mat.tocsr() for mat in cond_constrs[0].query.kronFactors()[0]]
        col_groups =  [(k, find_col_groups(mat)) for k, mat in enumerate(cond_constr_kron_facs) if k in all_expanded_dims]  # Format: [(i, [<list of nonzero cols in row 0 of kron_fac[i]>, <list of nonzero cols in row 1 of kron_fac[i]>, ...]), ...]
        cond_constr_col_groups = dict(col_groups)

        assert len(all_expanded_dims) == 2

        recipient_col_groups = []
        for postproc_constr_cols in postproc_constr_col_groups:
            for cond_constr_cols in cond_constr_col_groups[expanded_dim]:
                # The assert at the end of this block ensures that the condition in the next if statement evaluates to True in exactly one cond_constr_cols iteration:
                if np.all(np.isin(postproc_constr_cols, cond_constr_cols)):
                    recipient_col_groups.append(cond_constr_cols)
        assert len(recipient_col_groups) == len(postproc_constr_col_groups)

        # The assumptions above allow us to only consider the main histogram estimate from this point forward:
        sol0 = sol[0]

        choose_element = lambda inds, hist, delta, child, ind_description: self.choose_candidate_ind(inds, hist, delta, child, ind_description)
        # The assignment above results in the swap with lowest MSE being the swap that is chosen among the candidate swaps being considered. The following will result in a random choice of the candidates instead:
        # choose_element = lambda inds, hist, delta, child, ind_description: np.random.choice(inds)

        print(f"For {self.identifier}, postprocess initialization done in {time.time() - init_time} seconds")
        for child in range(self.childGeoLen):
            hist = sol0[..., child]
            for postproc_constr in postproc_constrs:
                postproc_constr_name = postproc_constr.name
                postproc_constr_rhs_vec = postproc_constr.rhsList[child]
                postproc_constr_sense = toGRBFromStr()[postproc_constr.sign]

                print(f"For {self.identifier}, postproc_constr_name, postproc_constr_rhs_vec, postproc_constr_sense: {postproc_constr_name, postproc_constr_rhs_vec, postproc_constr_sense}")
                for coarse_cols, postproc_constr_cols, rhs in zip(recipient_col_groups, postproc_constr_col_groups, postproc_constr_rhs_vec):
                    inds_lhs = tuple(slice(None) if k != expanded_dim else postproc_constr_cols for k in range(sol0.ndim - 1))
                    lhs = np.sum(hist[inds_lhs])

                    postproc_constr_not_satisfied = (lambda x, y: x > y) if postproc_constr_sense == "<" else (lambda x, y: x < y)
                    delta = -1 if postproc_constr_sense == "<" else 1
                    # Note that the following function will provide all indices of its input when postproc_constr_sense == ">":
                    make_candidate_hist_inds = (lambda x: list(zip(*np.nonzero(x)))) if postproc_constr_sense == "<" else (lambda x: list(zip(*np.nonzero(x >= 0))))
                    print(f"For {self.identifier}, considering constraint row for child {child} with coarse_cols, postproc_constr_cols, lhs, rhs: {coarse_cols, postproc_constr_cols, lhs, rhs}")

                    at_least_one_recipient_found = True
                    while postproc_constr_not_satisfied(lhs, rhs) and at_least_one_recipient_found:
                        # Consider changing the elements of hist for which mask is True in a random order:
                        nnz_sub_hist_inds = np.random.permutation(make_candidate_hist_inds(hist[inds_lhs]))
                        at_least_one_recipient_found = False

                        for nnz_sub_hist_ind in nnz_sub_hist_inds:
                            # Make sure to avoid setting the recipient index to an index used in a query level of postproc_constrs that is binding:
                            failing_or_binding_levels = check_constr(postproc_constr, postproc_constr.rhsList[child], [hist] + [sol[1][..., child]], return_binding_and_failed_levels=True)
                            postproc_constr_cols_to_skip = np.concatenate(tuple(postproc_constr_col_groups[level] for level in failing_or_binding_levels))

                            nnz_hist_ind = tuple(ind_k if dim != expanded_dim else postproc_constr_cols[ind_k] for dim, ind_k in enumerate(nnz_sub_hist_ind))

                            # Add delta to hist[cur_ind], find the recipient index recipient_ind, and then subtract delta from hist[recipient_ind]; since hist is a sub-array of
                            # sol[0], these updates to hist also impact sol[0]:
                            hist[nnz_hist_ind] += delta
                            recipient_ind = find_recipient_ind(child, expanded_dim, coarse_cols, remaining_constrs, hist, nnz_hist_ind, delta,
                                                               require_feasible_secondary_dim, self.require_feasible_postproc_dim, remaining_constr_col_groups,
                                                               cond_constr_col_groups, sol[1][..., child], postproc_constr_cols_to_skip, choose_element)

                            if recipient_ind is None:
                                # Attempt to move to next element of nnz_sub_hist_inds, but first invert the operation hist[nnz_hist_ind] += delta evaluated above
                                hist[nnz_hist_ind] -= delta
                                print(f"For {self.identifier}, for child {child}, failed to move a record from {nnz_hist_ind}. Current lhs, rhs, delta, and postproc_constr_sense are: {lhs, rhs, delta, postproc_constr_sense}.")
                            else:
                                resid[nnz_hist_ind + (child,)] += delta
                                resid[recipient_ind + (child,)] -= delta
                                hist[recipient_ind] -= delta
                                lhs = np.sum(hist[inds_lhs])
                                at_least_one_recipient_found = True
                                print(f"For {self.identifier}, for child {child}, moved a record from {nnz_hist_ind} to {recipient_ind}. Current lhs, rhs, delta, and postproc_constr_sense are: {lhs, rhs, delta, postproc_constr_sense}.")
                                if not postproc_constr_not_satisfied(lhs, rhs):
                                    break
            print(f"For {self.identifier}, postprocessing for child {child} done in {time.time() - init_time} seconds")

        resid = resid.flatten()
        print(f"For {self.identifier}, postprocessing resid quantiles: {np.quantile(resid, np.arange(0, 1.1, .1))}, mean resid: {np.mean(resid)}, MAD resid: {np.mean(np.abs(resid))}")

        return [sol0] + [sol[1]]

    def interleave_optimizations(self, outerPassNum=None, single_sub_model=None):
        """
        Perform optimization for a single outer pass
        :param outerPassNum:
        :return: L2 answer and rounded answer
        """
        l2_approach, rounder_approach = self.optimizers
        l2_opt_class, requires_ols_prepass = self.l2_optimizers_dict[l2_approach]
        rounder_opt_class = self.rounder_optimizers_dict[rounder_approach]

        # Note: Not using L2 with OLS pre-prepass, if needed, copy the prepass from L2PlusRounder
        L2_dpq_names = reduce(add, self.L2_DPqueryOrdering[outerPassNum].values())
        L2_queries = [list(filter(lambda dpq: dpq.name in L2_dpq_names, dpq_hist_set)) for dpq_hist_set in self.DPqueries]
        L2opt = self.createL2opt(l2opt=l2_opt_class, DPqueries=L2_queries,
                                 nnls=True, DPqueryOrdering=self.L2_DPqueryOrdering[outerPassNum],
                                 Constrain_to_Ordering=self.L2_Constrain_to_Ordering[outerPassNum], single_sub_model=single_sub_model)
        L2opt.run()
        self.checkModelStatus(L2opt.mstatus, self.acceptable_l2_statuses, f"OuterPass: {outerPassNum}")

        rq_names = reduce(add, self.Rounder_DPqueryOrdering[outerPassNum].values())
        rounder_queries = [list(filter(lambda rq: rq.name in rq_names, rq_hist_set)) for rq_hist_set in self.rounder_queries]
        Rounder = self.createRounder(L2opt, rounder_opt_class=rounder_opt_class, DPqueries=rounder_queries,
                                     DPqueryOrdering=self.Rounder_DPqueryOrdering[outerPassNum], single_sub_model=single_sub_model)
        Rounder.run()
        self.checkModelStatus(Rounder.mstatus, self.acceptable_rounder_statuses, f"OuterPass: {outerPassNum}")
        answer = Rounder.answer
        print(f"outerPass {outerPassNum} ---> L2: {L2_dpq_names}, L2_queries:: {L2_queries}, Rounder: {rq_names}, rounder_queries:: {rounder_queries}")

        final_rounder_pass = sorted(self.Rounder_DPqueryOrdering[outerPassNum].keys())[-1]
        for ihist, dp_queries in enumerate(self.rounder_queries):
            for st_dpq in dp_queries:
                if self.Rounder_revDPqueryOrdering[st_dpq.name] == (outerPassNum, final_rounder_pass):
                    q = st_dpq.query
                    queries = [StubQuery((int(np.prod(hist.shape) / self.childGeoLen), q.numAnswers()), "stub") for hist in answer]
                    coeffs = [0] * len(answer)
                    queries[ihist] = q
                    coeffs[ihist] = 1
                    multi_query = MultiHistQuery(tuple(queries), tuple(coeffs), f"{q.name}_multi")
                    if outerPassNum != self.outerPassNums[-1]:
                        constraints_indices_equal = []
                        for sc_child_ind, child_num in enumerate(st_dpq.indices):
                            pass_answer = multi_query.answer(np.hstack([hist.reshape((int(np.prod(hist.shape) / self.childGeoLen), self.childGeoLen))[:, child_num] for hist in answer]))
                            constraints_indices_equal.append((Constraint(multi_query, pass_answer, "=", f"outerPassNum_{outerPassNum}Constr_{q.name}_{child_num}"), child_num))
                        self.constraints.append(StackedConstraint(constraints_indices_equal))

        return L2opt.answer, Rounder.answer

    def reverseQueryOrdering(self, inputDict):
        """
            Takes a dict
            a = {int -> [str, ..., str]
            And inverts-plus decomposes it to
            b = {str -> int}
            such that set(b.keys()) == union_{k in a.keys()} set(a[k])
            str's are assumed to be distinct.
        """
        # from itertools import groupby
        # allStrs = reduce(add, [[dpqNamesList for dpqNamesList in innerDict.values()] for innerDict in inputDict.values()])
        # dpqNameFrequencies = [len(list(group)) for key, group in groupby(allStrs)]
        # assert max(dpqNameFrequencies) == 1, "dpqNames in inputDict must be distinct."

        # As we allow for a constrain_to dict, now, dpqNames no longer need to be distinct.
        reversedDict = {}
        for outerPassNum in inputDict.keys():
            for innerPassNum in inputDict[outerPassNum].keys():
                for dpqName in inputDict[outerPassNum][innerPassNum]:
                    reversedDict[dpqName] = (outerPassNum, innerPassNum)
        return reversedDict

    def getDPqueryOrderingAsPairs(self, inputDict):
        pairedDict = {}
        for outerPassNum in inputDict.keys():
            for innerPassNum in inputDict[outerPassNum].keys():
                pairedDict[(outerPassNum, innerPassNum)] = inputDict[outerPassNum][innerPassNum]
        return pairedDict

    def mse_obj_fxn(self, hist, child):
        n_list = tuple(np.prod(sh) for sh in self.parent_shape)
        obj_fxn = 0.
        for ihist, dp_queries in enumerate(self.DPqueries):
            if dp_queries is None:
                continue
            for st_dpq in dp_queries:
                query = st_dpq.query
                assert ihist == 0
                assert st_dpq.indices[child] == child
                var_k = st_dpq.VarList[child]
                dp_ans_k = st_dpq.DPanswerList[child]
                w_resid = (query.answer(hist) - dp_ans_k) / np.sqrt(var_k)
                obj_fxn += np.sum(w_resid ** 2)

        flat_hist = hist.flatten()
        if self.NoisyChild is not None:
            for ihist, weight in enumerate(zip(*deepcopy(self.noisy_child_weights))):
                if np.sum(np.abs(weight)) == 0 :
                    continue
                start = 0 if ihist == 0 else np.cumsum(n_list)[ihist - 1]
                end = np.cumsum(n_list)[ihist]
                hist_vars = np.arange(start, end, dtype=np.int64)
                weight_k = weight[child]
                assert ihist == 0
                w_resid = (flat_hist[hist_vars] - self.NoisyChild[hist_vars, child]) * np.sqrt(weight_k)
                obj_fxn += np.sum(w_resid ** 2)
        return obj_fxn

    def choose_candidate_ind(self, inds, hist, delta, child, inds_description):
        init_time = time.time()
        if len(inds) == 1:
            return inds[0]
        obj_fxns = []
        for ind in inds:
            hist[ind] -= delta
            obj_fxns.append(self.mse_obj_fxn(hist, child))
            hist[ind] += delta
        print(f"For child {child}, chose one of {len(inds)} indices {inds_description} in {time.time() - init_time} seconds")
        return inds[np.argmin(obj_fxns)]


def test_dim_change(child, dim_to_change, coarse_cols, remaining_constrs, hist, cur_ind, delta, sol1, postproc_constr_cols_to_skip=None):
    cur_col = cur_ind[dim_to_change]
    recipient_inds_with_constrs_satisfied = []
    all_recipient_inds = []
    recipient_inds_that_violate_nonnegativity = []
    failed_constrs_levels = {}

    # We never want to consider defining the recipient and donor as the same detailed cells, so we always skip cur_col:
    if postproc_constr_cols_to_skip is None:
        postproc_constr_cols_to_skip = [cur_col]
    elif cur_col not in postproc_constr_cols_to_skip:
        assert False, f"{postproc_constr_cols_to_skip}"
        postproc_constr_cols_to_skip.append(cur_col)

    for coarse_col in coarse_cols:
        if coarse_col not in postproc_constr_cols_to_skip:
            candidate_ind = tuple(ind_k if dim != dim_to_change else coarse_col for dim, ind_k in enumerate(cur_ind))

            # Do not consider changes that result in the histogram detailed cell counts being negative:
            if delta == 1 and hist[candidate_ind] == 0:
                recipient_inds_that_violate_nonnegativity.append(candidate_ind)
                failed_constrs_levels[coarse_col] = {constr.name:check_constr(constr, constr.rhsList[child], [hist] + [sol1], return_failed_levels=True) for constr in remaining_constrs}
                continue

            # We will test if remaining constraints hold for hist after subtracting delta from hist[candidate_ind]; to avoid calls to deepcopy(hist), we simply inverse this operation below:
            hist[candidate_ind] -= delta
            failed_constrs_levels[coarse_col] = {constr.name:check_constr(constr, constr.rhsList[child], [hist] + [sol1], return_failed_levels=True) for constr in remaining_constrs}

            all_satisfied = sum([len(failed_constrs_levels[coarse_col][constr.name]) for constr in remaining_constrs]) == 0
            if all_satisfied:
                recipient_inds_with_constrs_satisfied.append(candidate_ind)
            all_recipient_inds.append(candidate_ind)

            hist[candidate_ind] += delta

    return all_recipient_inds, recipient_inds_with_constrs_satisfied, failed_constrs_levels, recipient_inds_that_violate_nonnegativity


def find_recipient_ind(child, expanded_dim, coarse_cols, remaining_constrs, hist, cur_ind, delta, require_feasible_secondary_dim,
                       require_feasible_postproc_dim, remaining_constr_col_groups, cond_constr_col_groups, sol1, postproc_constr_cols_to_skip, choose_element):

    all_recipient_inds, recipient_inds_with_constrs_satisfied, failed_constrs_levels, recipient_inds_neg = test_dim_change(child, expanded_dim, coarse_cols, remaining_constrs, hist, cur_ind, delta, sol1, postproc_constr_cols_to_skip)

    # If possible, always choose recipient index that does not require modifying age:
    if len(recipient_inds_with_constrs_satisfied) > 0:
        return choose_element(recipient_inds_with_constrs_satisfied, hist, delta, child, "with remaining constraints satisfied")

    if require_feasible_postproc_dim and not require_feasible_secondary_dim:
        # This combination of config file parameters results in the donor record always being moved to a recipient record, even when the remaining constraints are not satisfied:
        return choose_element(all_recipient_inds, hist, delta, child, "without remaining constraints satisfied")

    if not require_feasible_postproc_dim and require_feasible_secondary_dim:
        # This combination of config file parameters results in the donor record only being moved to a recipient record when the remaining constraints are not satisfied:
        return None

    # When require_feasible_postproc_dim and require_feasible_secondary_dim, also adjust value in dimension rem_constrs_expanded_dim to ensure all constraints are satisfied:
    rem_constrs_expanded_dim = [dim for dim in list(cond_constr_col_groups.keys()) if dim != expanded_dim][0]
    # We only consider recipient columns that do not change the query answer estimates of the conditional constraint:
    recipient_cols_init = np.array([xk for xk in cond_constr_col_groups[rem_constrs_expanded_dim] if cur_ind[rem_constrs_expanded_dim] in xk][0])

    all_recipient_inds.extend(recipient_inds_neg)
    for recipient_ind in all_recipient_inds:
        recipient_cols = recipient_cols_init
        coarse_col = recipient_ind[expanded_dim]
        for rem_constr in remaining_constrs:
            failed_levels = failed_constrs_levels[coarse_col][rem_constr.name]
            if len(failed_levels) > 0:
                # We only need to consider the recipient columns that change the LHS of the constraint rows that failed previously:
                col_inds_to_not_use = np.unique(np.concatenate(tuple(remaining_constr_col_groups[rem_constr.name][rem_constrs_expanded_dim][failed_level] for failed_level in failed_levels)))
                recipient_cols = np.setdiff1d(recipient_cols, col_inds_to_not_use)
        _, new_recipient_inds_with_constrs_satisfied, _, _ = test_dim_change(child, rem_constrs_expanded_dim, recipient_cols, remaining_constrs, hist, recipient_ind, delta, sol1)
        recipient_inds_with_constrs_satisfied.extend(new_recipient_inds_with_constrs_satisfied)
    if len(recipient_inds_with_constrs_satisfied) == 0:
        print(f"In find_recipient_ind, could not find any recipient detailed cells that maintain feasibility of remaining constraints.")
        return None
    return choose_element(recipient_inds_with_constrs_satisfied, hist, delta, child, "with updated secondary dimension to satisfy remaining constraints")


def check_constr(constr, rhs, data, return_failed_levels=False, return_binding_and_failed_levels=False):
    """
    This checks that the data satisfies the constraint for a single rhs vector.
    Note that constr is a StackedConstraint object from programs/queries/constraints_dpqueries.py;
    this function does not check that the constraint is satisfied for each rhs vector in constr.rhsList.
    """
    # Difference between query applied to the data and the right hand side
    diff = constr.query.answer(data) - rhs

    if constr.sign == "=":
        status = abs(diff) == 0
    elif constr.sign == "ge":
        status = diff >= 0
    elif constr.sign == "le":
        status = diff <= 0
    else:
        raise Exception

    if return_binding_and_failed_levels:
        return np.nonzero(np.logical_or(np.invert(status), diff == 0))[0]

    if return_failed_levels:
        return np.nonzero(np.invert(status))[0]
    return np.all(status)
