from typing import List, Iterable
from time import time
from copy import deepcopy

import scipy.sparse as ss
import numpy as np

from programs.optimization.geo_optimizer_decomp import GeoOptimizerDecomp

from das_constants import CC
from programs.queries.querybase import MultiHistQuery
from programs.optimization.geo_optimizers import ASSERT_TYPE
from programs.optimization.optimizer import findNonzeroCols


class DataIndQueriesL2NPassDecomp(GeoOptimizerDecomp):
    """
        Class for defining multi-pass NNLS solver that uses histogram decomposition between outer optimization passes. See
        docstring of GeoOptimizerDecomp for description of cases in which histogram decomposition improves runtime.
    """

    def __init__(self, *, dpq_order, constrain_to_order=None, opt_tol=True, opt_tol_slack=0.01, **kwargs):
        """
        Inputs:
            dpq_order:              {int:[str,...,str]} indicating pass in which to target each DPquery
            constrain_to_order:     {int:[str,...,str]} indicating pass in which to constraint value of each DPquery
        """
        super().__init__(**kwargs)
        self.has_parent = self.parent[0] is not None
        self.dpq_order = dpq_order  # {int:[str,...,str]}, pass # -> [...,dpq_names,...]
        self.rev_dpq_order = self.reverseQueryOrdering(self.dpq_order)  # {str : set([int,...,int])}

        if constrain_to_order is not None:
            self.constrain_to_order = constrain_to_order
        else:
            self.constrain_to_order = self.dpq_order
        self.rev_constrain_to_order = self.reverseQueryOrdering(self.constrain_to_order)

        assert set(self.constrain_to_order.keys()) == set(self.dpq_order.keys()), f"All passes must appear >=1 time per order. constrain-to order: {self.constrain_to_order.keys()}, dpq_order: {self.dpq_order.keys()}"
        self.pass_nums = sorted(self.dpq_order.keys())  # Config file integers numbering each multi-pass pass

        # The following variables are used during inner optimization passes. Note that these inner passes can alternatively be
        # substituted for outer optimization passes, which has the advantage of avoiding numerical issues that are caused by the
        # feasible region being defined by a narrow region; see for example,
        # https://www.gurobi.com/documentation/9.1/refman/optimizing_over_thin_regio.html.
        self.opt_tol = opt_tol      # Should we optimize tol for float = modeled as pair of <=, >= in current problem?
        self.opt_tol_slack = opt_tol_slack  # If using opt_tol, what extra additive slack should be added?
        assert self.opt_tol, f"Hierarchical decomposition requires DataIndNPass_toleranceType=opt_tol in section {CC.GUROBI}"
        self.bar_conv_tol_for_opt_tol_solve = self.getfloat(CC.BARCONVTOL_FOR_OPT_TOL, section=CC.GUROBI, default=1e-4)

    def optimizationPassesLoop(self, model, obj_fxn, two_d_vars, n_list, child_sub, parent_mask, parent_sub, child_floor):
        """
            --- Primary entry point for understanding optimization logic ---
            Multi-pass optimization loop: optimize, add constraints for current pass target queries, optimize further, etc.
                (called by run method of parent class)
        """

        self.final_solution = np.zeros(two_d_vars.shape)
        if self.getboolean("simplify_consistency_constrs", section=CC.GUROBI, default=True):
            # If the detailed query is in the objective function, the coarsest supporting query matrix component for the main histogram is simply the identity matrix.
            # Constrain each detailed cell to sum to the corresponding cells of the parent in this case:

            supporting_query_mat = None if (CC.DETAILED in self.rev_dpq_order.keys()) else self.findCoarsestSupportingQueryMat(parent_mask)
        else:
            supporting_query_mat = None

        if self.getboolean("constrain_main_vars_to_zero", section=CC.GUROBI, default=True) and supporting_query_mat is not None:
            for row_ind in range(supporting_query_mat.shape[0]):
                col_inds = findNonzeroCols(supporting_query_mat, row_ind)
                if len(col_inds) > 1:
                    two_d_vars[col_inds[1:], :].ub = 0
                    two_d_vars[col_inds[1:], :].lb = 0

        for sub_model_index, sub_model in enumerate(self.sub_models):
            # Create a list of a single integer that will track the total number of auxiliary variables in the sub-model, which is modified in place (ie: as a side effect) in addObjFxnTerm():
            total_aux = [0]
            # Create a list for the constraints and variables in the sub-model, which is modified in place (ie: as a side effect) to add new sub-model components (variables/constraints)
            # in addObjFxnTerm() and findOptimalTolAndAddConstrs():
            sub_model_comps = []
            if self.use_parent_constraints:
                self.buildAndAddParentConstraints(model, parent_sub, two_d_vars, f"par_consist_{sub_model_index}", supporting_query_mat, sub_model, sub_model_comps)
            if self.constraints is not None:
                self.addStackedConstraints(model, parent_mask, two_d_vars, child_floor=child_floor, sub_model_comps=sub_model_comps, sub_model_index=sub_model_index)

            for pass_num in self.pass_nums:
                obj_fxn = self.buildObjFxnAddQueries_decomp(model, two_d_vars, n_list, child_sub, parent_mask, pass_num, sub_model_index, sub_model_comps, total_aux)
                # predicted number of variables in model is sum of number of auxiliary variables, main variables in each child, and `tol` variables from opt_tol solves:
                predicted_var_num = total_aux[0] + two_d_vars.shape[1] * len(sub_model) + pass_num
                self.setObjAndSolve(model, obj_fxn, predicted_var_num=predicted_var_num) # Optimize
                assert model.Status in self.acceptable_statuses, f"In DataIndQueriesL2NPassDecomp: for pass_num {pass_num} and sub_model_index {sub_model_index} in node w/ id {self.identifier} returned unacceptable status: {model.Status}. Acceptable statuses: {self.acceptable_statuses}"
                if pass_num != self.pass_nums[-1]:
                    self.findOptimalTolAndAddConstrs(model, two_d_vars, parent_mask, pass_num, sub_model_index, self.opt_tol_slack, sub_model_comps, total_aux)
            self.final_solution[sub_model, :] = two_d_vars.X[sub_model, :]
            if sub_model_index != (len(self.sub_models) - 1):
                model.remove(sub_model_comps)

    def buildObjFxnAddQueries_decomp(self, model, two_d_vars, n_list, child_sub, parent_mask, pass_num, sub_model_index=None, sub_model_comps=None, total_aux=None):
        """
            Build obj fxn for one optimization pass of one sub-model.
        """
        obj_fxn = 0.
        if (CC.DETAILED in self.rev_dpq_order.keys()) and (pass_num in self.rev_dpq_order[CC.DETAILED]):
            print(f"In {self.identifier} and L2_Decomp: Adding detailed cell query to obj_fxn in sub_model {sub_model_index}")
            obj_fxn = self.buildObjFxn_decomp(model, two_d_vars, n_list, child_sub, self.child_obj_term_wt, self.sub_models[sub_model_index],
                                              sub_model_comps=sub_model_comps, total_aux=total_aux)
        q_set_list = self.filterQueries(lambda dpqname: pass_num in self.rev_dpq_order[dpqname], self.DPqueries, self.rev_dpq_order)
        obj_fxn = self.addDPQueriesToModel(model, two_d_vars, obj_fxn, parent_mask, q_set_list=q_set_list, pass_num=pass_num, sub_model_index=sub_model_index, sub_model_comps=sub_model_comps, total_aux=total_aux)
        return obj_fxn

    def childDPQueryTerm(self, model, obj_fxn, A, x, b, weight, n_ans, qname, lb, pass_num=None, sub_model_index=None, sub_model_comps=None, total_aux=None, **kwargs):
        """
            Appends DPQuery term corresponding to pass :pass_num: to obj fxn
        """
        print(f"In {self.identifier}, In L2_Decomp: in pass num {pass_num} and sub-model {sub_model_index}, adding query {qname} to objective function")
        obj_fxn += self.addObjFxnTerm(model, A, x, b, weight, int(n_ans), f"dpq_objFxn_#{pass_num}_sub{sub_model_index}_{qname}", lb, sub_model_comps, total_aux)
        return obj_fxn

    @staticmethod
    def addObjFxnTerm(model, A, x, b, wt, n_aux, name, lb=0, sub_model_comps=None, total_aux=None):
        """
        Calculate quadratic terms for the objective function, of the form wt * (A @ x - b)^2.
        An auxiliary variable, Ax, is created, constrained to be equal to A @ x, and then entered
        into obj_fxn.
        Returns the quadratic term as gurobi MQuadExpr
        """
        import gurobipy as gb
        # The lower bound is not important because two_d_vars already has a lower bound of zero but it can alternatively be set to -wt@b.
        w_resid = model.addMVar(n_aux, vtype=gb.GRB.CONTINUOUS, lb=(-wt@b), name=name)
        new_obj_fxn_constrs = model.addConstr(wt @ A @ x - wt @ b == w_resid, name=f"{name}_cons")

        # The next two variables are modified in place:
        sub_model_comps.extend(new_obj_fxn_constrs.tolist())
        total_aux[0] += n_aux
        return w_resid @ w_resid

    @staticmethod
    def buildObjFxn_decomp(model, two_d_vars, n_list, child_sub, obj_fxn_weight_list, sub_model_vars=None, sub_model_comps=None, total_aux=None):
        """
        Adds squared error to the objective fxnction for the noisychild
        Inputs:
            :param n_list: List of number of non-zero variables in each histogram
            :param two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            :param child_sub: noisy children as numpy array (top node) or csr_matrix num_children X num_cells
            :param obj_fxn_weight_list: list of floats, coefficient for each cell of the objective function, one per histogram
            :param sub_model_vars: a list of integer variable indices indicating the elements of two_d_vars[:, k] that are included in the sub-model for each child k
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.MVar)
        obj_fxn = 0
        if child_sub is not None:
            for ihist, weight in enumerate(zip(*obj_fxn_weight_list)):
                if np.sum(np.abs(weight)) == 0:
                    continue
                start = 0 if ihist == 0 else np.cumsum(n_list)[ihist - 1]
                end = np.cumsum(n_list)[ihist]
                sub_model_hist_vars = np.arange(start, end, dtype=np.int64)
                sub_model_hist_vars = sub_model_hist_vars[np.isin(sub_model_hist_vars, sub_model_vars)]
                num_vars = len(sub_model_hist_vars)

                if num_vars == 0:
                    continue

                n_aux = len(sub_model_hist_vars)
                for child_num, weight_k in enumerate(weight):
                    w = ss.diags(np.full(num_vars, np.sqrt(weight_k)))
                    w_ans = w @ child_sub[sub_model_hist_vars, child_num]
                    w_resid = model.addMVar(n_aux, vtype=gb.GRB.CONTINUOUS, lb=-w_ans, name="det_cell_query_resids")
                    new_obj_fxn_constrs = model.addConstr(w_resid == w @ two_d_vars[sub_model_hist_vars, child_num] - w_ans, name="det_cell_query_resids_def")
                    sub_model_comps.extend(new_obj_fxn_constrs.tolist())
                    total_aux[0] += n_aux
                    obj_fxn += (w_resid @ w_resid)
        return obj_fxn

    def addDPQueriesToModel(self, model, two_d_vars, obj_fxn, parent_mask, q_set_list=None, sub_model_index=None, **kwargs):
        """
        Adds the DP queries information to the objective function
        Inputs:
            model: gurobi model object
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            obj_fxn: grb expression used as obj fxn (dictionary of obj_fxns in multipass methods)
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model (applies to joint flattened histogram array)
            q_set_list: List (by histogram) over query sets which are to be added (so that they can be pre-filtered, for multipass).
                        Default is self.DPqueries
            sub_model_index: the integer index of the sub-model
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        ASSERT_TYPE(two_d_vars, gb.MVar)
        lb = 0 if self.nnls else -gb.GRB.INFINITY
        if q_set_list is None:
            q_set_list = self.DPqueries
        for ihist, dp_queries in enumerate(q_set_list):
            if dp_queries is None:
                continue
            for st_dpq in dp_queries:
                query: MultiHistQuery = st_dpq.query
                name = st_dpq.name
                matrix_rep, n_ans, row_mask = self.makeCsrQueryMatrix(ihist, st_dpq, parent_mask, sub_model_index)
                var_k_last = None

                for child_num, var_k, dp_ans_k in zip(st_dpq.indices, st_dpq.VarList, st_dpq.DPanswerList):
                    if var_k_last != var_k:
                        var_k_last = var_k
                        weight = ss.diags(np.full(n_ans, 1. / np.sqrt(var_k)))
                    x = two_d_vars[:, child_num]
                    obj_fxn = self.childDPQueryTerm(model, obj_fxn, matrix_rep, x, dp_ans_k[row_mask], weight, n_ans, name + f"_child_{child_num}", lb, **kwargs)
                #w = ss.diags(np.expand_dims(np.array(weight), 1).repeat(num_vars, 1).T.ravel())
                #x = gb.MVar(np.array(two_d_vars[sub_model_hist_vars, :].tolist()).ravel())
                #obj_fxn += (x @ w @ x - 2 * child_sub[sub_model_hist_vars, :].ravel() @ w @ x)

        return obj_fxn

    def findOptimalTolAndAddConstrs(self, model, two_d_vars, parent_mask, pass_num, sub_model_index, opt_tol_slack, sub_model_comps, total_aux):
        """
            In pass # i, the pass # i estimates for the DPqueries
                {dpq : self.dpq_order[dpq.name] == pass_num}
            are appended as constraints to model, with a variable L1 tolerance, like:
                |dpQuery @ currentHistogram - dpQueryCachedEstimate| <= tol
            We use this to solve for the smallest tol that achieves feasibility. Afterward the additional constraint is added:
                tol == tol.X + opt_tol_slack + model.constrResidual
            Thus, the final estimates in the main L2 solve will be equivalent to (and, after presolve, very likely identical to):
                |dpQuery @ currentHistogram - dpQueryCachedEstimate| <= tol.X + opt_tol_slack + model.constrResidual

        Inputs:
            :param model:       GRB model, to which constraints & vars are added
            :param two_d_vars:  2-d (parent_masked_vars_per_geo X #_child_geos) GRB MVar variable object
            :param parent_mask: np 1-d boolean array, True on indexes corresponding to cells w/ >0.0 parent estimates; these
                                indicate which child variables need to be explicitly represented in two_d_vars &  in DP
                                query-as-matrix column sets
            :param pass_num:     integer indicating index of current pass_num
            :param sub_model_index: an integer index of the sub-model
            :param opt_tol_slack: a float indicating extra additive slack
            :param sub_model_comps: a list of components (constraints and variables) of the sub-model; variables and constraints being added to the sub-model will be appended to this list
            :param total_aux: a list with one integer indicating the number of auxiliary variables, which is updated in place as auxiliary variables are added
        """
        import gurobipy as gb
        obj_fxn = 0
        var_name = f"dpq_pass#{pass_num}_multipassToleranceVar"
        tol = model.addVar(lb=0.0, vtype=gb.GRB.CONTINUOUS, name=var_name)
        obj_fxn += np.ones(1) @ gb.MVar([tol]) # Obj fxn is just the scalar bound on slack, tol

        for ihist, dp_queries in enumerate(self.DPqueries):
            for st_dpq in dp_queries:
                print(f"In {self.identifier}, In L2_Decomp: in pass num {pass_num} and sub-model {sub_model_index}, considering whether to add query {st_dpq.name} to opt_tol constrs")
                if pass_num in self.rev_constrain_to_order[st_dpq.name]:
                    print(f"In L2_Decomp: in pass num {pass_num} and sub-model {sub_model_index}, adding query {st_dpq.name} to opt_tol constrs")
                    csrQ, n_ans, _ = self.makeCsrQueryMatrix(ihist, st_dpq, parent_mask, sub_model_index=sub_model_index)
                    for sc_child_ind, child_num in enumerate(st_dpq.indices):
                        c1_name = f"dpq_varTol_pass#{pass_num}Constr_{st_dpq.name}_{child_num}_{sub_model_index}+"
                        c2_name = f"dpq_varTol_pass#{pass_num}Constr_{st_dpq.name}_{child_num}_{sub_model_index}-"
                        x = two_d_vars[:, child_num]
                        tol_vec = gb.MVar([tol for _ in range(n_ans)])  # n_ans-len 1-d np.array, to match csrQ @ x shape
                        constrs = model.addConstr( csrQ @ x - csrQ @ x.X <= tol_vec, name=c1_name)
                        sub_model_comps.extend(constrs.tolist())
                        constrs = model.addConstr(-csrQ @ x + csrQ @ x.X <= tol_vec, name=c2_name)
                        sub_model_comps.extend(constrs.tolist())

        initial_barConvTol = model.Params.BarConvTol
        initial_crossover = model.Params.Crossover
        initial_method = model.Params.Method

        model.Params.Crossover = 0        # Disable crossover; unneeded here, & sometimes generates very large solve times
        model.Params.Method = 2           # Disabling crossover requires non-concurrent solve approach; we use barrier-only, here
        model.Params.BarConvTol = self.bar_conv_tol_for_opt_tol_solve
        # Predicted number of variables is same number as in optimizationPassesLoop, plus one for the new tolerance variable:
        predicted_var_num = total_aux[0] + two_d_vars.shape[1] * len(self.sub_models[sub_model_index]) + pass_num + 1
        self.setObjAndSolve(model, obj_fxn, predicted_var_num=predicted_var_num)
        constr = model.addConstr(tol.X + model.ConstrResidual + opt_tol_slack == tol, f"tol_constr_pass_{pass_num}_sub_model_{sub_model_index}")

        # Restore original barConvTol, method, and crossover settings:
        model.Params.BarConvTol = initial_barConvTol
        model.Params.Method = initial_method
        model.Params.Crossover = initial_crossover

        # sub_model_comps is modified in place:
        sub_model_comps.append(constr)
        sub_model_comps.append(tol)

    def reformSolution(self, two_d_vars, parent_mask, child_floor=None):
        """
        Translate the variables solution back to the NoisyChild shape
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            n: int, length of first dimension of two_d_vars
            child_floor: numpy multi array, floor(child), needed for rounder, None here
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        Outputs:
            result: list of Numpy Multiarray, The optimized solution back in the form of list of arrays with histogram shapes (times number of children)
        """
        joint_sol = np.zeros((int(np.sum(self.hist_sizes)), self.childGeoLen))
        joint_sol[parent_mask, :] = self.final_solution
        if self.nnls:
            joint_sol[joint_sol < 0] = 0

        return self.splitHistograms(joint_sol)
