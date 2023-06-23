from typing import List, Iterable
import numpy as np

from time import time
from programs.optimization.geo_optimizer_decomp import GeoOptimizerDecomp
from das_constants import CC

from programs.optimization.optimizer import findNonzeroCols
from programs.optimization.geo_optimizers import ASSERT_TYPE


class DataIndependentNpassRoundDecomp(GeoOptimizerDecomp):
    """
        This class solves the integer rounding problem for geography imputation in a sequence of passes,
        where |Q * (nnls - round(nnls)) + Q * rounder_binaries)| is the obj fxn term for each Q in the current pass.
        Takes a non-negative continuous/fractional solution and finds a nearby nonnegative integer solution.

        Let the set of detailed cells that are summed over when defining the i^th term of the objective function be denoted by a[i].
        A sufficient condition to ensure the mixed integer programming problems formulated within this class are solvable in
        polynomial time is for there to exist at most two hierarchical sequences of subsets of {a[1], a[2], ...}, which is a property
        that is known as 2-laminar in the controlled rounding literature.

        Note that objective function for individual histogram cells is treated separately, as [1 - 2 * (nnls - round(nnls))] * binary

        This class also supports histogram decomposition. See docstring of GeoOptimizerDecomp for description of cases in which
        histogram decomposition improves runtime.

    """
    # variables used in run() function
    rounder = True
    parent_constraints_name = CC.PARENT_CONSTRAINTS_ROUND
    child_obj_term_wt = None
    model_name = CC.MULTIPASS_ROUNDER

    def __init__(self, *, child, acceptable_rounder_statuses, DPqueries, dpq_order, **kwargs):
        super().__init__(**kwargs)
        self.child = child
        self.acceptable_statuses = acceptable_rounder_statuses

        self.DPqueries = DPqueries
        self.dpq_order = dpq_order
        self.rev_dpq_order = self.reverseQueryOrdering(self.dpq_order)
        self.pass_nums = sorted(self.dpq_order.keys())
        self.penalty_constrs = {}  # References to constraints for later removal from model
        self.penalty_constrs_update = [] # References to constraints for later removal from model
        self.final_solution = np.array([])  # numpy array containing final estimate
        print(f"{self.model_name} rev_dpq_order in geocode {self.identifier}: {self.rev_dpq_order}")

    def optimizationPassesLoop(self, model, obj_fxn, two_d_vars, n_list, child_sub, parent_mask, parent_sub, child_floor):
        """
            --- Primary entry point for understanding optimization logic ---
            Multi pass optimization loop: optimize, add constraints to this optimization, optimize further etc.
        """
        self.final_solution = np.zeros(two_d_vars.shape)
        if self.getboolean("simplify_consistency_constrs", section=CC.GUROBI, default=True):
            # If the detailed query is in the objective function, the coarsest supporting query matrix component for the main histogram is simply the identity matrix.
            # Constrain each detailed cell to sum to the corresponding cells of the parent in this case:
            supporting_query_mat = None if CC.DETAILED in self.rev_dpq_order.keys() else self.findCoarsestSupportingQueryMat(parent_mask)
        else:
            supporting_query_mat = None

        if self.getboolean("constrain_main_vars_to_zero", section=CC.GUROBI, default=True) and supporting_query_mat is not None:
            for row_ind in range(supporting_query_mat.shape[0]):
                col_inds = findNonzeroCols(supporting_query_mat, row_ind)
                if len(col_inds) > 1:
                    two_d_vars[col_inds[1:], :].ub = 0
                    two_d_vars[col_inds[1:], :].lb = 0

        for sub_model_index, sub_model in enumerate(self.sub_models):
            # Create a list of a single integer that will track the total number of auxiliary variables in the sub-model, which is modified in place (ie: as a side effect) in addDPQueriesToModel():
            total_aux = [0]
            # Create a list for the constraints and variables in the sub-model, which is modified in place (ie: as a side effect) to add new sub-model components (variables/constraints)
            # in addMultipassPenalties() and addMultipassConstraints():
            sub_model_comps = []
            if self.use_parent_constraints:
                self.buildAndAddParentConstraints(model, parent_sub, two_d_vars, CC.PARENT_CONSTRAINTS_ROUND, supporting_query_mat, sub_model, sub_model_comps)

            if self.constraints is not None:
                self.addStackedConstraints(model, parent_mask, two_d_vars, child_floor=child_floor, sub_model_comps=sub_model_comps, sub_model_index=sub_model_index)
            for pass_num in self.pass_nums:
                print(f"Rounder dataInd multipass starting run for pass_num {pass_num}")
                # Add multipass penalties
                obj_fxn = self.buildL1PenaltyObjFxn(model, two_d_vars, child_sub, parent_mask, pass_num, sub_model_index, sub_model_comps, total_aux)
                # Total number of predicted variables are total number of aux variables plus the number of sub-model main variables in all child histograms:
                predicted_var_num = total_aux[0] + two_d_vars.shape[1] * len(sub_model)
                self.setObjAndSolve(model, obj_fxn, predicted_var_num=predicted_var_num)
                # If not first pass, set up constraints to previous passes
                if pass_num != self.pass_nums[-1]:
                    q_set_list = self.filterQueries(lambda rqname: self.rev_dpq_order[rqname] == self.pass_nums[pass_num], self.DPqueries, self.rev_dpq_order)
                    self.addDPQueriesToModel(model, two_d_vars, obj_fxn, parent_mask, pass_num, child_sub, mode="constraints", q_set_list=q_set_list, sub_model_comps=sub_model_comps)
                assert model.Status in self.acceptable_statuses, f"In DataIndependentNpassRoundDecomp: for pass_num {pass_num} and sub_model_index {sub_model_index} in node w/ id {self.identifier} returned unacceptable status: {model.Status}. Acceptable statuses: {self.acceptable_statuses}"

            self.final_solution[sub_model, :] = two_d_vars.X[sub_model, :]
            if sub_model_index != (len(self.sub_models) - 1):
                model.remove(sub_model_comps)

    def getNZSubsets(self):
        """
           Calculate the child_floor, child_leftover, and parent_diff, i.e. the part of parent histograms which is leftover of what floored
           children sum to
           Outputs:
               child_floor: numpy multi array, floor(child)
               child_sub: child_leftover: numpy multi array, child-child_floor, subset to parent_mask (where the possible NZ are)
               n_list: how many possible non zeros (=> gurobi variables) in each histogram (and each children)
               parent_diff: numpy multi array, parent - sum of child_floor across geographies
               parent_mask: where the possible non-zeros are
               parent_diff_sub: parent_diff subset to the parent_mask
        """
        # Flatten each child histogram and stack them together into a joint array
        child = np.vstack([c.reshape(np.prod(ps), self.childGeoLen) for c, ps in zip(self.child, self.parent_shape)])
        child_floor = np.floor(child)
        child_leftover = child - child_floor

        if self.parent[0] is not None:
            # Flatten each parent histogram and stack them together into a joint array, and substract the sum of floored children
            parent_diff = np.hstack([parent.reshape(np.prod(ps)) for parent, ps in zip(self.parent, self.parent_shape)]) - np.sum(child_floor, len(child.shape) - 1)
            # Reshape back, since it's assumed it's a list in findDesiredIndices
            parent_diff = [parent_diff[hstart:hend].reshape(shape) for hstart, hend, shape in zip(self.hstarts, self.hends, self.parent_shape)]
        else:
            parent_diff = [None] * len(self.parent)

        n_list, parent_mask, parent_diff_sub = self.findDesiredIndices(self.parent_shape, arrays=parent_diff)

        child_sub = child_leftover[parent_mask, :]

        return child_floor, child_sub, n_list, parent_diff, parent_mask, parent_diff_sub

    def buildMainVars(self, model, n, name="main_cells"):
        """
        Builds the main variables for the models
        Inputs:
            model: gurobi model objects
            n: int, length of first dimension of variables
            name: str, give the variables a name
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        # First index is cell (among non-zero), second index is child
        two_d_vars: gb.MVar = model.addMVar((int(n), int(self.childGeoLen)), vtype=gb.GRB.BINARY, lb=0.0, ub=1.0, name=name)
        return two_d_vars

    @staticmethod
    def buildCellwiseObjFxn(two_d_vars, child_sub, sub_model_vars=None):
        """
        Adds the main objective function for the rounder
        Inputs:
            obj_fxn: gurobi expression for the objective function
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            child_sub: two-d numpy multi array, child-child_floor subset to mask
            n_list: list of number of gurobi vars in each histogram
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.MVar)

        if sub_model_vars is None:
            sub_model_vars = slice(0, two_d_vars.shape[0])

        obj_fxn = 0  # gb.MLinExpr()
        for child_num in range(two_d_vars.shape[1]):
            # |var - leftover| = {leftover if var=0; 1-leftover if var=1} = leftover*(1-var) + (1-leftover)*var =
            # = leftover + var - 2*leftover*var = leftover + (1-2*leftover)*var = (1-2*leftover)*var + const; const dropped
            obj_fxn += (1 - 2 * child_sub[sub_model_vars, child_num]) @ two_d_vars[sub_model_vars, child_num]
        return obj_fxn

    def buildL1PenaltyObjFxn(self, model, two_d_vars, child_sub, parent_mask, pass_num, sub_model_index, sub_model_comps, total_aux):
        """
            Build obj fxn for current pass, & adding corresponding DP queries
        """
        print(f"Received pass numbers: {self.pass_nums}")
        # Filter queries to those that are targeted in current pass
        q_set_list = self.filterQueries(lambda dpqname: pass_num in self.rev_dpq_order[dpqname], self.DPqueries, self.rev_dpq_order)
        obj_fxn = self.addDPQueriesToModel(model, two_d_vars, 0, parent_mask, pass_num, child_sub, mode="penalties", q_set_list=q_set_list, sub_model_index=sub_model_index, sub_model_comps=sub_model_comps, total_aux=total_aux)
        if CC.DETAILED in self.rev_dpq_order and self.rev_dpq_order[CC.DETAILED] == pass_num:
            obj_fxn += self.buildCellwiseObjFxn(two_d_vars, child_sub, self.sub_models[sub_model_index])
        return obj_fxn

    def addDPQueriesToModel(self, model, two_d_vars, obj_fxn, parent_mask, pass_num=0, child_sub=None, mode=None, q_set_list=None, sub_model_index=None, sub_model_comps=None, total_aux=None):
        """
            Appends DPQueries for current pass or constraints for preceding pass dpqs to obj fxn.
        """
        obj_fxn = 0
        for ihist, queries in enumerate(q_set_list):
            for st_dpq in queries:
                csrQ, n_ans, _ = self.makeCsrQueryMatrix(ihist, st_dpq, parent_mask, sub_model_index=sub_model_index)
                name = st_dpq.name + str(sub_model_index)
                for sc_child_ind, child_num in enumerate(st_dpq.indices):
                    x = two_d_vars[:, child_num]  # Rounder binary optimization variables
                    x_frac = child_sub[:, child_num]  # Fractional part of the NNLS solution
                    if len(x_frac) == 0:  # If L2 sol is integer for child_num, child_sub has len 0
                        continue
                    if mode == "constraints":
                        self.addMultipassConstraints(model, csrQ, x, x_frac, child_num, pass_num, name, sub_model_comps)
                    elif mode == "penalties":
                        obj_fxn += self.addMultipassPenalties(model, csrQ, x, x_frac, n_ans, child_num, pass_num, name, sub_model_comps)
                        # Note that total_aux is modified in place:
                        total_aux[0] += n_ans
                    else:
                        raise ValueError(f"Mode is '{mode}'. Mode should be `constraints' or 'penalties'")
        return obj_fxn

    def addMultipassPenalties(self, model, csrQ, x, x_frac, n_ans, child_num, pass_num, qname, sub_model_comps):
        import gurobipy as gb

        L1_penalty = model.addMVar(int(n_ans), vtype=gb.GRB.CONTINUOUS, lb=0.0, name=f"dpq_pass#{pass_num}_L1PenVar_{qname}_childNum{child_num}")
        # Note that sub_model_comps is modified in place:
        sub_model_comps.extend(L1_penalty.tolist())

        constr_base_name = f"dpq_pass#{pass_num}L1PenConstr_{qname}_{child_num}_"
        constrs = model.addConstr(csrQ @ x - np.around(csrQ @ x_frac) <= L1_penalty, name=constr_base_name + "+")
        sub_model_comps.extend(constrs.tolist())
        constrs = model.addConstr(-csrQ @ x + np.around(csrQ @ x_frac) <= L1_penalty, name=constr_base_name + "-")
        sub_model_comps.extend(constrs.tolist())
        return np.ones(n_ans) @ L1_penalty

    def addMultipassConstraints(self, model, csrQ, x, x_frac, child_num, pass_num, qname, sub_model_comps):
        constr_base_name = f"dpq_pass#{pass_num}MultipassConstr_{qname}_{child_num}_"
        constrs = model.addConstr(csrQ @ x - np.around(csrQ @ x.X) == 0., name=constr_base_name + "=")
        # Note that sub_model_comps is modified in place:
        sub_model_comps.extend(constrs.tolist())

    def reformSolution(self, two_d_vars, parent_mask: np.ndarray, child_floor=None):
        """
        Take the solution from the model and turn it back into the correctly shaped numpy multi array
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi MVar variable object
            n: int, length of first dimension of two_d_vars
            child_floor: numpy multi array, floor(child)
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        Outputs:
            result: list of Numpy Multiarray, The optimized solution back in the form of list of arrays with histogram shapes (times number of children)
        """
        joint_result = child_floor.reshape((int(np.sum(self.hist_sizes)), self.childGeoLen))
        if self.final_solution.shape[0] > 0:
            joint_result[parent_mask, :] = joint_result[parent_mask, :] + self.final_solution
        joint_result = joint_result.astype(int)

        return self.splitHistograms(joint_result)
