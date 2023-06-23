import pytest
import os
import tempfile
import numpy as np
import shutil
from programs.optimization.geo_optimizers import L2GeoOpt
from programs.optimization.optimizer import GeoOptimizer
from das_constants import CC


TOKENSERVER=os.environ['GRB_TOKENSERVER']
TOKENSERVER_PORT=os.environ['GRB_TOKENSERVER_PORT']

@pytest.fixture()
def gurobi_env():
    # gurobi_version = '900'
    # # gurobi_version = '810'
    # gurobi_path = os.path.expandvars(f'/mnt/apps5/gurobi{gurobi_version}/linux64/lib/python{sys.version_info.major}.{sys.version_info.minor}_utf32/')
    #
    # sys.path.insert(1, gurobi_path)

    import gurobipy as gb

    if not os.path.exists(CC.GUROBI_LIC_CREATE_FNAME):
        with tempfile.NamedTemporaryFile(suffix='.lic', mode='w') as tf:
            tf.write(f"TOKENSERVER={TOKENSERVER}\n")
            tf.write(f"PORT={TOKENSERVER_PORT}\n")
            tf.flush()
            shutil.copyfile(tf.name, CC.GUROBI_LIC_CREATE_FNAME)
    os.environ["GRB_LICENSE_FILE"] = CC.GUROBI_LIC_CREATE_FNAME

    env = gb.Env.OtherEnv('optimizer_unit_tests.log', 'Census', 'DAS', 0, '')
    return env


class TestL2GeoOpt:

    def test_buildObjFxn(self, gurobi_env):
        import gurobipy as gb
        m = gb.Model("model", env=gurobi_env)
        # 3 variables, 2 children
        two_d_vars: gb.MVar = m.addMVar((3, 2), vtype=gb.GRB.CONTINUOUS, lb=0)
        # 2 variables in first histogram, 1 in second
        n_list = [2, 1]
        child_sub = np.arange(6).reshape(3, 2)
        # First child weight is 1, second -- 2, for the first histogram. All zeros for the second.
        obj_fxn_weight_list = [(1, 0), (2, 0)]
        obj_fxn = L2GeoOpt.buildObjFxn(two_d_vars, n_list, child_sub, obj_fxn_weight_list)
        m.setObjective(obj_fxn)
        m.update()
        # we have w* (x - child_sub)^2 terms which come out to be
        # x @ w @ x - 2 * child_sub @ w @ x
        #
        # The standard variable names are C0 (1st var 1st child), C1 (1st var 2nd child), C2 (2nd var 1st child) etc.
        # The square terms should be C0^2 and C2^2  with the weight of 1, C1^2 and C3^2 with the weight of 2
        #
        # For the linear terms
        # child_sub is
        # array([[0, 1],
        #        [2, 3],
        #        [4, 5]])
        # for the first child first histogram it's 0 and 2, for the 2nd child it's 1 and 3
        # So there'll be
        # (listed as)     -2, cs,  w, var
        #                 -2 * 2 * 1 * C2 = -4.0 C2 for the first child
        # and             -2 * 1 * 2 * C1
        #                 -2 * 3 * 2 * C3 = -4.0 C1 - 12.0 C3 for the second child

        # This is not the most elegant, it's better to compare matrices of the MQuadExpr
        # But this makes it clear for illustration
        # If this test breaks, either change it to compare matrices, or see what the string is, make sure it has the same terms and change it accordingly
        assert str(m.getObjective()) == '<gurobi.QuadExpr: -4.0 C1 + -4.0 C2 + -12.0 C3 + [ C0 ^ 2 + 2.0 C1 ^ 2 + C2 ^ 2 + 2.0 C3 ^ 2 ]>'

        # May add more test examples with different histogram lengths and better matrix checks


class TestGeoOpt:

    @pytest.mark.parametrize("main_n, rounder, child", [
        (1, False, None),
        (2, False, None),
        (2, True, np.array([[20.5, 46.5, 47.5, 21.5, 22.,  12.],
                            [26.5, 52.5, 53.5, 26.5, 28.,  18.],
                            [  0.,   0.,   0.,   5.,  0.,   0.]])),
        (2, True, np.array([[20.5, 46.5, 47.5, 23.5, 22., 12.],
                            [26.5, 52.5, 53.5, 29.5, 28., 18.],
                            [   0.,  0.,    0.,   0., 0.,  0.]])),
        (3, False, None),
        (3, True, np.array([[9.16666667, 26.83333333, 27.83333333, 12.16666667, 10.66666667,  4.],
                           [15.16666667, 32.83333333, 33.83333333, 18.16666667, 16.66666667, 10.],
                           [21.16666667, 38.83333333, 39.83333333, 24.16666667, 22.66666667, 16.]]))
    ])
    def test_addGroupedChildTotalConstraint(self, gurobi_env, main_n, rounder, child):
        import gurobipy as gb
        m = gb.Model("model", env=gurobi_env)
        # 3 variables, 6 children
        nvar = 3
        nchild = 6
        if rounder:
            child_floor = np.floor(child)
            child_leftover = child - child_floor
            two_d_vars: gb.MVar = m.addMVar((main_n, nchild), vtype=gb.GRB.BINARY, lb=0, ub=1)
        else:
            child_floor = None
            child_leftover = None
            two_d_vars: gb.MVar = m.addMVar((main_n, nchild), vtype=gb.GRB.CONTINUOUS, lb=0)

        # Let's group children 0 and 3, children 1 and 2, and leave 4 and 5 in a group alone each
        # (e.g. children 0 and 3 are AIAN/non-AIAN for state0, 1 and 2 of state1, and 4 and 5 are state2 and state4 which don't have AIAN)
        # groups also have totals, that are to be enforced, attached
        child_groups = (
            ((0, 3), 100),
            ((1, 2), 200),
            ((4,), 50),
            ((5,), 30),
        )
        multiindices = [
            sorted(np.ravel_multi_index(i, (nvar, nchild)) for i in ((v, c) for v in range(nvar)[:main_n] for c in child_group[0])) for child_group in child_groups
        ]
        #parent_mask = np.ones((nvar), dtype=bool)
        GeoOptimizer.addGroupedChildTotalConstraint(m, nvar, two_d_vars, child_groups, main_n=main_n, rounder=rounder, child_floor=child_floor)
        m.update()
        with tempfile.NamedTemporaryFile(suffix='.lp', mode='w') as tf:
            # Output model into LP file
            m.write(tf.name)
            tf.flush()
            # Read the LP file back to make sure the constraints are there
            with open(tf.name, 'r') as tfr:
                constr_started = False
                lines = []
                for l in tfr:
                    if "Bounds" in l:
                        break
                    if constr_started:
                        lines.append(l.strip())
                    if "Subject To" in l:
                        constr_started = True
        # Note: in this examples the lines are short enough that they fit on one line. The test has to be changed to parsing the lines more thoroughly
        # if the example changes to one where lines are broken in the .lp output
        lines = sorted(lines)
        assert len(lines) == len(child_groups)

        # Check that the text lines for constraints in LP file a correct
        for group_num, (child_group, indices) in enumerate(zip(child_groups, multiindices)):
            lhs, rhs = lines[group_num].split(' = ')
            rhs = float(rhs)
            assert lhs == f"State_total#{group_num}: " + " + ".join(f"C{i}" for i in indices)
            rhs_from_invariant = child_group[1]
            if rounder:
                rhs_from_invariant -= child_floor[:, np.array(child_group[0])].sum()
            assert rhs == rhs_from_invariant

        # Set objective and optimize model
        t = gb.MVar(np.array(two_d_vars.tolist()).ravel())
        b = np.arange(main_n * nchild) if not rounder else child_leftover[:main_n, :].ravel()
        m.setObjective(t @ t - 2 * b @ t)
        m.update()
        m.optimize()

        # Get answer and add child_floor if it's rounder
        ans = np.zeros((nvar, nchild))
        ans[:main_n, :] = t.X.reshape((main_n, nchild))
        if child_floor is not None:
            ans += child_floor

        # Check that the constraints hold for the total solution
        for (ch_indices, total), indices in zip(child_groups, multiindices):
            assert abs(np.sum(ans[:, np.array(ch_indices)]) - total) < 1e-7
