import os
import sys
import numpy as np
import pytest
sys.path.append(os.path.dirname(__file__))
import programs.constraints.tests.constraint_test_util as ctu
from das_constants import CC

SIZE = CC.ATTR_HHSIZE
HHAGE = CC.ATTR_HHAGE
HHSEX = CC.ATTR_HHSEX
HHTYPE = CC.ATTR_HHTYPE #_DHCH
MULTI = CC.ATTR_HHMULTI
ELDERLY = CC.ATTR_HHELDERLY


@pytest.mark.parametrize("schemaname", [CC.DAS_Household2010, CC.DAS_TenUnit2010, CC.SCHEMA_HOUSEHOLD2010_TENVACS])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_no_vacant(data_answers_name, schemaname):
    cons_name = 'no_vacant'

    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    assert c.sign == "="
    assert c.rhs[0] == 0
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Select only size 0
    ctu.assertKronFactor(c, SIZE, np.array([[True] + [False] * 7]), schema)

    # Everyone on all other axes
    for dimname in ctu.excludeAx((SIZE,), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("schemaname", [CC.DAS_Household2010, CC.DAS_TenUnit2010, CC.SCHEMA_HOUSEHOLD2010_TENVACS])
@pytest.mark.parametrize("data_answers_name", ["data1"])
def test_living_alone(data_answers_name, schemaname):
    cons_name = 'living_alone'

    # Get the constraint, data with answers and schema
    cs, data_answers, schema = ctu.getConstraintsDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), (cons_name,))

    # Check size 1, and all rhs being zero
    for c in cs.values():
        assert c.sign == "="
        assert c.rhs[0] == 0

        # Select only size 1
        ctu.assertKronFactor(c, SIZE, np.array([[False] + [True] + [False] * 6]), schema)

    name = "living_alone_gt1"
    assert ctu.lhsFromMatrix(cs[name], data_answers["hist"][0])[0] == data_answers[name]
    # Select hhtypes implying > 1 person
    ans = [True] * 24
    ans[18] = False
    ctu.assertKronFactor(cs[name], HHTYPE, np.array([ans]), schema)
    # Everyone on all other axes
    for dimname in ctu.excludeAx((SIZE, HHTYPE), schema):
        ctu.assertKronFactorSelectAll(cs[name], dimname, schema)

    name = "living_alone_multi"
    assert ctu.lhsFromMatrix(cs[name], data_answers["hist"][0])[0] == data_answers[name]
    # Select multi=1
    ctu.assertKronFactor(cs[name], MULTI, np.array([[False, True]]), schema)
    # Everyone on all other axes
    for dimname in ctu.excludeAx((SIZE, MULTI), schema):
        ctu.assertKronFactorSelectAll(cs[name], dimname, schema)


    for eld in [0,1,2,3]:
        name = f"living_alone_eld{eld}"
        assert ctu.lhsFromMatrix(cs[name], data_answers["hist"][0])[0] == data_answers[name]
        # Select multi=0
        ctu.assertKronFactor(cs[name], MULTI, np.array([[True, False]]), schema)
        # Select hhtypes implying 1 person
        ans = [False] * 24
        ans[18] = True
        ctu.assertKronFactor(cs[name], HHTYPE, np.array([ans]), schema)
        # select elderly not "eld"
        ctu.assertKronFactor(cs[name], ELDERLY, np.array([[True] * eld + [False] + [True] * (3 - eld)]), schema)
        # select age under 60, 60-65, 65-75 or over 75
        ctu.assertKronFactor(cs[name], HHAGE, np.array([[not(bool(eld))] * 5 + list(np.array([1,2,3]) == eld) + [eld==3]]), schema)
        # Everyone on all other axes
        for dimname in ctu.excludeAx((SIZE, HHTYPE, MULTI, HHAGE, ELDERLY), schema):
            ctu.assertKronFactorSelectAll(cs[name], dimname, schema)
