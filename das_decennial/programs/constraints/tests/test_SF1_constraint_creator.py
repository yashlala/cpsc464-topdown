import os
import sys
import numpy as np
import pytest
sys.path.append(os.path.dirname(__file__))
import programs.constraints.tests.constraint_test_util as ctu
from das_constants import CC


NUM_HH_CATS = 15

schemaname = CC.DAS_SF1


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_no_parents_under_30(data_answers_name):
    cons_name = 'no_parents_under_30'

    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    assert c.sign == "="
    assert c.rhs[0] == 0
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Selecting relatives "6" (parents) and "8" (parents-in-law)
    ctu.assertKronFactor(c, CC.ATTR_REL, np.array([[False] * 6 + [True, False, True] + [False] * 34]), schema)
    # Selecting people under 30 (First 30 True, the rest is False)
    ctu.assertKronFactor(c, CC.ATTR_AGE, np.array([[True] * 30 + [False] * 86]), schema)

    # Everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_REL, CC.ATTR_AGE), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_no_kids_over_89(data_answers_name):
    cons_name = 'no_kids_over_89'

    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    assert c.sign == "="
    assert c.rhs[0] == 0
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Selecting relatives
    # 2 'Biological Son/Daughter',
    # 3 'Adopted Son/Daughter',
    # 4 'Stepson/Stepdaughter',
    # 9 'Son/Daughter-in-law',
    ctu.assertKronFactor(c, CC.ATTR_REL, np.array([[False, False, True, True, True] + [False] * 4 + [True] + [False] * 33]), schema)

    # # This one also includes 7 for "Grandchild"
    # ctu.assertKronFactor(c, CC.ATTR_HHGQ, np.array([[False, False, True, True, True] + [False, False, True, False, True] + [False] * 33]), schema)

    # Selecting people over 89 (First 90 False, the rest is True)
    ctu.assertKronFactor(c, CC.ATTR_AGE, np.array([[False] * 90 + [True] * 26]), schema)
    # Everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_REL, CC.ATTR_AGE), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_no_refhh_under_15(data_answers_name):
    cons_name = 'no_refhh_under_15'

    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    assert c.sign == "="
    assert c.rhs[0] == 0
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Selecting relatives
    # 'Householder (0)', 'Husband/Wife (1)', 'Father/Mother (6)', 'Parent-in-law (8)', 'Son/Daughter-in-law (9)', 'Housemate, Roommate (12)',
    # 'Unmarried Partner (13)'
    age_cut = 15
    ans = np.array([[False] * ctu.dimSch(CC.ATTR_REL, schema)])
    ans[0, [0, 1, 6, 8, 9, 12, 13]] = True
    ctu.assertKronFactor(c, CC.ATTR_REL, ans, schema)
    # Selecting people under age_cut (First age_cut True, the rest are False)
    ctu.assertKronFactor(c, CC.ATTR_AGE, np.array([[True] * age_cut + [False] * (ctu.dimSch(CC.ATTR_AGE, schema) - age_cut)]), schema)

    # Everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_REL, CC.ATTR_AGE), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("tot_inv", [True, False])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
@pytest.mark.parametrize("cons_name", ['hhgq_total_ub', 'hhgq_total_lb'])
def test_hhgq_total(data_answers_name, tot_inv, cons_name):

    # Create invariant names, remove 'tot' if checking without total
    inv_names = list(ctu.getAllImplementedInvariantNames(schemaname))
    if not tot_inv:
        inv_names.remove('tot')

    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_name)

    # Check sign, <= for upper bound, >= for lower
    assert c.sign == "le" if cons_name == 'hhgq_total_ub' else "ge"

    # Check right hand side
    if tot_inv:
        assert np.array_equal(c.rhs, data_answers[cons_name])
    else:
        assert np.array_equal(c.rhs, data_answers[cons_name + "_no_tot"])

    # Check right hand side from matrix application to the data
    if c.sign == "le":
        assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) <= c.rhs)
    else:
        assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) >= c.rhs)

    # Should get 1 + (schDim(HHGQ) - NUM_HH_CATS) rows x schDim(HHGQ) columns
    dim_hhgq = ctu.dimSch(CC.ATTR_REL, schema)
    dim_gq = dim_hhgq - NUM_HH_CATS

    kf_hhgq = c.query.queries[0].kronFactors()[ctu.indSch(CC.ATTR_REL, schema)]
    assert kf_hhgq.shape == (dim_gq + 1, dim_hhgq)
    # First row is selecting first NUM_HH_CATS, and dropping all the GQs, so first NUM_HH_CATS values True, then False
    assert np.array_equal(kf_hhgq[0, :], np.array([True] * NUM_HH_CATS + [False] * dim_gq))
    # The rest rows drop first NUM_HH_CATS values
    assert np.array_equal(kf_hhgq[1:, :NUM_HH_CATS],  np.broadcast_to(np.array([False]), (dim_gq, NUM_HH_CATS)))
    # and select only the corresponding GQ type, leaving a diagonal of True in lower right corner of the matrix
    assert np.array_equal(kf_hhgq[1:, NUM_HH_CATS:], np.diag([True]*dim_gq))

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_REL, ), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)
