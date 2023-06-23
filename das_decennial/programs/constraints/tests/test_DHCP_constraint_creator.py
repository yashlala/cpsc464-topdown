import os
import sys
import numpy as np
import pytest
sys.path.append(os.path.dirname(__file__))
import programs.constraints.tests.constraint_test_util as ctu
from das_constants import CC

NUM_HH_CATS = 18

schemaname = CC.SCHEMA_DHCP

data_to_parametrize = ['data1', 'data_1gqtype', 'data2']


@pytest.mark.parametrize('cons_name', ['relgq0_lt15', 'relgq1_lt15', 'relgq2_lt15', 'relgq2_lt15', 'relgq3_lt15', 'relgq4_lt15', 'relgq5_lt15',
                                       'relgq6_gt89', 'relgq7_gt89', 'relgq8_gt89', 'relgq10_lt30', 'relgq11_gt74', 'relgq12_lt30',
                                       'relgq13_lt15_gt89', 'relgq16_gt20', 'relgq18_lt15', 'relgq19_lt15', 'relgq20_lt15', 'relgq21_lt15',
                                       'relgq22_lt15', 'relgq23_lt17_gt65', 'relgq24_gt25', 'relgq25_gt25', 'relgq26_gt25', 'relgq27_lt20',
                                       'relgq31_lt17_gt65', 'relgq32_lt3_gt30', 'relgq33_lt16_gt65', 'relgq34_lt17_gt65', 'relgq35_lt17_gt65',
                                       'relgq37_lt16','relgq38_lt16', 'relgq39_lt16_gt75', 'relgq40_lt16_gt75'])
@pytest.mark.parametrize('data_answers_name', data_to_parametrize)
def test_struct_zero(cons_name, data_answers_name):
    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname),
                                                                 cons_name)
    assert c.sign == '='
    assert c.rhs[0] == 0
    assert ctu.lhsFromMatrix(c, data_answers['hist'][0])[0] == data_answers[cons_name]


@pytest.mark.parametrize("tot_inv", [True, False])
@pytest.mark.parametrize("data_answers_name", data_to_parametrize)
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
    dim_hhgq = ctu.dimSch(CC.ATTR_RELGQ, schema)
    dim_gq = dim_hhgq - NUM_HH_CATS

    kf_hhgq = c.query.queries[0].kronFactors()[ctu.indSch(CC.ATTR_RELGQ, schema)]
    assert kf_hhgq.shape == (dim_gq + 1, dim_hhgq)
    # First row is selecting first NUM_HH_CATS, and dropping all the GQs, so first NUM_HH_CATS values True, then False
    assert np.array_equal(kf_hhgq[0, :], np.array([True] * NUM_HH_CATS + [False] * dim_gq))
    # The rest rows drop first NUM_HH_CATS values
    assert np.array_equal(kf_hhgq[1:, :NUM_HH_CATS],  np.broadcast_to(np.array([False]), (dim_gq, NUM_HH_CATS)))
    # and select only the corresponding GQ type, leaving a diagonal of True in lower right corner of the matrix
    assert np.array_equal(kf_hhgq[1:, NUM_HH_CATS:], np.diag([True]*dim_gq))

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_RELGQ, ), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", data_to_parametrize)
def test_householder_ub(data_answers_name):
    cons_name = "householder_ub"
    inv_names = list(ctu.getAllImplementedInvariantNames(schemaname))
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_name)

    # Check right hand side
    assert np.all(c.rhs == data_answers[cons_name])

    # Check left hand side
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) == data_answers[cons_name + "_lhs"])

    # Check sign
    assert c.sign == "le"
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) <= c.rhs)

    dim_hhgq = ctu.dimSch(CC.ATTR_RELGQ, schema)
    kf_hhgq = c.query.queries[0].kronFactors()[ctu.indSch(CC.ATTR_RELGQ, schema)]
    assert kf_hhgq.shape == (1, dim_hhgq)

    # True for 0,1 -- householder living alone and householder not living alone
    assert np.array_equal(kf_hhgq[0, :], np.array([True] * 2 + [False] * (dim_hhgq - 2)))

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_RELGQ,), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", data_to_parametrize)
def test_partners_ub(data_answers_name):
    cons_name = "spousesUnmarriedPartners_ub"
    inv_names = list(ctu.getAllImplementedInvariantNames(schemaname))
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_name)

    # Check right hand side
    assert np.all(c.rhs == data_answers[cons_name])

    # Check left hand side
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) == data_answers[cons_name + "_lhs"])

    # Check sign
    assert c.sign == "le"
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) <= c.rhs)

    dim_hhgq = ctu.dimSch(CC.ATTR_RELGQ, schema)
    kf_hhgq = c.query.queries[0].kronFactors()[ctu.indSch(CC.ATTR_RELGQ, schema)]
    assert kf_hhgq.shape == (1, dim_hhgq)

    # True for 2, 3, 4, 5 -- all kinds of spouses and unmarried partners
    assert np.array_equal(kf_hhgq[0, :], np.array([False] * 2 + [True] * 4 + [False] * (dim_hhgq - 6)))

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_RELGQ,), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", data_to_parametrize)
def test_over100(data_answers_name):
    cons_name = "people100Plus_ub"
    inv_names = list(ctu.getAllImplementedInvariantNames(schemaname))
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_name)

    # Check right hand side
    assert np.all(c.rhs == data_answers[cons_name])

    # Check left hand side
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) == data_answers[cons_name + "_lhs"])

    # Check sign
    assert c.sign == "le"
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) <= c.rhs)

    # People in households (REL part, up to 18)
    ctu.assertKronFactor(c, CC.ATTR_RELGQ, np.array([[True] * 18 + [False] * 24]), schema)
    # People over 100 years old
    ctu.assertKronFactor(c, CC.ATTR_AGE, np.array([[False] * 100 + [True] * 16]), schema)

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_RELGQ, CC.ATTR_AGE), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("cons_name_rel_index", [("parents_ub", 10), ("parentInLaws_ub", 12)])
@pytest.mark.parametrize("data_answers_name", data_to_parametrize)
def test_parents_ub(cons_name_rel_index, data_answers_name):
    cons_name, rel_index = cons_name_rel_index
    inv_names = list(ctu.getAllImplementedInvariantNames(schemaname))
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_name)

    # Check right hand side
    assert np.all(c.rhs == data_answers[cons_name])

    # Check left hand side
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) == data_answers[cons_name + "_lhs"])

    # Check sign
    assert c.sign == "le"
    assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) <= c.rhs)

    # Parents are 10, parent-in-law are 12
    ctu.assertKronFactor(c, CC.ATTR_RELGQ, np.array([[False] * rel_index + [True] + [False] * (42-rel_index-1)]), schema)

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((CC.ATTR_RELGQ, ), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)
