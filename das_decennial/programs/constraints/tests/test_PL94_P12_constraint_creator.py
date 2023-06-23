import os
import sys
import numpy as np
import pytest
sys.path.append(os.path.dirname(__file__))
import programs.constraints.tests.constraint_test_util as ctu
from das_constants import CC

AGECAT_VA_CUT = 4

schemaname = CC.DAS_PL94_P12


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_voting_age(data_answers_name):
    cons_name = 'voting_age'

    ## Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    # Check sign and right hand side, in constraint and from query matrix applied to the data
    assert c.sign == "="
    assert c.rhs[0] == data_answers[cons_name]
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Check Kronecker factors
    # The values below AGECAT_VA_CUT are non-voting, selecting the other ones
    ctu.assertKronFactorMiltiple(c, CC.ATTR_AGECAT, range(AGECAT_VA_CUT, ctu.dimSch(CC.ATTR_AGECAT, schema)), schema    )

    # Selecting everyone on other axes
    for dimname in (CC.ATTR_HHGQ, CC.ATTR_SEX, CC.ATTR_HISP, CC.ATTR_CENRACE):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_nurse_nva_0(data_answers_name):
    cons_name = 'nurse_nva_0'

    ## Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    # Check sign and right hand side which is zero (in constraint) since it's a structural zero.
    assert c.sign == "="
    assert c.rhs[0] == 0
    # If constraint query matrix is applied to the data it doesn't have to be zero
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Check Kronecker factors
    # Selecting only nursing homes (index=3)
    ctu.assertKronFactorSingle(c, CC.ATTR_HHGQ, 3, schema)

    # Selecting only non-voting age (categories up to AGECAT_VA_CUT)
    ctu.assertKronFactorMiltiple(c, CC.ATTR_AGECAT, range(AGECAT_VA_CUT), schema)

    # Selecting everyone on other axes
    for dimname in (CC.ATTR_SEX, CC.ATTR_HISP, CC.ATTR_CENRACE):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
@pytest.mark.parametrize("cons_name", ['hhgq_va_ub', 'hhgq_va_lb'])
def test_hhgq_va(data_answers_name, cons_name):
    ## Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    # Check sign and right hand side
    assert c.sign == "le" if cons_name == 'hhgq_va_ub' else "ge"
    assert np.array_equal(c.rhs, data_answers[cons_name])

    if c.sign == "le":
        assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) <= c.rhs)
    else:
        assert np.all(ctu.lhsFromMatrix(c, data_answers["hist"][0]) >= c.rhs)

    # Check Kronecker factors
    # Each column of the HHGQ matrix selects that GQ type (so only one True value -- on the diagonal)
    ctu.assertKronFactorDiagonal(c, CC.ATTR_HHGQ, schema)
    # For non-voting, select first AGECAT_VA_CUT categories, for voting select the other categories
    ctu.assertKronFactor(c, CC.ATTR_AGECAT, np.array([
                              [True] * AGECAT_VA_CUT + [False] * (ctu.dimSch(CC.ATTR_AGECAT, schema) - AGECAT_VA_CUT),
                              [False] * AGECAT_VA_CUT + [True] * (ctu.dimSch(CC.ATTR_AGECAT, schema) - AGECAT_VA_CUT)
                          ]), schema)

    # Selecting everyone on all other axes
    for dimname in (CC.ATTR_SEX, CC.ATTR_HISP, CC.ATTR_CENRACE):
        ctu.assertKronFactorSelectAll(c, dimname, schema)
