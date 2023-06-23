import os
import sys
import pytest
sys.path.append(os.path.dirname(__file__))
import programs.constraints.tests.constraint_test_util as ctu
from das_constants import CC

schemaname = CC.DAS_PL94


@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_nurse_nva_0(data_answers_name):
    cons_name = 'nurse_nva_0'


    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    # Check sign and right hand side which is zero (in constraint) since it's a structural zero.
    assert c.sign == "="
    assert c.rhs[0] == 0
    # If constraint query matrix is applied to the data it doesn't have to be zero
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Check Kronecker factors
    # Selecting only nursing homes (index=3)
    ctu.assertKronFactorSingle(c, CC.ATTR_HHGQ, 3, schema)

    # Selecting only non-voting age (index=0)
    ctu.assertKronFactorSingle(c, CC.ATTR_VOTING, 0, schema)

    # Selecting everyone on other axes
    for dimname in ctu.excludeAx((CC.ATTR_HHGQ, CC.ATTR_VOTING), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)
