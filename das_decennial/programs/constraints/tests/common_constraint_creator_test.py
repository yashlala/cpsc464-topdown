import numpy as np
import pytest
from operator import le, ge, eq
from das_constants import CC
import programs.constraints.tests.constraint_test_util as ctu

compop = {
    'le': le,
    'ge': ge,
    '=': eq
}


@pytest.mark.parametrize("schemaname", [CC.DAS_PL94, CC.DAS_PL94_P12, CC.DAS_PL94_CVAP, CC.DAS_1940, CC.DAS_SF1, CC.DAS_Household2010, CC.DAS_DHCP_HHGQ, CC.SCHEMA_DHCP, CC.SCHEMA_HOUSEHOLD2010_TENVACS, CC.SCHEMA_DHCH])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_create(data_answers_name, schemaname):
    constraint_names = ctu.getAllImplementedConstraintNames(schemaname)
    ctu.getConstraintsDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname),
                                                     constraint_names)[0]

    # Check number of constraints created (equal to number in the list above)
    # (in Household2010 we have multiple constraints per creating function, so this is no longer true), commenting
    # assert len(cons.values()) == len(constraint_names)

    # The test still makes sure that creation of all constraints goes through

@pytest.mark.parametrize("schemaname", [CC.DAS_PL94, CC.DAS_PL94_P12, CC.DAS_PL94_CVAP, CC.DAS_1940, CC.DAS_SF1, CC.DAS_Household2010, CC.DAS_DHCP_HHGQ, CC.SCHEMA_DHCP, CC.SCHEMA_HOUSEHOLD2010_TENVACS, CC.SCHEMA_DHCH])
@pytest.mark.parametrize("excl_inv, constraint, err_msg", [
    # excl_inv - invariant to exclude from the list
    # constraint - which constraint to attempt to calculate
    # err_msg - which error message should be thrown
    ("tot", "hhgq_va_ub", "To calculate hhgq_va_ub/lb constraints, the invariant 'tot' should be present as well"),
    ("gqhh_tot", "hhgq_total_ub", "To calculate hhgq_total_ub/lb constraints, the invariant 'gqhh_tot' should be present as well"),
    ("gqhh_tot", "hhgq_total_lb", "To calculate hhgq_total_ub/lb constraints, the invariant 'gqhh_tot' should be present as well"),
])
def test_missing_invariant_err(excl_inv, constraint, err_msg, schemaname):

    if constraint not in ctu.getAllImplementedConstraintNames(schemaname):
        return
    with pytest.raises(ValueError) as err:
        # Exclude invariant from the list and try to calculate constraints
        ctu.getConstraintDataAnswersSchema(schemaname, "data1", list(x for x in ctu.getAllImplementedInvariantNames(schemaname) if x != excl_inv), constraint)
    assert err_msg in str(err.value)
    # assert err_msg in caplog.text

@pytest.mark.parametrize("schemaname", [CC.DAS_PL94, CC.DAS_PL94_P12, CC.DAS_PL94_CVAP, CC.DAS_1940, CC.DAS_SF1, CC.DAS_Household2010, CC.DAS_DHCP_HHGQ, CC.SCHEMA_DHCP, CC.SCHEMA_HOUSEHOLD2010_TENVACS, CC.SCHEMA_DHCH])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_total(data_answers_name, schemaname):
    cons_name = 'total'
    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    # Check sign and right hand side

    # Probably Household2010 tests will have to be separated back into their own file
    assert c.sign =="le" if schemaname in [CC.DAS_Household2010, CC.SCHEMA_HOUSEHOLD2010_TENVACS] else "="

    assert compop[c.sign](data_answers[cons_name], c.rhs[0])

    # Check Kronecker factors
    # Everything True, since selecting every person in all axes
    for dimname in schema.dimnames:
        ctu.assertKronFactorSelectAll(c, dimname, schema)

    # Check again that everything is True
    assert np.array_equal(c.query.queries[0].matrixRep().toarray(), np.broadcast_to(np.array([True]), (1, np.prod(schema.shape))))

    # The number of people (rows) we have in the table d in the fixture above
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]


@pytest.mark.parametrize("schemaname", [CC.DAS_PL94, CC.DAS_PL94_CVAP])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
def test_voting_age(data_answers_name, schemaname):
    cons_name = 'voting_age'

    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    # Check sign and right hand side, in constraint and from query matrix applied to the data
    assert c.sign == "="
    assert c.rhs[0] == data_answers[cons_name]
    assert ctu.lhsFromMatrix(c, data_answers["hist"][0])[0] == data_answers[cons_name]

    # Check Kronecker factors
    # Selecting voting age, excluding non-voting (index for "voting" is 1)
    ctu.assertKronFactorSingle(c, CC.ATTR_VOTING, 1, schema)

    # Selecting everyone on other axes
    for dimname in ctu.excludeAx((CC.ATTR_VOTING,), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


        ctu.assertKronFactorSelectAll(c, dimname, schema)


# name of data-with-answers set in *_testdata.py file, whether 'tot', i.e. the total number of people, is invariant
@pytest.mark.parametrize("schemaname", [CC.DAS_PL94, CC.DAS_1940, CC.DAS_PL94_P12, CC.DAS_PL94_CVAP, CC.DAS_DHCP_HHGQ])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
@pytest.mark.parametrize("tot_inv", [True, False])
@pytest.mark.parametrize("cons_name", ['hhgq_total_ub', 'hhgq_total_lb'])
def test_hhgq_total(data_answers_name, tot_inv, cons_name, schemaname):
    hhgq_attr_name = CC.ATTR_HHGQ_1940 if schemaname == CC.DAS_1940 else CC.ATTR_HHGQ

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

    assert np.all(compop[c.sign](ctu.lhsFromMatrix(c, data_answers["hist"][0]), c.rhs))

    # Check Kronecker factors
    # Each column of the matrix selects that GQ type (so only one True value -- on the diagonal)
    ctu.assertKronFactorDiagonal(c, hhgq_attr_name, schema)

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((hhgq_attr_name,), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)


@pytest.mark.parametrize("schemaname", [CC.DAS_PL94, CC.DAS_PL94_CVAP])
@pytest.mark.parametrize("data_answers_name", ["data1", "data_1gqtype", "data2"])
@pytest.mark.parametrize("cons_name", ['hhgq_va_ub', 'hhgq_va_lb'])
def test_hhgq_va(data_answers_name, cons_name, schemaname):
    # Get the constraint, data with answers and schema
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname), cons_name)

    hhgq_attr_name = CC.ATTR_HHGQ

    # Check sign and right hand side
    assert c.sign == "le" if cons_name == 'hhgq_va_ub' else "ge"
    assert np.array_equal(c.rhs, data_answers[cons_name])

    assert np.all(compop[c.sign](ctu.lhsFromMatrix(c, data_answers["hist"][0]), c.rhs))

    # Check Kronecker factors
    # Each column of the HHGQ matrix selects that GQ type (so only one True value -- on the diagonal)
    # Each column of the VOTING matrix selects that voting age group (so only one True value -- on the diagonal)
    for dimname in (hhgq_attr_name, CC.ATTR_VOTING):
        ctu.assertKronFactorDiagonal(c, dimname, schema)

    # Selecting everyone on all other axes
    for dimname in ctu.excludeAx((hhgq_attr_name, CC.ATTR_VOTING), schema):
        ctu.assertKronFactorSelectAll(c, dimname, schema)
