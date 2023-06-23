import numpy as np
# from programs.utils import table2hists
from das_constants import CC

from programs.schema.schemas.schemamaker import SchemaMaker
from programs.invariants.invariantsmaker import InvariantsMaker
import programs.datadict as dd
import programs.constraints.tests._1940_testdata as _1940_testdata
import programs.constraints.tests.PL94_testdata as PL94_testdata
import programs.constraints.tests.PL94_P12_testdata as PL94_P12_testdata
import programs.constraints.tests.PL94_CVAP_testdata as PL94_CVAP_testdata
import programs.constraints.tests.SF1_testdata as SF1_testdata
import programs.constraints.tests.Household2010_testdata as Household2010_testdata
import programs.constraints.tests.Household2010_vacs_testdata as Household2010_vacs_testdata
import programs.constraints.tests.DHCH_testdata as DHCH_testdata
import programs.constraints.tests.TenUnit2010Testdata as TenUnit2010Testdata
import programs.constraints.tests.DHCP_HHGQ_testdata as DHCP_HHGQ_testdata
import programs.constraints.tests.DHCP_testdata as DHCP_testdata

data = {
        CC.DAS_1940: _1940_testdata,
        CC.DAS_PL94: PL94_testdata,
        CC.DAS_PL94_P12: PL94_P12_testdata,
        CC.DAS_PL94_CVAP: PL94_CVAP_testdata,
        CC.DAS_SF1: SF1_testdata,
        CC.DAS_Household2010: Household2010_testdata,
        CC.SCHEMA_HOUSEHOLD2010_TENVACS: Household2010_vacs_testdata,
        CC.SCHEMA_DHCH: DHCH_testdata,
        CC.DAS_TenUnit2010: TenUnit2010Testdata,
        CC.DAS_DHCP_HHGQ: DHCP_HHGQ_testdata,
        CC.SCHEMA_DHCP: DHCP_testdata,
}


def indSch(var, schema):
    """ Index of the variable in schema """
    return schema.dimnames.index(var)


def dimSch(var, schema):
    """ Length of the variable dimension in schema """
    return schema.shape[indSch(var, schema)]


def excludeAx(variables, schema):
    """ Return schema variable names except vars"""
    return set(schema.dimnames)-set(variables)


def getAllImplementedInvariantNames(schemaname):
    schema = SchemaMaker.fromName(schemaname)
    # We add 'tot_hu' for housing invariants, and 'tot' for persons invariants
    inv_names = ['tot_hu'] if ((CC.ATTR_HHSEX in schema.dimnames) or (schema.name is CC.SCHEMA_DHCH)) else ['tot']
    inv_names = inv_names + ['gqhh_tot', 'gq_vect', 'gqhh_vect']
    if CC.VOTING_TOTAL in schema.recodes:
        inv_names = inv_names + ['va']

    return inv_names


def getAllImplementedConstraintNames(schemaname):
    return dd.getConstraintsModule(schemaname).ConstraintsCreator.implemented


def inv(schemaname, p_h, h_h, names):
    """ Return Invariants dictionary """
    return InvariantsMaker.make(schema=schemaname, raw=p_h, raw_housing=h_h, invariant_names=names)


def getConsDict(ph_data, inv_d, cons_names, cc):
    """ Return Constraints dictionary from data, invariants and constraint names"""
    return cc(hist_shape=(ph_data[0].shape, ph_data[1].shape), invariants=inv_d, constraint_names=cons_names).calculateConstraints().constraints_dict


def getConstraintsDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_names):
    """ Return constraints, name of data set in data-with-answers and schema"""
    schema = SchemaMaker.fromName(schemaname)
    cc = dd.getConstraintsModule(schemaname).ConstraintsCreator
    data_answers = getattr(data[schemaname], data_answers_name)
    # Create histograms and from them invariants, and this constraint
    person, housing = data_answers["hist"]  # histData(data_answers["data"], schemaname)
    constraints = getConsDict((person, housing), inv(schemaname, person, housing, inv_names), cons_names, cc)
    return constraints, data_answers, schema


def getConstraintDataAnswersSchema(schemaname, data_answers_name, inv_names, cons_name):
    """
    Return constraint, name of data set in data-with-answers and schema.
    Calls getConstraintsDataAnswersPersonSchema with single constraint
    """
    constraints, data_answers, schema = getConstraintsDataAnswersSchema(schemaname, data_answers_name, inv_names, (cons_name,))
    return constraints[cons_name], data_answers, schema


def assertKronFactor(constraint, axis, answer, schema, qindex=0):
    """ Asserting that Kron factor of the constraint correspoinding to :axis: returns :answer: """
    assert np.array_equal(constraint.query.queries[qindex].kronFactors()[indSch(axis, schema)], answer)


def assertKronFactorSelectAll(constraint, axis, schema, query_index=0):
    """
    Selecting everyone on the axis, so the answer is the vector of True equal to axis length
    Checking that Kron factor of the constraint returns such a vector
    """
    assert np.array_equal(constraint.query.queries[query_index].kronFactors()[indSch(axis, schema)], np.broadcast_to(np.array([True]), (1, dimSch(axis, schema))))


def assertKronFactorSingle(constaint, axis, index, schema):
    """
    For constraints on a single value of the variable (having the indicated index), the kron factors are vectors, with only one true value,
    with :index: corresponding to the value on which there is a constraint
    """
    ans = [False] * dimSch(axis, schema)
    ans[index] = True
    assertKronFactor(constaint, axis, np.array([ans]), schema)


def assertKronFactorMiltiple(constaint, axis, indices, schema):
    """
    For constraints on a single group of values of the variable (having the indicated indices), the kron factors are vectors, with true values
    with :indices: corresponding to the values belonging to a group on which there is a constraint
    """
    ans = [False] * dimSch(axis, schema)
    for index in indices:
        ans[index] = True
    assertKronFactor(constaint, axis, np.array([ans]), schema)


def assertKronFactorDiagonal(constaint, axis, schema):
    """ For vector constraints, i.e. on each value, the kron factors are diagonals, i.e. selecting single value in each column"""
    assertKronFactor(constaint, axis, np.diag([True] * dimSch(axis, schema)), schema)


def lhsFromMatrix(constraint, person, qindex=0):
    """ Left-hand side of the constraint application to data, calculated from matrix representation of the query applied to data """
    return constraint.query.queries[qindex].matrixRep().toarray().dot(person.toDense().ravel())
