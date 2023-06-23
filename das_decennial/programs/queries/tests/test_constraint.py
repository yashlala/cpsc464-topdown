import pytest
import numpy as np
import programs.queries.querybase as querybase
from programs.queries.constraints_dpqueries import Constraint
from exceptions import IncompatibleAddendsError

@pytest.fixture
def data():
    """ sets up data for testing """
    shape = (3, 4, 5)
    size: int = np.prod(shape)
    return np.arange(size).reshape(shape)


@pytest.fixture
def query(data):
    """ make a query """
    return querybase.SumoverQuery(data.shape, add_over_margins=(0, 2))

@pytest.fixture
def constraint(query, data):
    return Constraint(query, query.answer(data), "=", 'teststandard')


def test_slotsFrozen(constraint):
    with pytest.raises(AttributeError):
        constraint.newAttribute = "This shouldn't work."


@pytest.mark.parametrize("sign", ['=', 'le', 'ge'])
def test_create(data, query, sign):
    """ Tests creation of constraints """
    c1 = Constraint(query, query.answer(data), sign, 'cons1')
    assert c1.name == 'cons1'
    assert c1.query == query
    assert c1.rhs.size == 4
    assert c1.sign == sign
    assert np.array_equal(c1.rhs, np.array([330, 405, 480, 555]))
    c2 = Constraint(query, np.zeros(query.answer(data).shape).astype(int), sign)
    assert c2.name == ''
    assert c2.query == query
    assert c2.rhs.size == 4
    assert c2.sign == sign
    assert np.array_equal(c2.rhs, np.array([0, 0, 0, 0]))


@pytest.mark.parametrize("queryarg, rhs, sign, err_type, err_msg",
[
    ('query',            None, '=',  TypeError, f"Query must be of class {querybase.AbstractLinearQuery.__module__}.{querybase.AbstractLinearQuery.__name__}"),
    (   None,            None, 'a', ValueError, "Sign a is given for constraint"),
    (   None,   np.zeros(100), '=', ValueError, "rhs size must be the same as the query answer"),
    (   None,               1, '=',  TypeError, "rhs must be of class numpy.ndarray"),
])
def test_createErrorRaising(data, query, queryarg, rhs, sign, err_type, err_msg):
    """ Tests that constructor raises appropriate errors """
    if not queryarg:
        queryarg = query

    if rhs is None:
        rhs = query.answer(data)

    with pytest.raises(err_type) as err:
        Constraint(queryarg, rhs, sign)
    assert err_msg in str(err.value)
    # assert err_msg in caplog.text


def test_repr(constraint, capsys):
    """ Test that __repr__ function doesn't raise error and prints what's expected"""
    print(constraint)
    assert "Constraint: teststandard" in capsys.readouterr()[0]


@pytest.mark.parametrize("samequery", [True, False])
@pytest.mark.parametrize("samerhs", [True, False])
@pytest.mark.parametrize("samesign", [True, False])
@pytest.mark.parametrize("samename", [True, False])
@pytest.mark.parametrize("rhstype", [int, float, np.int16, np.int32, np.int64, np.float16, np.float32, np.float64, np.float128, np.complex64, np.complex128, bool])
def test_eq(constraint, query, data, samequery, samerhs, samesign, samename, rhstype):
    """ Test __eq__ constraint comparison"""
    if not samequery:
       query = querybase.SumoverQuery(data.shape, add_over_margins=(0, 1, 2))

    rhs = (query.answer(data) + 1e-14).astype(rhstype)
    constraint.rhs = constraint.rhs.astype(rhstype)
    if not samerhs:
        rhs = rhs + 1

    sign = '=' if samesign else 'le'
    name = 'teststandard' if samename else 'othername'

    c2 = Constraint(query, rhs, sign, name)

    if samequery and samerhs and samesign and samename:
        assert c2 == constraint
    else:
        assert c2 != constraint


@pytest.mark.parametrize("sign", ['=', 'le', 'ge'])
def test_add(constraint, query, sign):
    """ Test addition of two constraints """
    newrhs = np.random.rand(constraint.rhs.size)
    constraint.sign = sign
    c2 = Constraint(query, newrhs, sign, 'teststandard')
    sum_cons = constraint + c2
    assert sum_cons.query == constraint.query
    assert sum_cons.query == c2.query
    assert sum_cons.sign == constraint.sign
    assert sum_cons.sign == c2.sign
    assert np.allclose(sum_cons.rhs, constraint.rhs + c2.rhs)

    # Let's check that the sum is good too, and can be added further
    sum_cons1 = sum_cons + c2
    assert sum_cons1.query == sum_cons.query
    assert sum_cons1.query == c2.query
    assert sum_cons1.sign == sum_cons.sign
    assert sum_cons1.sign == c2.sign
    assert np.allclose(sum_cons1.rhs, sum_cons.rhs + c2.rhs)


# MH Note: 20200501 Removed query from wrongattr attributes to check against because we no longer do the comparison
@pytest.mark.parametrize("wrongattr", ["sign", "name"])
def test_addValueErrorsRaising(constraint, data, wrongattr):
    """ Test raising errors when trying to add Constraints with different queries, signs or names"""
    err_msg = f" cannot be added: addends have different {wrongattr}"
    argdict = {
        'query': constraint.query,
        'sign': constraint.sign,
        'name': constraint.name,
    }
    wrongargdict ={
        'query': querybase.SumoverQuery(data.shape, add_over_margins=(0, 1, 2)),
        'sign': 'le',
        'name': 'othername',
    }
    argdict[wrongattr] = wrongargdict[wrongattr]
    argdict['rhs'] = argdict['query'].answer(data)
    c2 = Constraint(**argdict)
    with pytest.raises(IncompatibleAddendsError) as err:
        cons_sum = constraint + c2
    assert err_msg in str(err.value)
    # assert err_msg in caplog.text


def test_addTypeErrorsRaising(constraint):
    """ Test raising errors when trying to add Constraint with something else"""
    other = 1
    err_msg = f"Constraint '{constraint.name}' + {other} cannot be added: addends have different type"
    with pytest.raises(IncompatibleAddendsError) as err:
        cons_sum = constraint + other
    assert err_msg in str(err.value)
    # assert err_msg in caplog.text

    with pytest.raises(IncompatibleAddendsError) as err:
        cons_sum = other + constraint
    assert err_msg in str(err.value)
    # assert err_msg in caplog.text


def test_check(constraint, data):
    """ Test the check method"""
    assert constraint.check(data)
    constraint.rhs = constraint.rhs + 0.01
    assert not constraint.check(data)
    assert constraint.check(data, tol=0.1)
    constraint.rhs = constraint.rhs + 3
    constraint.sign = 'le'
    assert constraint.check(data)
    constraint.rhs = constraint.rhs - 10
    constraint.sign = 'ge'
    assert constraint.check(data)
