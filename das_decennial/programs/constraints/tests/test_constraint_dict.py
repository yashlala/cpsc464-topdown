import pytest
import numpy as np
import programs.constraints.Constraints_PL94 as cPL94
from programs.invariants.invariantsmaker import InvariantsMaker
from programs.schema.schemas.schemamaker import SchemaMaker
from das_utils import table2hists
from programs.constraints.constraint_dict import ConstraintDict
from das_constants import CC

d1 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 0, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 2],
                [1, 1, 0, 15, 3]
            ])

d2 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 1, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 4],
                [1, 1, 0, 15, 3]
            ])

invn = ('tot', 'va', 'gqhh_vect', 'gqhh_tot', 'gq_vect')
consn = ("total", "hhgq_total_lb", "hhgq_total_ub", "voting_age", "hhgq_va_ub", "hhgq_va_lb", "nurse_nva_0")


def convertPL94(d):
    return table2hists(d, SchemaMaker.fromName(CC.SCHEMA_PL94), CC.ATTR_HHGQ)


def makeCons(ph, consnames):
    inv_dict = InvariantsMaker.make(schema=CC.SCHEMA_PL94, raw=ph[0], raw_housing=ph[1], invariant_names=invn)
    return ConstraintDict(cPL94.ConstraintsCreator(hist_shape=(ph[0].shape, ph[1].shape),
                                    invariants=inv_dict,
                                    constraint_names=consnames).calculateConstraints().constraints_dict)


def makeConswWrongSign(ph, consnames):
    """ Substitute one constraint sign with a different one to test for throwing error"""
    cons = makeCons(ph, consnames)
    cons["total"].sign = "le"
    return cons


def makeConswWrongQuery(ph, consnames):
    """ Substitute one constraint sign with a different one to test for throwing error"""
    cons = makeCons(ph, consnames)
    cons["total"].query = cons["voting_age"].query
    return cons


def test_create_constraint_dict_err():
    with pytest.raises(TypeError) as err:
        ConstraintDict([(123, "1"), (456, "2")])
    assert "(<class 'str'>) is not a queries.constraints_dpqueries.Constraint!" in str(err.value)
    # assert err_msg in caplog.text


def test_setitem_constraint_dict_err():
    c = ConstraintDict()
    with pytest.raises(TypeError) as err:
        c[1] = "234"
    assert "(<class 'str'>) being assigned to key '1' is not a queries.constraints_dpqueries.Constraint!" in str(err.value)
    # assert err_msg in caplog.text


def test_update_constraint_dict_err():
    c = ConstraintDict()
    with pytest.raises(TypeError) as err:
        c.update([(789, [])])
    assert "(<class 'list'>) is not a queries.constraints_dpqueries.Constraint!" in str(err.value)
    # assert err_msg in caplog.text


def test_setdefault_constraint_dict_err():
    c = makeCons(convertPL94(d1), consn)
    with pytest.raises(TypeError) as err:
        c.setdefault('aaa', 0)
    assert "(<class 'int'>) is not a queries.constraints_dpqueries.Constraint!" in str(err.value)
    # assert err_msg in caplog.text


def test_fromkeys_constraint_dict_err():
    with pytest.raises(TypeError) as err:
        ConstraintDict.fromkeys((1, 2), 3)
    assert "(<class 'int'>) being assigned to key '1' is not a queries.constraints_dpqueries.Constraint!" in str(err.value)
    # assert err_msg in caplog.text


def test_create_constraint_dict():
    ConstraintDict()
    b = makeCons(convertPL94(d1), consn)
    c = makeCons(convertPL94(d2), consn)
    c[1] = b['total']


def test_addConstraints():
    cons1 = makeCons(convertPL94(d1), consn)
    cons2 = makeCons(convertPL94(d2), consn)
    sum_cons = cons1 + cons2
    assert set(sum_cons.keys()) == set(cons1.keys())
    assert set(sum_cons.keys()) == set(cons2.keys())
    for cname, c in sum_cons.items():
        assert c == cons1[cname] + cons2[cname]

    # Let's check that the sum is good too, and can be added further
    sum_cons1 = sum_cons + cons1
    for cname, c in sum_cons1.items():
        assert c == cons1[cname] + sum_cons[cname]


@pytest.mark.parametrize("test_input,errtypemsg", [
    # test_input: two constraints_dicts
    # errtypemsg: Error type, error message in response to trying to add those two constraints_dicts

    # Different key sets
    (
            (makeCons(convertPL94(d1), consn), makeCons(convertPL94(d2), consn[:-1])),
            (ValueError, 'Constraint dicts cannot be added, having different key sets')
    ),

    # Adding ConstraintDict with something else:
    (
            (makeCons(convertPL94(d1), consn), {'a': 1}),
            (TypeError, 'Cannot add a ConstraintDict (<class \'programs.constraints.constraint_dict.ConstraintDict\'>) with')
    ),

    # Adding ConstraintDict with something else:
    (
            (makeCons(convertPL94(d1), consn), 1),
            (TypeError, 'Cannot add a ConstraintDict (<class \'programs.constraints.constraint_dict.ConstraintDict\'>) with 1(<class \'int\'>)')
    ),
    # Adding ConstraintDict with something else:
    (
            (1, makeCons(convertPL94(d1), consn)),
            (TypeError, 'Cannot add a ConstraintDict (<class \'programs.constraints.constraint_dict.ConstraintDict\'>) with 1(<class \'int\'>)')
    )
])
def test_addConstraintsErrorsRaising(test_input, errtypemsg):
    cons1, cons2 = test_input
    err_type, err_msg = errtypemsg
    with pytest.raises(err_type) as err:
        cons_sum = cons1 + cons2
    assert err_msg in str(err.value)
    # assert err_msg in caplog.text
