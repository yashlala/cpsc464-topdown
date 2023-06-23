import os
import sys
import pytest
import numpy as np
sys.path.append(os.path.dirname(__file__))

import programs.constraints.tests.constraint_test_util as ctu
from das_constants import CC

schemaname = CC.SCHEMA_DHCH

data_to_parametrize = ['data1', 'data_1gqtype', 'data2']


@pytest.mark.parametrize('cons_name', ['living_alone', 'size2', 'size3', 'size4', 'age_child', 'hh_elderly'])
@pytest.mark.parametrize('data_answers_name', data_to_parametrize)
def test_struct_zero(cons_name, data_answers_name):
    cs, data_answers, schema = ctu.getConstraintsDataAnswersSchema(schemaname, data_answers_name, ctu.getAllImplementedInvariantNames(schemaname),
                                                                   (cons_name,))

    for cname, c in cs.items():
        assert c.sign == '='
        assert c.rhs[0] == 0
        assert ctu.lhsFromMatrix(c, data_answers['hist'][0])[0] == data_answers[cname]


def test_tot_hh():
    cons_name='tot_hh'
    data_answers_name='data1'
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, (), cons_name)

    # Total households from household table
    assert c._query.queries[0].answer(data_answers['hist'][0].toDense()) == data_answers['total']

    # Total households from unit table (the units that are tenured)
    assert c._query.queries[1].answer(data_answers['hist'][1].toDense()) == data_answers[CC.HHGQ_HOUSEHOLD_TOTAL]

    # Difference is equal
    assert c._query.answer(np.hstack([data_answers['hist'][0].sparse_array.toarray()[0], data_answers['hist'][1].sparse_array.toarray()[0]])) == data_answers['total'] - data_answers[CC.HHGQ_HOUSEHOLD_TOTAL]


def test_gq_vect():
    cons_name = 'gq_vect'
    data_answers_name = 'data1'
    c, data_answers, schema = ctu.getConstraintDataAnswersSchema(schemaname, data_answers_name, ('gqhh_vect',), cons_name)
    c._query.queries[1].answer(data_answers['hist'][1].toDense())
    print(c)