import numpy as np
from programs.invariants.tests.invariant_test_generic_class import InvariantTestGenericNoUnits
from das_constants import CC


class TestInvariants1940(InvariantTestGenericNoUnits):

    schema_name = CC.SCHEMA_1940
    hhgq_attr = CC.ATTR_HHGQ_1940
    invariant_names = ('tot', )

    d = np.array(
        [
            # columns: 'hhgq1940', sex1940, 'age1940', 'hispanic1940', 'race1940', 'citizen1940', 'unit UID' (shape (8, 2, 116, 2 6, 2) + unitUID)
            # each row is a person
            [0, 1, 20, 1, 5, 1, 0],
            [1, 0, 30, 0, 4, 0, 1],
            [2, 1,  2, 0, 3, 1, 2],
            [2, 0,  3, 0, 2, 0, 2]
        ]
    )

    def test_gqhh_vect(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_vect',))
        assert np.array_equal(inv['gqhh_vect'], np.array([1, 1, 1, 0, 0, 0, 0, 0]))

    def test_gq_vect(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gq_vect',))
        assert inv['gq_vect'] == 2

    def test_gqhh_tot(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_tot',))
        assert inv['gqhh_tot'] == 3

    def test_all_inv(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('tot', 'gqhh_vect', 'gqhh_tot', 'gq_vect',))
        assert len(inv.items()) == 4
        assert inv['tot'] == 4
        assert np.array_equal(inv['gqhh_vect'], np.array([1, 1, 1, 0, 0, 0, 0, 0]))
        assert inv['gq_vect'] == 2
        assert inv['gqhh_tot'] == 3
