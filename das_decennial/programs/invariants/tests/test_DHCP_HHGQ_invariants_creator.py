import numpy as np
from programs.invariants.tests.invariant_test_generic_class import InvariantTestGeneric
from das_constants import CC

class TestInvariantsDHCPHHGQ(InvariantTestGeneric):

    schema_name = CC.SCHEMA_REDUCED_DHCP_HHGQ

    d = np.array(
        [  # columns: 'hhgq', 'sex', 'age', 'hispanic', 'cenrace', 'citizen', 'unit UID' (shape (8, 2, 116, 2, 63, 2) + unit UID)
            # each row is a person
            [0, 1, 10, 1, 20, 1, 0],
            [2, 0, 20, 0, 1, 0, 1],
            [5, 1, 45, 0, 10, 1, 2],
            [5, 0, 80, 0, 15, 0, 2],
            [0, 1, 90, 0, 15, 1, 3],
            [1, 1, 10, 1, 20, 1, 4],
            [1, 0, 20, 0, 1, 0, 5],
            [1, 1, 45, 0, 10, 1, 6],
            [1, 0, 80, 0, 15, 0, 7],
            [1, 1, 90, 0, 15, 1, 8]
        ]
    )
    units = np.array(
        [
            # each row is a unit
            # columns: 'hhgq','unit UID',
            [0, 0],
            [2, 1],
            [5, 2],
            [0, 3],
            [1, 4],
            [1, 5],
            [1, 6],
            [2, 7],
            [1, 8],
        ]
    )
    invariant_names = ('tot', 'gq_vect', 'gqhh_tot',)

    def test_gq_vect(self):
       inv = self.get_inv_dict(self.p_h_data1(), ('gq_vect',))
       assert inv['gq_vect'] == 7

    def test_gqhh_tot(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_tot',))
        assert inv['gqhh_tot'] == 9
