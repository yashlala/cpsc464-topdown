import numpy as np
from programs.invariants.tests.invariant_test_generic_class import InvariantTestGeneric
from das_constants import CC

class TestInvariantsDHCP(InvariantTestGeneric):

    schema_name = CC.SCHEMA_DHCP
    hhgq_attr = CC.ATTR_RELGQ

    d = np.array(
        [  # columns: 'relgq', 'sex', 'age', 'hispanic', 'cenrace', 'unit UID' (shape (42, 2, 116, 2, 63) + unit UID)
            # each row is a person
            [0, 1, 10, 1, 20, 0],
            [2, 0, 20, 0,  1, 1],
            [24, 1, 45, 0, 10, 2],
            [24, 0, 80, 0, 15, 2],
            [0, 1, 90, 0, 15, 3],
            [1, 1, 10, 1, 20, 4],
            [1, 0, 20, 0,  1, 5],
            [1, 1, 45, 0, 10, 6],
            [1, 0, 80, 0, 15, 7],
            [1, 1, 90, 0, 15, 8]
        ]
    )
    units = np.array(
        [
            # each row is a unit
            # columns: 'hhgq','unit UID',
            [0, 0],
            [0, 1],
            [24, 2],
            [0, 3],
            [0, 4],
            [0, 5],
            [0, 6],
            [0, 7],
            [0, 8],
        ]
    )
    invariant_names = ('tot', 'gq_vect', 'gqhh_tot',)


    def test_gq_vect(self):
       inv = self.get_inv_dict(self.p_h_data1(), ('gq_vect',))
       assert inv['gq_vect'] == 1 # hhgq > 1, there is 1: 24

    def test_gqhh_tot(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_tot',))
        assert inv['gqhh_tot'] == 9
