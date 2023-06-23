import numpy as np
from programs.invariants.tests.invariant_test_generic_class import InvariantTestGeneric
from das_constants import CC

class TestInvariantsSF1(InvariantTestGeneric):
    schema_name = CC.SCHEMA_SF1
    hhgq_attr = CC.ATTR_REL

    invariant_names = ('tot', )

    d = np.array(
        [
            # columns: 'rel', 'sex', 'age', 'hispanic', 'cenrace', 'unit UID' (shape (43, 2, 116, 2, 63) + unit UID)
            # each row is a person
            [0, 1, 10, 1, 20, 0],
            [15, 1, 20, 0, 30, 1],
            [16, 0, 33, 0, 58, 2],
            [16, 0, 80, 0, 10, 2],
            [40, 0, 80, 0, 10, 3],
            [5, 0, 80, 0, 10, 0]
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

    # def test_va(self):
    #     inv = self.get_inv_dict(self.p_h_data1(), ('va',))
    #     assert inv['va'] == 2

    # def test_gqhh_vect(self):
    #     inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_vect',))
    #     # There's 1 of 15, one of 16 and 1 of 0
    #     assert np.array_equal(inv['gqhh_vect'], np.array([1,1,1]+[0]*(41-18)+[1]+[0]*2))

    def test_gq_vect(self):
       inv = self.get_inv_dict(self.p_h_data1(), ('gq_vect',))
       assert inv['gq_vect'] == 3  # hhgq > 1, there are 3: 2,2,5


    def test_gqhh_tot(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_tot',))
        assert inv['gqhh_tot'] == 9


    # def test_all_inv(self):
    #     #inv = self.get_inv_dict(self.p_h_data1(), ('tot', 'va', 'gqhh_vect', 'gqhh_tot', 'gq_vect',))
    #     inv = self.get_inv_dict(self.p_h_data1(), ('tot', 'gq_vect', 'gqhh_vect', 'gqhh_tot',))
    #     #assert len(inv.items()) == 5
    #     assert len(inv.items()) == 4
    #     assert inv['tot'] == 4
    # #    assert inv['va'] == 2
    #     assert np.array_equal(inv['gqhh_vect'], np.array([1]+[0]*(15-1)+[1,1]+[0]*(44-18)))
    #     assert inv['gq_vect'] == 2
    #     assert inv['gqhh_tot'] == 3
