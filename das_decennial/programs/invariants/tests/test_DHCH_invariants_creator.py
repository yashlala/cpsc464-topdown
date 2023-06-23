import numpy as np
from programs.invariants.tests.invariant_test_generic_class import InvariantTestGeneric
from das_constants import CC

class TestInvariantsHousehold2010(InvariantTestGeneric):

    schema_name = CC.SCHEMA_DHCH
    hhgq_attr = CC.ATTR_TENVACGQ

    d = np.array(
        [
            # each row is a household
            # (shape (2, 9, 2, 7, 8, 24, 4, 2) + unit UID)
            # columns: 'hhsex', 'hhage', 'hisp', 'race', 'size', 'hhtype', 'elderly', 'multi', 'unit UID'
            # [1, 8, 1, 0, 1, 20, 0, 0, 0],
            # [0, 6, 0, 2, 2, 1, 2, 1, 1],
            # [1, 3, 0, 4, 4, 10, 0, 0, 2],
            # [0, 3, 0, 3, 7, 15, 3, 1, 2],
            # [1, 2, 0, 6, 0, 15, 1, 0, 3]

            # columns: 'hhsex', 'hhage', 'hisp', 'race', elderly, 'tenshort',  'hhtype', 'unit UID'
            [1, 8, 1, 0, 0, 0, 20, 0],
            [0, 6, 0, 2, 1, 1, 1, 1],
            [1, 3, 0, 4, 0, 1, 18, 2],
            [0, 3, 0, 3, 1, 1, 15, 3],
            [1, 2, 0, 6, 0, 0, 15, 4],
        ],
    )
    # We need a separate unit table that has vacant units (otherwise we could have just tacked on the 'hhgq' column to
    # persons like for all other schemas
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
            [20, 7],
            [1, 8],
            [28, 9],
        ]
    )

    invariant_names = ('tot_hu', 'tot', 'gqhh_vect')

    def test_total_hu(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('tot_hu',))
        assert inv['tot_hu'] == 8

    def test_tot(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('tot',))
        assert inv['tot'] == 5

    def test_gqhh_vect(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_vect',))
        # expected_result = [0]*522
        # expected_result[0]
        assert np.array_equal(inv['gqhh_vect'], np.array([8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]))
