import numpy as np
from programs.invariants.tests.invariant_test_generic_class import InvariantTestGeneric
from das_constants import CC

class TestInvariantsHouseholdSmall(InvariantTestGeneric):

    schema_name = CC.SCHEMA_HOUSEHOLDSMALL

    d = np.array(
        [
            # each row is a household
            # (shape (2, 9, 2, 7, 8, 24, 4, 2) + unit UID)
            # columns: 'hhsex', 'hhage', 'hisp', 'race', 'size', 'hhtype', 'elderly', 'multi', 'unit UID'
            [1, 20,  0],
            [0, 1,  1],
            [1, 10,  2],
            [0, 15,  2],
            [1, 15,  3]
        ],
    )
    # We need a separate unit table that has vacant units (otherwise we could have just tacked on the 'hhgq' column to
    # persons like for all other schemas
    units = np.array(
        [
            # each row is a unit
            # columns: 'tenvacgq','unit UID',
            [0, 0],
            [2, 1],
            [5, 2],
            [0, 3],
            [1, 4],
            [1, 5],
            [1, 6],
            [20, 7],
            [1, 8],
        ]
    )

    invariant_names = ('tot_hu', 'gqhh_vect')


    def test_total_housing(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('tot_hu',))
        assert inv['tot_hu'] == 8

    def test_gqhh_vect(self):
        inv = self.get_inv_dict(self.p_h_data1(), ('gqhh_vect',))
        assert np.array_equal(inv['gqhh_vect'], np.array([8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))