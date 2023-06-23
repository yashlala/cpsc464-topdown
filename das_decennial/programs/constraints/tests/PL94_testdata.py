import numpy as np
from programs.schema.schemas.schemamaker import SchemaMaker
from programs.constraints.Constraints_PL94 import ConstraintsCreator
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_PL94)
unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED)
# Need hhgq_cap to put correct answers into the data below
hhgq_cap = ConstraintsCreator(hist_shape=(schema.shape, unit_schema.shape), invariants=(), constraint_names=()).hhgq_cap

# Testing data with answers (right hand sides of the constraints)
data1 = {
    'data': [   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 0, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 2],
                [1, 1, 0, 15, 3]
            ],

    # 5 total people (5 rows)
    'total': 5,

    # 3 rows 1,3,5 have va=1
    'voting_age': 3,

    # 1 row (4th) with nursing home (3 in first column) and 0 (va) in second column
    'nurse_nva_0': 1,

    # 1 unit of type 0, 2 units of type 1, 1 unit of type 3, the rest don't require people
    'hhgq_total_lb': np.array([1, 2, 0, 1, 0, 0, 0, 0]),
    'hhgq_total_lb_no_tot': np.array([1, 2, 0, 1, 0, 0, 0, 0]),

    # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
    # 'tot' is invariant:
    # 3 units of type other than 0 => 3 people go there leaving 2 out of 5
    # 2 units of type other than 1 => 2 people go there leaving 3 out of 5
    # 3 units of type other than 3 => 3 people go there leaving 2 out of 5
    # The other types have no units, so no people can go there
    'hhgq_total_ub': np.array([2, 3, 0, 2, 0, 0, 0, 0]),
    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': np.array([1, 2, 0, 1, 0, 0, 0, 0]) * hhgq_cap,

    # We have 2 minors total (2nd & 4th rows) and 3 adults total (1st, 3rd, 5th rows)
    # hhgq=0: 3 facilities of other types,
    #           for minors, can fill them with the 3 adults, leaving 2 minors,
    #           for adults, have to fill them with 2 minors and 1 adult, leaving 2 adults
    # hhgq=1: 2 facilities of other types,
    #           for minors, can fill them with the 2 adults, leaving 2 minors,
    #           for adults, have to fill them with 2 minors, leaving 3 adults
    # hhgq=3: 3 facilities of other types,
    #           for minors, can fill them with the 3 adults, leaving 2 minors,
    #           for adults, have to fill them with 2 minors and 1 adult, leaving 2 adults
    # other hhgq are 0
    'hhgq_va_ub': np.array([[2, 2, 0, 2, 0, 0, 0, 0],  # minors / nva
                            [2, 3, 0, 2, 0, 0, 0, 0]]  # adults / va
                           ).transpose().ravel(),

    # We have 2 minors total and 3 adults total
    # hhgq=0: 1 facility
    #           for minors, can fill it with 1 adult, 0 minors needed.
    #           for adults, can fill it with 1 minor, 0 adults needed
    # hhgq=1: 2 facilities
    #           for minors, can fill them with 2 adults, 0 minors needed.
    #           for adults, can fill them with 2 minors, 0 adults needed
    # hhgq=3: 1 facility
    #           for minors, can fill it with 1 adult, 0 minors needed.
    #           for adults, can fill it with 1 minor, 0 adults needed.
    # other hhgq are 0
    'hhgq_va_lb': np.array([[0, 0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0, 0, 0]]).transpose().ravel(),
}

data_1gqtype = {
    'data': [   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [5, 1, 1, 20, 0],
                [5, 0, 0, 1, 1],
                [5, 1, 0, 10, 2],
                [5, 0, 0, 15, 2],
                [5, 1, 0, 15, 3]
            ],

    # 5 total people (5 rows)
    'total': 5,

    # 3 rows 1,3,5 have va=1
    'voting_age': 3,

    # No nursing homes
    'nurse_nva_0': 0,

    # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
    # 'tot' is invariant: 4 units of type 5, everyone (5 people) has to be in those, since there are no units of other types
    'hhgq_total_lb': np.array([0, 0, 0, 0, 0, 5, 0, 0]),

    # 'tot' not invariant: 4 units of type 5, no units of there type
    'hhgq_total_lb_no_tot': np.array([0, 0, 0, 0, 0, 4, 0, 0]),

    # 'tot' is invariant: everyone (5 people) goes to this type
    'hhgq_total_ub': np.array([0, 0, 0, 0, 0, 5, 0, 0]),

    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': np.array([0, 0, 0, 0, 0, 4, 0, 0]) * hhgq_cap,

    # There are 2 people of non-voting age in HHGQ type 5 and 3 of voting. That's the upper bound for type 5 and 0 for every other type
    'hhgq_va_ub': np.array([[0, 0, 0, 0, 0, 2, 0, 0],  # minors / nva
                            [0, 0, 0, 0, 0, 3, 0, 0]]  # adults / va
                           ).transpose().ravel(),

    # There are 2 people of non-voting age in HHGQ type 5 and 3 of voting. That's the lower bound for type 5 and 0 for every other type
    'hhgq_va_lb': np.array([[0, 0, 0, 0, 0, 2, 0, 0],  # minors / nva
                            [0, 0, 0, 0, 0, 3, 0, 0]]  # adults / va
                           ).transpose().ravel(),
}

data2 = {
    'data': [   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 1, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 4],
                [1, 1, 0, 15, 3]
            ],

    # 5 total people (5 rows)
    'total': 5,

    # 3 rows 1,3,5 have va=1
    'voting_age': 4,

    # 1 row (4th) with nursing home (3 in first column) and 0 (va) in second column
    'nurse_nva_0': 1,

    # 1 unit of type 0, 2 units of type 1, 2 unit of type 3, the rest don't require people
    'hhgq_total_lb': np.array([1, 2, 0, 2, 0, 0, 0, 0]),
    'hhgq_total_lb_no_tot': np.array([1, 2, 0, 2, 0, 0, 0, 0]),

    # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
    # 'tot' is invariant:
    # 4 units of type other than 0 => 4 people go there leaving 1 out of 5
    # 3 units of type other than 1 => 3 people go there leaving 2 out of 5
    # 3 units of type other than 3 => 3 people go there leaving 2 out of 5
    # The other types have no units, so no people can go there
    'hhgq_total_ub': np.array([1, 2, 0, 2, 0, 0, 0, 0]),
    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': np.array([1, 2, 0, 2, 0, 0, 0, 0]) * hhgq_cap,

    # We have 1 minor total (4th row) and 4 adults total
    # hhgq=0: 4 facilities of other types,
    #           for minors, can fill them with the 4 adults, leaving 1 minor,
    #           for adults, have to fill them with 1 minors and 3 adults, leaving 1 adult
    # hhgq=1: 3 facilities of other types,
    #           for minors, can fill them with the 3 adults, leaving 1 minor,
    #           for adults, have to fill them with 1 minor amd 2 adults, leaving 2 adults
    # hhgq=3: 3 facilities of other types,
    #           for minors, can fill them with the 3 adults, leaving 1 minor,
    #           for adults, have to fill them with 1 minor amd 2 adults, leaving 2 adults
    # other hhgq are 0
    'hhgq_va_ub': np.array([[1, 1, 0, 1, 0, 0, 0, 0],  # minors / nva
                            [1, 2, 0, 2, 0, 0, 0, 0]]  # adults / va
                           ).transpose().ravel(),

    # We have 1 minors total and 4 adults total
    # hhgq=0: 1 facility
    #           for minors, can fill it with 1 adult, 0 minors needed.
    #           for adults, can fill it with 1 minor, 0 adults needed
    # hhgq=1: 2 facilities
    #           for minors, can fill them with 2 adults, 0 minors needed.
    #           for adults, can fill them with 1 minor, 1 adult needed
    # hhgq=3: 2 facilities
    #           for minors, can fill them with 2 adults, 0 minors needed.
    #           for adults, can fill them with 1 minor, 1 adult needed
    # other hhgq are 0
    'hhgq_va_lb': np.array([[0, 0, 0, 0, 0, 0, 0, 0],  # minors
                            [0, 1, 0, 1, 0, 0, 0, 0]   # adults
                            ]).transpose().ravel(),
}

for data in [data1, data2, data_1gqtype]:
    data['hist'] = table2hists(np.array(data['data']), schema, CC.ATTR_HHGQ)