import numpy as np
from programs.constraints.Constraints_1940 import ConstraintsCreator
from programs.schema.schemas.schemamaker import SchemaMaker
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.DAS_1940)
unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED)
# Need hhgq_cap to put correct answers into the data below
hhgq_cap = ConstraintsCreator(hist_shape=(schema.shape, unit_schema.shape), invariants=(), constraint_names=()).hhgq_cap

# Testing data with answers (right hand sides of the constraints)
data1 = {
    'data': [
                # columns: 'hhgq1940', sex1940, 'age1940', 'hispanic1940', 'race1940', 'citizen1940', 'unit UID' (shape (8, 2, 116, 2, 6, 2) + unitUID)
                # each row is a person
                [0, 1, 30, 1, 2, 0, 0],
                [1, 0,  1, 1, 5, 1, 1],
                [3, 1,  2, 1, 0, 1, 2],
                [3, 0,  3, 1, 1, 0, 2],
                [1, 1,  4, 0, 3, 0, 3]

            ],

    # 5 total people (5 rows)
    'total': 5,

    # For HU(0): Everyone can go to the other units, so 0
    # 2 units of type 1, 1 unit of type 3, the rest don't require people
    'hhgq_total_lb': np.array([0, 2, 0, 1, 0, 0, 0, 0]),
    'hhgq_total_lb_no_tot': np.array([0, 2, 0, 1, 0, 0, 0, 0]),

    # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
    # 'tot' is invariant:
    # 3 units of type other than 0 => 3 people go there leaving 2 out of 5
    # 2 units of type other than 1 => 2 people go there leaving 3 out of 5
    # 3 units of type other than 3 => 3 people go there leaving 2 out of 5
    # The other types have no units, so no people can go there
    'hhgq_total_ub': np.array([2, 4, 0, 3, 0, 0, 0, 0]),
    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': np.array([1, 2, 0, 1, 0, 0, 0, 0]) * hhgq_cap,

}

data_1gqtype = {
    'data': [   # columns: 'hhgq1940', sex1940, 'age1940', 'hispanic1940', 'race1940', 'citizen1940', 'unit UID' (shape (8, 2, 116, 2 6, 2) + unitUID)
                # each row is a person
                [5, 1, 30, 1, 2, 0, 0],
                [5, 0,  1, 1, 3, 1, 1],
                [5, 1,  2, 1, 4, 1, 2],
                [5, 0,  3, 1, 3, 0, 2],
                [5, 1,  4, 0, 0, 0, 3]
            ],

    # 5 total people (5 rows)
    'total': 5,

    # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
    # 'tot' is invariant: 4 units of type 5, everyone (5 people) has to be in those, since there are no units of other types
    'hhgq_total_lb': np.array([0, 0, 0, 0, 0, 5, 0, 0]),

    # 'tot' not invariant: 4 units of type 5, no units of there type
    'hhgq_total_lb_no_tot': np.array([0, 0, 0, 0, 0, 4, 0, 0]),

    # 'tot' is invariant: everyone (5 people) goes to this type
    'hhgq_total_ub': np.array([0, 0, 0, 0, 0, 5, 0, 0]),

    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': np.array([0, 0, 0, 0, 0, 4, 0, 0]) * hhgq_cap,

}

data2 = data1
# data2 = {
#     'data': [   # columns: 'hhgq1940', sex1940, 'age1940', 'hispanic1940', 'race1940', 'citizen1940', 'unit UID' (shape (8, 2, 116, 2 6, 2) + unitUID)
#                 # each row is a person
#                 [0, 1, 30, 1, 2, 0, 0],
#                 [1, 1,  1, 1, 5, 1, 1],
#                 [3, 1,  2, 1, 0, 1, 2],
#                 [3, 0,  3, 1, 1, 0, 2],
#                 [1, 1,  4, 0, 3, 0, 3]
#             ],
#
#     # 5 total people (5 rows)
#     'total': 5,
#
#     # For HU(0): Everyone can go to the other units, so 0
#     # 2 units of type 2, 1 unit of type 3, the rest don't require people
#     'hhgq_total_lb': np.array([0, 2, 0, 1, 0, 0, 0, 0]),
#     'hhgq_total_lb_no_tot': np.array([0, 2, 0, 1, 0, 0, 0, 0]),
#
#     # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
#     # 'tot' is invariant:
#     # 3 units of type other than 0 => 3 people go there leaving 2 out of 5
#     # 1 units of type other than 1 => 1 people go there leaving 4 out of 5
#     # 2 units of type other than 3 => 2 people go there leaving 3 out of 5
#     # The other types have no units, so no people can go there
#     'hhgq_total_ub': np.array([2, 4, 0, 3, 0, 0, 0, 0]),
#     # 'tot' not invariant: multiply number of units by hhgq_cap vector
#     'hhgq_total_ub_no_tot': np.array([1, 2, 0, 1, 0, 0, 0, 0]) * hhgq_cap,
#
# }

for data in [data1, data2, data_1gqtype]:
    data['hist'] = table2hists(np.array(data['data']), schema, CC.ATTR_HHGQ_1940)
