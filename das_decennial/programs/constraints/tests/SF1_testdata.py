import numpy as np
import scipy.sparse as ss
from collections import Counter
from programs.constraints.Constraints_SF1 import ConstraintsCreator
from programs.schema.schemas.schemamaker import SchemaMaker
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_SF1)
unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10)
# Need hhgq_cap to put correct answers into the data below
hhgq_cap = ConstraintsCreator(hist_shape=(schema.shape, unit_schema.shape), invariants=(), constraint_names=()).hhgq_cap


units1 = [
            # each row is a unit
            # columns: 'hhgq','unit UID',
            [0, 0],
            [0, 1],
            [0, 2],
            [0, 3],
            [1, 4],
            [15, 5],
            [16, 6],
            [20, 7],
            [29, 8],
       ]
counts1 = Counter(np.array(units1)[:,0])
gq_counts1 = tuple(zip(*((k-1,v) for k,v in counts1.items() if k > 1)))
hu_count1 = sum([v for k,v in counts1.items() if k < 2])
all_counts1 = ((0,)+gq_counts1[0], (hu_count1,)+gq_counts1[1])


# Testing data with answers (right hand sides of the constraints)
data1 = {
    'data': [   # columns: 'rel', 'sex', 'age', 'hispanic', 'cenrace', 'unit UID' (shape (43, 2, 116, 2, 63) + unit UID)
                # each row is a person
                [ 8, 1, 10, 1, 20, 0],
                [ 6, 0, 20, 0, 1, 1],
                [ 3, 1, 45, 0, 10, 2],
                [ 3, 0, 80, 0, 15, 2],
                [ 2, 1, 90, 0, 15, 3],
                [16+13, 1, 10, 1, 20, 6],
                [15+13, 0, 20, 0, 1,  5],
                [16+13, 1, 45, 0, 10, 6],
                [20+13, 0, 80, 0, 15, 7],
                [29+13, 1, 90, 0, 15, 8],
            ],
    'units': units1,
    # 10 total people (10 rows)
    'total': 10,

    # 4 rows 2,3,4,5,7,8,9,10 have age>18
    'voting_age': 8,

    # rows 1 and 2 are parents under 30
    'no_parents_under_30': 2,

    # row 5
    'no_kids_over_89': 1,

    # row 1
    'no_refhh_under_15': 1,

    # None of the rows with nursing home (11 in first column) have <18 age (third column)
    'nurse_nva_0': 0,

    # 4 household units put no bound, since they can be vacant; 4 GQ set numbers for each type
    'hhgq_total_lb': ss.coo_matrix((gq_counts1[1], (gq_counts1[0], np.zeros(len(gq_counts1[0]), dtype=int))), shape=(29,1)).toarray().flatten(),
    # If total number of people not invariant, then 1 person per gq unit type (0 for housing units)
    'hhgq_total_lb_no_tot': ss.coo_matrix((gq_counts1[1], (gq_counts1[0], np.zeros(len(gq_counts1[0]), dtype=int))), shape=(29,1)).toarray().flatten(),

    # There are 4 GQ units, so 4 people should go there.
    # Thus for housing units the ub is 6 = 10 - 4; zero for non-existent GQ types, and 7 = 10 - 3 (for 3 other GQ types) for existing GQ types
    'hhgq_total_ub': np.array([6]+[0]*13+[7]*2+[0]*3+[7]+8*[0]+[7]),
    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': ss.coo_matrix((all_counts1[1], (all_counts1[0], np.zeros(len(all_counts1[0]), dtype=int))), shape=(29,1)).toarray().flatten()*hhgq_cap,


}

units_1gqtype = [
    # each row is a unit
    # columns: 'hhgq','unit UID',
    [0, 0],
    [0, 1],
    [0, 2],
    [0, 3],
    [1, 4],
    [20, 5],
    [20, 6],
    [20, 7],
    [20, 8],
]
counts_1gqtype = Counter(np.array(units_1gqtype)[:, 0])
gq_counts_1gqtype = tuple(zip(*((k - 1, v) for k, v in counts_1gqtype.items() if k > 1)))
hu_count_1gqtype = sum([v for k, v in counts_1gqtype.items() if k < 2])
all_counts_1gqtype = ((0,) + gq_counts_1gqtype[0], (hu_count_1gqtype,) + gq_counts_1gqtype[1])

# Testing data with answers (right hand sides of the constraints)
data_1gqtype = {
    'data': [  # columns: 'rel', 'sex', 'age', 'hispanic', 'cenrace', 'unit UID' (shape (43, 2, 116, 2, 63) + unit UID)
        # each row is a person
        [8, 1, 10, 1, 20, 0],
        [6, 0, 20, 0, 1, 1],
        [3, 1, 45, 0, 10, 2],
        [3, 0, 80, 0, 15, 2],
        [2, 1, 90, 0, 15, 3],
        [20 + 13, 1, 10, 1, 20, 6],
        [20 + 13, 0, 20, 0, 1, 5],
        [20 + 13, 1, 45, 0, 10, 6],
        [20 + 13, 0, 80, 0, 15, 7],
        [20 + 13, 1, 90, 0, 15, 8],
    ],
    'units': units1,
    # 10 total people (10 rows)
    'total': 10,

    # 4 rows 2,3,4,5,7,8,9,10 have age>18
    'voting_age': 8,

    # rows 1 and 2 are parents under 30
    'no_parents_under_30': 2,

    # row 5
    'no_kids_over_89': 1,

    # row 1
    'no_refhh_under_15': 1,

    # None of the rows with nursing home (11 in first column) have <18 age (third column)
    'nurse_nva_0': 0,

    # 4 household units put no bound, since they can be vacant; 4 GQ set numbers for each type
    'hhgq_total_lb': ss.coo_matrix((gq_counts_1gqtype[1], (gq_counts_1gqtype[0], np.zeros(len(gq_counts_1gqtype[0]), dtype=int))), shape=(29, 1)).toarray().flatten(),
    # If total number of people not invariant, then 1 person per gq unit type (0 for housing units)
    'hhgq_total_lb_no_tot': ss.coo_matrix((gq_counts_1gqtype[1], (gq_counts_1gqtype[0], np.zeros(len(gq_counts_1gqtype[0]), dtype=int))),
                                          shape=(29, 1)).toarray().flatten(),

    # There are 4 GQ units, so 4 people should go there.
    # Thus for housing units the ub is 6 = 10 - 4; zero for non-existent GQ types, and all people (10) for the 20s type
    'hhgq_total_ub': np.array([6] + [0] * 18 + [10] + 9 * [0]),
    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': ss.coo_matrix((all_counts_1gqtype[1], (all_counts_1gqtype[0], np.zeros(len(all_counts_1gqtype[0]), dtype=int))),
                                          shape=(29, 1)).toarray().flatten() * hhgq_cap,

}
#
# data2 = {
#     'data':  [   # columns: 'rel', 'sex', 'age', 'hispanic', 'cenrace', 'unit UID' (shape (43, 2, 116, 2, 63) + unit UID)
#                 # each row is a person
#                 [0, 1, 10, 1, 20, 0],
#                 [15, 1, 20, 0, 1, 1],
#                 [2, 1, 90, 0, 10, 0],
#                 [40, 0, 80, 0, 15, 4],
#                 [30, 1, 35, 0, 15, 3]
#             ],
#
#     # 5 total people (5 rows)
#     'total': 5,
#
#     # 4 rows 2,3,4,5 have age>18
#     'voting_age': 4,
#
#     # No nursing homes
#     'nurse_nva_0': 0,
#
#      # no parents
#     'no_parents_under_30': 0,
#
#     # row 3
#     'no_kids_over_89': 1,
#
#     # row 1
#     'no_refhh_under_15': 1,
#
#     # 1 unit of type 0, 1 unit of type 1 (corresp to rel 15), 1 unit of type 16 (=rel 30), 1 unit of rel 40/type 26, the rest don't require people
#     'hhgq_total_lb': np.array([1]+[1]+[0]*14+[1]+[0]*9 + [1] + [0]*2),
#     'hhgq_total_lb_no_tot': np.array([1]+[1]+[0]*14+[1]+[0]*9 + [1] + [0]*2),
#
#     # Two different answers for hhgq_total depending on whether total number of people is invariant ('tot' in invariants)
#     # 'tot' is invariant:
#     # 3 units of type other than 0 (rel 0-14) => 3 people go there leaving 2 out of 5
#     # 3 units of type other than 1 (rel 15) => 3 people go there leaving 2 out of 5
#     # 3 units of type other than 16 (rel 30) => 3 people go there leaving 2 out of 5
#     # 3 units of type other than 26 (rel 40) => 3 people go there leaving 2 out of 5
#     # The other types have no units, so no people can go there
#     'hhgq_total_ub': np.array([2]+[2]+[0]*14+[2]+[0]*9 + [2] + [0]*2),
#     # 'tot' not invariant: multiply number of units by hhgq_cap vector
#     'hhgq_total_ub_no_tot': np.array([1]+[1]+[0]*14+[1]+[0]*9 + [1] + [0]*2) * hhgq_cap,
# }

for data, units in zip([data1, data_1gqtype], [units1, units_1gqtype]): #, data2, data_1gqtype]:
    data['hist'] = table2hists(np.array(data['data']), schema), table2hists(np.array(units), unit_schema, CC.ATTR_REL, units=True)

data2 = data1