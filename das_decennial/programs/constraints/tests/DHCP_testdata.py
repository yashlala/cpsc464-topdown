import numpy as np
from programs.constraints.Constraints_DHCP import ConstraintsCreator
from programs.schema.schemas.schemamaker import SchemaMaker
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_DHCP)
unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_DHCP)
# Need hhgq_cap to put correct answers into the data below
hhgq_cap = ConstraintsCreator(hist_shape=(schema.shape, unit_schema.shape), invariants=(), constraint_names=()).hhgq_cap


# Testing data with answers (right hand sides of the constraints)
data1 = {
    'units': [
        [ 3, 0],
        [ 0, 1],
        [18, 2],
        [ 0, 3],
        [ 5, 4],
        [23, 5],
        [ 5, 6],
        [ 9, 7],
        [ 0, 8]
    ],

    'data': [# columns: 'relgq', 'sex', 'age', 'hispanic', 'cenrace', 'unit UID' (shape (42, 2, 116, 2, 63) + unit UID)
                # each row is a person

        # there are 10 people
        # 9 units
        # 3 people in 3 different households (11,17,15)
        # 3 people in 2 different GQ 22
        # 1 people in 1 of each GQ in [20, 35, 40, 26]
        [ 3+17, 0,  67, 1, 29, 0],
        [   11, 1,  46, 0,  1, 1],   # Grandchild
        [18+17, 1,  42, 0, 47, 2],
        [   17, 1,  15, 0, 18, 3],   # Other non-relative
        [ 5+17, 0,  57, 1, 32, 4],
        [23+17, 1,   2, 0, 57, 5],
        [ 5+17, 0,  53, 1, 53, 6],
        [ 5+17, 0,   2, 0, 18, 6],
        [ 9+17, 1,  43, 1, 32, 7],
        [   15, 0, 104, 1, 19, 8]    # Roommate
    ],

    # 10 total people (10 rows)
    'total': 10,

    # For HU(0): Everyone can go to the other units, so 0
    # For (1): 5 units, 5 people
    # For (2): 1 unit one person
    # For (5): 1 unit one person
    # [0] = Household (combination of all rel levels) => 0 lower bound since everyone can go to other units
    # for all others,

    # people
    #array([3, 0, 0, 1, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0])

    # units
    #array([3, 0, 0, 1, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0])

    # lower bound, total invariant
    'hhgq_total_lb': np.array([0, 0, 0, 1, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0]),

    # householder_ub = 3

    # upper bound, total invariant
    # 0: 6 go to other units => 4 remain
    # 3: 5 go to other units => 5 remain
    # 5: 4 go to other units => 6 remain
    # 9: 5 go to other units => 5 remain
    # 18: 5 go to other units => 5 remain
    # 23: 5 go to other units => 5 remain
    'hhgq_total_ub': np.array([4, 0, 0, 5, 0, 6, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 5, 0]),

    # lower bound, no total invariant
    'hhgq_total_lb_no_tot': np.array([0, 0, 0, 1, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0]),

    # upper bound, no total invariant
    'hhgq_total_ub_no_tot': np.array([3, 0, 0, 1, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0]) * hhgq_cap,

    'relgq0_lt15': 0,
    'relgq1_lt15': 0,
    'relgq2_lt15': 0,
    'relgq3_lt15': 0,
    'relgq4_lt15': 0,
    'relgq5_lt15': 0,
    'relgq6_gt89': 0,
    'relgq7_gt89': 0,
    'relgq8_gt89': 0,
    'relgq10_lt30': 0,
    'relgq11_gt74': 0,
    'relgq12_lt30': 0,
    'relgq13_lt15_gt89': 0,
    'relgq16_gt20': 0,
    'relgq18_lt15': 0,
    'relgq19_lt15': 0,
    'relgq20_lt15': 0,
    'relgq21_lt15': 0,
    'relgq22_lt15': 1,
    'relgq23_lt17_gt65': 0,
    'relgq24_gt25': 0,
    'relgq25_gt25': 0,
    'relgq26_gt25': 1,
    'relgq27_lt20': 0,
    'relgq31_lt17_gt65': 0,
    'relgq32_lt3_gt30': 0,
    'relgq33_lt16_gt65': 0,
    'relgq34_lt17_gt65': 0,
    'relgq35_lt17_gt65': 0,
    'relgq37_lt16': 0,
    'relgq38_lt16': 0,
    'relgq39_lt16_gt75': 0,
    'relgq40_lt16_gt75': 1,

    'householder_ub': 3,
    'householder_ub_lhs': 0,
    'spousesUnmarriedPartners_ub': 3,
    'spousesUnmarriedPartners_ub_lhs': 0,
    'people100Plus_ub': 6,
    'people100Plus_ub_lhs': 1,
    'parents_ub': 6,
    'parents_ub_lhs': 0,
    'parentInLaws_ub': 6,
    'parentInLaws_ub_lhs': 0,
}


for data in [data1]:
    data['hist'] = table2hists(np.array(data['data']), schema), table2hists(np.array(data['units']), unit_schema, CC.ATTR_RELGQ, units=True)

data_1gqtype = data1
data2 = data1
