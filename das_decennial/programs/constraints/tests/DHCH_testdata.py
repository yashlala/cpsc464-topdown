import numpy as np
from programs.constraints.tests.UnitTVGTestdata import units, gq_counts
from programs.schema.schemas.schemamaker import SchemaMaker, _unit_schema_dict
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_DHCH)
unit_schema = SchemaMaker.fromName(_unit_schema_dict[CC.SCHEMA_DHCH])

# Need hhgq_cap to put correct answers into the data below
# hhgq_cap = ConstraintsCreator(invariants=(), constraint_names=()).hhgq_cap
# Testing data with answers (right hand sides of the constraints)
data1 = {
    'data': [
        # each row is a household
        # (shape (2, 9, 2, 7, 8, 4, 2, 522) + unit UID)
# CC.SCHEMA_DHCH:
#         HH_SEX, HH_AGE, HH_HISPANIC, HH_RACE, HH_ELDERLY, HH_TENSHORT, HH_HHTYPE
#     ],
        #columns: 'hhsex', 'hhage', 'hisp', 'race', elderly, 'tenshort',  'hhtype', 'unit UID'
                    [1,     0,       1,      0,      3,         0,        34,        0],
                    [0,     6,       0,      2,      1,         1,        76,        1],
                    [1,     8,       0,      4,      0,         1,        436,        2],
                    [0,     3,       0,      3,      1,         1,        392,        3],
                    [1,     2,       0,      6,      0,         0,        393,        4],
            ],
    'units' : units,


    # 5 total households
    'total': 5,
    CC.HHGQ_HOUSEHOLD_TOTAL: 4, # from units file
    'gq_vect': [5, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
    'living_alone_eld0': 1,  # Line 4: HH under 60, presence of elderly over 60, living alone
    'living_alone_eld1': 0,
    'living_alone_eld2': 0,
    'living_alone_eld3': 0,
    'living_alone_childalone': 1, # Line 5: HH > 24, living alone, presence of a child (who hence is householder)
    'size2_eld0': 0,
    'size2_eld1': 0,
    'size2_eld2': 1,    # Line 2: Size 2, children present, someone between 60 present, hh age is 65 to 74
    'size2_eld3': 0,
    'size2_le25_eld3': 0,
    'size3_eld0': 0,
    'size3_eld1': 0,
    'size3_eld2': 0,
    'size3_eld3': 1,   # Line 3: Size 3, 2 children present, hh age 75+, no one over 64 present
    'size3_le25_eld3': 0,
    'size4_le25_eld3': 1,  # Line 1: size 4, two children, couple,  hhage 15-24, elderly 75+ present (=> hh's partner)
    'age_child_1': 0,
    'age_child_2': 1,  # Line 3: Female householder, over 75, children below 6
    'age_child_3': 1,  # Line 3:  Female householder, over 75, children below 18
    'hh_elderly_eld3': 1, # Line 3:  householder over 75, no one over 60 present
    'hh_elderly_2': 1,  # Line 2: householder 65-74, no one over 65 present
    'hh_elderly_3': 0,

}

data_1gqtype = {
    'data': [
        # each row is a household
        # (shape (2, 9, 2, 7, 8, 4, 2, 522) + unit UID)
        #columns: 'hhsex', 'hhage', 'hisp', 'race', elderly, 'tenshort',  'hhtype', 'unit UID'
                    [1,     0,       1,      0,      3,         0,        0,        0],
                    [0,     5,       0,      2,      2,         1,        162,       1],
                    [0,     8,       0,      4,      0,         1,        10,        2],
                    [0,     3,       0,      3,      3,         1,        15,        3],
                    [1,     2,       0,      6,      1,         0,        15,        4],
            ],
    'units' : units,

    # 5 total households (5 rows)
    'total': 5,
    'living_alone_eld0': 0,
    'living_alone_eld1': 0,
    'living_alone_eld2': 0,
    'living_alone_eld3': 0,
    'living_alone_childalone': 0,
    'size2_eld0': 0,
    'size2_eld1': 1,    # Line 2: size=2 with child, hhage 60 to 64, someone between 65 and 74 present
    'size2_eld2': 0,
    'size2_eld3': 0,
    'size2_le25_eld3': 0,
    'size3_eld0': 0,
    'size3_eld1': 0,
    'size3_eld2': 0,
    'size3_eld3': 0,
    'size3_le25_eld3': 1,  # Line 1: size=3, couple with child, hhage < 24, presence of 75+ elderly
    'size4_le25_eld3': 0,
    'age_child_1': 1,   # line 3: hhsex male, children < 6, hhage 75+
    'age_child_2': 0,
    'age_child_3': 0,
    'hh_elderly_eld3': 1, # line 3: hhage 75+, no one over 60
    'hh_elderly_2': 0,
    'hh_elderly_3': 0,
}



for data in [data1, data_1gqtype]:
    # data['hist'] = table2hists(np.array(data['data']), schema, 'hhsex') # NOTE THAT THE 'hhsex' axis is a stub.

    data['hist'] = table2hists(np.array(data['data']), schema), table2hists(np.array(data['units']), unit_schema, CC.ATTR_HHTYPE_DHCH, units=True)

data2 = data_1gqtype
