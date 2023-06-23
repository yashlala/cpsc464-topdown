import numpy as np
from programs.constraints.tests.UnitTVGTestdata import units
from programs.schema.schemas.schemamaker import SchemaMaker, _unit_schema_dict
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_HOUSEHOLD2010_TENVACS)
unit_schema = SchemaMaker.fromName(_unit_schema_dict[CC.SCHEMA_HOUSEHOLD2010_TENVACS])

# Need hhgq_cap to put correct answers into the data below
# hhgq_cap = ConstraintsCreator(invariants=(), constraint_names=()).hhgq_cap
# Testing data with answers (right hand sides of the constraints)
data1 = {
    'data': [
        # each row is a household
        # (shape (2, 9, 2, 7, 8, 24, 2, 4, 2) + unit UID)
        #columns: 'hhsex', 'hhage', 'hisp', 'race', 'size', 'hhtype', 'elderly', 'multi', 'ten2lev', 'unit UID'
                    [1,     8,       1,      0,      1,      20,        0,        0,             0,          0],
                    [0,     6,       0,      2,      1,      1,         2,        1,             1,          1],
                    [1,     3,       0,      4,      1,      18,        1,        0,             1,          2],
                    [0,     3,       0,      3,      7,      15,        3,        1,             1,          3],
                    [1,     2,       0,      6,      0,      15,        1,        0,             0,          4],
            ],
    'units' : units,


    # 5 total households
    'total': 5,
    # Last row has size 0
    'no_vacant': 1,
    # First row has size 1
    'living_alone': 3,
    'living_alone_gt1': 2,
    'living_alone_multi': 1,
    'living_alone_eld0': 1,
    'living_alone_eld1': 0,
    'living_alone_eld2': 0,
    'living_alone_eld3': 0
}

data_1gqtype = {
    'data': [
        # each row is a person
        # (shape (2, 9, 2, 7, 8, 24, 4, 2) + unit UID)
        #columns: 'hhsex', 'hhage', 'hisp', 'race', 'size', 'hhtype', 'elderly', 'multi', 'ten2lev', 'unit UID'
                    [1,     8,       1,      0,      1,      20,        0,        0,    0,      0],
                    [0,     6,       0,      2,      2,      1,         2,        1,    1,      1],
                    [1,     3,       0,      4,      4,      10,        0,        0,    1,      2],
                    [0,     3,       0,      3,      7,      15,        3,        1,    1,      3],
                    [1,     2,       0,      6,      0,      15,        1,        0,    0,      4]
            ],
    'units' : units,

    # 5 total people (5 rows)
    'total': 5,
    # Last row has size 0
    'no_vacant': 1,
}



for data in [data1, data_1gqtype]:
    # data['hist'] = table2hists(np.array(data['data']), schema, 'hhsex') # NOTE THAT THE 'hhsex' axis is a stub.

    data['hist'] = table2hists(np.array(data['data']), schema), table2hists(np.array(data['units']), unit_schema, CC.ATTR_HHGQ, units=True)

data2 = data_1gqtype
