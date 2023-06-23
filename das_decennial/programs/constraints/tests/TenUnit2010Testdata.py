import numpy as np
from programs.schema.schemas.schemamaker import SchemaMaker
from programs.constraints.tests.UnitSimpleRecodedTestdata import units
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_TEN_UNIT_2010)
unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10)

# Need hhgq_cap to put correct answers into the data below
# hhgq_cap = ConstraintsCreator(hist_shape=(schema.shape, unit_schema.shape), invariants=(), constraint_names=()).hhgq_cap
# Testing data with answers (right hand sides of the constraints) TODO: NOTE THAT ALL THREE DATA SETS ARE THE SAME FOR NOW
data1 = {
    'data': [
        # each row is a household
        # (shape (2, 9, 2, 7, 8, 24, 2, 4, 2, 4) + unit UID)
        #columns: 'hhsex', 'hhage', 'hisp', 'race', 'size', 'hhtype', 'elderly', 'multi', 'tenure', 'unit UID'
                    [1,     8,       1,      0,      1,      20,        0,        0,       0,          0],
                    [0,     6,       0,      2,      1,      1,         2,        1,       1,          1],
                    [1,     3,       0,      4,      1,      18,        1,        0,       3,          2],
                    [0,     3,       0,      3,      7,      15,        3,        1,       1,          2],
                    [1,     2,       0,      6,      0,      15,        1,        0,       2,          3],
                    [1,     5,       0,      4,      1,      18,        0,        0,       0,          2],
            ],
    'units' : units,


    # 5 total households
    'total': 6,
    # Last row has size 0
    'no_vacant': 1,
    # First row has size 1
    'living_alone': 3,
    'living_alone_gt1': 2,
    'living_alone_multi': 1,
    'living_alone_eld0': 1,
    'living_alone_eld1': 1,
    'living_alone_eld2': 0,
    'living_alone_eld3': 0,
}


for data in [data1]:
    # data['hist'] = table2hists(np.array(data['data']), schema, 'hhsex') # NOTE THAT THE 'hhsex' axis is a stub.

    data['hist'] =  table2hists(np.array(data['data']), schema), table2hists(np.array(data['units']), unit_schema, CC.ATTR_HHGQ, units=True)

data2 = data
data_1gqtype = data