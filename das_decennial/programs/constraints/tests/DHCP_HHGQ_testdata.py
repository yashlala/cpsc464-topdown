import numpy as np
from programs.constraints.Constraints_DHCP_HHGQ import ConstraintsCreator
from programs.schema.schemas.schemamaker import SchemaMaker
from programs.constraints.tests.UnitSimpleRecodedTestdata import units
from das_utils import table2hists
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_REDUCED_DHCP_HHGQ)
unit_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_SIMPLE_RECODED)
# Need hhgq_cap to put correct answers into the data below
hhgq_cap = ConstraintsCreator(hist_shape=(schema.shape, unit_schema.shape), invariants=(), constraint_names=()).hhgq_cap

# Testing data with answers (right hand sides of the constraints)
data1 = {
    'data': [   # columns: 'sex', 'age', 'hispanic', 'cenrace', 'citizen', 'unit UID' (shape (8, 2, 116, 2, 63, 2) + unit UID)
                # each row is a person
                [0, 1, 10, 1, 20, 1, 0],
                [2, 0, 20, 0,  1, 0, 1],
                [5, 1, 45, 0, 10, 1, 2],
                [5, 0, 80, 0, 15, 0, 2],
                [0, 1, 90, 0, 15, 1, 3],
                [1, 1, 10, 1, 20, 1, 4],
                [1, 0, 20, 0,  1, 0, 5],
                [1, 1, 45, 0, 10, 1, 6],
                [1, 0, 80, 0, 15, 0, 7],
                [1, 1, 90, 0, 15, 1, 8]
            ],

    'units': units,

    # 10 total people (10 rows)
    'total': 10,

    # For HU(0): Everyone can go to the other units, so 0
    # For (1): 5 units, 5 people
    # For (2): 1 unit one person
    # For (5): 1 unit one person
    'hhgq_total_lb': np.array([0, 5, 1, 0, 0, 1, 0, 0]),
    # 2 housing units, 5 type 1, 1 type 2, 1 type 5
    # no one has to be in housing units so it's [0, .....]
    # for type 1, has to be 1 person per unit, which is 5
    # for type 2 and type 5 it's 1 and 1
    'hhgq_total_lb_no_tot': np.array([0, 5, 1, 0, 0, 1, 0, 0]),

    # For HU(0): 7 people have to go to other units, so 3 remain
    # 1: 2 people to other units, so 8
    # 2: 6 people to other units, so 4
    # 5: 6 people to other units, so 4
    'hhgq_total_ub': np.array([3, 8, 4, 0, 0, 4, 0, 0]),
    # 'tot' not invariant: multiply number of units by hhgq_cap vector
    'hhgq_total_ub_no_tot': np.array([2, 5, 1, 0, 0, 1, 0, 0]) * hhgq_cap,

}


for data in [data1]:
    data['hist'] = table2hists(np.array(data['data']), schema), table2hists(np.array(units), unit_schema, CC.ATTR_HHGQ, units=True)

data_1gqtype = data1
data2 = data1
