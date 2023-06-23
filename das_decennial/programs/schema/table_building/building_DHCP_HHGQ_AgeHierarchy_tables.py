import programs.schema.table_building.tablebuilder as tablebuilder
from programs.schema.schemas.schemamaker import SchemaMaker
from das_constants import CC

schema = SchemaMaker.buildSchema()

def getTableDict():
    tabledict = {
        ################################################
        # Hierarchical age category tables
        ################################################
        # Prefix Query
        "prefixquery": [recode for recode in schema.recodes.keys() if ('prefix_agecats' in recode) and not ('prefix_agecats' == recode)] ,

        # Range Query
        "rangequery": [recode for recode in schema.recodes.keys() if ('range_agecats' in recode) and not ('range_agecats' == recode)] ,

        # Prefix Query
        "binarysplitquery":  [recode for recode in schema.recodes.keys() if ('binarysplit_agecats' in recode) and not ('binarysplit_agecats' == recode)] ,

    }

    return tabledict

def getTableBuilder(testtables=None):
    """
    :param testtables: Will run the testTableDefs function to ensure recodes
     from the same dimension aren't crossed in a table. This is useful if you
     get the error because it identifies the table and the cell of the list where
     the error occurs.

    :return:
    """
    schema = SchemaMaker.fromName(CC.SCHEMA_REDUCED_DHCP_HHGQ)
    tabledict = getTableDict()
    if testtables==True:

        tablebuilder.testTableDefs(schema, tabledict)

    else:
        schema = SchemaMaker.fromName(CC.SCHEMA_REDUCED_DHCP_HHGQ)
        tabledict = getTableDict()
        builder = tablebuilder.TableBuilder(schema, tabledict)
        return builder


############################################################
## Consolidated tables
############################################################

        '''
        # Tables P12A-I
        # Combines individual P12A-I tables
        "P12A-I": [# P12A-G
            "total", "majorRaces", "majorRaces * sex", "majorRaces * sex * agecat",

            # P12H
            "hispTotal", "hispTotal * sex", "hispTotal * sex * agecat",

            # P12I
            "whiteAlone * notHispTotal", "whiteAlone * notHispTotal * sex", "whiteAlone * notHispTotal * sex * agecat"],

        # Tables PCO43A-I
        # Combines individual PCO43A-I tables
        "PCO43A-I": [
            # PCO43A-G
            "majorRaces * gqTotal", "majorRaces * gqTotal * sex", "majorRaces * gqTotal * sex * agecat43",
            "majorRaces * sex * agecat43 * institutionalized", "majorRaces * sex * agecat43 * majorGQs",

            # PCO43H
            "hispTotal * gqTotal", "hispTotal * gqTotal * sex", "hispTotal * gqTotal * sex * agecat43",
            "hispTotal * sex * agecat43 * institutionalized", "hispTotal * sex * agecat43 * majorGQs",

            # PCO43I
            "whiteAlone * notHispTotal * gqTotal", "hispTotal * gqTotal * sex", "whiteAlone * notHispTotal * gqTotal * sex * agecat43",
            "whiteAlone * notHispTotal * sex * agecat43 * institutionalized", "whiteAlone * notHispTotal * sex * agecat43 * majorGQs"
        ],

        # PCT13A-I
        #Combines individual tables PCT13A-PCT13I
        #Universe: Population in households for major race group, hispanic, or white along/non-hispanic
        "PCT13A-I": [
            # PCT13A-G
            "majorRaces * hhTotal", "majorRaces * hhTotal * sex", "majorRaces * hhTotal * sex * agecat",

            # PCT13H
            "hispTotal * hhTotal", "hispTotal * hhTotal * sex", "hispTotal * hhTotal * sex * agecat",

            #PCT13I
            "whiteAlone * notHispTotal * hhTotal", "whiteAlone * notHispTotal * hhTotal * sex", "whiteAlone * notHispTotal * hhTotal * sex * agecat"
        ],

        '''
