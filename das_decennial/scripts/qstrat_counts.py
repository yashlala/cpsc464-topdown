"""
A stand-alone script for computing the number of scalars in each query in a target strategy.
"""
import sys
sys.path.append("..")
from das_constants import CC
from programs.schema.schemas.schemamaker import SchemaMaker
from programs.strategies.dhcp_strategies import DecompTestStrategyDHCP_20220518
from programs.strategies.dhch_strategies import DecompTestStrategyDHCH_20220713, DecompTestStrategyDHCH_20220713_PR

us_levels = ["Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"]
pr_levels = ["Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"]

def printScalarCounts(msg, levels, test_strategy, qtype, schema):
    # qtype is either 'DPqueries' or 'UnitDPqueries'
    print(msg + f" ({qtype})")
    for level in levels:
        qs = schema.getQueries(test_strategy[qtype][level])
        print(f"\tGeolevel {level}:")
        for qname, q in qs.items():
            print(f"\t\t{qname} -> {q.numAnswers()}")

def countUS_DHCPScalars():
    main_schema = SchemaMaker.fromName("DHCP_SCHEMA")
    secondary_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_DHCP)
    test_strategy = DecompTestStrategyDHCP_20220518.make(us_levels)
    printScalarCounts(f"US DHCP 20.1", us_levels, test_strategy, 'DPqueries', main_schema)
    printScalarCounts(f"US DHCP 20.1", us_levels, test_strategy, 'UnitDPqueries', secondary_schema)

def countUS_DHCHScalars():
    main_schema = SchemaMaker.fromName("DHCH_SCHEMA_TEN_3LEV")
    secondary_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_TENVACSGQ)
    strat_obj = DecompTestStrategyDHCH_20220713()
    test_strategy = strat_obj.make(us_levels)
    printScalarCounts(f"US DHCH 20.1", us_levels, test_strategy, 'DPqueries', main_schema)
    printScalarCounts(f"US DHCH 20.1", us_levels, test_strategy, 'UnitDPqueries', secondary_schema)

def countPR_DHCPScalars():
    main_schema = SchemaMaker.fromName("DHCP_SCHEMA")
    secondary_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_DHCP)
    test_strategy = DecompTestStrategyDHCP_20220518.make(us_levels) # Identical to US DHCP strategy [due to same qs in all levs]
    printScalarCounts(f"PR DHCP 20.1", pr_levels, test_strategy, 'DPqueries', main_schema)
    printScalarCounts(f"PR DHCP 20.1", pr_levels, test_strategy, 'UnitDPqueries', secondary_schema)

def countPR_DHCHScalars():
    main_schema = SchemaMaker.fromName("DHCH_SCHEMA_TEN_3LEV")
    secondary_schema = SchemaMaker.fromName(CC.SCHEMA_UNIT_TABLE_10_TENVACSGQ)
    strat_obj = DecompTestStrategyDHCH_20220713_PR()
    test_strategy = strat_obj.make(us_levels)
    printScalarCounts(f"PR DHCH 20.1", pr_levels, test_strategy, 'DPqueries', main_schema)
    printScalarCounts(f"PR DHCH 20.1", pr_levels, test_strategy, 'UnitDPqueries', secondary_schema)

def countScalars():
    countUS_DHCPScalars()
    countUS_DHCHScalars()
    countPR_DHCPScalars()
    countPR_DHCHScalars()

if __name__ == "__main__":
    countScalars()
