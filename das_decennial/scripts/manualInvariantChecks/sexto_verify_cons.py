# Example: spark-submit sexto_verify_cons.py

# sexto's original notes: Hopefully copied all the correct code bits over. You mignt run into a misplaced paranthesis or two trying to replicate if a grabbing the wrong execution line. But this should be enough to give people a general idea of how I carried out the checks.
#
# Running cmds interactively from pyspark

# 4/14/2020, user007 notes: switched this from copy/paste interactive code to a script that can be run stand-alone

# 5/12/2020, user007 notes: to help Some Human translate this to sparkSQL for use in TRR, went through these checks in detail, making notes in comments about how they work and what they compute. Also made notes where spec versions etc were unclear or appear dated.

from operator import add

import os, sys
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

RUN_DF_PER_CHECKS = False
RUN_DF_UNITS_CHECKS = True
H1_ONLY = True

dfper_s3_path = "${DAS_S3ROOT}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_National_dpQueries1_officialCNSTATrun/data-run1.0-epsilon4.0-DHCP_MDF.txt"
# Old example: "${DAS_S3ROOT}/users/user007/DemonstrationProducts_Sept2019_fixedTotals_BAK2/full_person_unpickled/td4/run_0000/persons.csv"

dfunits_s3_path = "${DAS_S3ROOT}/users/user007/cnstatDdpSchema_SinglePassRegular_nat_H1_Only_withMeasurements_v8/MDF_UNIT-run1.0-epsilon0.5-H1.csv"

if RUN_DF_PER_CHECKS:
    # Loads the Units table extract from the CEF
    # Note that the original table10 is old; downloading and inspecting a part file, it has variables:
    #   #MAFID  geocode hhgq
    # where hhgq should match hhgq in http://{HOST_NAME}/mediawiki/index.php/Tables_Needed(to_be_extracted_from_CEF)#Table_10:_PL94_Housing_File (actually, full schema should match; link is to schema for original Table10, it seems)
    cefu = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table10/", header=True, sep='\\t')
    # Converts to rdd of nested tuples ((geocode, hhgq), 1), where hhgq is a 9-level variable indicating record's housing type
    # Note that the '1' will just be used for counting how many records exist of a given type, when aggregating
    # Then reduceByKey's to convert to ((geocode, hhgq), #_of_hhgqs_of_type) tuples, one per (geocode, hhgq)
    cefu = cefu.rdd.map(lambda row: ((row["geocode"], row["hhgq"]), 1)).reduceByKey(add)

    # Loads Person MDF; for schema, see https://{GIT_HOST_NAME}/CB-DAS/etl_2020/tree/master/mdf
    dfper = spark.read.csv(dfper_s3_path, header=True, sep='|', comment='#')
    # Converts to rdd of nested tuples ((geocode, GQTYPE), 1), where GQTYPE is a 29-level variable indicating record's GQ type
    # Then reduceByKey's to convert to ((geocode, GQTYPE), #_of_persons_in_GQs_of_type) tuples, one per (geocode, GQTYPE)
    dfper = dfper.rdd.map(lambda row: ((row["TABBLKST"]+row["TABBLKCOU"]+row["TABTRACTCE"]+row["TABBLKGRPCE"]+row["TABBLK"], row["GQTYPE"]),1)).reduceByKey(add)

    levelMap = {}
    levelMap['0'] = '000' # Occupied HU: corresponds to not-in-universe level for MDF GQTYPE
    levelMap['1'] = '000' # Vacant HU: corresponds to not-in-universe level for MDF GQTYPE
    levelMap['2'] = '101' # And the first 6 major GQ types by *-01 3-digit code (ignores remaining 3-digit codes, which were not present in 2019 CNSTAT DDP release, i.e., when this code was initially authored)
    levelMap['3'] = '201'
    levelMap['4'] = '301'
    levelMap['5'] = '401'
    levelMap['6'] = '501'
    levelMap['7'] = '601'
    levelMap['8'] = '701'

    # Converts gq cef codes to match gq mdf codes (as of 2019 CNSTAT DDP release)
    cefu=cefu.map(lambda x: ((x[0][0],levelMap[x[0][1]]),x[1]))
    # reduceByKey again to combine distinct ((geocode, '000'), count) tuples after levelMap
    cefu=cefu.reduceByKey(add)

    # Join MDF to CEF on (geocode, GQTYPE), retaining MDF tuples that do not find a CEF match
    # Elements now have the form ((geocode, GQTYPE), (mdf_persons_count, cef_units_count))
    test = dfper.leftOuterJoin(cefu)

    # test that gq lb holds. should return []
    # Removes HUs (NIUs / 000s), then filters to elements with fewer MDF persons than CEF GQ units
    # If the result is empty, then we always had at least one person per GQ unit in each geocode
    verifyList = test.filter(lambda x: x[0][1]!='000').filter(lambda x: x[1][0] < x[1][1]).collect()
    print(f"gq lb holds if this is empty: {verifyList}")

    # test that hhgq ub holds. should return []
    # Filters to (geocode, GQTYPE) where there are no CEF units of GQTYPE
    # Then filters to (geocode, GQTYPE) where there is at least 1 MDF person in units of GQTYPE in geocode
    # If this is empty, then we never have a person in a unit of GQTYPE when there are no units of GQTYPE
    verifyList = test.filter(lambda x: x[1][1] == 0).filter(lambda x: x[1][0] > 0).collect()
    print(f"hhgq ub holds if this is empty: {verifyList}")

    # test that 99 * # units >= # people in hh. should return []
    # Filters to just HUs (NIUs / 000s)
    # Then filters to (geocode, 000)'s that have more than 99 persons per HU
    # Empty if there are no geocodes with too many people per HU
    verifyList = test.filter(lambda x: x[0][1]=="000").filter(lambda x: x[1][0] > 99 * x[1][1]).collect()
    print(f"ii*#units >= #ppl_in_hh holds if this is empty: {verifyList}")

    print("Persons file verification is complete.")

if RUN_DF_UNITS_CHECKS:
    # Housing invariant checks.

    # Loads the Units table extract from the CEF, with schema http://{HOST_NAME}/mediawiki/index.php/Tables_Needed(to_be_extracted_from_CEF)#Table_10:_PL94_Housing_File
    # (actually, schema is a bit larger; this CEF extract contains: #MAFID    TEN    VACS    gqtype    geocode)
    # whereas schema is for original Table10
    cefu = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table10_20190610/", sep="\t", header=True)
    # For use with full-schema MDF: (uses all GQTYPEs)
    # Converts to rdd of nested tuples ((geocode, GQTYPE), 1), where GQTYPE is as defined in MDF spec
    # Then reduceByKey's to count number of each GQTYPE Unit in each geocode
    #cefu = cefu.rdd.map(lambda row: ((row["geocode"], row["gqtype"]), 1)).reduceByKey(add)

    # For use with H1-only MDF: (filters CEF to just HUs)
    # Filters to just HUs, then reduceByKey's to count number of HUs in each geocode
    cefu = cefu.rdd.map(lambda row: ((row["geocode"], row["gqtype"]), 1)).filter(
                        lambda row: row[0][1] == "000").map(
                        lambda row: (row[0][0], row[1]) ).reduceByKey(add)
    cefuEx = cefu.take(1)
    print(f"cefuEx: {cefuEx}")

    # Reads MDF Households file; spec as given in https://{GIT_HOST_NAME}/CB-DAS/etl_2020/tree/master/mdf (possibly not most recent spec; depends on input MDF)
    #dfhh = spark.read.csv("s3://uscb-decennial-ite-das/DHC_DemonstrationProduct_Fixed/DHCH_BAK3_unpickled/td2/run_0000/MDF_UNIT.txt", sep="|", header=True, comment="#") # Example full CNSTAT DDP MDF
    dfhh = spark.read.csv(dfunits_s3_path, sep="|", header=True, comment="#")

    # For full-spec MDF: (counts number of Units of each GQTYPE in each geocode)
    #dfhh = dfhh.rdd.map(lambda row: ((row["TABBLKST"]+row["TABBLKCOU"]+row["TABTRACTCE"]+row["TABBLKGRPCE"]+row["TABBLK"], row["GQTYPE"]),1)).reduceByKey(add)
    #dfhh = dfhh.rdd.map(lambda row: ((row["geocode"], row["GQTYPE"]),1)).reduceByKey(add)

    # For H1-only MDF: (counts number of HUs in MDF Units file in each geocode)
    dfhh = dfhh.rdd.map(lambda row: (row["geocode"], int(row["vacant_count"])+int(row["occupied_count"])) ).reduceByKey(add) #For full MDF
    dfhhEx = dfhh.take(1)
    print(f"dfhhEx: {dfhhEx}")

    # Full outer joins on (geocode, GQTYPE) or just on geocode, depending on which of the above code snippets is uncommented
    # Retains elements of both CEFU and DFHH that don't have a match in the other RDD
    test = dfhh.fullOuterJoin(cefu)

    example = test.take(20)
    print(f"Units Examples: {example}")
    #raise NotImplementedError("Stop")

    def replaceNones(x):
        if x[1][1] is None:
            return (x[0], (x[1][0], 0))
        else:
            return x
    # Replaces None in elements like ((geocode, GQTYPE), (MDF_Units_Count, None)) with 0
    # (these elmemnts result when .fullOuterJoin finds >0 MDF Units with GQTYPE in geocode, but no such Units in CEF
    # (does nothing if MDF_Units_Count is None, rather than CEF_Units_Count)
    test = test.map(lambda x: replaceNones(x))
    # test that total housing units and total GQ facilities by type match. should return []
    # Filters to elements where MDF Units & CEF Units count disagree
    verifyList = test.filter(lambda x: x[1][0] != x[1][1]).collect()
    #print(f"tot HUs, GQ facilities by type are correct if this is empty: {verifyList}")
    print(f"tot HUs are correct if this is empty: {verifyList}")

    # visual inspection for no bad values in MDF UNIT.

    if not H1_ONLY:
        dfhh = spark.read.csv("s3://uscb-decennial-ite-das/DHC_DemonstrationProduct_Fixed/DHCH_BAK3_unpickled/td2/run_0000/MDF_UNIT.txt", sep="|", header=True, comment="#")

        #Skipping geocode checks
        geolevels = ["TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK"]


        # Takes several minutes to run through all of the variables. not optimized but gets the job done.
        print(f"For visual comparison to allowable values in spec :::")
        for name in dfhh.columns:
            if name not in geolevels:
                #manual check that no bad values appear according to MDF spec v4.
                print(f"Column {name} has unique values {dfhh.rdd.map(lambda row: row[name]).distinct().collect()}")

        # I might add geocode verification later but didn't want to do that visual. Something semi-automated would be better.


        # checking a few structural zeros:
        # Filters to Units with positive HHSIZE
        occupied = dfhh.filter(lambda row: int(row["HHSIZE"]) > 0)

        # Checks that RTYPE, GQTYPE, TEN, VACS == 2, 000, 9, 0 when HHSIZE > 0, where
        #   RTYPE = 2 (is a Housing Unit, not a GQ)
        #   GQTYPE = 000 (Not In Universe, i.e., is an HU not a GQ)
        #   TEN = 9  (this is not actually a valid value per MDF spec, but I think was used as a placeholder for a missing var as of the authorship of this code; presumably any non-0 code would now be acceptable)
        #   VACS = 0 (Not In universe)
        # i.e., households of size 1 must correspond to occupied housing units
        occupied.map(lambda row: (row["RTYPE"], row["GQTYPE"], row["TEN"], row["VACS"])).distinct().collect() #should only see (2,000,9,0)

        # No specific check; this was just a visual inspection of the HHSIZE x HHT combinations for impossibilities
        # Can map back to MDF spec, although this will be more complex with the 522-level HHT variable
        occupied.map(lambda row: (row["HHSIZE"], row["HHT"])).distinct().collect() # visually check no impossible household size/type pairs occur eg size=1 hht=01, married family

        # did a bunch of other sanity checks like those two will add on as time permits.

        print("Units file verification is complete.")
