# Example: spark-submit user007_checkStateTotal_and_GQHH_Invariants.py

import os, sys
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions

TEST = False # Test on small RI-only subset of CEF & MDF data
TEST_SIZE = 20 # If TEST is false, this is still used for printing limits

#persons_csv_mdf_s3_path = "${DAS_S3ROOT}/users/user007/DemonstrationProducts_Sept2019_fixedTotals_BAK2/full_person_unpickled/td4/run_0000/persons.csv"
persons_csv_mdf_s3_path = "${DAS_S3ROOT}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_National_dpQueries1_officialCNSTATrun/data-run1.0-epsilon4.0-DHCP_MDF.txt"

def generateUdf(target):
    def levelFlag(level):
        return 1 if str(level)==str(target) else 0
    return pyspark.sql.functions.udf(levelFlag, IntegerType())

def getCEFUnitsDF(spark):
    if TEST:
        df = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table10/ri44.txt", header=True, sep='\\t')
    else:
        df = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table10/", header=True, sep='\\t')
    df.show(n=TEST_SIZE)
    print(f"CEF units df as initially read (after dropped cols) ^^^ :: {df.schema}")

    if TEST:
        df = df.limit(TEST_SIZE).persist()
        df.show(n=TEST_SIZE)
        print(f"CEF units df limited to {TEST_SIZE} rows ^^^ :: {df.schema}")

    for target in range(9): # Add binary flag columns for each level of HHGQ
        df = df.withColumn(f"level{target}", generateUdf(target)(df['hhgq']))
    df.show(n=TEST_SIZE)
    print(f"df with binary flags added ^^^ :: {df.schema}")

    df = df.groupBy(df["geocode"]).sum()
    df.show(n=TEST_SIZE)
    print(f"CEF units df after summing ^^^ :: {df.schema}")

    #df = df.filter(df["sum(level3)"] >= 1)
    return df

def getDHCPPersonsDF(spark):
    df = spark.read.csv(persons_csv_mdf_s3_path, header=True, sep='|', comment='#')
    df = df.drop("SCHEMA_TYPE_CODE", "SCHEMA_BUILD_ID", "EPNUM", "RTYPE", "RELSHIP", "QSEX", "QAGE", "CENHISP", "CENRACE", "CITIZEN", "LIVE_ALONE")
    if TEST:
        df = df.filter(df["TABBLKST"] == "44")
    df = df.withColumn("geocode", pyspark.sql.functions.concat_ws('', df.TABBLKST, df.TABBLKCOU, df.TABTRACTCE, df.TABBLKGRPCE, df.TABBLK))
    df = df.drop("TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK")
    df.show(n=TEST_SIZE)
    print(f"DHCP MDF df after merging to single geocode col ^^^ :: {df.schema}")

    if TEST:
        df = df.limit(TEST_SIZE).persist()
        df.show(n=TEST_SIZE)
        print(f"DHCP MDF df limited to {TEST_SIZE} rows ^^^ :: {df.schema}")

    for target in ['000','101','201','301','401','501','601','701']: # Add binary flag columns for each level of HHGQ
        df = df.withColumn(f"level{target}", generateUdf(target)(df['GQTYPE']))
    df.show(n=TEST_SIZE)
    print(f"DHCP MDF df with binary flags added ^^^ :: {df.schema}")

    df = df.groupBy(df["geocode"]).sum()
    df.show(n=TEST_SIZE)
    print(f"DHCP MDF df after summing ^^^ :: {df.schema}")
    return df

def findLBViolations(joinedDF):
    levelMap = {}
    #levelMap['000'] = ['0','1']
    levelMap['101'] = '2'
    levelMap['201'] = '3'
    levelMap['301'] = '4'
    levelMap['401'] = '5'
    levelMap['501'] = '6'
    levelMap['601'] = '7'
    levelMap['701'] = '8'

    for gqtype in ['101', '201', '301', '401', '501', '601', '701']:
        print(f"Checking by block whether # Persons in gqtype {gqtype} in MDF is >= # GQ facilities in Table 10 of type {levelMap[gqtype]}")
        # gqtype is MDF count/var; must be >= its levelMap[gqtype] facility type in input CEF Table10
        violations = joinedDF.filter(joinedDF[f"sum(level{gqtype})"] < joinedDF[f"sum(level{levelMap[gqtype]})"])
        numViolations = violations.count()
        print(f"# violations detected: {numViolations}")

def findUBViolations(joinedDF):
    levelMap = {}
    levelMap['000'] = ['0','1']
    levelMap['101'] = '2'
    levelMap['201'] = '3'
    levelMap['301'] = '4'
    levelMap['401'] = '5'
    levelMap['501'] = '6'
    levelMap['601'] = '7'
    levelMap['701'] = '8'

    # 3-digit codes are MDF (persons); 1-digit codes are Table 10 CEF input extract (units)

    print(f"Checking if 99 * #Housing Units >= # Ppl in Housing Units")
    violationsDF = joinedDF.filter(99 * (joinedDF["sum(level0)"] + joinedDF["sum(level1)"]) < joinedDF["sum(level000)"])
    print(f"Found # violations {violationsDF.count()}")

    print(f"Checking if no Housing Units => no ppl in housing units")
    noHHsDF = joinedDF.filter(joinedDF["sum(level0)"] + joinedDF["sum(level1)"] == 0)
    print(f"Found # geocodes w/ no housing units: {noHHsDF.count()}")
    noPeopleDF = noHHsDF.filter(joinedDF["sum(level000)"] == 0)
    print(f"Subset of those w/ no ppl of type (should be all): {noPeopleDF.count()}")

    for gqtype in ['101', '201', '301', '401', '501', '601', '701']:
        print(f"Checking by block whether # Persons in gqtype {gqtype} in MDF is 0 if # GQ facilities in Table 10 of type {levelMap[gqtype]} is 0")
        noFacilitiesDF = joinedDF.filter(joinedDF[f"sum(level{levelMap[gqtype]})"] == 0)
        print(f"Found # geocodes w/ no facilities of type: {noFacilitiesDF.count()}")
        noPeopleDF = noFacilitiesDF.filter(joinedDF[f"sum(level{gqtype})"] == 0)
        print(f"Subset of those w/ no ppl of type (should be all): {noPeopleDF.count()}")

def checkStateTotalPops(spark):
    print("Lastly, checking state total pops against 2010 AFF counts. 2010 counts (ST/FIPS/POP) were:")
    fips = ["02","01","05","04","06","08","09","11","10","12","13","15","19","16","17","18","20","21","22","25","24","23","26","27","29","28","30","37","38","31","33","34","35","32","36","39","40","41","42","72","44","45","46","47","48","49","51","50","53","55","54","56"]
    abbrs = ["ak","al","ar","az","ca","co","ct","dc","de","fl","ga","hi","ia","id","il","in","ks","ky","la","ma","md","me","mi","mn","mo","ms","mt","nc","nd","ne","nh","nj","nm","nv","ny","oh","ok","or","pa","pr","ri","sc","sd","tn","tx","ut","va","vt","wa","wi","wv","wy"]
    affPops = {}
    affPops["ak"] = 710231
    affPops["al"] = 4779736
    affPops["ar"] = 2915918
    affPops["az"] = 6392017
    affPops["ca"] = 37253956
    affPops["co"] = 5029196
    affPops["ct"] = 3574097
    affPops["dc"] = 601723
    affPops["de"] = 897934
    affPops["fl"] = 18801310
    affPops["ga"] = 9687653
    affPops["hi"] = 1360301
    affPops["ia"] = 3046355
    affPops["id"] = 1567582
    affPops["il"] = 12830632
    affPops["in"] = 6483802
    affPops["ks"] = 2853118
    affPops["ky"] = 4339367
    affPops["la"] = 4533372
    affPops["ma"] = 6547629
    affPops["md"] = 5773552
    affPops["me"] = 1328361
    affPops["mi"] = 9883640
    affPops["mn"] = 5303925
    affPops["mo"] = 5988927
    affPops["ms"] = 2967297
    affPops["mt"] = 989415
    affPops["nc"] = 9535483
    affPops["nd"] = 672591
    affPops["ne"] = 1826341
    affPops["nh"] = 1316470
    affPops["nj"] = 8791894
    affPops["nm"] = 2059179
    affPops["nv"] = 2700551
    affPops["ny"] = 19378102
    affPops["oh"] = 11536504
    affPops["ok"] = 3751351
    affPops["or"] = 3831074
    affPops["pa"] = 12702379
    affPops["pr"] = 3725789
    affPops["ri"] = 1052567
    affPops["sc"] = 4625364
    affPops["sd"] = 814180
    affPops["tn"] = 6346105
    affPops["tx"] = 25145561
    affPops["ut"] = 2763885
    affPops["va"] = 8001024
    affPops["vt"] = 625741
    affPops["wa"] = 6724540
    affPops["wi"] = 5686986
    affPops["wv"] = 1852994
    affPops["wy"] = 563626

    df = spark.read.csv(persons_csv_mdf_s3_path, header=True, sep='|', comment='#')
    for (fip, abbr) in zip(fips, abbrs):
        stateCount = df.filter(df["TABBLKST"] == fip).count()
        print(f"{fip} for {abbr} has # ppl in MDF vs AFF 2010: {stateCount} vs {affPops[abbr]}")

def main():
    print("Initiating GQ/Housing Units invariant checker (developed for late 2019 Demo Products 'manual' QA).")
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    unitsDF = getCEFUnitsDF(spark)
    personsDF = getDHCPPersonsDF(spark)

    unitsDF.show(n=TEST_SIZE)
    unitsDFSize = unitsDF.count()
    print(f"unitsDF has # recs {unitsDFSize} ======= ^^^ :: {unitsDF.schema}")
    personsDF.show(n=TEST_SIZE)
    personsDFSize = personsDF.count()
    print(f"personsDF has # recs {personsDFSize} ======= ^^^ :: {personsDF.schema}")

    joinedDF = personsDF.join(unitsDF, 'geocode', 'left_outer').na.fill(0).persist()
    joinedDF.show(n=TEST_SIZE)
    joinedDFSize = joinedDF.count()
    print(f"joinedDF has # recs {joinedDFSize} ====== ^^^ :: {joinedDF.schema}")

    findLBViolations(joinedDF)
    findUBViolations(joinedDF)

    checkStateTotalPops(spark)

if __name__ == "__main__":
    main()
