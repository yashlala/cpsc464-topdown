# Example:

import os, sys
from typing import Dict, Iterable

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))
from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
from operator import add
from das_framework.ctools.s3 import s3open
import numpy as np

TEST_SIZE = 25


def makeDictRow(rddElem) -> Dict:
    row = {}
    for index, elem in enumerate(rddElem):
        row[f"{index}"] = elem
    return row


def unpackDPqueries(node):
    dpQueries = []
    for dpq_name in node.dp_queries.keys():
        if node.geocode != "":
            dpQueries.append((dpq_name, str(node.geocode), int(node.dp_queries[dpq_name].DPanswer[0]),
                              int(node.dp_queries[dpq_name].DPanswer[1])))
        else:
            dpQueries.append(
                (dpq_name, "0", int(node.dp_queries[dpq_name].DPanswer[0]), int(node.dp_queries[dpq_name].DPanswer[1])))
    return dpQueries


def saveHeader(path: str, var_list: Iterable) -> None:
    """Saves header to the requested S3 location. This header will then be combined with the contents by the s3cat command"""
    with s3open(path, "w", fsync=True) as f:
        f.write("|".join(var_list))
        f.write("\n")


def sparseDict(dense):
    sparse_dict = {}
    for index, count in np.ndenumerate(dense):
        if count > 0:
            sparse_dict[index] = count
    return sparse_dict


def splitGeocode(gc):
    split_geocode = (gc[:2], gc[2:5], gc[5:11], gc[11:12], gc[12:])
    return split_geocode


def generateUdf_tot(target):
    def levelFlag(level) -> int:
        return 1

    return F.udf(levelFlag, IntegerType())


def generateUdf(target):
    def levelFlag(level) -> int:
        return 1 if str(level) == str(target) else 0

    return F.udf(levelFlag, IntegerType())


def getCEFUnitsDF(spark: SparkSession) -> DataFrame:
    df = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table10/", header=True, sep='\\t')
    df.show(n=TEST_SIZE)
    print(f"CEF units df as initially read (after dropped cols) ^^^ :: {df.schema}")

    for target in range(9):  # Add binary flag columns for each level of HHGQ
        df = df.withColumn(f"level{target}", generateUdf(target)(df['hhgq']))
    df.show(n=TEST_SIZE)
    print(f"df with binary flags added ^^^ :: {df.schema}")

    df = df.groupBy(df["geocode"]).sum()
    df.show(n=TEST_SIZE)
    print(f"CEF units df after summing ^^^ :: {df.schema}")
    return df


def getCEFPersonsDF(spark: SparkSession) -> DataFrame:
    df = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table8/ri44.txt", header=True, sep='\\t')
    df.show(n=TEST_SIZE)
    print(f"CEF persons df as initially read (after dropped cols) ^^^ :: {df.schema}")

    for target in range(2):  # Add binary flag columns for each level of HHGQ
        df = df.withColumn(f"level{target}", generateUdf(target)(df['voting']))
    df.show(n=TEST_SIZE)
    print(f"df with binary flags added ^^^ :: {df.schema}")

    df = df.groupBy(df["geocode"]).sum()
    df.show(n=TEST_SIZE)
    print(f"CEF persons df after summing ^^^ :: {df.schema}")
    return df


def getCEFPersonsDF_total(spark: SparkSession) -> DataFrame:
    df = spark.read.csv("${DAS_S3INPUTS}/title13_input_data/table8/", header=True, sep='\\t')
    df.show(n=TEST_SIZE)
    print(f"CEF persons df as initially read (after dropped cols) ^^^ :: {df.schema}")

    for target in range(1):  # Add binary flag columns for each level of HHGQ
        df = df.withColumn(f"level{target}", generateUdf(target)(df['voting']))
    df.show(n=TEST_SIZE)
    print(f"df with binary flags added ^^^ :: {df.schema}")

    df = df.groupBy(df["geocode"]).sum()
    df.show(n=TEST_SIZE)
    print(f"CEF persons df after summing ^^^ :: {df.schema}")
    return df


def noPeopleInOccupiedHUs(gc_cef_mdf) -> int:
    cef_units = gc_cef_mdf[1][0]
    mdf_persons = gc_cef_mdf[1][1]
    # If no MDF persons in HUs, but there are Occupied HUs in CEF, add 1
    if cef_units[0] > 0 and (mdf_persons is None or mdf_persons[0] == 0):
        return 1
    return 0


def peopleInVacantHUs(gc_cef_mdf):
    cef_units = gc_cef_mdf[1][0]
    mdf_persons = gc_cef_mdf[1][1]
    if cef_units[0] == 0:  # Return 0 if there are occupied HUs; if no occupied HUs, return # of persons
        if mdf_persons is not None:
            return mdf_persons[
                0]  # This is a measure of error: how many MDF persons are in HUs in areas with only vacant HUs?
    return 0


def vaDoesNotMatch(gc_cef_mdf) -> int:
    print(f"gc_cef_mdf: {gc_cef_mdf}")
    cef_va = gc_cef_mdf[1][0] if gc_cef_mdf[1][0] is not None else (0, 0)
    mdf_va = gc_cef_mdf[1][1] if gc_cef_mdf[1][1] is not None else (0, 0)
    va_does_not_match = 1 if abs(mdf_va[1] - cef_va[1]) > 0.00001 else 0
    return va_does_not_match
    # return abs(mdf_va[1] - cef_va[1])


def nvaDoesNotMatch(gc_cef_mdf) -> int:
    cef_nva = gc_cef_mdf[1][0] if gc_cef_mdf[1][0] is not None else (0, 0)
    mdf_nva = gc_cef_mdf[1][1] if gc_cef_mdf[1][1] is not None else (0, 0)
    nva_does_not_match = 1 if abs(mdf_nva[0] - cef_nva[0]) > 0.00001 else 0
    return nva_does_not_match
    # return abs(mdf_nva[0] - cef_nva[0])


def l1Error(gc_cef_mdf) -> float:
    cef = np.array(gc_cef_mdf[1][0])
    mdf = np.array(gc_cef_mdf[1][1]) if gc_cef_mdf[1][1] is not None else np.zeros(cef.shape)
    return float(np.sum(np.abs(mdf - cef)))


def convertNodesRDDToMDF(spark_session: SparkSession, input_path: str, schema_name: str) -> None:
    ########
    # Retrieve CEF units data; convert to RDD on len-9 HHGQ vector (includes vacant HUs count)
    ########

    cef_units_df = getCEFUnitsDF(spark_session)
    cef_rdd = cef_units_df.rdd.map(lambda row: (row[0], np.array([k for k in row[1:]])))

    # Retrieve MDF persons pickled data; convert to dense len-8 HHGQ vector (does not include vacant HUs; is Persons count)
    mdf_rdd = spark_session.sparkContext.pickleFile(input_path)
    mdf_rdd = mdf_rdd.map(lambda node_dict: (node_dict["geocode"], node_dict["syn"].toDense().sum((1, 2, 3))))

    # Join CEF units, MDF persons for error calculations
    cef_mdf_rdd = cef_rdd.leftOuterJoin(mdf_rdd)

    # Check that, summing over all blocks w/ 0 Occupied HUs in CEF, there is at least 1 syn Person in HUs
    persons_with_no_occupied_HUs_error = cef_mdf_rdd.map(lambda gc_cef_mdf: peopleInVacantHUs(gc_cef_mdf)).reduce(add)
    # Check # blocks w/ >0 Occupied HUs in CEF, but 0 Persons in HUs in MDF
    num_blocks_with_mdf_persons_but_no_hus_error = cef_mdf_rdd.map(
        lambda gc_cef_mdf: noPeopleInOccupiedHUs(gc_cef_mdf)).reduce(add)

    print(
        f"# of persons in HUs in MDF, in blocks with only Vacant HUs in the CEF: {persons_with_no_occupied_HUs_error}")
    print(
        f"# blocks w/ no persons in HUs in MDF, but >0 occupied HUs in CEF: {num_blocks_with_mdf_persons_but_no_hus_error}")
    assert persons_with_no_occupied_HUs_error > 0
    assert num_blocks_with_mdf_persons_but_no_hus_error > 0

    ########
    # Check in at least 1 block, number Voting Age persons in MDF disagrees with CEF by at least 1
    ########

    cef_persons_df = getCEFPersonsDF(spark_session)
    cef_persons_df.show(n=20)
    print("First 20 records of cef_persons df ^^^^")

    cef_persons_rdd = cef_persons_df.rdd.map(lambda row: (row[0], np.array([k for k in row[1:]])))
    mdf_rdd = spark_session.sparkContext.pickleFile(input_path)
    mdf_rdd = mdf_rdd.map(lambda node_dict: (node_dict["geocode"], node_dict["syn"].toDense().sum((0, 2, 3))))

    cef_mdf_rdd = cef_persons_rdd.leftOuterJoin(mdf_rdd)

    # num_blocks_where_mdf_NVA_does_not_match_CEF_NVA = cef_mdf_rdd.map(lambda gc_cef_mdf: nvaDoesNotMatch(gc_cef_mdf)).reduce(add)
    # num_blocks_where_mdf_VA_does_not_match_CEF_VA = cef_mdf_rdd.map(lambda gc_cef_mdf: vaDoesNotMatch(gc_cef_mdf)).reduce(add)
    # print(f"CEF, MDF NVA block-level # blocks disagreement: {num_blocks_where_mdf_NVA_does_not_match_CEF_NVA}")
    # print(f"CEF, MDF VA block-level # blocks disagreement: {num_blocks_where_mdf_VA_does_not_match_CEF_VA}")
    # assert num_blocks_where_mdf_NVA_does_not_match_CEF_NVA > 0
    # assert num_blocks_where_mdf_VA_does_not_match_CEF_VA > 0

    VA_NVA_l1_error = cef_mdf_rdd.map(lambda gc_cef_mdf: l1Error(gc_cef_mdf)).reduce(add)
    print(f"CEF, MDF VA-NVA block-level L1 error: {VA_NVA_l1_error}")
    assert VA_NVA_l1_error > 0

    ########
    # Check in at least 1 block, Block Total Pop in MDF disagrees with CEF by at least 1
    ########
    cef_persons_df = getCEFPersonsDF_total(spark_session)
    cef_persons_rdd = cef_persons_df.rdd.map(lambda row: (row[0], np.array([k for k in row[1:]])))

    mdf_rdd = spark_session.sparkContext.pickleFile(input_path)
    mdf_rdd = mdf_rdd.map(lambda node_dict: (node_dict["geocode"], node_dict["syn"].toDense().sum((0, 1, 2, 3))))

    cef_mdf_rdd = cef_persons_rdd.leftOuterJoin(mdf_rdd)
    l1_error_block_total_pop = cef_mdf_rdd.map(lambda gc_cef_mdf: l1Error(gc_cef_mdf)).reduce(add)
    print(f"CEF, MDF Tot Pop block-level L1 error: {l1_error_block_total_pop}")
    assert l1_error_block_total_pop > 0


def main() -> None:
    """ runs examples:

        ${DAS_S3ROOT}/runs/tests/full_uspr/user00709301515/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-BlockNodeDicts
    """
    input_path = "${DAS_S3ROOT}/runs/tests/full_uspr/user00709301515/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-BlockNodeDicts"
    schema_name = "PL94_2020_SCHEMA"

    spark_session = SparkSession.builder.getOrCreate()
    convertNodesRDDToMDF(spark_session, input_path, schema_name)


if __name__ == "__main__":
    main()
