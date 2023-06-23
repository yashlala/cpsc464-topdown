# Example:

import os, sys
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from das_utils import clearPath

from das_framework.ctools.s3 import s3open
import programs.s3cat as s3cat

def makeDictRow(rddElem):
    row = {}
    for index, elem in enumerate(rddElem):
        row[f"{index}"] = elem
    return row

def unpackDPqueries(node):
    dpQueries = []
    for dpq_name in node.dp_queries.keys():
        if node.geocode != "":
            dpQueries.append( (dpq_name, str(node.geocode), int(node.dp_queries[dpq_name].DPanswer[0]), int(node.dp_queries[dpq_name].DPanswer[1])) )
        else:
            dpQueries.append( (dpq_name, "0", int(node.dp_queries[dpq_name].DPanswer[0]), int(node.dp_queries[dpq_name].DPanswer[1])) )
    return dpQueries

def saveHeader(path, var_list):
    """Saves header to the requested S3 location. This header will then be combined with the contents by the s3cat command"""
    with s3open(path, "w", fsync=True) as f:
        f.write("|".join(var_list))
        f.write("\n")

def convertNodesRDDToJSON(spark_session, input_path, output_path):
    rdd = spark_session.sparkContext.pickleFile(input_path)
    rdd = rdd.map(lambda zippedNode: zippedNode.unzipNoisy())
    rdd = rdd.flatMap(lambda node: unpackDPqueries(node))
    schema = StructType([
                    StructField("DPQueryName", StringType(), True),
                    StructField("geocode", StringType(), True),
                    StructField("Vacant", IntegerType(), True),
                    StructField("Occupied", IntegerType(), True)
                ])
    df = rdd.map(lambda rddElem: Row(**makeDictRow(rddElem))).toDF()
    """
    df = df.withColumn("DPQueryName", col("0").cast("String"))
    df = df.withColumn("geocode", col("1").cast("String"))
    df = df.withColumn("Vacant", col("2").cast("String"))
    df = df.withColumn("Occupied", col("3").cast("String"))
    df = df.drop("0").drop("1").drop("2").drop("3")
    """
    #df = df.coalesce(1)
    df.write.csv(output_path, mode="overwrite", header="false", sep="|")

    var_list = ["DPQueryName", "geocode", "vacant", "Occupied"]
    saveHeader(output_path+'1_header', var_list)

    s3cat.s3cat(output_path[:-1],   # s3cat expects no trailing /
                demand_success=True,
                suffix="",
                verbose=True)

    """
    # To collect and inspect node contents (for debugging; use small geolevels only):
    nodesList = sorted(rdd.collect(), key=lambda node: node.geocode)
    #print(f"Unpickled nodes: {nodesList}")
    for node in nodesList:
        #unzippedNode = node.unzipNoisy()
        #for dpq_name in unzippedNode.dp_queries.keys():
        for dpq_name in node.dp_queries.keys():
            print(f"Geocode: {node.geocode}, DP query: {dpq_name} -> {node.dp_queries[dpq_name].DPanswer}")
    """

def main():
    spark_session = SparkSession.builder.getOrCreate()
    # Example:
    # fiery-ITE-MASTER:hadoop@ip-{IP_NAME}$ aws s3 ls ${DAS_S3ROOT}/users/user007/cnstatDdpSchema_SinglePassRegular_nat_H1_Only_withMeasurements_v8/noisy_measurements-eps0.5-run1/
    #                          PRE application_1586267880679_0010-Block.pickle/
    #                          PRE application_1586267880679_0010-Block_Group.pickle/
    #                          PRE application_1586267880679_0010-County.pickle/
    #                          PRE application_1586267880679_0010-National.pickle/
    #                          PRE application_1586267880679_0010-State.pickle/
    #                          PRE application_1586267880679_0010-Tract.pickle/
    #                          PRE application_1586267880679_0010-Tract_Group.pickle/
    #           2020-04-07 20:27:01       6386 application_1586267880679_0010-bylevel_pickled_rdds.config

    geolevels = ["National", "State", "County", "Tract_Group", "Tract", "Block_Group", "Block"]
    for index, geolevel in enumerate(geolevels):
        print(f"Converting measurements for geolevel # {index} {geolevel} in {geolevels}")
        app_id = "application_1586267880679_0010"
        eps = "0.5"
        run = "1"
        base_path = "${DAS_S3ROOT}/users/user007/cnstatDdpSchema_SinglePassRegular_nat_H1_Only_withMeasurements_v8/"
        input_path = base_path + f"noisy_measurements-eps{eps}-run{run}/"
        input_path += f"{app_id}-{geolevel}.pickle/"

        output_path = base_path + f"noisy_measurements-eps{eps}-run{run}/"
        output_path += f"{app_id}-{geolevel}.csv/"

        print(f"Trying to clear & then write to: {output_path}")
        clearPath(output_path)

        convertNodesRDDToJSON(spark_session, input_path, output_path)

if __name__ == "__main__":
    main()
