######################################################
# To Run this script:
#
# cd into das_decennial/analysis/
# analysis=[path to analysis script] bash run_analysis.sh
#
# More info on analysis can be found here:
# https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/analysis/readme.md
######################################################

import das_utils as du
import os
import analysis.tools.datatools as datatools
from analysis.tools.initializetools import initializeAnalysis
import analysis.tools.sdftools as sdftools
import analysis.constants as AC
from pyspark.sql import functions as sf
from programs.schema.schema import Schema
import analysis.vignettes.toytools as toytools


if __name__ == "__main__":
    """
    It's entirely possible to take advantage of Analysis's Spark DataFrame-based functionality within the DAS.
    The error_metrics module is the spot that makes the most sense for accessing Analysis.

    The steps to prepare the data for use is:
    1. Use analysis.tools.datatools.rdd2df to transform the RDD of GeounitNode objects into the histogram form
       required by Analysis
    2. Use the tools to analyze the data
        - analysis.tools.crosswalk is used to access and join the GRFC to the data for geolevel aggregation support
        - analysis.tools.sdftools is the location of most of the Spark DataFrame operations for answering queries
          and calculating a wide variety of metrics
    """
    # Note that this script is not running the DAS; it's only used to demostrate that it's possible to
    # take an RDD of GeounitNodes (as is typically found in the DAS) and use Analysis to calculate metrics

    # since we're not in the DAS in this script, we will use setuptools to extract the SparkSession object
    ################################################################
    # Set the save_location to your own JBID (and other folder(s))
    # it will automatically find your JBID
    # if something different is desired, just pass what is needed
    # into the setuptools.setup function.
    ################################################################
    jbid = os.environ.get('JBID', 'temp_jbid')
    save_folder = "analysis_results/"

    save_location = du.addslash(f"{jbid}/{save_folder}")

    spark_loglevel = "ERROR"
    analysis = initializeAnalysis(save_location=save_location, spark_loglevel=spark_loglevel)

    # save the analysis script?
    # toggle to_linux=True|False to save|not save this analysis script locally
    # toggle to_s3=True|False to save|not save this analysis script to s3
    analysis.save_analysis_script(to_linux=False, to_s3=False)

    # save/copy the log file?
    analysis.save_log(to_linux=False, to_s3=False)

    # zip the local results to s3?
    analysis.zip_results_to_s3(flag=False)

    spark = analysis.spark



    # build an example schema
    schema = Schema("example", ['a', 'b', 'c'], (2,3,5))
    sdftools.print_item(schema, "Toy example Schema")

    # build a set of GeounitNodes to use
    geocodes = ['000', '001', '002', '003', '010', '011', '012', '020', '022']
    geocode_dict = {3: 'block', 2: 'county'}

    # build geounits
    geounits = toytools.getToyGeounitData_GeounitNode(schema, geocodes, geocode_dict, raw_params={'low': 0, 'high': 100})

    rdd = spark.sparkContext.parallelize(geounits).persist()

    sdftools.print_item(rdd.take(1), "One of the toy example geounits")

    # use Analysis to transform the rdd of geounitnodes into a spark dataframe
    df = datatools.rdd2df(rdd, schema)
    sdftools.print_item(df, "Toy example DF", 300)


    # aggregate geolevels
    df = df.withColumn("block", sf.col(AC.GEOCODE)[0:3]).persist()
    df = df.withColumn("county", sf.col(AC.GEOCODE)[0:2]).persist()
    df = df.withColumn("nation", sf.col(AC.GEOCODE)[0:1]).persist()
    sdftools.show(df, "df with geolevel crosswalk columns")
    df = sdftools.aggregateGeolevels(spark, df, ['block', 'county', 'nation'])
    sdftools.show(df, "df after geolevel aggregation", 1000)

    # answer total query
    qdf = sdftools.answerQuery(df, schema, "total", labels=False, merge_dims=False)
    sdftools.show(qdf, "Query df with the query 'total'", 1000)

    # calculate the L1
    qdf = sdftools.getL1(qdf)
    qdf = qdf.orderBy([AC.GEOLEVEL, AC.GEOCODE, AC.QUERY])
    sdftools.show(qdf, "L1", 1000)

    # Calculate the quantiles for each (geolevel, query) group for the orig, protected, and L1 columns
    # i.e. look at the distributions of geocode/geounit (orig, protected, L1) values for each unique (geolevel, query) tuple
    columns = [AC.ORIG, AC.PRTCTD, "L1"]

    groupby = [AC.GEOLEVEL, AC.QUERY]
    quantiles = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    qdf = sdftools.getGroupQuantiles(qdf, columns, groupby, quantiles)
    qdf = qdf.orderBy([AC.GEOLEVEL, AC.QUERY, 'quantile'])
    sdftools.show(qdf, "Quantiles", 1000)
