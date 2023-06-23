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
from pyspark.sql import functions as sf
from programs.schema.schema import Schema
import analysis.vignettes.toytools as toytools
import analysis.constants as AC


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
    geounits = toytools.getToyGeounitData(schema, geocodes, geocode_dict)

    rdd = spark.sparkContext.parallelize(geounits).persist()

    sdftools.print_item(rdd.take(1), "One of the toy example geounits")

    # use Analysis to transform the rdd of geounitnodes into a spark dataframe
    df = datatools.rdd2df(rdd, schema)
    sdftools.print_item(df, "Toy example DF", 300)

    # perform analyses
    # L1
    df = sdftools.getL1(df, colname="L1_cell", col1=AC.PRTCTD, col2=AC.ORIG)
    sdftools.print_item(df, "Toy example L1", 300)

    # adding in a simple row-counting column
    df = df.withColumn("row_count", sf.lit(1)).persist()
    sdftools.print_item(df, "Totals + rowcounter column")

    # total within each geocode
    df = sdftools.answerQuery(df, schema, "total", labels=False)
    sdftools.print_item(df, "Totals within each geounit", 300)

    # L1 of total
    df = sdftools.getL1(df, colname="L1_total", col1=AC.PRTCTD, col2=AC.ORIG)
    sdftools.print_item(df, "Totals + rowcounter column + L1")
