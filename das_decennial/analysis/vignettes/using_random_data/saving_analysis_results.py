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
import das_framework.ctools.s3 as s3
import analysis.constants as AC

if __name__ == "__main__":
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
    sdftools.show(schema, "Toy example Schema")

    # build a set of GeounitNodes to use
    geocodes = ['000', '001', '002', '003', '010', '011', '012', '020', '022']
    geocode_dict = {3: 'block', 2: 'county'}

    # build geounits
    geounits = toytools.getToyGeounitData(schema, geocodes, geocode_dict)

    rdd = spark.sparkContext.parallelize(geounits).persist()

    sdftools.show(rdd.take(1), "One of the toy example geounits")

    # use Analysis to transform the rdd of geounitnodes into a spark dataframe
    df = datatools.rdd2df(rdd, schema)
    sdftools.show(df, "Toy example DF", 300)

    # perform analyses
    # L1
    df = sdftools.getL1(df, colname="L1_cell", col1=AC.PRTCTD, col2=AC.ORIG)
    sdftools.show(df, "Toy example L1", 300)

    # adding in a simple row-counting column
    df = df.withColumn("row_count", sf.lit(1)).persist()
    sdftools.show(df, "Totals + rowcounter column")

    # total within each geocode
    df = sdftools.answerQuery(df, schema, "total", labels=False)
    sdftools.show(df, "Totals within each geounit", 300)

    # L1 of total
    df = sdftools.getL1(df, colname="L1_total", col1=AC.PRTCTD, col2=AC.ORIG)
    sdftools.show(df, "Totals + rowcounter column + L1")



    # In this vignette, we will look at saving a variety of items using the datatools
    # module and analysis object's save_location attributes.

    # 0. A look at the Analysis and PickledDASExperiment objects and their
    #    save_location_s3 and save_location_linux paths attributes

    # Saving to S3
    # 1. Spark DataFrame / RDD as a pickle file
    # 2. Spark DF as a csv
    # 3. Spark DF as a csv, partitioning by column(s)
    # 4. Saving a list or other objects to S3
    # 5. Creating a data visualization and saving it to S3 as a pdf

    # Saving locally (to linux)
    # 6. Spark DataFrame to Pandas DF as a csv
    # 7. Saving a list or other objects locally
    # 8. Creating a data viz and saving it locally

    """
    0.

    The analysis object (and experiment object, too, in cases where we are
    analyzing pickled DAS Experiment data) has functions and attributes that
    help with saving the results of analysis.

    These tools do not need to be used in order to save the results, but they
    can assist with maintaining and organizing the results.

    Analysis has the attributes:
        save_location_s3
        save_location_linux

    These attributes provide the user with a handy directory structure
    for organizing where to save the results of THIS analysis.

    Note that each time Analysis is run, a new "mission name" and "timestamp" will be generated.

    And also note that the "save_location" provided to the setuptools.setup function creates the user and
    customized directory.

    Example:

    The S3 save directory for the Analysis object for one run of Analysis:
    "${DAS_S3INPUTS}/users/user007/analysis_reports/LITERATE_REGION_2020-01-21_0860_Title_13/"

    Breaking this down we get:

    The S3 bucket for the DAS
    "${DAS_S3INPUTS}/users/"
                                      The user's JBID and custom analysis directory (provided manually to setuptools.setup)
                                      "user007/analysis_reports/"
                                                                The mission name and timestamp autogenerated by Analysis for this run
                                                                "LITERATE_REGION_2020-01-21_0860_Title_13/"



    The Linux save directory for the Analysis object for one run of Analysis:
    "/mnt/users/user007/analysis_reports/LITERATE_REGION_2020-01-21_0860_Title_13/"

    The same breakdown occurs for the Linux path, minus the first section, which is

    The linux path for the DAS
    "/mnt/users/"


    Experiments are named and will extend the path by one more directory when they are used (i.e. when looking
    at pickled DAS output).

    As an example, if we were using an experiment here, called "my_experiment", then we would use the
    experiment's save_location_s3 and save_location_linux attributes instead, and we would get the following
    save location paths:

    S3:
    "${DAS_S3INPUTS}/users/user007/analysis_reports/LITERATE_REGION_2020-01-21_0860_Title_13/my_experiment/"

    Linux:
    "/mnt/users/user007/analysis_reports/LITERATE_REGION_2020-01-21_0860_Title_13/my_experiment/"
    """
    sdftools.show(analysis.save_location_s3, "S3 save directory")
    sdftools.show(analysis.save_location_linux, "Linux save directory")


    """
    1.

    Spark RDDs can be easily saved as pickle files using the function
    rdd.saveAsPickleFile(s3_location)

    Spark DFs can be saved as a pickle file, as well, but require
    accessing the DF's "rdd" attribute first (and repeating the steps
    above for saving an RDD).

    The pickled Spark DF's rdd can be loaded and returned to DF form
    using the df_rdd.toDF() function.
    """
    # saving rdd of geounits to s3
    savepath = analysis.save_location_s3 + "toy_data_geounits.pickle"
    sdftools.show(savepath, "S3 location of the toy geounit data in pickle form")
    rdd.saveAsPickleFile(savepath)

    # saving df's rdd of analysis results to s3
    savepath = analysis.save_location_s3 + "toy_data_df_results.pickle"
    sdftools.show(savepath, "S3 location of the toy df analysis results in pickle form")
    df.rdd.saveAsPickleFile(savepath)


    """
    Saving to S3
    2. Spark DF as a csv
    3. Spark DF as a csv, partitioning by column(s)

    Saving locally (to Linux)
    6. Spark DataFrame to Pandas DF as a csv

    Spark DataFrames are oftentimes saved as csv files. When saving to S3, the DF's can be large and when saved, they
    will appear in a folder populated with part- files that are each csvs.

    When saving locally (to Linux), we can save them to HDFS, but most of the time we will choose to transform a Spark DF
    into a Pandas DF where we can use the to_csv function to save the csv file. This should be done with care since the
    transformation from Spark DF to Pandas DF involves collecting all partition data onto the master node, which can cause
    memory overflow errors if the Spark DF is large.
    """
    ### Saving Spark DF as a csv
    # i.e. a folder in S3 with multiple csv files, one for each partition
    path = analysis.save_location_s3 + "toy_data_df_results.csv"
    sdftools.show(path, "S3 location of the toy df analysis results in csv form")
    # if we don't want to save the header, then set it as false (or just remove the option call)
    df.write.option("header", "true").format("csv").save(path)


    ### Saving Spark DF as a csv, partitioning by columns(s)
    path = analysis.save_location_s3 + "toy_data_df_results_by_geocode.csv"
    sdftools.show(path, "S3 location of the toy df analysis results in csv form, partitioned by geocode")
    # can be a list; spark will split by each column requested, in the order specified
    partitionby = ['geocode']
    df.write.partitionBy(partitionby).option("header", "true").format("csv").save(path)

    path = analysis.save_location_s3 + "toy_data_df_results_by_query.csv"
    sdftools.show(path, "S3 location of the toy df analysis results in csv form, partitioned by query")
    # can be a list; spark will split by each column requested, in the order specified
    partitionby = ['query']
    df.write.partitionBy(partitionby).option("header", "true").format("csv").save(path)

    path = analysis.save_location_s3 + "toy_data_df_results_by_partition.csv"
    sdftools.show(path, "S3 location of the toy df analysis results in csv form, partitioned by columns")
    # can be a list; spark will split by each column requested, in the order specified
    partitionby = ['query', 'geocode']
    df.write.partitionBy(partitionby).option("header", "true").format("csv").save(path)


    ### Saving Spark DF as csv locally (via Pandas DF)
    path = analysis.save_location_linux + "toy_data_df_results.csv"
    sdftools.show(path, "Linux location of the toy df analysis results in csv form")
    pdf = df.toPandas()
    sdftools.show(pdf, "The Pandas DF based on the Spark DF")
    # create the directory locally, otherwise pandas_df.to_csv will throw an error
    sdftools.show(du.getdir(path), "The directory being created to house the pandas df data")
    du.makePath(du.getdir(path))
    pdf.to_csv(path, index=False)


    ### Saving Pandas DF to S3
    # save locally and then copy to s3
    path_linux = analysis.save_location_linux + "toy_data_pandas_df_results.csv"
    du.makePath(du.getdir(path))
    pdf.to_csv(path_linux, index=False)

    path_s3 = analysis.save_location_s3 + "toy_data_pandas_df_results.csv"
    s3.put_s3url(path_s3, path_linux)


    sdftools.show(analysis.save_location_s3, "S3 Analysis Results Save Location")
