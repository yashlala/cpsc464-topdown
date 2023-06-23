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
from analysis.tools.initializetools import initializeAnalysis
import analysis.tools.datatools as datatools
import analysis.tools.sdftools as sdftools
import analysis.constants as AC
from pyspark.sql import functions as sf


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


    ######################################################################
    # Use the orig and protected run data to generate Spark DFs
    # then join those DFs on the 'geocode' and schema dimnames colums
    ######################################################################
    # 1. Need to create an experiment for the cef-related DAS runs AND
    #    an experiment for the mdf-related DAS runs
    path_without_orig = f"{AC.S3_BASE}user007/withoutRawForBrett_topdown_ri44/"
    syn_experiment = analysis.make_experiment("without_orig", path_without_orig)

    path_with_orig = f"{AC.S3_BASE}user007/withRawForBrett_topdown_ri44/"
    raw_experiment = analysis.make_experiment("with_orig", path_with_orig)

    # 2. Pass the cef experiment object and the mdf experiment object to
    #    datatools.getJoinedDF, which will create the joined MDF and CEF DF
    df = datatools.getJoinedDF(syn_experiment, raw_experiment)
    sdftools.show(df, "Joined Experiment DF")
    sdftools.show(df.count(), "# rows in Joined Experiment DF")

    groupby = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP]
    sdftools.show(df.agg(*(sf.countDistinct(sf.col(c)).alias(c) for c in groupby)), f"Distinct counts of each column in {groupby}")
