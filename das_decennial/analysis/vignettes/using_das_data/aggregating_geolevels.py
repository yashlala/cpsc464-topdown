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
import analysis.tools.sdftools as sdftools
import analysis.constants as AC
from das_constants import CC


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


    #######################################################
    # Create an experiment using one or more DAS Run paths
    #######################################################
    paths = [
        f"{AC.S3_BASE}user007/Aug29_experiments_hierarchicalAgeRangeTopDown_branchFactor4_output_danVariant1-2/td4/"
    ]

    experiment = analysis.make_experiment("danVariant1-2", paths)
    sdftools.print_item(experiment.__dict__, "Experiment Attributes")

    ##############################
    # Work with the Experiment DF
    ##############################
    df = experiment.getDF()
    schema = experiment.schema
    sdftools.print_item(df, "Experiment DF")

    ######################
    # Aggregate Geolevels
    ######################
    geolevels = [CC.STATE, CC.COUNTY]

    # OPTIONAL
    # ========
    # to make the aggregation of geolevels in the geolattice trunk (e.g. State, County, Tract, Block_Group, Block)
    # go faster (i.e. no joining with the crosswalk file, which can be slow), create columns using the
    # "geocode substring trick"
    df = sdftools.buildGeolatticeTrunkColumns(df, geolevels)

    df = sdftools.aggregateGeolevels(spark, df, geolevels)
    sdftools.print_item(df, "Geolevel DF")

    sdftools.print_item(df.select([AC.GEOCODE, AC.GEOLEVEL]).distinct().count(), "Distinct counts of Geocodes and Geolevels")
