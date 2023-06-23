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
        f"{AC.S3_BASE}user007/Aug29_experiments_hierarchicalAgeRangeTopDown_branchFactor4_output_danVariant1-2/td4/run_0000/"
    ]

    experiment = analysis.make_experiment("danVariant1-2", paths)
    sdftools.print_item(experiment.__dict__, "Experiment Attributes")

    ##############################
    # Get the Experiment DF
    ##############################
    df = experiment.getDF()
    schema = experiment.schema
    sdftools.print_item(df, "Experiment DF")

    geolevels = [
        CC.STATE,
        CC.COUNTY,
        CC.TRACT_GROUP,
        CC.TRACT,
        CC.BLOCK_GROUP,
        CC.BLOCK,
        CC.SLDL,
        CC.SLDU
    ]

    queries = [
        'total',
        'hhgq',
        'votingage * citizen',
        'numraces * hispanic',
        'cenrace * hispanic',
        'sex * age',
        'detailed'
    ]

    ###################################
    # Calculating Geolevel 1-TVD
    ###################################

    # 1. Aggregate Blocks to get Geographic Units at all desired Geographic Levels
    geoleveldf = sdftools.aggregateGeolevels(spark, df, geolevels)

    # 2. Answer Queries
    querydf = sdftools.answerQueries(geoleveldf, schema, queries, labels=True)

    # 3. Calculate Geolevel 1-TVD
    colname_tvd = "1-TVD"
    groupby = [AC.GEOLEVEL, AC.BUDGET_GROUP, AC.PLB, AC.RUN_ID, AC.QUERY]
    df = sdftools.getGeolevelTVD(querydf, colname=colname_tvd, col1=AC.PRTCTD, col2=AC.ORIG, groupby=groupby)

    # 4. Save Geolevel 1-TVD data and create graphics
    # 4a. Save Geolevel 1-TVD data to S3
    #savepath = experiment.save_location_s3 + "geolevel_1-TVD.csv"
    #sdftools.show(savepath, "Saving the Geolevel 1-TVD metric data to S3 as csv")
    #df.write.option("header", "true").format("csv").save(savepath)

    # 4b. Save Geolevel 1-TVD data locally
    savepath = experiment.save_location_linux + "geolevel_1-TVD.csv"
    # create the directory where the csv file will be saved
    # note: need to extract the directory of the file when using du.makePath since it
    #       will create a directory out of whatever is provided, even if that is
    #       intended to be a file
    #       e.g. savepath is a path to a file called geolevel_1-tvd.csv
    #       but if du.makePath(savepath), then it will turn geolevel_1-tvd.csv into
    #       a directory geolevel_1-tvd.csv/, which isn't what we want
    #du.makePath(du.getdir(savepath))
    #sdftools.show(savepath, "Saving the Geolevel 1-TVD metric data to Linux as csv")
    #pdf = df.toPandas()
    #sdftools.show(pdf.to_string(), "Geolevel 1-TVD Pandas DF")
    #pdf.to_csv(savepath, index=False)

    # 4c. Create graphics and save them locally
    savedir = experiment.save_location_linux + "plots/"
    # since plots is intended to be a directory, we can simply create the directory
    # using du.makePath(savedir)
    #du.makePath(savedir)
    #sdftools.show(savedir, "Creating and saving graphs locally here")
    #product = schema.name
    #state = "VA"
    #graphtools.geolevel_tvd_lineplot(pdf, savedir, product, state)
    #graphtools.geolevel_tvd_heatmap(pdf, savedir, product, state)
