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


    #######################################################
    # Create an experiment using one or more DAS Run paths
    #######################################################
    paths = [
        f"{AC.S3_BASE}user007/Aug29_experiments_hierarchicalAgeRangeTopDown_branchFactor4_output_danVariant1-2/td4/run_0000/"
    ]

    experiment = analysis.make_experiment("danVariant1-2", paths)
    sdftools.print_item(experiment.__dict__, "Experiment Attributes")

    ##############################
    # Work with the Experiment DF
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
    # Calculating Signed Error Metrics
    ###################################

    # 1. Aggregate Blocks to get Geographic Units at all desired Geographic Levels
    geoleveldf = sdftools.aggregateGeolevels(spark, df, geolevels)

    # 2. Answer Queries
    querydf = sdftools.answerQueries(geoleveldf, schema, queries, labels=True)

    # 3. Calculate the Signed Error, with the CEF counts binned
    bins = [0, 1, 10, 100, 1000, 10000]
    df = sdftools.getSignedErrorByTrueCountRuns(querydf, bins)

    # 4. Calculate quantiles within groups
    quantiles = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    groupby = ['orig_count_bin', AC.GEOLEVEL, AC.QUERY, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP]

    # 4a. Calculate within-group quantiles for 'signed error'
    signed_error_quantile_df = sdftools.getGroupQuantiles(df, columns=['signed_error'], groupby=groupby, quantiles=quantiles)
    sdftools.show(signed_error_quantile_df.persist(), f"Signed error quantiles by group '{groupby}'", 1000)

    # 4b. Calculate within-group quantiles for 'relative error'
    re_quantile_df = sdftools.getGroupQuantiles(df, columns=['re'], groupby=groupby, quantiles=quantiles)
    sdftools.show(re_quantile_df, f"Relative error quantiles by group '{groupby}'", 1000)

    # 5. Calculate averages within groups
    # 5a. Calculate signed error averages by group
    signed_error_avg_df = df.groupBy(groupby).agg(sf.avg("signed_error")).persist()
    sdftools.show(signed_error_avg_df, "signed_error_average_by_run", 1000)

    # 5b. Calculate relative error averages by group
    re_avg_df = df.groupBy(groupby).agg(sf.avg("re")).persist()
    sdftools.show(re_avg_df, "re_average_by_run", 1000)



    # Save the results to S3 for future use/analysis
    # 4a. Save signed error quantiles
    savepath = experiment.save_location_s3 + "signed_error_quantiles_per_group.csv"
    sdftools.show(savepath, "Saving signed error quantiles per group as csv")
    signed_error_quantile_df.write.option("header", "true").format("csv").save(savepath)

    # 4b. Save relative error quantiles
    savepath = experiment.save_location_s3 + "relative_error_quantiles_per_group.csv"
    sdftools.show(savepath, "Saving relative error quantiles per group as csv")
    re_quantile_df.write.option("header", "true").format("csv").save(savepath)

    # 5a. Save signed error averages
    savepath = experiment.save_location_s3 + "signed_error_averages_per_group.csv"
    sdftools.show(savepath, "Saving signed error averages per group as csv")
    signed_error_avg_df.write.option("header", "true").format("csv").save(savepath)

    # 5b. Save relative error averages
    savepath = experiment.save_location_s3 + "relative_error_averages_per_group.csv"
    sdftools.show(savepath, "Saving relative error averages per group as csv")
    re_avg_df.write.option("header", "true").format("csv").save(savepath)
