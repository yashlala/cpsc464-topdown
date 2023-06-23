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
    # Get the Experiment DF
    ##############################

    # To simulate the protected and orig dfs for the join
    # 1. going to use experiment.getDF() twice and will
    #    drop the orig column in one and the protected column
    #    in the other
    # 2. going to drop all zero rows in each of them (since
    #    the zeros only exist due to (orig > 0 or protected > 0)
    schema = experiment.schema
    order_cols = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP] + schema.dimnames
    all_cols = order_cols + [AC.ORIG, AC.PRTCTD]

    limit_num = None

    limit_df = experiment.getDF().sort(order_cols).persist()
    if limit_num is not None:
        limit_df = limit_df.sort(order_cols).limit(limit_num).persist()
        sdftools.show(limit_df, f"DF with {limit_num} rows", limit_num)

    exp_df = limit_df.persist()

    # 1.
    orig_df = limit_df.drop(AC.PRTCTD).sort(order_cols).persist()
    protected_df = limit_df.drop(AC.ORIG).sort(order_cols).persist()
    sdftools.show(orig_df, "DF with only CEF values", 40)
    sdftools.show(protected_df, "DF with only MDF values", 40)
    sdftools.show(orig_df.count(), "CEF row count")
    sdftools.show(protected_df.count(), "MDF row count")

    # 2.
    orig_df = orig_df.filter(sf.col(AC.ORIG) > 0).sort(order_cols).persist()
    protected_df = protected_df.filter(sf.col(AC.PRTCTD) > 0).sort(order_cols).persist()
    sdftools.show(orig_df, "CEF DF with only nonzeros", 40)
    sdftools.show(protected_df, "MDF DF with only nonzeros", 40)
    sdftools.show(orig_df.count(), "CEF row count after removing zeros")
    sdftools.show(protected_df.count(), "MDF row count after removing zeros")

    # To combine the two DFs, use a full outer join
    df = protected_df.join(orig_df, on=order_cols, how="full_outer").sort(order_cols).persist()
    sdftools.show(df, "Full outer join of the CEF and MDF DFs", 40)
    sdftools.show(df.count(), "DF row count of the full outer join")

    # and replace all null values with zeros
    # note that the row count of the joined DF should match the row count of the experiment DF we started with
    df = df.fillna({AC.ORIG: 0, AC.PRTCTD: 0}).sort(order_cols).persist()
    sdftools.show(df, "Full outer join of the CEF and MDF DFs after replacing null values with zeros", 40)
    sdftools.show(df.count(), "DF row count of the full outer join")

    # do a set difference to see if the final DF and the experiment DF we started with have any rows that are different
    # note that the results should be empty for both set differences
    df = df.select(all_cols).sort(order_cols).persist()
    exp_df = exp_df.select(all_cols).sort(order_cols).persist()
    sdftools.show(df.subtract(exp_df), "Joined DF set_subtract the Experiment DF we started with", 40)
    sdftools.show(exp_df.subtract(df), "The Experiment DF we started with set_subtract the Joined DF", 40)
