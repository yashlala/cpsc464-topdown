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

    mdf = "${DAS_S3INPUTS}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_va_dpQueries5_version7/data-run1.0-epsilon4.0-DHCP_MDF.txt"

    df = spark.read.csv(mdf, header=True, sep='|', comment='#')
    df.show(1000)

    # get "histogram" counts (i.e. get a count of the unique rows)
    df = df.groupBy(df.columns).count().persist()
    df.show(1000)

    # create the geocode column
    df = df.withColumn(AC.GEOCODE, sf.concat(sf.col("TABBLKST"), sf.col("TABBLKCOU"), sf.col("TABTRACTCE"), sf.col("TABBLKGRPCE"), sf.col("TABBLK"))).persist()

    # age sql
    age_sql = ['case']
    age_sql += [f"when QAGE = {the_age} then '{the_age}'" for the_age in range(0,116)]
    age_sql += ['else -1']
    age_sql += ['end']
    age_sql = "\n".join(age_sql)
    print(age_sql)

    # sex sql
    sex_sql = ['case']
    sex_sql += [f"when QSEX = '1' then '0'"]
    sex_sql += [f"when QSEX = '2' then '1'"]
    sex_sql += ['else -1']
    sex_sql += ['end']
    sex_sql = "\n".join(sex_sql)
    print(sex_sql)

    # hispanic sql
    hisp_sql = ['case']
    hisp_sql += [f"when CENHISP = '1' then '0'"]
    hisp_sql += [f"when CENHISP = '2' then '1'"]
    hisp_sql += ['else -1']
    hisp_sql += ['end']
    hisp_sql = "\n".join(hisp_sql)
    print(hisp_sql)

    df = df.withColumn("age", sf.expr(age_sql)).persist()
    df = df.withColumn("sex", sf.expr(sex_sql)).persist()
    df = df.withColumn("hispanic", sf.expr(hisp_sql)).persist()

    df.show(1000)
    # drop the columns we don't need for the schema
    #drop_columns = ['SCHEMA_TYPE_CODE', 'SCHEMA_BUILD_ID', 'EPNUM', 'CITIZEN', 'LIVE_ALONE']
    #df = df.drop(*drop_columns).persist()
