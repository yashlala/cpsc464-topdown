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


class Transform_dhcp_hhgq_mdf_to_analysis_hist():
    def __init__(self, spark, mdf_path, budget_group, plb, run_id):
        self.schema_recode_dict = {
            "hhgq": {},
            "sex": {},
            "age": {},
            "hispanic": {},
            "cenrace": {},
            "citizen": {}
        }

        self.mdf_path = mdf_path
        self.df = spark.read.csv(self.mdf_path, header=True, sep='|', comment='#')
        self.budget_group = budget_group
        self.plb = plb
        self.run_id = run_id

        self.df = self.makeGeocodeColumn(self.df)

    def makeGeocodeColumn(self, df):
        df = df.withColumn(AC.GEOCODE, sf.concat(sf.col("TABBLKST"), sf.col("TABBLKCOU"), sf.col("TABTRACTCE"), sf.col("TABBLKGRPCE"), sf.col("TABBLK"))).persist()
        return df

    def recodeSchema(self, df):
        return df

    #def makeSchemaSQL(self, schema_recode_dict):
    #    return sqldict





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
    drop_columns = ['SCHEMA_TYPE_CODE', 'SCHEMA_BUILD_ID', 'EPNUM', 'LIVE_ALONE']
    df = df.drop(*drop_columns).persist()
    df.show(1000)


    cenrace_mdf_levels = [str(x).zfill(2) for x in range(1,64)]
    cenrace_sql = ['case']

    cenrace_sql += [f"when CENRACE = {x} then {str(int(x)-1)}" for x in cenrace_mdf_levels]

    cenrace_sql += ["else -1"]

    cenrace_sql += ["end"]

    cenrace_sql = "\n".join(cenrace_sql)
    df = df.withColumn("cenrace", sf.expr(cenrace_sql)).persist()
    df.show(1000)

    df = df.withColumn(AC.PLB, sf.lit("4")).persist()
    df = df.withColumn(AC.RUN_ID, sf.lit("run_0001")).persist()
    df = df.withColumn(AC.BUDGET_GROUP, sf.lit("bg0")).persist()

    df.show(1000)

    # citizen sql
    cit_sql = ['case']
    cit_sql += [f"when CITIZEN = '2' then '0'"]
    cit_sql += [f"when CITIZEN = '1' then '1'"]
    cit_sql += ['else -1']
    cit_sql += ['end']
    cit_sql = "\n".join(cit_sql)
    print(cit_sql)
    df = df.withColumn("citizen", sf.expr(cit_sql)).persist()
    df.show(1000)
    df.groupBy('RTYPE').count().show()
    df.groupBy('GQTYPE').count().show()
#    df.RTYPE.unique()
 #   df.GQTYPE.unique()
    # hhgq sql
    hhgq_sql = ['case']
    hhgq_sql += [f"when RTYPE = '3' and GQTYPE = '000' then '0'"]
    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '101' then '1'"]

    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '201' then '2'"]
    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '301' then '3'"]
    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '401' then '4'"]
    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '501' then '5'"]
    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '601' then '6'"]
    hhgq_sql += [f"when RTYPE = '5' and GQTYPE = '701' then '7'"]
    hhgq_sql += ['else -1']
    hhgq_sql += ['end']
    hhgq_sql = "\n".join(hhgq_sql)

    df = df.withColumn("hhgq", sf.expr(hhgq_sql)).persist()
    df.groupBy('hhgq').count().show()
    drop_columns2 = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK', 'RTYPE', 'GQTYPE', 'QSEX', 'QAGE', 'CENHISP','RELSHIP']
    df = df.drop(*drop_columns2).persist()

    #df.show(1000)
    df=df[['geocode','run_id','plb','budget_group','hhgq','sex','age','hispanic','cenrace','citizen','count']] #rearrange column names to match histogram
    df=df.withColumnRenamed("count", AC.PRTCTD)
    df.show(1000)
