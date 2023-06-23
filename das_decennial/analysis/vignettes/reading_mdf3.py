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
from das_constants import CC
from pyspark.sql import functions as sf
from programs.schema.schemas.schemamaker import SchemaMaker



class MDF2HistDF_DHCP_HHGQ():
    def __init__(self, analysis, schema, mdf_path, cef_path=None, budget_group="Unspecified", plb="Unspecified", run_id="Unspecified", debugmode=True):
        self.debugmode = debugmode
        self.analysis = analysis
        self.spark = self.analysis.spark
        self.mdf_path = mdf_path
        self.schema = schema

        self.mdf = self.spark.read.csv(self.mdf_path, header=True, sep='|', comment='#')
        sdftools.show(self.mdf, "MDF to be transformed into an Analysis Hist DF using 'DHCP_HHGQ' Schema")

        self.budget_group = budget_group
        self.plb = plb
        self.run_id = run_id

        self.protected_df = self._transform(self.mdf).persist()

        if cef_path is not None:
            self.cef_path = cef_path

            # assume cef_path's data is a pickled rdd
            cef_experiment = self.analysis.make_experiment("_CEF_DATA_", cef_path, schema_name=self.schema.name, dasruntype=AC.EXPERIMENT_FRAMEWORK_FLAT)
            self.orig_df = datatools.getOrigDF(cef_experiment).persist()
            if self.debugmode: sdftools.show(self.orig_df, "CEF sparse histogram DF -- loaded from pickled data")

            self.df = self._join_mdf_and_cef().persist()
        else:
            self.df = self.protected_df

        sdftools.show(self.df, "Analysis Hist DF with 'DHCP_HHGQ' Schema")

    def _join_mdf_and_cef(self):
        label = {
            AC.PLB: self.plb,
            AC.RUN_ID: self.run_id,
            AC.BUDGET_GROUP: self.budget_group,
            AC.ORIG: 0,
            AC.PRTCTD: 0
        }
        order_cols = [AC.GEOCODE] + self.schema.dimnames

        df = self.protected_df.join(self.orig_df, on=order_cols, how="full_outer").persist()
        if self.debugmode: sdftools.show(df, "Joined Protected and Orig sparse histogram DFs")
        df = df.fillna(label).persist()
        if self.debugmode: sdftools.show(label, "Fill in NAs with these values")
        if self.debugmode: sdftools.show(df, "Joined sparse histogram DF with NAs replaced by the appropriate values")

        column_order = [AC.GEOCODE, AC.RUN_ID, AC.PLB, AC.BUDGET_GROUP] + self.schema.dimnames + [AC.ORIG, AC.PRTCTD]
        df = df.select(column_order).persist()

        return df

    def _transform(self, mdf):
        df = mdf
        df = self._recode_geocode(df).persist()
        if self.debugmode: sdftools.show(df, "Protected DF after recoding to get geocode variable")

        df = self._recode_schema(df).persist()
        if self.debugmode: sdftools.show(df, "Protected DF after recoding schema variables")

        df = self._add_metadata_columns(df).persist()
        if self.debugmode: sdftools.show(df, "Protected DF after adding the metadata columns")

        df = self._drop_unneeded_columns(df).persist()
        if self.debugmode: sdftools.show(df, "Protected DF after dropping the columns not needed for Analysis")

        df = self._get_counts(df).persist()
        if self.debugmode: sdftools.show(df, "Protected DF after counting number of records per record type (i.e. after forming the sparse histogram)")
        return df

    def _get_counts(self, df):
        df = df.groupBy(df.columns).count().persist()
        df = df.withColumnRenamed("count", AC.PRTCTD).persist()
        return df

    def _recode_geocode(self, df):
        df = df.withColumn(AC.GEOCODE, sf.concat(sf.col("TABBLKST"), sf.col("TABBLKCOU"), sf.col("TABTRACTCE"), sf.col("TABBLKGRPCE"), sf.col("TABBLK"))).persist()
        return df

    def _drop_unneeded_columns(self, df):
        keep_columns = [AC.GEOCODE, AC.PLB, AC.RUN_ID, AC.BUDGET_GROUP, 'hhgq', 'sex', 'age', 'hispanic', 'cenrace', 'citizen']
        df = df.select(keep_columns).persist()
        return df

    def _recode_schema(self, df):
        df = self._recode_hhgq(df).persist()
        df = self._recode_sex(df).persist()
        df = self._recode_age(df).persist()
        df = self._recode_hispanic(df).persist()
        df = self._recode_cenrace(df).persist()
        df = self._recode_citizen(df).persist()
        return df

    def _recode_hhgq(self, df):
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
        if self.debugmode: sdftools.show(hhgq_sql, "HHGQ recode sql statement")
        df = df.withColumn("hhgq", sf.expr(hhgq_sql)).persist()
        if self.debugmode: sdftools.show(df, "Protected DF with hhgq recode")
        return df

    def _recode_sex(self, df):
        sex_sql = ['case']
        sex_sql += [f"when QSEX = '1' then '0'"]
        sex_sql += [f"when QSEX = '2' then '1'"]
        sex_sql += ['else -1']
        sex_sql += ['end']
        sex_sql = "\n".join(sex_sql)
        if self.debugmode: sdftools.show(sex_sql, "Sex recode sql statement")
        df = df.withColumn("sex", sf.expr(sex_sql)).persist()
        if self.debugmode: sdftools.show(df, "Protected DF with sex recode")
        return df

    def _recode_age(self, df):
        age_sql = ['case']
        age_sql += [f"when QAGE = {the_age} then '{the_age}'" for the_age in range(0,116)]
        age_sql += ['else -1']
        age_sql += ['end']
        age_sql = "\n".join(age_sql)
        if self.debugmode: sdftools.show(age_sql, "Age recode sql statement")
        df = df.withColumn("age", sf.expr(age_sql)).persist()
        if self.debugmode: sdftools.show(df, "Protected DF with age recode")
        return df

    def _recode_hispanic(self, df):
        hisp_sql = ['case']
        hisp_sql += [f"when CENHISP = '1' then '0'"]
        hisp_sql += [f"when CENHISP = '2' then '1'"]
        hisp_sql += ['else -1']
        hisp_sql += ['end']
        hisp_sql = "\n".join(hisp_sql)
        if self.debugmode: sdftools.show(hisp_sql, "Hispanic recode sql statement")
        df = df.withColumn("hispanic", sf.expr(hisp_sql)).persist()
        if self.debugmode: sdftools.show(df, "Protected DF with hispanic recode")
        return df

    def _recode_cenrace(self, df):
        cenrace_mdf_levels = [str(x).zfill(2) for x in range(1,64)]
        cenrace_sql = ['case']
        cenrace_sql += [f"when CENRACE = {x} then {str(int(x)-1)}" for x in cenrace_mdf_levels]
        cenrace_sql += ["else -1"]
        cenrace_sql += ["end"]
        cenrace_sql = "\n".join(cenrace_sql)
        if self.debugmode: sdftools.show(cenrace_sql, "Cenrace recode sql statement")
        df = df.withColumn("cenrace", sf.expr(cenrace_sql)).persist()
        if self.debugmode: sdftools.show(df, "Protected DF with cenrace recode")
        return df

    def _recode_citizen(self, df):
        cit_sql = ['case']
        cit_sql += [f"when CITIZEN = '2' then '0'"]
        cit_sql += [f"when CITIZEN = '1' then '1'"]
        cit_sql += ['else -1']
        cit_sql += ['end']
        cit_sql = "\n".join(cit_sql)
        if self.debugmode: sdftools.show(cit_sql, "Citizen recode sql statement")
        df = df.withColumn("citizen", sf.expr(cit_sql)).persist()
        if self.debugmode: sdftools.show(df, "Protected DF with citizen recode")
        return df

    def _add_metadata_columns(self, df):
        df = df.withColumn(AC.PLB, sf.lit(self.plb)).persist()
        df = df.withColumn(AC.RUN_ID, sf.lit(self.run_id)).persist()
        df = df.withColumn(AC.BUDGET_GROUP, sf.lit(self.budget_group)).persist()
        return df



def mdf2histdf(analysis, schema, mdf_path, cef_path, budget_group, plb, run_id):
    supported_schemas = {
        CC.SCHEMA_REDUCED_DHCP_HHGQ: MDF2HistDF_DHCP_HHGQ
    }
    assert schema.name in supported_schemas, f"Only the following schemas are supported: {supported_schemas}"
    transformer_class = supported_schemas[schema.name]
    transformer = transformer_class(analysis, schema, mdf_path, cef_path, budget_group, plb, run_id)
    return transformer.df


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

    schema = SchemaMaker.fromName(CC.SCHEMA_REDUCED_DHCP_HHGQ)

    #mdf_path = "${DAS_S3INPUTS}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_va_dpQueries5_version7/data-run1.0-epsilon4.0-DHCP_MDF.txt"
    mdf_path = "${DAS_S3INPUTS}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_va_dpQueries13_version7/data-run1.0-epsilon4.0-DHCP_MDF.txt"
    cef_path = "${DAS_S3INPUTS}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_va_dpQueries13_version7/data-run1.0-epsilon4.0-BlockNodeDicts/"
    budget_group = "4.0"
    plb = "4.0"
    run_id = "run_0001"

    df = mdf2histdf(analysis, schema, mdf_path, cef_path, budget_group, plb, run_id)
    df.show()
