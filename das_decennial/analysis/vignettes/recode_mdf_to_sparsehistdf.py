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
from das_constants import CC
from programs.schema.schemas.schemamaker import SchemaMaker


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

    mdf_path = "${DAS_S3INPUTS}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_va_dpQueries13_version7/data-run1.0-epsilon4.0-DHCP_MDF.txt"
    cef_path = "${DAS_S3INPUTS}/users/user007/cnstatDdpSchema_DataIndUserSpecifiedQueriesNPass_va_dpQueries13_version7/data-run1.0-epsilon4.0-BlockNodeDicts/"
    budget_group = "4.0"
    plb = "4.0"
    run_id = "run_0001"

    df = datatools.Recode2020CensusMDFToSparseHistogramDF(analysis, schema, mdf_path, cef_path, budget_group, plb, run_id).df
    df.show()
