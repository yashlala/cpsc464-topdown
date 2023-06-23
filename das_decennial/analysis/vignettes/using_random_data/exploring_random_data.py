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
from programs.schema.schemas.schemamaker import SchemaMaker
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



    schema_name = "DHCP_HHGQ"
    schema = SchemaMaker.fromName(name=schema_name)
    sdftools.print_item(schema, "Schema")

    num_geocodes = 5
    density = 0.00001
    scale = 10
    geounits = getToyGeounitData_dict(num_geocodes, schema, density, scale)

    sdftools.print_item(geounits, "Randomly-generated Geounits")

    rdd = spark.sparkContext.parallelize(geounits)
    sdftools.print_item(rdd, "RDD of random geounits")

    df = datatools.rdd2df(rdd, schema)
    df = df.select(['geocode'] + schema.dimnames + [AC.ORIG, AC.PRTCTD])
    sdftools.print_item(df, "DF of random geounit data", 1000)
