######################################################
# To Run this script:
#
# cd into das_decennial/analysis/
# analysis=[path to analysis script] bash run_analysis.sh
#
# More info on analysis can be found here:
# https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/analysis/readme.md
######################################################

from analysis.tools.initializetools import initializeAnalysis
import analysis.tools.sdftools as sdftools


if __name__ == "__main__":
    ################################
    # define experiments to analyze
    ################################
    S3_BASE = "${DAS_S3INPUTS}/users"

    #################
    # setup analysis
    #################
    save_location       = f"user007/analysis_reports/"
    save_location_s3    = f"{S3_BASE}/user007/analysis_reports/"
    save_location_linux = f"/mnt/users/user007/analysis_reports/"
    spark_loglevel = "ERROR"
    analysis = initializeAnalysis(save_location=save_location, spark_loglevel=spark_loglevel)

    ####################################
    # Accessing Noisy Measurements
    ####################################
    noisy_measurements_state_path = f"{S3_BASE}/user007/dhcp_eps4/run36_of_25/full_person/noisy_measurements/application_1574779932308_0073-State.pickle/"
    noisy_measurements_state = analysis.spark.sparkContext.pickleFile(noisy_measurements_state_path)
    nm_state = noisy_measurements_state
    print(nm_state)

    geounit = nm_state.take(1).pop()

    print(geounit)
    geocode = geounit.geocode
    print(geocode)
    dp_queries = geounit.dp_queries

    experiment_name = "dhcp_eps4_run36"
    experiment_path = f"{S3_BASE}/user007/dhcp_eps4/run36_of_25/full_person/"
    experiment = analysis.make_experiment(experiment_name, experiment_path)

    df = experiment.getDF()
    sdftools.print_item(df, "Experiment DF", show=100)

    geolevel_df = sdftools.aggregateGeolevels(experiment.spark, df, ['STATE'])
    sdftools.print_item(geolevel_df, "Geolevel DF")

    filtered_df = df.filter(df.geocode == geocode).persist()
    sdftools.print_item(filtered_df, "Experiment DF", show=1000)
