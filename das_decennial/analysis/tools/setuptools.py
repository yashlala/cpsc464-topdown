from pyspark.sql import SparkSession
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import os
import das_utils as du
import analysis.tools.sdftools as sdftools
import analysis.tools.datatools as datatools

def setup(save_location=None, spark_loglevel="ERROR", cli_args=True, logname=None, num_core_nodes=None,
          analysis_script=None):
    """
    Create a new spark session and Analysis object (from DataTools) to run experiments
    :param save_location: filepath of desired location
    :param spark_loglevel: level at which to set logging
    :param cli_args: optional boolean: use CLI arguments (will engage argument parser) or function args. default True
    :param logname: optional filepath of logfile. default None (if CLI)
    :param num_core_nodes: optional int: number of nodes. default None (if CLI)
    :param analysis_script: name of Analysis script to run. default None (if CLI)
    :return: Analysis object from the arguments
    """
    assert save_location is not None, "Need to specify local directory where the results of analysis will be saved"
    assert spark_loglevel in ["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"], \
        "Invalid Spark loglevel"


    # if parsing args from CLI (default True)
    if cli_args:
        parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
        parser.add_argument("--logname", help="logname used to output log data and for saving analysis results")
        parser.add_argument("--num_core_nodes", help="number of core nodes in the cluster upon spark start")
        parser.add_argument("--analysis_script", help="the analysis script being called by the run script")

        args, unknown = parser.parse_known_args()

        logname = args.logname.split('.')[0] # get rid of the .log part
        logname = logname.split('/')[1] # get rid of the logs/ part for the save_location and app_name
    # otherwise get args from function header
    else:
        assert logname is not None and num_core_nodes is not None and analysis_script is not None, \
            "If not using command line, must pass logname, num_core_nodes, and analysis_script"
        assert type(logname) is str and type(analysis_script) is str, "logname and analysis_script must be strings"
        assert type(num_core_nodes) is int and num_core_nodes > 0, "num_core_nodes must be positive integer"

        class Arguments():
            def __init__(self, logname, num_core_nodes, analysis_script):
                self.logname = logname
                self.num_core_nodes = num_core_nodes
                self.analysis_script = analysis_script
        args = Arguments(logname=logname, num_core_nodes=num_core_nodes, analysis_script=analysis_script)

    #save_location = f"/mnt/users/{os.environ['JBID']}/analysis_results/{logname}/"
    save_location = f"{du.addslash(save_location)}{logname}/"

    app_name = f"DAS_Analysis | {os.environ['JBID']} | {logname}"

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    sdftools.print_item(du.pretty(dict(spark.sparkContext.getConf().getAll())), "Spark configurations being used")

    sdftools.print_item(args.num_core_nodes, "Number of Core Nodes on this cluster")
    sdftools.print_item(spark_loglevel, f"Spark loglevel")
    spark.sparkContext.setLogLevel(spark_loglevel)

    analysis_script_path = args.analysis_script
    sdftools.print_item(analysis_script_path, "The Analysis Script's path")

    return datatools.Analysis(spark, save_location, analysis_script_path)
