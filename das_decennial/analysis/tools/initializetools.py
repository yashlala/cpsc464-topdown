import os
import sys

# add spark lib to path to import pyspark in setuptools from command line
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

# das_decennial directory
ddec_dir = os.path.dirname(__file__)
if ddec_dir not in sys.path:
    sys.path.append(os.path.dirname((os.path.dirname(os.path.dirname(__file__)))))

import pyspark
from pyspark.sql import SparkSession
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import das_utils as du
import analysis.tools.sdftools as sdftools
import analysis.tools.datatools as datatools


valid_spark_loglevels = ["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"]


def parseArgs():
    """
    Parse command-line arguments to pass to setup
    :return: logname (just filename), number of nodes to run on, analysis script name
    """

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--logname", help="logname used to output log data and for saving analysis results", type=str)
    parser.add_argument("--num_core_nodes", help="number of core nodes in the cluster upon spark start", type=int)
    parser.add_argument("--analysis_script", help="the analysis script being called by the run script", type=str)

    args = parser.parse_args()
    assert "logname" in args, "must include --logname [logname]"
    assert "num_core_nodes" in args, "must include --num_core_nodes [num_core_nodes]"
    assert "analysis_script" in args, "must include --analysis_script [analysis_script]"

    assert args.num_core_nodes > 0, "number of core nodes must be positive integer"

    assert len(args.logname) > 9, "logname must be \"logs/[filename].log\""
    assert args.logname[:5] == "logs/", "logname must be \"logs/[filename].log\""
    assert args.logname[-4:] == ".log", "logname must be \"logs/[filename].log\""
    logname = args.logname.split('.')[0]  # get rid of the .log part
    logname = logname.split('/')[1]  # get rid of the logs/ part for the save_location and app_name

    return logname, args.num_core_nodes, args.analysis_script


def initializeAnalysis(save_location, spark_loglevel="ERROR", logname=None, num_core_nodes=None, analysis_script=None):
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

    # assert valid inputs
    assert type(save_location) is str, "Save location must be string"
    assert spark_loglevel in valid_spark_loglevels, "Invalid Spark loglevel"

    # if all three args are none, parse from command line
    if logname is None and num_core_nodes is None and analysis_script is None:
        logname, num_core_nodes, analysis_script = parseArgs()
    else:
        assert type(logname) is str, "Logname must be string"
        assert type(num_core_nodes) is int, "num_core_nodes must be positive integer"
        assert num_core_nodes > 0, "num_core_nodes must be positive integer"
        assert type(analysis_script) is str, "analysis_script must be string"

    #save_location = f"/mnt/users/{os.environ['JBID']}/analysis_results/{logname}/"
    save_location = f"{du.addslash(save_location)}{logname}/"

    app_name = f"DAS_Analysis | {os.environ['JBID']} | {logname}"

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    sdftools.print_item(du.pretty(dict(spark.sparkContext.getConf().getAll())), "Spark configurations being used")

    sdftools.print_item(num_core_nodes, "Number of Core Nodes on this cluster")
    sdftools.print_item(spark_loglevel, f"Spark loglevel")
    spark.sparkContext.setLogLevel(spark_loglevel)

    analysis_script_path = analysis_script
    sdftools.print_item(analysis_script_path, "The Analysis Script's path")

    return datatools.Analysis(spark, save_location, analysis_script_path)


# run parseArgs, for CLI testing
if __name__ == "__main__":
    parseArgs()
