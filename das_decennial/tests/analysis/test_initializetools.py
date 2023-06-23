import pytest
from analysis.tools.initializetools import parseArgs
from analysis.tools.initializetools import initializeAnalysis
import subprocess
from os.path import dirname


@pytest.mark.parametrize(
    "save_location,spark_loglevel,logname,num_core_nodes,analysis_script,error_message", [
        (None, "ERROR", "test_logname", 1, "test_script", "Save location must be string"),
        ("test_location", "ERROR", None, 1, "test_script", "Logname must be string"),
        ("test_location", "ERROR", "test_logname", "flarf", "test_script", "num_core_nodes must be positive integer"),
        ("test_location", "ERROR", "test_logname", 0, "test_script", "num_core_nodes must be positive integer"),
        ("test_location", "ERROR", "test_logname", 1, "test_script", "analysis_script must be string")
    ]
)
def test_initialize_errors(save_location, spark_loglevel, logname, num_core_nodes, analysis_script, error_message):
    """
    Test the setup function in analysis.tools.setuptools, which creates an analysis object
    :param save_location: local directory where results of analysis will be saved
    :param spark_loglevel: the loglevel to set spark session
    :param logname: name of logfile to write
    :param num_core_nodes: number of nodes on which the script will run
    :param analysis_script: name of analysis script. not used in setup currently
    :param error_message: desired error message
    """
    with pytest.raises(Exception) as excinfo:
        initializeAnalysis(save_location=save_location,
                           spark_loglevel=spark_loglevel,
                           logname=logname,
                           num_core_nodes=num_core_nodes,
                           analysis_script=analysis_script)
        assert str(excinfo.value) == error_message


@pytest.mark.parametrize(
    "save_location,spark_loglevel,logname,num_core_nodes,analysis_script,output", [
        ("test_location", "ERROR", "test_logname", 1, "test_script",
         {"save_location": "test_location/test_logname/",
          "save_location_s3": "${DAS_S3ROOT}/users/test_location/test_logname/",
          "save_location_linux": "/mnt/users/test_location/test_logname/"}
         ),
        ("test_location", "ERROR", "test_logname", 1, "test_script",
         {"save_location": "test_location/test_logname/",
          "save_location_s3": "${DAS_S3ROOT}/users/test_location/test_logname/",
          "save_location_linux": "/mnt/users/test_location/test_logname/"}
         )
    ]
)
def test_initialize(save_location, spark_loglevel, logname, num_core_nodes, analysis_script, output):
    """
    Test the setup function in analysis.tools.setuptools, which creates an analysis object
    :param save_location: local directory where results of analysis will be saved
    :param spark_loglevel: the loglevel to set spark session
    :param logname name of logfile to write
    :param num_core_nodes number of core nodes on which to run script
    :param analysis_script name of anaysis script to run (not used in setup at the moment)
    :param output: set of expected outputs
    :return:
    """
    analysis_object = initializeAnalysis(save_location=save_location,
                                         spark_loglevel=spark_loglevel,
                                         logname=logname,
                                         num_core_nodes=num_core_nodes,
                                         analysis_script=analysis_script)

    assert analysis_object.save_location == output["save_location"]
    assert analysis_object.save_location_s3 == output["save_location_s3"]
    assert analysis_object.save_location_linux == output["save_location_linux"]


@pytest.mark.parametrize("logname,num_core_nodes,analysis_script,error_message", [
        ("short", "1", "test_script", "logname must be \"logs/[filename].log\""),
        ("logs/.log", "1", "test_script", "logname must be \"logs/[filename].log\""),
        ("nostart.log", "1", "test_script", "logname must be \"logs/[filename].log\""),
        ("logs/noend", "1", "test_script", "logname must be \"logs/[filename].log\""),
        ("", "1", "test_script", "logname must be \"logs/[filename].log\""),
        (None, "1", "test_script", "logname must be \"logs/[filename].log\""),
    ]
)
def test_parseArgs_errors(logname, num_core_nodes, analysis_script, error_message):
    """
    Test assertion errors in parseArgs function
    :param logname: name of logfile.
    :param num_core_nodes: number of core nodes. Irrelevant
    :param analysis_script: Name of analysis script. Irrelevant
    :param misc: miscellaneous arguments to pass
    :param error_message: desired error message
    :return:
    """
    arg_names = ["--logname", "--num_core_nodes", "--analysis_script"]

    with pytest.raises(Exception) as excinfo:
        prog = "./analysis/tools/initializetools.py"
        result = subprocess.run(["python3", prog,
                        arg_names[0], logname,
                        arg_names[1], num_core_nodes,
                        arg_names[2], analysis_script],
                       cwd=dirname(dirname(dirname(__file__))))
        assert str(excinfo.value) == error_message
        assert result.returncode != 0


@pytest.mark.parametrize("logname,num_core_nodes,analysis_script,desired_output", [
        ("logs/testlog.log", "1", "test_script", "flarf")
    ]
)
def test_parseArgs(logname, num_core_nodes, analysis_script, desired_output):

    arg_names = ["--logname", "--num_core_nodes", "--analysis_script"]
    prog = "./analysis/tools/initializetools.py"
    result = subprocess.run(["python3", prog,
                             arg_names[0], logname,
                             arg_names[1], num_core_nodes,
                             arg_names[2], analysis_script],
                            cwd=dirname(dirname(dirname(__file__))))
    print(result.stdout)
    assert result.returncode == 0
