#!/usr/bin/env python3
"""
das2020_driver is the driver program for the 2020 Disclosure Avoidance System

"""
import json
import logging
import os
import subprocess
import sys
import time
import re
import threading
import traceback
import datetime
import xml.etree.ElementTree as ET
from configparser import ConfigParser
from typing import Any
import syslog

import pandas as pd


SPARK_HOME = 'SPARK_HOME'
SPARK_HOME_DEFAULT = '/usr/lib/spark'

if SPARK_HOME not in os.environ:
    os.environ[SPARK_HOME] = SPARK_HOME_DEFAULT

if not os.path.exists(os.environ[SPARK_HOME]):
    raise FileNotFoundError(os.environ[SPARK_HOME])

# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))


# make sure 'import ctools' works from anywhere
from os.path import dirname,abspath,basename
sys.path.append( os.path.join( dirname(abspath(__file__)), "das_framework" ))

from pathlib import Path
from das_constants import CC

from das_utils import das_productionp

# NOTE: When you add more modules, be sure to add them to das_setup.py:setup_func
# ctools support
import das_framework.ctools as ctools
import das_framework.ctools.clogging as clogging
import das_framework.ctools.cspark as cspark
import das_framework
from das_framework.driver import DAS

# Tools for running in the EMR environment
import programs.dashboard         as dashboard
import programs.emr_control       as emr_control

sys.path.append(os.path.join( dirname(abspath(__file__)), "scripts" ))
from ctools.paths import mkpath
import requests
import syslog
import os
import os.path
import csv

MIN_FRAMEWORK_VERSION='1.1.0'
MIN_CTOOLS_VERSION='1.0.0'
DAS_FACILITY = syslog.LOG_LOCAL1
CERTIFICATE_TEMPLATE = os.path.join(os.path.dirname(abspath(__file__)), CC.CERTIFICATE_FILENAME)
DAS_STEP_ENV_VAR = 'DAS_STEP'
DAS_NAME_ENV_VAR = 'DAS_NAME'
MISSION_NAME_ENV_VAR = 'MISSION_NAME'
DAS_FACILITY = syslog.LOG_LOCAL1
CLUSTER_RUN_TYPE_ENV_VAR='CLUSTERRUNTYPE'
INSTANCEID_ENV_VAR='INSTANCEID'
TESTPOINT_POST_ENDPOINT = mkpath("${DAS_DASHBOARD_URL}/api/testpoints/post")

if os.getenv('DAS_DASHBOARD_UNSAFE_TLS'):
    TLS_VERIFY_MODE=False
else:
    TLS_VERIFY_MODE=True


def display_mission(name: str):
    """Display the current mission on stdout in a nicely formatted manner"""
    block_width = 43

    def dl(line: str, fill: str = ' ', ) -> str:
        return '==={}===\n'.format(line.center(block_width, fill))

    print(dl("", fill='=')+
          dl("DAS RUN AT {}".format(time.asctime()))+
          dl(name)+
          dl("", fill='='))


def requestResize(section: str) -> None:
    emr_core_nodes = config.getint(section=section, option=CC.EMR_CORE_NODES, fallback=None)
    emr_task_nodes = config.getint(section=section, option=CC.EMR_TASK_NODES, fallback=None)

    if (emr_core_nodes is not None) or (emr_task_nodes is not None):
        willResize = emr_control.requestInstanceCounts(emr_core_nodes, emr_task_nodes, background = True)
        if willResize:
            if emr_core_nodes is not None:
                message = f"Requesting resize to {emr_core_nodes} core nodes"
                if emr_task_nodes is not None:
                    message += f" and {emr_task_nodes} task nodes"
            else:
                message = f"Requesting resize to {emr_task_nodes} task nodes"
        else:
            message = f"No need to resize cluster"
        dashboard.das_log(message)


def dashboard_heartbeat(*, config: ConfigParser, main_thread):
    """This is the dashboard heartbeat thread. It does the following:
    1. Sends a message to the dashboard server saying the current das2020_driver.py process is alive.
    2. Reports the stacktrace for the main thread.
    3. Optionally prints this on stdout.
    Note that the print heartbeat can be different than the heartbeat_frequency, which describes how often we report.
    """
    print_heartbeat           = config.getboolean(section=CC.MONITORING_SECTION,
                                                  option=CC.PRINT_HEARTBEAT,
                                                  fallback=True)
    heartbeat_frequency       = config.getint(section=CC.MONITORING_SECTION,
                                              option=CC.HEARTBEAT_FREQUENCY,
                                              fallback=CC.HEARTBEAT_FREQUENCY_DEFAULT)
    print_heartbeat_frequency = config.getint(section=CC.MONITORING_SECTION,
                                              option=CC.PRINT_HEARTBEAT_FREQUENCY,
                                              fallback=CC.HEARTBEAT_FREQUENCY_DEFAULT)

    send_stacktrace           = config.getboolean(section=CC.MONITORING_SECTION,
                                                  option=CC.SEND_STACKTRACE,
                                                  fallback=False)
    last_print_time = 0
    while True:
        msg = None
        if send_stacktrace:
            frame = sys._current_frames().get(main_thread.ident, None)
            if frame:
                # Because seeing stack traces is frightening, prefix it with something happy.
                msg = "Executing:\n" + "".join(traceback.format_stack(frame))
        if print_heartbeat:
            if last_print_time + print_heartbeat_frequency < time.time():
                print("================ HEARTBEAT: {} pid={} ================".format(time.asctime(),os.getpid()))
                if msg is not None:
                    print(">" + msg.replace("\n","\n>"))
                    print("================ HEARTBEAT END ================")
                last_print_time = time.time()
        dashboard.heartbeat(msg)
        sleep_time = min(heartbeat_frequency, print_heartbeat_frequency )
        time.sleep( sleep_time )


# If we are running within a das-vm-config repo, testpoints are at ../bin/DAS_TESTPOINTS.csv
# Otherwise use the ones at /mnt/gits/das-vm-config/bin/DAS_TESTPOINTS.csv
def get_testpoint_filename():
    """find and return the testpoint filename. """
    TESTPOINT_FILENAMES = [
        os.path.join(os.path.dirname(__file__), "../../../../bin/DAS_TESTPOINTS.csv"),
        "/mnt/gits/das-vm-config/bin/DAS_TESTPOINTS.csv"]

    for fname in TESTPOINT_FILENAMES:
        if os.path.exists(fname):
            return fname
    raise FileNotFoundError("Cannot find testpoint file in " + str(TESTPOINT_FILENAMES))


def log_tp(testpoint: str, additional: str = None, mission_name: str = None) -> None:
    """Log a specified testpoint to the given filename, with optional additonal information.
    Verifies that the testpoint specified exists.
    Log to .err or .info as specified by the last character in the testpoint.
    Automatically adds mission name
    """
    das_step = os.getenv(DAS_STEP_ENV_VAR)
    das_name = os.getenv(DAS_NAME_ENV_VAR)
    instance_id = os.getenv(INSTANCEID_ENV_VAR)
    cluster_type = os.getenv(CLUSTER_RUN_TYPE_ENV_VAR)
    success_flag = 0 if 'F' in testpoint else 1

    if mission_name is None:
        mission_name = os.getenv(MISSION_NAME_ENV_VAR)

    das_step = "DAS_STEP:" if das_step is None else f"DAS_STEP:{das_step}"
    das_name = "DAS_NAME:" if das_name is None else f"DAS_NAME:{das_name}"
    mission_name = "MISSION_NAME:" if mission_name is None else f"MISSION_NAME:{mission_name}"
    instance_id = "INSTANCE_ID:" if instance_id is None else f"INSTANCE_ID:{instance_id}"
    cluster_type = "S" if cluster_type is None else cluster_type[0].upper()

    priority = syslog.LOG_ERR if testpoint.endswith('F') else syslog.LOG_NOTICE
    with open(get_testpoint_filename(), "r") as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            if row and row[0] == testpoint:
                # Write Syslog Statement
                appendage = ":" + additional if additional else ""
                syslog.openlog(facility=DAS_FACILITY)
                syslog.syslog(priority, f"{das_name} {das_step} {mission_name} TESTPOINT:{row[0]} {row[1]}{appendage}")
                syslog.closelog()

                try:
                    # Message Format: aws_instanceid|mission_name|cluster_type|testpoint|message|success
                    # Example: "i-123abc456efg|mymission5|S|T02-004S|mymessage|1
                    cmd = f"""{{"aws_instanceid": "{instance_id}",
                              "mission_name": "{mission_name}",
                              "cluster_type": "{cluster_type}",
                              "testpoint": "{row[0]}",
                              "message": "{row[1]}{appendage}",
                              "success": "{success_flag}"
                             }}
                          """

                    print(f"TestPoint Message: {cmd}")
                    testmsg = json.loads(cmd)
                    response = requests.post(TESTPOINT_POST_ENDPOINT, data=testmsg, verify=TLS_VERIFY_MODE)

                except requests.exceptions.RequestException as e:  # This is the correct syntax
                    print(f"Testpoint ERROR: Posting the testpoint to the dashboard server has failed with the following traceback [response: {response}]")
                    print(e)
                return

    # This has been changed from an exception to just a print statement.  A failure to log a testpoint shouldnt stop an experiment
    # from executing, but it should be logged.
    print(f"Testpoint Error: Unknown testpoint: {testpoint}")

class DASDelegate():
    """The DASDelegate class receives messages when different parts of the das runs.
    These methods are passed the das object as their parameter. This pattern is borrowed from SmallTalk and Objective-C.
    Current delegate methods handle logging and the DVS"""

    def __init__(self, mission_name: str, step_id: str = "TXX"):
        self.mission_name = mission_name
        # This is the prefix for test point, and is defaulted to TXX if it is undefined
        # in the config files, or the environment variable is not set
        if os.environ.get("TESTPOINT_STEPID"):
            # Environment variable can override config file
            self.step_id = os.environ.get("TESTPOINT_STEPID")
        elif config.get("logging", "testpoint_stepid", fallback=None) is not None:
            # Config file is used if not defined in the ENV variable
            self.step_id = config.get("logging", "testpoint_stepid", fallback=None)
        else:
            # defaults to TXX if step_id is not provided, or if config file is not set,
            # and if the os ENV variable is not set
            self.step_id = step_id

    def willRun(self, das: DAS):
        log_tp(f"{self.step_id}-001S", mission_name=self.mission_name)
        if das.setup_data.dvs_enabled:
            # Data Vintaging System
            # We previously put the DVS_Singleton into the das object.
            # We stopped doing that because the DAS object is pickled and sent to the workers,
            # and we don't want to do that with the DVS object.

            from  programs.python_dvs.dvs import DVS_Singleton
            ds = DVS_Singleton()
            ds.add_kv(key='x-mission',value=self.mission_name)
            ds.add_git_commit( src=__file__ ) # add our git commit

    def permitRun(self, das: DAS):
        in_production=das_productionp()
        if not in_production:
            in_production=das.config.getboolean(CC.SETUP_SECTION,'production_mode',fallback=False)

        if in_production:
            dp_mechanism=das.config.get(CC.BUDGET,CC.DP_MECHANISM,fallback=False)
            if dp_mechanism and dp_mechanism != CC.DISCRETE_GAUSSIAN_MECHANISM:
                raise RuntimeError(f"DP mechanism {dp_mechanism} is not permitted for production runs")

    def log_testpoint(self, testpoint: str, additional: str = None):
        log_tp(f"{self.step_id}-{testpoint}", additional, mission_name=self.mission_name)

    def willRunReader(self, das: DAS, reader_name=None):
        log_tp(f"{self.step_id}-005S", additional=reader_name, mission_name=self.mission_name)

    def willRunEngine(self, das: DAS):
        log_tp(f"{self.step_id}-006S", mission_name=self.mission_name)

    def willRunErrorMetrics(self, das: DAS):
        log_tp(f"{self.step_id}-007S", mission_name=self.mission_name)

    def willRunWriter(self, das: DAS):
        log_tp(f"{self.step_id}-008S", mission_name=self.mission_name)

    def willRunValidator(self, das: DAS):
        log_tp(f"{self.step_id}-009S", mission_name=self.mission_name)

    def didRun(self, das: DAS):
        #
        # import is below, rather than at top of file, so that dvs is not imported on workers.
        #
        if das.setup_data.dvs_enabled:
            from programs.python_dvs.dvs import DVS_Singleton, DVSException
            ds = DVS_Singleton()
            try:
                ds.commit()
            except DVSException as e:
                logging.warning("DVSException: %s",str(e))


def get_git_hash(git_folder: Path):
    """Returns a string of the git_folder name, a space, and the commit point
    :param git_folder: root of the git folder. May be a string or a Path object.
    TODO: - clean this up so that it returns a tuple of the (remote URL,commit point).
    - We have code in DVS that does this now.
    """
    commit_point     = [subprocess.check_output([CC.GIT_EXECUTABLE, "show"], encoding='utf8', cwd=git_folder).split('\n')[0]]
    try:
        porcelaingit = subprocess.check_output(
            [CC.GIT_EXECUTABLE, "status", "--porcelain", "--untracked-files=no"],
            encoding='utf-8', cwd=git_folder)
    except (subprocess.CalledProcessError) as e:
        logging.error(f"Git porcelain-no-untracked-files failed: subprocess returned error {e}, {sys.exc_info()}")
        commit_point = commit_point + " -- Git to check whether commit is clean hasn't run! --"
        porcelaingit = ""

    if len(porcelaingit) > 0:
        commit_point.append("(modifications present at run time)")
    else:
        try:
            porcelaingit_wfiles = subprocess.check_output(
                [CC.GIT_EXECUTABLE, "status", "--porcelain"],
                encoding='utf-8', cwd=git_folder)
        except (subprocess.CalledProcessError) as e:
            logging.error(f"Git porcelain failed: subprocess returned error {e}, {sys.exc_info()}")
            commit_point = commit_point + " -- Git to check the presence of untracked files hasn't run! --"
            porcelaingit_wfiles = ""

        commit_point.append("(untracked files present at run time)" if len(porcelaingit_wfiles) > 0 else "")
    return tuple([git_folder.name] + commit_point)


# def generate_certificate( config, certificate_path ):
#     # Do the import here so we don't need to do it on every node
#     from das_framework.certificate import CertificatePrinter
#     cp = CertificatePrinter(title="Certificate of Disclosure Avoidance")
#     cp.add_params({"DATE"   : datetime.datetime.now().isoformat()[0:19],
#                    "NAME"   : config[CC.WRITER_SECTION].get(CC.CERTIFICATE_NAME, ''),
#                    "PERSON1": config[CC.WRITER_SECTION].get(CC.CERTIFICATE_PERSON1, ''),
#                    "TITLE1" : config[CC.WRITER_SECTION].get(CC.CERTIFICATE_TITLE1, ''),
#                    "PERSON2": config[CC.WRITER_SECTION].get(CC.CERTIFICATE_PERSON2, ''),
#                    "TITLE2" : config[CC.WRITER_SECTION].get(CC.CERTIFICATE_TITLE2, ''),
#                    "GIT_COMMIT" : ctools.latex_tools.latex_escape(" ".join(get_git_hash( Path(abspath(__file__)))))
#                })
#     cp.add_config(config)
#     cp.typeset(certificate_path)



def add_git_commit_to_config(das: DAS):
    das.annotate(f"{CC.SAVE_GIT_COMMIT} set to True")
    repo_info = get_repo_info()
    repo_info = "|".join(map(lambda d: " ".join(d), repo_info.itertuples(index=False)))
    # Add all the git has repo information to the das config. This is later used to add the info to the metatadata file.
    das.config.set(CC.READER, CC.GIT_COMMIT, repo_info)

    das.annotate(f"Git commit info: {repo_info}")


def get_repo_info() -> pd.DataFrame:
    current_dir = Path(abspath(__file__)).parent
    current_folder = Path(current_dir).name
    parent_folder = Path(current_dir).parent.name
    # This sets if we are going to be using the parent folder of the current file. This is the case if the parent
    # folder is das-vm-config.
    working_folder_path = Path(current_dir).parent if parent_folder == 'das-vm-config' else Path(current_dir)
    # Run git submodule status --recursive with the cwd set to either the current file path or to the parent if
    # the parent is das-vm-config.
    all_submodules = subprocess.run(["git", "submodule", "status", "--recursive"], stdout=subprocess.PIPE,
                                    cwd=working_folder_path).stdout.decode("utf-8").split("\n")
    # Run through the submodules and clean up the strings as well as add the root repo to the list.
    all_submodules = [Path(working_folder_path) / Path(submodule.strip().split(" ")[1]) for submodule in
                      all_submodules if submodule.strip()]
    all_submodules.append(Path(working_folder_path))
    # Call get_git_hash on all the repos including the root repo.
    repo_info = pd.DataFrame([get_git_hash(git_folder=submodule) for submodule in all_submodules])
    repo_info.columns = ['Submodule', 'Commit', "Modified/Untracked"]

    return repo_info


def do_dry_run(args: Any):
    """Optionally implement the dry-read and dry-write arguments for a dry-run"""
    print("DRY-RUN-STDOUT")
    print("DRY-RUN-STDERR",file=sys.stderr)
    if args.dry_read and args.dry_write:
        subprocess.check_call(['aws','s3','cp',args.dry_read,args.dry_write])
        print(f"DRY-RUN {args.dry_read} -> {args.dry_write}")
        return
    if args.dry_read or args.dry_write:
        raise RuntimeError("--dry-read requires --dry-write and vice-versa")


def produce_certificate(config: ConfigParser, certificate_path, git_commit: str="*None provided*", das=None):
    # Do the import here so we don't need to do it on every node
    import programs.strategies.print_alloc as palloc
    import pandas as pd
    writer = config[CC.WRITER_SECTION]
    if das is not None:
        levels = das.engine.budget.levels
        privacy_framework = das.engine.setup.privacy_framework
        total_epsilon = das.engine.budget.total_epsilon
        level_alloc = das.engine.budget.geolevel_prop_budgets_dict
    else:
        # TODO: Do we want to re-read in this case or say 'not indicated'?
        levels = config.get(option=CC.GEODICT_GEOLEVELS, section=CC.GEODICT).split(",")
        privacy_framework = config.get(option=CC.PRIVACY_FRAMEWORK, section=CC.BUDGET, fallback=CC.PURE_DP)
        total_epsilon = "*None indicated*"

    strategy_name = config.get(section=CC.BUDGET, option=CC.STRATEGY)
    epsilon_names = {CC.PURE_DP: r"$\varepsilon$", CC.ZCDP: r"zCDP-implied $\varepsilon$"}
    framework_names = {CC.PURE_DP: r"$\varepsilon$-Differential Privacy", CC.ZCDP: r"Zero Concentrated Differential Privacy (mapped to $(\varepsilon, \delta)$-DP)"}
    dpstring = ""
    dpstring += f"Privacy framework: {framework_names[privacy_framework]}\n\n"
    if privacy_framework == CC.ZCDP and das is not None:
        rho = 1 / das.engine.budget.global_scale_sq
        dpstring += r"$\rho = " + str(rho) + f"$ $(\\approx {float(rho):.2f}$)" + "\n\n"
        delta_mant_exp = str(float(das.engine.budget.delta)).split('e')
        delta_latex_str = delta_mant_exp[0] + r" \times 10^{" + delta_mant_exp[1] + r"}"
        dpstring += r"The mapping of $\rho$ to $(\varepsilon, \delta)$-DP is best represented by the full $(\varepsilon, \delta)$ curve, but giving a single value below." + "\n\n"
        dpstring += r"At $\delta=" + str(das.engine.budget.delta) + f"$ (${delta_latex_str}$): "

    dpstring += f"Total {epsilon_names[privacy_framework]} = {total_epsilon} ($\\approx {float(total_epsilon):.2f}$)\n\n"
    dpstring += r"\vspace{0.5cm}"

    if das is not None:
        df = pd.DataFrame(das.engine.budget.geolevel_prop_budgets_dict.items())
        df.columns = ["Geography level", "Budget proportion"]
        df["In percent"] = df["Budget proportion"].apply(lambda d: f"{float(d):.1%}")
        dpstring += "Geographic level allocations:\n\n" + str(df.to_latex(index=False)) + "\n" + r"\vspace{0.5cm}"
    dpstring += f"Within-geolevel query PLB allocations:\n\n\\tiny{str(palloc.makeDataFrame(strategy_name, levels=levels).to_latex())}\n\\normalsize\n" + r"\vspace{0.5cm}"
    if config.getboolean(option=CC.PRINT_PER_ATTR_EPSILONS, section=CC.BUDGET, fallback=False) and das is not None:
        dpstring += "\clearpage\n\nPer-attribute semantics:\n\n"
        if privacy_framework == CC.ZCDP:
            dpstring += r"\begin{tabular}{lr}"+ "\n\n\\toprule\n\n"
            dpstring += r"Attribute Name & $\rho$ \\" + "\n\n"
            dpstring += "\midrule\n\n"
            for attr_name, attr_rho in das.engine.budget.per_attr_rho.items():
                an = attr_name.replace('_','\_')
                dpstring += f"{an} & {float(attr_rho):.2f} \\\\ \n\n"
            dpstring += r"\bottomrule" + "\n\n" + r"\end{tabular}" + r"\hspace{3cm}"

            dpstring += r"\begin{tabular}{lr}" + "\n\n\\toprule\n\n"
            dpstring += r"Geography & $\rho$ \\" + "\n\n"
            dpstring += "\midrule\n\n"
            for geolevel, geolevel_rho in das.engine.budget.per_geolevel_rho.items():
                gl = geolevel.replace('_','\_')
                dpstring += f"Block-within-{gl} & {float(geolevel_rho):.2f} \\\\ \n\n"
            dpstring += r"\bottomrule" + "\n\n" + r"\end{tabular}" + r"\vspace{0.5cm}"

            dpstring += "\n\n\n\n"

            dpstring += r"At $\delta=" + str(das.engine.budget.delta) + f"$ (${delta_latex_str}$):" + "\n\n"

        dpstring += r"\begin{tabular}{lr}"+ "\n\n\\toprule\n\n"
        dpstring += r"Attribute Name & " + epsilon_names[privacy_framework] + r"\\" + "\n\n"
        dpstring += "\midrule\n\n"
        for attr_name, attr_eps in das.engine.budget.per_attr_epsilons.items():
            an = attr_name.replace('_','\_')
            dpstring += f"{an} & {float(attr_eps):.2f} \\\\ \n\n"
        dpstring += r"\bottomrule" + "\n\n" + r"\end{tabular}" + r"\hspace{3cm}"
        # df = pd.DataFrame(das.engine.budget.per_attr_epsilons.items())
        # df.columns = ["Attribute Name", "$\\varepsilon$"]
        # dpstring += str(df.to_latex(index=False)) + "\n\n"

        dpstring += r"\begin{tabular}{lr}" + "\n\n\\toprule\n\n"
        dpstring += r"Geography & " + epsilon_names[privacy_framework] + r"\\" + "\n\n"
        dpstring += "\midrule\n\n"
        for geolevel, geolevel_eps in das.engine.budget.per_geolevel_epsilons.items():
            gl = geolevel.replace('_','\_')
            dpstring += f"Block-within-{gl} & {float(geolevel_eps):.2f} \\\\ \n\n"
        dpstring += r"\bottomrule" + "\n\n" + r"\end{tabular}" + r"\vspace{0.5cm}"
        # df = pd.DataFrame(das.engine.budget.per_geolevel_epsilons.items())
        # df.columns = ["Geography Name", "$\\varepsilon$"]
        # dpstring += str(df.to_latex(index=False)) + "\n\n"

    from das_framework.certificate import CertificatePrinter
    cp = CertificatePrinter(title="Certificate of Disclosure Avoidance", template=CERTIFICATE_TEMPLATE)
    cp.add_params({"DATE"   : datetime.datetime.now().isoformat()[0:19],
                   "NAME"   : config[CC.WRITER_SECTION][CC.CERTIFICATE_NAME],
                   "TITLE1" : config[CC.WRITER_SECTION][CC.CERTIFICATE_TITLE1],
                   "TITLE2" : config[CC.WRITER_SECTION][CC.CERTIFICATE_TITLE2],
                   "TITLE3": config[CC.WRITER_SECTION][CC.CERTIFICATE_TITLE3],
                   "PERSON1": config[CC.WRITER_SECTION][CC.CERTIFICATE_PERSON1],
                   "PERSON2": config[CC.WRITER_SECTION][CC.CERTIFICATE_PERSON2],
                   "PERSON3": config[CC.WRITER_SECTION][CC.CERTIFICATE_PERSON3],
                   "GIT-COMMIT"   : git_commit.replace("_","\_") + "\n\n\\tiny\n\nOther repositories:\n\n" + get_repo_info().to_latex(index=False) + "\n" + r"\vspace{0.5cm}\normalsize"+"\n\n",
                   "DPPARAMS"     : dpstring,
                   "MISSION-NAME" : str(os.getenv('MISSION_NAME')).replace("_","\_"),
                   "DRB-CLR-NUM"  : config.get(section=CC.WRITER_SECTION, option=CC.DRB_CLR_NUM, fallback="None")
               })
    cp.add_config(config)
    cp.typeset(certificate_path)
    logging.info("typeset certificate to %s",certificate_path)



if __name__=="__main__":
    """Driver program to run the DAS and then upload the statistics"""


    if das_framework.__version__ < MIN_FRAMEWORK_VERSION:
        raise RuntimeError("das_framework is out of date; please update to version {}".format(MIN_FRAMEWORK_VERSION))
    if (not hasattr(ctools,'__version__')) or ctools.__version__ < MIN_CTOOLS_VERSION:
        raise RuntimeError("das_framework.ctools is out of date; please update to version {}".format(MIN_FRAMEWORK_VERSION))


    ## Make the py4j logger not horrible
    ## See https://stackoverflow.com/questions/34248908/how-to-prevent-logging-of-pyspark-answer-received-and-command-to-send-messag
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # these seem to be not the problem:
    # logging.getLogger('pyspark').setLevel(logging.ERROR)
    # logging.getLogger("matplotlib").setLevel(logging.ERROR)

    # Option processing goes first
    # If we make a BOM, it happens here, and we never return
    additional_args = [ (['--pdf_only'], {'help':'Only generate the PDF certificate', }),
                        (['--dry-read'], {'help':'Specifies an S3 file where data is read from. Requires --dry-run'}),
                        (['--dry-write'],{'help':'Specifies an S3 file where data is written to. Requires --dry-read'}) ]
    (args, config) = das_framework.driver.main_setup( additional_args=additional_args)
    config.validate()
    config_lower = [val.lower() for val in config]
    ###
    ### Validate Configuration File
    ###
    # Look for deprecated variables
    DEPRECATED_CONFIG_VARIABLES = [(CC.OUTPUT_FNAME, CC.WRITER)]
    for (var, section) in DEPRECATED_CONFIG_VARIABLES:
        if var in config[section]:
            raise RuntimeError(f"config file contains deprecated variable {var} in section [{section}]")

    ###
    ### Set up Gurobi
    ###

    # Add the appropriate Gurobi directory to the path.
    # This must be done *before* 'import gurobi' is executed because it can only be imported once.
    # (importing on the executors is done separately, since they don't run __main__).

    if 'gurobipy' in sys.modules:
        raise RuntimeError("Gurobipy has already been imported. It should only be imported in engine modules.")

    # Make sure that gurobi_path gets expanded with python version enviornment variable being properly set
    os.environ[CC.PYTHON_VERSION] = f'python{sys.version_info.major}.{sys.version_info.minor}'
    try:
        gurobi_path = config[CC.GUROBI_SECTION][CC.GUROBI_PATH]
    except KeyError:
        raise RuntimeError(f"[{CC.GUROBI_SECTION}] {CC.GUROBI_PATH} is not defined in config file")
    gurobi_path = os.path.expandvars( gurobi_path )
    sys.path.insert(0, gurobi_path)

    # import gurobi so it will appear on the BOM and certificate
    import gurobipy as gb

    ### If we are making the certificate, generate it now, since gurobi is loaded.
    if args.pdf_only:
        produce_certificate( config, args.pdf_only)
        exit(0)

    # If RFC 748 (planned failure probability) for Gurobi license acquisition is enabled, give a warning.
    gurobi_lic_fail_rate = config.getfloat(section=CC.GUROBI, option=CC.GUROBI_LIC_FAIL_RATE, fallback=CC.GUROBI_LIC_FAIL_DEFAULT)
    if gurobi_lic_fail_rate != CC.GUROBI_LIC_FAIL_DEFAULT:
        logging.warning(f'WARNING: Gurobi license acquisition failure rate is set to {gurobi_lic_fail_rate}.')


    ###
    ### Set up the UNIX environment variables as necessary
    ###

    applicationId = clogging.applicationId()
    logging.info("applicationId: %s",applicationId)

    os.environ[CC.APPLICATIONID_ENV] = applicationId
    os.environ[CC.PYTHON_VERSION] = f'python{sys.version_info.major}.{sys.version_info.minor}'

    if CC.CLUSTERID_ENV not in os.environ:
        logging.warning("{} environment variable not set; setting to {}".format(CC.CLUSTERID_ENV, CC.CLUSTERID_UNKNOWN))
        os.environ[CC.CLUSTERID_ENV] = CC.CLUSTERID_UNKNOWN

    ###env
    ### Set up the Mission. It may be already set; if so, use it. Otherwise a new one will be added.
    ### Make sure that the mappers on the worker node have access to the das_run_id
    ###

    mission_name = dashboard.get_mission_name()
    display_mission(mission_name)
    dashboard.das_log(mission_name + ' starting', extra={'start':'now()'})
    if CC.ENVIRONMENT_SECTION.lower() not in config_lower:
        config.add_section(CC.ENVIRONMENT_SECTION)
    config[CC.ENVIRONMENT_SECTION][CC.DAS_RUN_UUID] = os.environ[CC.DAS_RUN_UUID]

    ###
    ### print the message in the config file, if there is one
    ###
    try:
        message = config[CC.ALERT_SECTION][CC.MESSAGE]
    except KeyError as e:
        pass
    else:
        dashboard.das_log(message)

    ###
    ### Create the DAS object
    ###

    output_path_raw = config[CC.WRITER][CC.OUTPUT_PATH]
    delegate = DASDelegate(mission_name = mission_name)

    try:
        das = das_framework.driver.main_make_das(args, config, delegate=delegate)
    except RuntimeError as e:
        ### Something is wrong with the Configuration or setup
        delegate.log_testpoint("003F")
        raise e

    ### Announce that setup validation succeeded
    delegate.log_testpoint("003S")

    ###
    ### Announce that we are starting up!
    ###
    delegate.log_testpoint("004S")

    # Update the DFXML
    ET.SubElement(das.dfxml_writer.doc, CC.DAS_DFXML).text = str(CC.DAS_DFXML_VERSION)

    # Have all annotations go to das_log, but no need to print, because annotations print
    das.add_annotation_hook(lambda message: dashboard.das_log(message, verbose=False))

    # Create the sourcecode release and give it the same name as the logfile, except change
    # ".log" to ".release.zip"
    assert das.logfilename.endswith(".log")
    release_zipfilename = re.sub(r".log$", CC.RELEASE_SUFFIX, das.logfilename)
    assert not os.path.exists(release_zipfilename)
    cmd = [sys.executable, __file__, '--logfile', '/dev/null', '--make_release', release_zipfilename, args.config]
    logging.info("Creating release: {}".format(" ".join(cmd)))
    try:
        subprocess.check_call(cmd)
    except subprocess.SubprocessError as e:
        logging.error(f"Creating release failed: subprocess returned error {e}, {sys.exc_info()}")
    assert os.path.exists(release_zipfilename)

    ###
    ### Set up Logging and instrumentation
    ###

    # Make sure Spark log file directories exist
    for option in [CC.SPARK_LOCAL_DIR, CC.SPARK_EVENTLOG_DIR]:
        try:
            os.makedirs(config[CC.SPARK][option], exist_ok=True)
        except KeyError as e:
            pass

    ###################################################################
    ### The heartbeat prints regular status information on the screen
    ###
    heartbeat_frequency = config.getint(section=CC.GUROBI_SECTION,option=CC.HEARTBEAT_FREQUENCY,
                                        fallback=CC.HEARTBEAT_FREQUENCY_DEFAULT)
    if heartbeat_frequency>0:
        heartbeat_thread = threading.Thread(target=dashboard_heartbeat, name='heartbeat', daemon=True,
                                            kwargs={'config':config,
                                                    'main_thread':threading.current_thread() })
        heartbeat_thread.start()


    ###
    ### Populate the Logs
    ###

    # put the applicationId into the environment and log it
    git_commit = " ".join(get_git_hash( Path(dirname(abspath(__file__)))))
    try:
        git_hash   = git_commit.split()[2]
    except (ValueError,TypeError,KeyError) as e:
        pass
    try:
        num_geolevels = config[CC.GEODICT][CC.GEODICT_GEOLEVELS].count(",") + 1
    except KeyError:
        num_geolevels = None
    dashboard.das_log(extra={CC.APPLICATIONID: applicationId,
                             CC.NUM_GEOLEVELS: num_geolevels,
                             CC.GIT_COMMIT: git_hash},
                      log_spark=True, debug=True)
    logging.info(json.dumps({CC.APPLICATIONID: applicationId,
                             CC.START: time.time(),
                             CC.FUNC: ' __main__' }))

    # Write the application ID alongside the log
    appid_filename=re.sub(r".log$", '.appid', das.logfilename)
    appid_file = open(appid_filename,"w")
    appid_file.write(applicationId)
    appid_file.close()
    print( f"das.logfilename={das.logfilename} appidfilename={appid_filename} appid={applicationId}")

    # Write the config alongside the log too
    flat_config_filename=re.sub(r".log$", '.config', das.logfilename)
    config.write(open(flat_config_filename,"w"))

    # Dump the spark configuration
    if cspark.spark_running():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        for (key, val) in spark.sparkContext.getConf().getAll():
            logging.info("{}={}".format(key, val))

    # Save the git commit SHA-1 hash to the config file, so we know on
    # which commit it ran
    #
    # WARNING: It is not guaranteed to get the same code that ran,
    # since files can be modified and not committed at the run time
    # (should add message)

    if config.getboolean(CC.WRITER, CC.SAVE_GIT_COMMIT, fallback=False):
        add_git_commit_to_config(das)

    ###
    ### Upload all of the [sections] and name=value configurations to the dashboard,
    ### where they are stored in an SQL database to allow searching for all runs.
    ### The flattened config file is also available separately as a full text file stored in the ZIP file
    ### and also stored on the PDF certificate.

    dashboard.config_upload(config)

    ###
    ### Run the main das algorithm
    ###

    delegate.log_testpoint("002S")
    try:
        if args.dry_run:
            das.annotate("--dry-run specified; will not run TopDown algorithm.")
            do_dry_run(args)
        else:
            das_framework.driver.main_run_das(das, shutdown_logging_on_exit=False)
    except Exception as e:
        print(str(e),file=sys.stderr)
        delegate.log_testpoint("001F")
        raise e

    ###
    ### SOL-124.135.250.020 : Produce a certificate
    ###
    certificate_path     = re.sub(r".log$", config[CC.WRITER_SECTION][CC.CERTIFICATE_SUFFIX], das.logfilename)
    produce_certificate(config, certificate_path, git_commit=git_commit, das=das)
    das.writer.add_output_path( certificate_path )

    ## The certificate is automatically added to the .zip file because it has the same prefix as the logfile
    ## the run_cluster.sh script makes the zip file and uploads to s3

    ###
    ### Start the orderly shutdown
    ###

    logging.info(json.dumps({CC.APPLICATIONID: applicationId,
                             CC.TERMINATED: True,
                             CC.STOP: time.time(),
                             CC.FUNC: ' __main__'}))

    # Terminate the DFXML writer. Previously we uploaded the DFXML file to S3. Now it jus gets uploaded with the ZIP file.
    if das.dfxml_writer.filename:
        das.dfxml_writer.exit()


    total_seconds = int(das.running_time())
    minutes = int(total_seconds / 60)
    seconds = total_seconds % 60

    dashboard.das_log(mission_name + ' finished', extra={'stop': 'now()', 'exit_code':0 })
    dashboard.SQS_Client().flush()

    if das.output_paths:
        das.log_and_print("\nDAS OUTPUT FILES:")
        for path in das.output_paths:
            das.log_and_print('    '+path)
        das.log_and_print("")

    dashboard.das_log(mission_name + ' finished', extra={'stop': 'now()', 'exit_code':0 })
    dashboard.SQS_Client().flush()
    requestResize(CC.TAKEDOWN_SECTION)
    das.log_and_print(f"{sys.argv[0]}: Elapsed time: {total_seconds} seconds ({minutes} min, {seconds} seconds)")
    delegate.log_testpoint("010S") # DAS application completed
    logging.shutdown()
    exit(0)
