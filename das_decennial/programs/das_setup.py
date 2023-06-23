# DAS Spark setup module
# Some Human
# Last modified: 7/12/2018

"""
    This is the setup module for DAS development on Research 2 and on EMR clusters.
    It launches Spark.
"""
import csv
import os
import logging
import tempfile
import subprocess
from typing import Tuple, Dict, List
from hdfs_logger import setup as hdfsLogSetup
from programs.schema.schemas.schemamaker import SchemaMaker, _unit_schema_dict
from programs.invariants.invariantsmaker import InvariantsMaker
from programs.s3cat import get_tmp
import programs.datadict as datadict
from programs.engine.budget import Budget
from das_utils import ship_files2spark
from das_framework.driver import AbstractDASSetup
from exceptions import DASConfigError
from das_constants import CC

SHIP_SUBDIRS = ('programs', 'das_framework', 'analysis')

# Bring in the spark libraries if we are running under spark
# This is done in a try/except so that das_setup.py can be imported for test continious test framework
# even when we are not running under Spark
#
try:
    from pyspark import SparkConf, SparkContext, SQLContext
    from pyspark.sql import SparkSession
except ImportError:
    pass

# Bring in the DAS modules. The debugging is designed to help us understand
# why it's failing sometimes under spark. We can probably clean some of this out.
# For example, why is it looking for the git repository???
# try:
#     from das_framework.driver import AbstractDASSetup
# except ImportError:
#     logging.debug("System path: {}".format(sys.path))
#     logging.debug("Working dir: {}".format(os.getcwd()))
#     path = os.path.realpath(__file__)
#     pathdir = os.path.dirname(path)
#     while True:
#         if os.path.exists(os.path.join(pathdir, ".git")):
#             logging.debug("Found repository root as: {}".format(pathdir))
#             break
#         pathdir = os.path.dirname(pathdir)
#         if os.path.dirname(path) == path:
#             raise ImportError("Could not find root of git repository")
#     path_list = [os.path.join(root, name) for root, dirs, files in os.walk(pathdir)
#                  for name in files if name == "driver.py"]
#     logging.debug("driver programs found: {}".format(path_list))
#     assert len(path_list) > 0, "Driver not found in git repository"
#     assert len(path_list) == 1, "Too many driver programs found"
#     sys.path.append(os.path.dirname(path_list[0]))
#     from das_framework.driver import AbstractDASSetup
# TK - FIXME - Get the spark.eventLog.dir from the config file and set it here, rather than having it set in the bash script
#              Then check to make sure the directory exists.


class DASDecennialSetup(AbstractDASSetup):
    """
        DAS setup class for 2018 development on research 2 and emr clusters.
    """
    levels: Tuple[str, ...]
    schema: str
    hist_shape: Tuple[int, ...]
    hist_vars: Tuple[str, ...]
    postprocess_only: bool
    inv_con_by_level: Dict[str, Dict[str, List[str]]]
    dir4sparkzip: str
    plb_allocation: Dict[str, float]   # Dictionary of Privacy Loss Budget proportion spent of the geounit, by its geocode.
    spine_type: str  # AIAN, NON-AIAN, OPTIMIZED
    geocode_dict: Dict[int, str]  # Lengths of substrings of geocode as key, geocode names as value, THE STANDARD ONE
    qalloc_string = ""  # String for query allocation to be filled by engine module, and accessed by other modules like writer

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        # Whether to run in spark (there is a local/serial mode, for testing and debugging)
        self.use_spark = self.getboolean(CC.SPARK, section=CC.ENGINE, default=True)

        # schema keyword
        self.schema = self.getconfig(CC.SCHEMA, section=CC.SCHEMA)
        self.log_and_print(f"schema keyword: {self.schema}")
        self.schema_obj = SchemaMaker.fromName(self.schema)
        self.unit_schema_obj = SchemaMaker.fromName(_unit_schema_dict[self.schema])

        # Partition size options:
        self.part_size_noisy = self.getint(CC.PART_SIZE_NOISY, section=CC.ENGINE, default=0)
        self.part_size_optimized = self.getint(CC.PART_SIZE_OPTIMIZED, section=CC.ENGINE, default=0)
        self.part_size_optimized_block = self.getint(CC.PART_SIZE_OPTIMIZED_BLOCK, section=CC.ENGINE, default=0)

        # Query stratagy / budget allocation
        self.budget = Budget(self, config=self.config, das=self.das)
        # Geographical level names
        self.levels = self.budget.levels
        self.geolevel_prop_budgets = self.budget.geolevel_prop_budgets
        self.privacy_framework = self.budget.privacy_framework
        self.dp_mechanism_name = self.budget.dp_mechanism_name
        self.log_and_print(f"Privacy mechanism: {self.dp_mechanism_name}")
        self.qalloc_string = self.budget.getAllocString()

        # Bottom level geolevel we are interested in. Defaults to the first level in self.levels, which is the lowest
        self.geo_bottomlevel = self.getconfig(CC.GEO_BOTTOMLEVEL, section=CC.GEODICT, default=self.levels[0])

        # Create geocode dict
        geolevel_leng = self.gettuple(CC.GEODICT_LENGTHS, section=CC.GEODICT)
        assert len(geolevel_leng) == len(self.levels), "Geolevel names and geolevel lengths differ in size"
        self.geocode_dict = {int(gl_length): gl_name for gl_name, gl_length in zip(self.levels, geolevel_leng)}
        self.log_and_print(f"<span style='color:blue; font-size:x-large'>Check geocode-dict: {self.geocode_dict}</span>")

        self.spine_type = self.getconfig(CC.SPINE, section=CC.GEODICT, default="non_aian_spine")
        if self.spine_type not in CC.SPINE_TYPE_ALLOWED:
            raise DASConfigError(msg=f"spine type must be {'/'.join(CC.SPINE_TYPE_ALLOWED)} rather than {self.spine_type}.", option=CC.SPINE, section=CC.BUDGET)

        self.grfc_path = os.path.expandvars(self.getconfig(CC.GRFC_PATH, section=CC.READER, default='${DAS_S3INPUTS}/2010-convert/grfc'))
        self.aian_areas = self.gettuple(CC.AIAN_AREAS, section=CC.GEODICT, sep=CC.REGEX_CONFIG_DELIM)
        self.aian_ranges_path = self.getconfig(key=CC.AIAN_RANGES_PATH, section=CC.GEODICT, default=CC.AIAN_RANGES_PATH_DEFAULT)
        self.strong_mcd_states = self.gettuple(CC.STRONG_MCD_STATES, section=CC.GEODICT, sep=CC.REGEX_CONFIG_DELIM, default=CC.STRONG_MCD_STATES_DEFAULT)

        self.input_data_vintage = self.getconfig(key=CC.INPUT_DATA_VINTAGE, section=CC.READER)
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.input_data_vintage}")

        self.log_and_print(f"geolevels: {self.levels}")

        self.postprocess_only = self.getboolean(CC.POSTPROCESS_ONLY, section=CC.ENGINE, default=False)
        self.validate_input_data_constraints = self.getboolean(CC.VALIDATE_INPUT_DATA_CONSTRAINTS, section=CC.READER, default=True)

        self.inv_con_by_level = {}
        for level in self.levels:
            self.inv_con_by_level[level] = {
                "invar_names": self.gettuple(f"{CC.THEINVARIANTS}.{level}", section=CC.CONSTRAINTS, default=()),
                "cons_names": self.gettuple(f"{CC.THECONSTRAINTS}.{level}", section=CC.CONSTRAINTS, default=())
            }

        try:
            # Person table histogram shape (set here and then checked/set in the reader module init)
            self.hist_shape = self.schema_obj.shape
            self.unit_hist_shape = self.unit_schema_obj.shape
            # Person table histogram variables (set here and then checked/set in the reader module init)
            self.hist_vars = self.schema_obj.dimnames
        except AssertionError:
            self.log_warning_and_print(f"Schema {self.schema} is not supported")

        noisy_partitions_by_level = self.gettuple_of_ints(CC.NOISY_PARTITIONS_BY_LEVEL, section=CC.WRITER_SECTION, default=",".join(("0",) * len(self.levels)))
        self.annotate(f'noisy_partitions_by_level: {noisy_partitions_by_level}')
        assert len(noisy_partitions_by_level) == len(self.levels), f'Config Error: noisy_partitions_by_level should be the same length as the geolevels. Found instead: self.levels: {self.levels}, noisy_partitions_by_level: {noisy_partitions_by_level }'

        self.noisy_partitions_dict = {self.levels[index]: noisy_partitions_by_level[index] for index in range(len(self.levels))}
        self.annotate(f'noisy_partitions_dict: {self.noisy_partitions_dict}')

        self.dvs_enabled = self.getboolean(CC.DVS_ENABLED, section=CC.DVS_SECTION, default=False)

        self.prim_geo_s3_path = self.getconfig(CC.PRIM_GEO_S3_PATH, section=CC.GEODICT, default=CC.DEFAULT_PRIM_GEO_S3_PATH)
        #self.prim_crosswalk = self.make_prim_crosswalk_dict(prim_geo_s3_path)

    @staticmethod
    def make_prim_crosswalk_dict(prim_geo_s3_path):
        # Note that prim_geo_s3_path should be either the file 'primgeo_crosswalk_2010tabblk.csv' or 'primgeo_crosswalk_2020tabblk.csv'
        # in the s3 prefix: $DAS_S3ROOT/users/user007/crosswalk_prims/
        # with tempfile.NamedTemporaryFile(dir=get_tmp(), mode='wb') as tf:
        #     subprocess.check_call(['aws', 's3', 'cp', '--quiet', '--no-progress', prim_geo_s3_path, tf.name])
        #     with open(tf.name, 'r', newline='') as csvfile:
        #         reader = csv.reader(csvfile)
        #         header = reader.__next__()
        #         prim_crosswalk_dict = {row[0]:row[1] for row in reader}
        spark = SparkSession.builder.getOrCreate()
        prim_crosswalk_dict =  dict(spark.read.csv(prim_geo_s3_path, header=True).rdd.map(lambda r: (r['tabblk'], r['primgeo'])).collect())
        return prim_crosswalk_dict

    def makeInvariants(self, *args, **kwargs):
        return InvariantsMaker.make(*args, schema=self.schema, **kwargs)

    def makeConstraints(self, *args, **kwargs):
        return datadict.getConstraintsModule(self.schema).ConstraintsCreator(*args, **kwargs).calculateConstraints().constraints_dict

    def setup_func(self):
        """
            Starts spark up in local mode or client mode for development and testing on Research 2.
        """
        # If we are making the BOM, just return
        if self.das.make_bom_only():
            return self
        # Validate the config file. DAS Developers: Add your own!
        pyspark_log = logging.getLogger('py4j')
        pyspark_log.setLevel(logging.ERROR)
        hdfsLogSetup(self.config)
        conf = SparkConf().setAppName(self.getconfig(f"{CC.SPARK}.{CC.NAME}"))

        # !!! SETTING THE SPARK CONF THIS WAY DOESN'T HAVE EFFECT, PUT IT IN THE RUN SCRIPT AS ARGUMENTS TO spark-submit
        # This should not have much effect in Python, since after the the data is pickled, its passed as bytes array to Java
        # Kryo is a Java serializer. But might be worth to just turn it on to see if there's a noticeable difference.
        # conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Currently we don't (or almost don't) use Spark SQL, so this is probably irrelevant
        # conf.set("spark.sql.execution.arrow.enabled", "true")
        # conf.set("spark.task.maxFailures", "30")
        # conf.set("spark.executor.pyspark.memory", "500K")
        # conf.set("spark.memory.fraction", "0.2")  # tuning GC

        sc = SparkContext.getOrCreate(conf=conf)

        # !!! THIS DOESN'T WORK EITHER, PUT IT IN THE RUN SCRIPT AS ARGUMENTS TO spark-submit
        # SparkSession(sc).conf.set("spark.default.parallelism", "300")

        sc.setLogLevel(self.getconfig(f"{CC.SPARK}.{CC.LOGLEVEL}"))
        ship_files2spark(SparkSession(sc), allfiles=True, subdirs=SHIP_SUBDIRS)

        # if sc.getConf().get("{}.{}".format(CC.SPARK, CC.MASTER)) == "yarn":
        #     # see stackoverflow.com/questions/36461054
        #     # --py-files sends zip file to workers but does not add it to the pythonpath.
        #     try:
        #         sc.addPyFile(os.environ[CC.ZIPFILE])
        #     except KeyError:
        #         logging.info("Run script did not set environment variable 'zipfile'.")
        #         exit(1)
        # return SparkSession(sc)
        return self
