"""
pickle_picker:

Script for manipulating pickled data.
Based on convert.py
"""

import os
import sys
import time
import logging
import pickle
from configparser import ConfigParser
from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr
from programs.writer.mdf2020writer import MDF2020HouseholdWriter, MDF2020PersonWriter, addEmptyAndGQ
from programs.writer.rowtools import makeHistRowsFromMultiSparse
from das_constants import CC
import programs.das_setup as ds
from das_framework.ctools.s3 import s3open

from pyspark import RDD


if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

PERSON = "persons"
HOUSEHOLD = "household"

try:
    from pyspark.sql import SparkSession, DataFrame
except ImportError:
    logging.error("This program must be run under spark-submit")
    exit(1)

from das_framework.das_stub import DASStub

class NonConvertingMDF2020HouseholdWriter(MDF2020HouseholdWriter):
    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        def node2SparkRows(node: dict):
            # nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))

            # node already comes as a dict, but let's still clear everything except for SYN, INVAR and GEOCODE.
            nodedict = {CC.SYN: node[CC.SYN], CC.GEOCODE: node[CC.GEOCODE]}
            nodedict[CC.INVAR] = node[CC.INVAR] if CC.INVAR in node else node['_invar']

            households = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            units = addEmptyAndGQ(nodedict, schema, households, row_recoder=self.row_recoder,
                                  gqtype_recoder=HHGQUnitDemoProductAttr.das2mdf)
            return units

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)
        return df


class NonConvertingMDF2020PersonWriter(MDF2020PersonWriter):
    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        def node2SparkRows(node: dict):
            # nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))
            nodedict = {CC.SYN: node[CC.SYN],CC.GEOCODE: node[CC.GEOCODE]}
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            return persons

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)

        return df

DEFAULT_PERSON = "${DAS_S3ROOT}/users/user007/DemonstrationProducts_Sept2019/full_person_BAK/td4/run_0000/persons"
DEFAULT_CONFIG = "${DAS_S3ROOT}/users/user007/DemonstrationProducts_Sept2019/full_person_BAK/td4/run_0000/config.ini"


def old_main(s3path: str, config_path: str) -> None:
    print('Beginning of pickle picker')
    logging.info("Beginning of pickle picker")

    spark = SparkSession.builder.getOrCreate()
    files_shipped = False

    logging.basicConfig(filename="convert.log", format="%(asctime)s %(filename)s:%(lineno)d (%(funcName)s) %(message)s")
    invar_loaded = False
    print(f'Source data: {s3path}')
    print(f'Config file located at: {config_path}')

    config = ConfigParser()
    config.read_string( s3open(config_path).read() )

    """
    print(f'existing writer section: {str(list(config.items(section=CC.WRITER_SECTION)))}')
    output_datafile_name = config.get(CC.WRITER_SECTION, CC.OUTPUT_DATAFILE_NAME)
    print(f'section:writer, output_datafile_name: {output_datafile_name}')
    output_path = f'{experiment.folder}_unpickled/{sub_folder}/run_000{str(run_number)}'
    config.set(CC.WRITER_SECTION, CC.OUTPUT_PATH, output_path)
    config.set(CC.WRITER_SECTION, CC.S3CAT, '1')
    config.set(CC.WRITER_SECTION, CC.S3CAT_SUFFIX, '.csv')
    config.set(CC.WRITER_SECTION, CC.OVERWRITE_FLAG, '0')
    config.set(CC.WRITER_SECTION, CC.WRITE_METADATA, '1')
    config.set(CC.WRITER_SECTION, CC.CLASSIFICATION_LEVEL, 'C_U_I//CENS')
    config.set(CC.WRITER_SECTION, CC.NUM_PARTS, '5000')
    print(f'modified writer section: {str(list(config.items(section=CC.WRITER_SECTION)))}')
    print(f'section:schema: {str(list(config.items(section=CC.SCHEMA)))}')

    # print(f'str(nodes_dict_rdd.take(1)): {str(nodes_dict_rdd.take(1))}')



    print(f"Reading pickled data: {s3path}")
    """

    # Ship the files to spark and get the setup object
    das_stub = DASStub()
    das_stub.t0 = time.time()
    das_stub.output_paths = []
    setup      = ds.DASDecennialSetup(config=config, name='setup', das=das_stub)
    setup_data = setup.setup_func()
    nodes_dict_rdd = spark.sparkContext.pickleFile(s3path)
    """
    a_node_dict = nodes_dict_rdd.take(1)[0]
    if not (experiment.type is PERSON):
        if CC.INVAR not in a_node_dict and '_invar' not in a_node_dict:
            if not invar_loaded:
                invar_rdd = spark\
                    .sparkContext\
                    .pickleFile('${DAS_S3ROOT}/users/user007/experiments/full_household/Sept12_TestMUD_VA_PLB_Experiment/td001/run_0000/data') \
                    .map(lambda nd: (nd[CC.GEOCODE], nd['_invar']))
                invar_loaded = True
            nodes_dict_rdd = nodes_dict_rdd\
                .map(lambda nd: (nd[CC.GEOCODE], nd[CC.SYN]))\
                .join(invar_rdd)\
                .map(lambda g_sk: {CC.GEOCODE: g_sk[0], CC.SYN: g_sk[1][0], CC.INVAR: g_sk[1][1]})

    # print(nodes_dict_rdd.count())
    # from rdd_like_list import RDDLikeList
    # nodes_dict_rdd = RDDLikeList(nodes_dict_rdd.take(10))

    if experiment.type is PERSON:
        print('Using Person Writer')
        w = NonConvertingMDF2020PersonWriter(config=config, setup=setup_data, name='writer', das=das_stub)
    else:
        print('Using Household Writer')
        w = NonConvertingMDF2020HouseholdWriter(config=config, setup=setup_data, name='writer', das=das_stub)

    print('Writing')
    """

    # calls programs.writer.write() which takes an engine_tuple
    # engine_tuple is blocknoderdd
    # w.write((nodes_dict_rdd, None))
    # For testing, just take the first record and print it
    record = nodes_dict_rdd.take(1)
    print("record:",record)


def process_file(fname: str) -> None:
    data = pickle.loads(open(fname,"rb").read())
    print("data:",data)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Explain what's in one of the pickle files" )
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("s3path", nargs='?', help="File to analyze", type=str, default=DEFAULT_PERSON)
    parser.add_argument("config", nargs='?', help="Config file", type=str, default=DEFAULT_CONFIG)

    args = parser.parse_args()

    if not args.s3path.startswith("s3"):
        raise RuntimeError("s3path must start with s3")
    old_main(args.s3path, args.config)
