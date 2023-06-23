"""
Simple script for taking existing results from specific experiments in pickled format
and turning them into CSV (pipe-delimited, actually) microdata files instead
"""

import os
import sys
import logging

from configparser import ConfigParser

from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr
from programs.writer.mdf2020writer import MDF2020HouseholdWriter, MDF2020PersonWriter, addEmptyAndGQ
from programs.writer.rowtools import makeHistRowsFromMultiSparse

from programs.das_setup import DASDecennialSetup
from das_framework.das_stub  import DASStub

from das_framework.ctools.s3 import s3open
from das_constants import CC

from pyspark import RDD


if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame


class NonConvertingMDF2020HouseholdWriter(MDF2020HouseholdWriter):
    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        def node2SparkRows(node: dict):
            # nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))

            # node already comes as a dict, but let's still clear everything except for CC.SYN, CC.INVAR and CC.GEOCODE.
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
            nodedict = {CC.SYN: node[CC.SYN], CC.GEOCODE: node[CC.GEOCODE]}
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            return persons

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)

        return df

# DHC-P Experiment # 1 (default manual strategy


class ExperimentPath:
    def __init__(self, type, folder, sub_folders, runs):
        self.type = type
        self.folder = folder
        self.sub_folders = sub_folders
        self.runs = runs


PERSON = "persons"
HOUSEHOLD = "household"

# Ugly. Can be done by dynamically by querying S3.
EXPERIMENTS = [
    ExperimentPath(PERSON,
        "${DAS_S3ROOT}/users/user007/DemonstrationProducts_Sept2019/full_person_BAK",
        [
            "td4"
        ],
        1
     )
]

if __name__ == '__main__':

    print('Beginning of conversion script.')

    spark = SparkSession.builder.getOrCreate()
    files_shipped = False

    logging.basicConfig(filename="convert.log", format="%(asctime)s %(filename)s:%(lineno)d (%(funcName)s) %(message)s")
    for experiment in EXPERIMENTS:
        invar_loaded = False
        print(f'Converting {experiment.type} experiment at: {experiment.folder}')
        for sub_folder in experiment.sub_folders:
            for run_number in range(experiment.runs):
                full_path = f'{experiment.folder}/{sub_folder}/run_000{str(run_number)}'
                config_path = f'{experiment.folder}/{sub_folder}/run_000{str(run_number)}/config.ini'

                print(f'Converting experiment at (full path) {full_path}')
                print(f'Config file located at: {config_path}')

                config_file = s3open(config_path).read()

                print(f'type(config file): {type(config_file)}')
                print(f'config file: {config_file}')

                config = ConfigParser()
                config.read_string(config_file)

                print(f'existing writer section: {str(list(config.items(section=CC.WRITER_SECTION)))}')
                output_datafile_name = config.get(CC.WRITER_SECTION, CC.OUTPUT_DATAFILE_NAME)
                print(f'section:writer, output_datafile_name: {output_datafile_name}')

                output_path = f'{experiment.folder}_unpickled/{sub_folder}/run_000{str(run_number)}'
                full_path = f'{full_path}/{output_datafile_name}/'
                config.set(CC.WRITER_SECTION, CC.OUTPUT_PATH, output_path)
                config.set(CC.WRITER_SECTION, CC.S3CAT, '1')
                config.set(CC.WRITER_SECTION, CC.S3CAT_SUFFIX, '.csv')
                config.set(CC.WRITER_SECTION, CC.OVERWRITE_FLAG, '0')
                config.set(CC.WRITER_SECTION, CC.WRITE_METADATA, '1')
                config.set(CC.WRITER_SECTION, CC.CLASSIFICATION_LEVEL, 'C_U_I//CENS')
                config.set(CC.WRITER_SECTION, CC.NUM_PARTS, '5000')

                print(f'modified writer section: {str(list(config.items(section=CC.WRITER_SECTION)))}')

                print(f'section:schema: {str(list(config.items(section=CC.SCHEMA)))}')

                print(f'Converting experiment at (full path) {full_path} to {output_path}')

                # print(f'str(nodes_dict_rdd.take(1)): {str(nodes_dict_rdd.take(1))}')

                das_stub = DASStub()

                setup_instance = DASDecennialSetup(config=config, name='setup', das=das_stub)
                if not files_shipped:
                    setup_instance = setup_instance.setup_func()  # This ships files to spark
                    files_shipped = True

                print(f"Reading pickled data: {full_path}")

                nodes_dict_rdd = spark.sparkContext.pickleFile(full_path)

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
                    w = NonConvertingMDF2020PersonWriter(config=setup_instance.config, setup=setup_instance, name='writer',
                                                         das=das_stub)
                else:
                    print('Using Household Writer')
                    w = NonConvertingMDF2020HouseholdWriter(config=setup_instance.config, setup=setup_instance, name='writer', das=das_stub)

                print('Writing')
                w.write((nodes_dict_rdd, None))
                print(f'Finished Converting experiment at (full path) {full_path}')
        print(f'Finished Converting {experiment.type} experiment at: {experiment.folder}')
    print('End of conversion script.')
