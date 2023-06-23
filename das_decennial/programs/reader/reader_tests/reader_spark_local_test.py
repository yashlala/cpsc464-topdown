import os
import numpy as np
import pytest
import pickle
from configparser import ConfigParser, NoOptionError

from pyspark.sql import SparkSession

import programs.reader.sql_spar_table
from programs.nodes.nodes import GeounitNode
from exceptions import DASConfigError
from das_constants import CC
import das_utils
from das_framework.das_stub import DASStub

READER = CC.READER
TABLES = CC.TABLES
PATH = CC.PATH
TABLE_CLASS = CC.TABLE_CLASS
VARS = CC.VARS
SEX = CC.ATTR_SEX
LEGAL = CC.LEGAL
CENRACE = CC.ATTR_CENRACE
VAR_TYPE = CC.VAR_TYPE

from programs.tests.spark_fixture import spark  # Might show up as not used, but it is a fixture, it's used in the function arguments. Do not remove.
spark_fixture = spark                           # This line is so that the previous import is formally used

PL94_SHAPE = (8, 2, 2, 63)

def npArray(a):
    return das_utils.npArray(a, PL94_SHAPE)


@pytest.fixture()
def datafiles():
    #### We don't need to put these files to HDFS when running in local mode

    import subprocess
    datafiles = ['person.csv', 'unit.csv']
    subprocess.run(
        ['hdfs', 'dfs', '-put'] + list(map(lambda fn: os.path.join(os.path.dirname(__file__), fn), datafiles)) + ['.'])
    yield datafiles
    subprocess.run(['hdfs', 'dfs', '-rm', *datafiles])

config_changes_list = [
    {READER:
         {
             "PersonData.class": "programs.reader.table.DenseHistogramTable",
             "UnitData.class": "programs.reader.table.UnitFromPersonTable",
         }
    },
    {READER:
         {
             "PersonData.class": "programs.reader.spar_table.SparseHistogramTable",
             "UnitData.class": "programs.reader.spar_table.UnitFromPersonRepartitioned",
         }
     },
    {READER:
         {
             "PersonData.class": "programs.reader.sql_spar_table.SQLSparseHistogramTable",
             "UnitData.class": "programs.reader.spar_table.UnitFromPersonRepartitioned",
         }
    }
]


@pytest.fixture(scope="function")
def s_config():
    curdir = os.path.dirname(__file__)
    return f"""
[setup]
spark.name: DAS
spark.loglevel: ERROR

[schema]
schema: PL94

[budget]
queriesfile: foo
geolevel_budget_prop: 0.25, 0.25, 0.25, 0.25
strategy: DetailedOnly
global_scale: 1

[geodict]
geolevel_names: Block,Block_Group,Tract,County
geolevel_leng: 16,12,11,1
aian_areas:Legal_Federally_Recognized_American_Indian_Area,American_Indian_Joint_Use_Area,Hawaiian_Home_Land,Alaska_Native_Village_Statistical_Area,State_Recognized_Legal_American_Indian_Area,Oklahoma_Tribal_Statistial_Area

[{READER}]
input_data_vintage: 1000
{READER}: programs.reader.table_reader.DASDecennialReader
{TABLES}: PersonData UnitData

main_table: PersonData
unit_table: UnitData

PersonData.class: programs.reader.table.DenseHistogramTable
UnitData.class: programs.reader.table.UnitFromPersonTable

PersonData.path: file:///{os.path.join(curdir, 'person.csv')}
UnitData.path: file:///{os.path.join(curdir, 'unit.csv')}

delimiter: ,
header: True

PersonData.variables: MAFID geocode white black aian asian nhopi other hispanic sex householder votingage hhgq
UnitData.variables: MAFID geocode white black aian asian nhopi other hispanic sex householder votingage hhgq
linkage: geocode


geocode.{VAR_TYPE}: str
geocode.{LEGAL}: 0000000000000000-9999999999999999
MAFID.{VAR_TYPE}: str
MAFID.{LEGAL}: 000000000-999999999
householder.{VAR_TYPE}: int
householder.{LEGAL}: 0,1
{SEX}.{VAR_TYPE}: int
{SEX}.{LEGAL}: 0,1
votingage.{VAR_TYPE}: int
votingage.{LEGAL}: 0,1
hispanic.{VAR_TYPE}: int
hispanic.{LEGAL}: 0,1
white.{VAR_TYPE}: int
white.{LEGAL}: 0,1
black.{VAR_TYPE}: int
black.{LEGAL}: 0,1
aian.{VAR_TYPE}: int
aian.{LEGAL}: 0,1
asian.{VAR_TYPE}: int
asian.{LEGAL}: 0,1
nhopi.{VAR_TYPE}: int
nhopi.{LEGAL}: 0,1
other.{VAR_TYPE}: int
other.{LEGAL}: 0,1
hhgq.{VAR_TYPE}: int
hhgq.{LEGAL}: 0-7

PersonData.recoder: programs.reader.e2e_recoder.table8_recoder

PersonData.recode_variables: {CENRACE}


{CENRACE}: white black aian asian nhopi other

{CENRACE}.{VAR_TYPE}: int
{CENRACE}.{LEGAL}: 0-62



PersonData.geography: geocode
PersonData.histogram: hhgq votingage hispanic {CENRACE}

UnitData.geography: geocode
UnitData.histogram: hhgq
UnitData.unique: MAFID

{CC.SAVE_READER_CHECKPOINTS} = False

[constraints]
theInvariants.Block: tot,va,gqhh_vect,gqhh_tot
theConstraints.Block: total,hhgq_total_lb,hhgq_total_ub,nurse_nva_0

[writer]
    """

@pytest.fixture(params=config_changes_list)
def config(s_config, request):
    config = ConfigParser()
    config.read_string(s_config)
    ### The following are used for when the files are put to HDFS
    # config['reader']['PersonData.path'] = 'person.csv'
    # config['reader']['UnitData.path'] = 'unit.csv'
    for section_name, section in request.param.items():
        for option, value in section.items():
            config.set(section_name, option, value)
    return config

@pytest.fixture()
def reader_instance(spark, config, dd_das_stub):
    import programs.reader.table_reader as tr_spark
    import programs.das_setup as ds
    setup_instance = ds.DASDecennialSetup(config=config, name='setup', das=dd_das_stub)
    return tr_spark.DASDecennialReader(config=setup_instance.config, setup=setup_instance, name='reader', das=dd_das_stub)

@pytest.mark.parametrize('config, y', [({'schema':{'schema':'AAA'}}, 'b')], indirect=['config'])
def test_indirect(config,y):
    assert config.get('schema', 'schema') == 'AAA'

@pytest.mark.parametrize("remove_option, msg, errtype", [
    (TABLES, f"No option \'{TABLES}\' in section: \'{READER}\'", NoOptionError),
    (f"UnitData.{PATH}", f"No option \'UnitData.{PATH}\' in section: \'{READER}\'", NoOptionError),
    (f"PersonData.{TABLE_CLASS}", f"Key \"PersonData.{TABLE_CLASS}\" in config section [{READER}] not found when specifying module to load", KeyError),
    (f"PersonData.{VARS}", f"No option \'PersonData.{VARS}\' in section: \'{READER}\'", NoOptionError),
    (f"{SEX}.{LEGAL}", f"Missing variable {SEX} specifications: No option '{SEX}.{LEGAL}' in config section: [{READER}]", DASConfigError),
    (f"{CENRACE}.{LEGAL}", f"Missing variable {CENRACE} specifications: No option '{CENRACE}.{LEGAL}' in config section: [{READER}]", DASConfigError),
])
def test_configReaderValidate(s_config, remove_option, msg, errtype, dd_das_stub):
    import programs.reader.table_reader as tr_spark
    import programs.das_setup as ds
    config = ConfigParser()
    config.read_string(s_config)
    config.remove_option(READER, remove_option)
    setup_instance = ds.DASDecennialSetup(config=config, name='setup', das=dd_das_stub)
    with pytest.raises(errtype) as err:
        tr_spark.DASDecennialReader(config=setup_instance.config, setup=setup_instance, name='reader', das=dd_das_stub)
    if errtype == KeyError:
        assert msg.lower() in err.value.args[0].lower()
    else:
        assert msg.lower() in err.value.message.lower()


def test_reader_init(reader_instance):
    r = reader_instance #(spark, config)
    assert r.setup.hist_shape == PL94_SHAPE
    assert tuple(r.setup.hist_vars) == ("hhgq", "votingage", "hispanic", f"{CENRACE}")
    assert r.setup.levels[0] == "Block"
    assert r.unit_table_name == "UnitData"
    assert r.main_table_name == "PersonData"
    assert r.data_names == ["PersonData", "UnitData"]


@pytest.fixture(scope="function")
def read_data(reader_instance, datafiles):
    l = []
    for table in reader_instance.tables.values():
        table1 = table.load()
        table1 = table.pre_recode(table1)
        table1 = table.process(table1)
        l.append(table1)
    return l
    #return [table.process(table.pre_recode(table.load(), reader_instance.config)) for table in reader_instance.tables.values()]

#@pytest.mark.parametrize('config', [{'schema':{'schema':'AAA'}}], indirect=['config'])
def test_read_tables(read_data, config):
    person_data, unit_data = tuple(map(lambda d: d.collect()[0], read_data))
    assert person_data[0] == ('1234567890ABCDEF',)
    assert unit_data[0] == ('1234567890ABCDEF',)
    assert np.array_equal(npArray(unit_data[1]), np.array([1, 0, 0, 0, 0, 0, 0, 0]))
    assert npArray(person_data[1])[0][1][0][0] == 1

@pytest.fixture(scope="function")
def join_data(read_data):
    return read_data[0].rightOuterJoin(read_data[1]).collect()[0]


def test_join(join_data):
    assert np.sum(join_data[1][0]).astype(int) == 2
    assert np.sum(join_data[1][1]).astype(int) == 1


def test_make_block_node(reader_instance, join_data):
    bn = reader_instance.makeBlockNode(join_data)
    assert np.array_equal(bn.getDenseRaw(), npArray(join_data[1][0]))
    assert bn.geocode == join_data[0][0]
    assert bn.dp is None
    assert bn.syn is None
    assert bn.syn_unrounded is None
    assert bn.dp_queries is None

def test_compare_with_saved(reader_instance, join_data):
    """
    WILL CHANGE IF TEST DATA  in .txt FILES or CONFIG CHANGES!
    """
    bn = reader_instance.makeBlockNode(join_data)
    fname = os.path.join(os.path.dirname(__file__), 'geounitnode.pickle')
    # with(open(fname, 'wb')) as f:
    #     pickle.dump(bn.toDict(keep_attrs=bn.__slots__), f)
    sbn = GeounitNode.fromDict(pickle.load(open(fname, 'rb')))
    assert sbn == bn


def test_read(reader_instance, join_data):
    block_nodes = reader_instance.read()
    assert np.array_equal(block_nodes.collect()[0].getDenseRaw(), npArray(join_data[1][0]))


def test_processPersonTable(reader_instance, spark: SparkSession, dd_das_stub):
    data = [
        ("1", "2", 1, 0),
        ("1", "2", 1, 0),
        ("1", "2", 1, 1),
        ("2", "1", 1, 1)
    ]
    num_partitions = 10
    gv = ('ga', 'gb', )
    hv = ('a', 'b',)
    variables = gv + hv

    r = reader_instance
    r.num_reader_partitions = 8
    r.config.set(READER, "PersonData.variables", "ga gb a b")
    r.config.set(READER, f"PersonData.geography", "ga gb")
    r.config.set(READER, "ga.type", "str")
    r.config.set(READER, "ga.legal", "0-9")
    r.config.set(READER, "gb.type", "str")
    r.config.set(READER, "gb.legal", "0-9")
    r.config.set(READER, f"PersonData.histogram", "a b")
    r.config.set(READER, "a.type", "int")
    r.config.set(READER, "b.type", "int")
    r.config.set(READER, "a.legal", "0,1")
    r.config.set(READER, "b.legal", "0,1")
    t_class = das_utils.class_from_config(r.config, f"PersonData.{TABLE_CLASS}", READER)
    t = t_class(name="PersonData", das=dd_das_stub, config=r.config, reader_instance=r)
    t.data_shape = (2, 2)
    df = spark.createDataFrame(data, variables).repartition(num_partitions)
    nodes = dict(t.process(df).collect())
    assert np.array_equal(das_utils.npArray(nodes[("1", "2")], (2, 2)), np.array([[0, 0], [2, 1]]))
    assert np.array_equal(das_utils.npArray(nodes[("2", "1")], (2, 2)), np.array([[0, 0], [0, 1]]))

@pytest.mark.parametrize("idx, shape",[
    ((5,0,0), (10,3,2)),
    ((5,2,1), (10,3,2)),
    ((1,2,3,4,5), (6,7,8,9,10))
])
def test_np_rmi(idx, shape):
    assert programs.reader.sql_spar_table.SQLSparseHistogramTable.nprmi(idx, shape) == np.ravel_multi_index(idx, shape)
