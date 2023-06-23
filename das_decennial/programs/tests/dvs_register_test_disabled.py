import sys
from os.path import abspath,dirname

DAS_DECENNIAL_DIR = dirname(dirname(dirname(abspath(__file__))))
if DAS_DECENNIAL_DIR not in sys.path:
    sys.path.append(DAS_DECENNIAL_DIR)

from programs.dvs_register import *
import programs.python_dvs.dvs as dvs
import pytest

DAS_S3ROOT = os.environ["DAS_S3ROOT"]
S3_PREFIX = "unit-test-data/das_decennial/programs/dvs_register_test"

@pytest.fixture(scope="session")
def LOGFILE(tmp_path_factory):
    # Convert DAS_S3ROOT in logfile into relevant Bucket
    input_file = os.path.join(dirname(abspath(__file__)), "demo-log.log")
    tmpdir = tmp_path_factory.mktemp("dvs_register_test")
    file = open(input_file, "r")
    output_path = f'{tmpdir}/demo-log.log'
    with open(output_path, 'w') as f:
        for line in file:
            newline = line.replace('DAS_S3ROOT', DAS_S3ROOT)
            print(newline, file=f)
    return output_path

def test_extract_paths(LOGFILE):
    print(LOGFILE)
    tu = extract_paths( LOGFILE )
    print(json.dumps(tu,indent=4,default=str))
    print(tu[dvs.COMMIT_METHOD])
    assert isinstance(tu,dict)

<<<<<<< HEAD:programs/tests/dvs_register_test.py
    P1 = f'{DAS_S3ROOT}/{S3_PREFIX}/P1'
=======
    P1 = '${DAS_S3INPUTS}/title13_input_data/table8/ri44.txt'
>>>>>>> 50e923402 (Parameterized cloud environment configuration, fixed bugs, separated S3INPUTS and S3ROOT.):programs/tests/dvs_register_test_disabled.py
    assert dvs.COMMIT_BEFORE in tu
    assert P1 in tu[dvs.COMMIT_BEFORE]

    P2 = f'{DAS_S3ROOT}/{S3_PREFIX}/P2'
    assert dvs.COMMIT_AFTER in tu
    assert P2 in tu[dvs.COMMIT_AFTER]

    P3 = 'das_decennial commit a68488b617e09c9e9e67000f958a83dc7f782d08\n'
    assert P3 in tu[dvs.COMMIT_METHOD]

def test_dvs_dc():
    dc = dvs.DVS()

def test_dvs_dc_create(LOGFILE):
    dc = dvs.DVS()
    dvs_dc_populate( dc, LOGFILE, ephemeral=True)

def test_helpers():
<<<<<<< HEAD:programs/tests/dvs_register_test.py
    assert s3prefix_exists(f"{DAS_S3ROOT}/{S3_PREFIX}/")
    assert s3path_exists(f"{DAS_S3ROOT}/{S3_PREFIX}/P1")
    assert not s3path_exists(f"{DAS_S3ROOT}/{S3_PREFIX}/not_real")
    assert (characterize_line(f"2020-04-06 12:46:31,994 s3cat.py:128 (verbprint) s3cat({DAS_S3ROOT}/{S3_PREFIX}/P3, demand_success=True, suffix=.csv) DOWNLOAD")
            ==(dvs.COMMIT_AFTER,f"{DAS_S3ROOT}/{S3_PREFIX}/P3.csv"))
=======
    assert s3prefix_exists("${DAS_S3INPUTS}/title13_input_data/table1a_20190709/")
    assert s3path_exists("${DAS_S3INPUTS}/title13_input_data/table1a_20190709/wy56.txt")
    assert s3path_exists("s3://uscb-decennial-ite-das/production-runs/2020-dpst-mdf-rerun-2/20200717/2020-dpst-mdf-rerun-2/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-BlockNodeDicts/part-00000")
    assert not s3path_exists("${DAS_S3INPUTS}/title13_input_data/table1a_20190709")
    assert not s3path_exists("${DAS_S3INPUTS}/title13_input_data/table1a_20190709/.txt")
    assert (characterize_line("2020-04-06 12:46:31,994 s3cat.py:128 (verbprint) s3cat(s3://uscb-decennial-ite-das/production-runs/2020-dpst-mdf-rerun-2/20200717/2020-dpst-mdf-rerun-2/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-DHCP2020_MDF/part-00000-1e618826-76ff-4fb7-abf7-8fe682a5f276-c000, demand_success=True, suffix=.csv) DOWNLOAD")
            ==(dvs.COMMIT_AFTER,"s3://uscb-decennial-ite-das/production-runs/2020-dpst-mdf-rerun-2/20200717/2020-dpst-mdf-rerun-2/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-DHCP2020_MDF/part-00000-1e618826-76ff-4fb7-abf7-8fe682a5f276-c000.csv"))
>>>>>>> 50e923402 (Parameterized cloud environment configuration, fixed bugs, separated S3INPUTS and S3ROOT.):programs/tests/dvs_register_test_disabled.py
