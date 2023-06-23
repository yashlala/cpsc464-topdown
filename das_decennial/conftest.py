import os
import sys
import pytest

from configparser import ConfigParser

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python','lib', 'py4j-src.zip'))

# Remove the folder above das_decennial from sys path for testing
above_root = os.getcwd()
above_root = above_root.replace('/das_decennial', '')
sys.path.remove(above_root)

# das_decennial directory
ddec_dir = os.path.dirname(__file__)
if ddec_dir not in sys.path:
    sys.path.append(ddec_dir)

# das_framwork dir to find ctools (for modules other than das_framework)
df_dir = os.path.join(ddec_dir, "das_framework")
if df_dir not in sys.path:
    sys.path.append(df_dir)

os.environ['CTOOLS_PARENT'] = df_dir

from das_framework.das_stub import DASStub

class DasDelegateStub:
    def log_testpoint(self, testpoint: str, additional: str = None) -> None:
        pass


def pytest_addoption(parser: ConfigParser) -> None:
    parser.addoption("--prod", action="store", default="False")


def pytest_generate_tests(metafunc) -> None:
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    option_value = metafunc.config.option.prod
    if 'prod' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("prod", [option_value])


@pytest.fixture()
def dd_das_stub() -> DASStub:
    return DASStub()


collect_ignore = [
    "_attic",
    "das_framework/tests/spark_sql_das_engine_test.py",
    "programs/strategies/tests/strategies_test.py", # needs fixing
    "hdmm"
]
