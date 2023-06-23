import pytest
import os
import sys
from os.path import dirname, abspath

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

MYDIR = dirname( abspath(__file__ ))
if MYDIR not in sys.path:
    sys.path.append(MYDIR)

from das_stub import DASStub

@pytest.fixture()
def df_das_stub() -> DASStub:
    return DASStub()
