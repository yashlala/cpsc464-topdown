import os
import pytest
import sys
import warnings

from das_utils import ship_files2spark

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'


warnings.filterwarnings("ignore", message="DeprecationWarning")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")


@pytest.fixture(scope="module")
def spark(local=True):
    # Note: the parameter "local" does not work in the fixture like this. It is left as TODO/reminder
    # So far we haven't needed yarn for tests, local was enough
    # Add the directory with pyspark and py4j (in shape of zip file)
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

    from pyspark.sql import SparkSession

    if local:
        # Create local spark session (so that we don't have to put local files to HDFS)
        spark = SparkSession.builder.appName('DAS 2020 Unit Tests Spark PyTest Fixture')\
            .config("spark.submit.deployMode", "client")\
            .config("spark.authenticate.secret", "111")\
            .config("spark.master", "local[*]")\
            .getOrCreate()
    else:
        spark = SparkSession.builder.appName('DAS 2020 Unit Tests Spark PyTest Fixture').getOrCreate()

    ship_files2spark(spark, subdirs=['../','programs'], subdirs2root=['programs/engine/tests'])
    yield spark
    spark.stop()
