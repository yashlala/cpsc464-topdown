#!/usr/bin/env
# Simple test for logging on the worker nodes.
# usage: spark-submit daslogtest.py

import logging
import operator
import os
import sys

from pyspark.sql import SparkSession

PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)


import das_framework.ctools.clogging as clogging
import das_framework.ctools.env as env


def log_demo(val):
    import subprocess

    subprocess.call("logger -n {IP_ADDR} -p local1.info  bar".split(" "))

    master_ip = os.environ['MASTER_IP']

    subprocess.call(f"logger -n {IP_ADDR} -p local1.info master_ip={master_ip} added_syslog={clogging.added_syslog}".split(" "))

    clogging.setup_syslog(logging.handlers.SysLogHandler.LOG_LOCAL1, master_ip)
    subprocess.call(f"logger -n {IP_ADDR} -p local1.info master_ip={master_ip} added_syslog={clogging.added_syslog}".split(" "))
    logging.warning("info {}".format(val))
    return val


if __name__=="__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.addFile( clogging.__file__ )
    sc.addFile( env.__file__ )
    count = sc.parallelize(range(1,100)).map(log_demo).reduce(operator.add)
    print("Count:",count)
