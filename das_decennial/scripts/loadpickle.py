# This script allows to easily load pickled geounit nodes to ipython console, so that they can be inspected
# To be run from ipyspark interactively (not bothering with setting up imports and spark session for now)
# As in run
# PYSPARK_DRIVER_PYTHON=ipython pyspark
# in shell
# and then in ipython pyspark:
# run -i scripts/loadpickle.py

import os
from das_utils import ship_files2spark

if 'PYSPARK_SUBMIT_ARGS' not in os.environ:
    raise RuntimeError("This script is designed to be run from within pyspark or ipyspark")

# pylint: disable=E0602

das_s3root = os.environ["DAS_S3ROOT"]
jbid = os.environ["JBID"]
path = f"{das_s3root}/users/{jbid}/pl942020/us_repeatedruns/noisy_measurements/application_1606972206804_0766-StateOptimized.pickle"
#path = f"${DAS_S3INPUTS}/users/user007/CVAP/11162020170318/data"
ship_files2spark(spark)

rdd = sc.pickleFile(path)

# ### THIS IS WHERE YOU CAN CONTINUE WORKING WITH LOADED PICKLED NODES IN IPYTHON CONSOLE
#
# # Example of what to do
# st = nodes_rdd.collect()[0].unzipNoisy()
# print(st.dp_queries['detailed'])
