import os, sys
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, Row

#in_prefix = "${DAS_S3INPUTS}/users/user007/dhcppl942020/test_20211027/noisy_measurements/application_1635281136389_0001-"
in_prefix = "${DAS_S3INPUTS}/users/user007/dhcppl942020/test_20211019/noisy_measurements/application_1634692282794_0001-"
in_suffix = "Optimized.pickle"
geolevels = ["Tract", "Block_Group", "Block"]

spark_session = SparkSession.builder.getOrCreate()
res = []
for geolevel in geolevels:
    input_path = in_prefix + geolevel + in_suffix
    rdd = spark_session.sparkContext.pickleFile(input_path)
    partitions_list = list(rdd.map(lambda node: 0).glom().map(lambda part: len(part)).collect())
    row = [max(partitions_list), sum([part == 0 for part in partitions_list]), len(partitions_list)]
    res.append(row)
    print(f"{geolevel}:\n{row}\n")

print("max partition size, number of empty partitions, and total number of partitions for each geolevel:")
print(res)
