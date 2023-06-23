"""
Class that does RDD operations locally in a loop, instead of in Spark. Mainly, for debugging and testing purposes

Usage:
    If you have a piece of program flow where you want to have an easier access into
    processes within map or reduce operations, by having them run locally (sequentially), on the master, rather than executor processes (in parallel),
    and directly see their output printed, be able to set debugging breakpoints, etc.
        ....
        rdd1 = rdd0.map(...)
        ...
        rddN = rddNm1.reduce(...)
        ....
    convert the RDD at the starting monitoring point to FakeRDD, and convert
    the RDD at the end of monitoring back to regular RDD:
        ....
        rdd1 = FakeRDD(rdd0).map(...)
        ...
        rddN = rddNm1.reduce(...).RDD()
        ....
    Then all .map(), .flatMap(), .reduce() etc.(*) operations that happen with the chain of RDDs between rdd0 and rddN
    will run by first collecting to the master node and performing sequentially, and then creating a new (single partition) RDD.


    (*) So far (6/19/19) only .map(), .flatMap(), .reduceByKey(), .mapValues() and .flatMapValues() are implemented
"""

import functools
import pyspark.rdd
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

def emptydec(rdd_operation):
    """ Empty decorator. Maybe something will be needed later"""
    def wrapper(*args, **kwargs):
        ans = rdd_operation(*args, **kwargs)
        return ans

    return wrapper


class FakeRDD(pyspark.rdd.RDD):
    def __init__(self, rdd: pyspark.rdd.RDD):
        self._jrdd = rdd._jrdd
        self.is_cached = rdd.is_cached
        self.is_checkpointed = rdd.is_checkpointed
        self.ctx = rdd.ctx
        self._jrdd_deserializer = rdd._jrdd_deserializer
        self._id = rdd._id
        self.partitioner = rdd.partitioner

    def RDD(self):
        return pyspark.rdd.RDD(self._jrdd, self.ctx, jrdd_deserializer=self._jrdd_deserializer)

    @emptydec
    def map(self, f, preservesPartitioning=False):
        spark = SparkSession.builder.getOrCreate()
        return FakeRDD(spark.sparkContext.parallelize([f(el) for el in self.collect()], numSlices=1))

    @emptydec
    def flatMap(self, f, preservesPartitioning=False):
        spark = SparkSession.builder.getOrCreate()
        l = [f(el) for el in self.collect()]
        flat_list = [item for sublist in l for item in sublist]
        return FakeRDD(spark.sparkContext.parallelize(flat_list, numSlices=1))

    @emptydec
    def filter(self, f):
        return FakeRDD(super().filter(f))

    @emptydec
    def distinct(self, numPartitions=None):
        return FakeRDD(super().distinct(numPartitions))

    @emptydec
    def sample(self, withReplacement, fraction, seed=None):
        return FakeRDD(super().sample(withReplacement, fraction, seed))

    @emptydec
    def randomSplit(self, weights, seed=None):
        return FakeRDD(super().randomSplit(weights, seed))

    @emptydec
    def groupBy(self, f, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return FakeRDD(super().groupBy(f, numPartitions, partitionFunc))

    @emptydec
    def reduceByKey(self, func, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        spark = SparkSession.builder.getOrCreate()

        grouped_by_key = {}
        for k, v in self.collect():
            if k not in grouped_by_key:
                grouped_by_key[k] = []
            grouped_by_key[k].append(v)

        reduced = [(k, functools.reduce(func, values)) for k, values in grouped_by_key.items()]

        return FakeRDD(spark.sparkContext.parallelize(reduced, numSlices=1))

    @emptydec
    def reduce(self, f):
        return functools.reduce(f, self.collect())

    @emptydec
    def reduceByKeyLocally(self, func):
        return FakeRDD(super().reduceByKeyLocally(func))

    @emptydec
    def join(self, other, numPartitions=None):
        return FakeRDD(super().join(other, numPartitions))

    @emptydec
    def leftOuterJoin(self, other, numPartitions=None):
        return FakeRDD(super().leftOuterJoin(other, numPartitions))

    @emptydec
    def rightOuterJoin(self, other, numPartitions=None):
        return FakeRDD(super().rightOuterJoin(other, numPartitions))

    @emptydec
    def fullOuterJoin(self, other, numPartitions=None):
        return FakeRDD(super().fullOuterJoin(other, numPartitions))

    @emptydec
    def partitionBy(self, numPartitions, partitionFunc=pyspark.rdd.portable_hash):
        return FakeRDD(super().partitionBy(numPartitions, partitionFunc))

    @emptydec
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return FakeRDD(super().combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions, partitionFunc))

    @emptydec
    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None,
                       partitionFunc=pyspark.rdd.portable_hash):
        return FakeRDD(super().aggregateByKey(zeroValue, seqFunc, combFunc, numPartitions, partitionFunc))

    @emptydec
    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return FakeRDD(super().foldByKey(zeroValue, func, numPartitions, partitionFunc))

    @emptydec
    def groupByKey(self, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return FakeRDD(super().groupByKey(numPartitions, partitionFunc))

    @emptydec
    def flatMapValues(self, f):
        # TODO: Not tested
        spark = SparkSession.builder.getOrCreate()
        l = [(k, f(v)) for k, v in self.collect()]
        flat_list = [item for sublist in l for item in sublist]
        return FakeRDD(spark.sparkContext.parallelize(flat_list, numSlices=1))

    @emptydec
    def mapValues(self, f):
        # TODO: Not tested
        spark = SparkSession.builder.getOrCreate()
        return FakeRDD(spark.sparkContext.parallelize([(k, f(v)) for k, v in self.collect()], numSlices=1))

    @emptydec
    def groupWith(self, other, *others):
        return FakeRDD(super().groupWith(other, *others))

    @emptydec
    def cogroup(self, other, numPartitions=None):
        return FakeRDD(super().cogroup(other, numPartitions))

    @emptydec
    def sampleByKey(self, withReplacement, fractions, seed=None):
        return FakeRDD(super().sampleByKey(withReplacement, fractions, seed))

    @emptydec
    def subtractByKey(self, other, numPartitions=None):
        return FakeRDD(super().subtractByKey(other, numPartitions))

    @emptydec
    def subtract(self, other, numPartitions=None):
        return FakeRDD(super().subtract(other, numPartitions))

    @emptydec
    def keyBy(self, f):
        return FakeRDD(super().keyBy(f))

    @emptydec
    def repartition(self, numPartitions):
        return FakeRDD(super().repartition(numPartitions))

    @emptydec
    def coalesce(self, numPartitions, shuffle=False):
        return FakeRDD(super().coalesce(numPartitions, shuffle))

    @emptydec
    def zip(self, other):
        return FakeRDD(super().zip(other))

    @emptydec
    def zipWithIndex(self):
        return FakeRDD(super().zipWithIndex())

    @emptydec
    def zipWithUniqueId(self):
        return FakeRDD(super().zipWithUniqueId())


class FakeDataFrame(DataFrame):

    def __init__(self, df: DataFrame):
        self._jdf = df._jdf
        self.sql_ctx = df.sql_ctx
        self._sc =df._sc
        self.is_cached = df.is_cached
        self._schema = df._schema
        self._lazy_rdd = df._lazy_rdd

    @property
    def rdd(self):
        return FakeRDD(super().rdd)

    @emptydec
    def repartition(self, numPartitions, *cols):
        return FakeDataFrame(super().repartition(numPartitions, *cols))

    @emptydec
    def withColumn(self, colName, col):
        return FakeDataFrame(super().withColumn(colName, col))
