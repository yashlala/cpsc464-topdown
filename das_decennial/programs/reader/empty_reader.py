"""
    This is an empty reader module that returns an rdd; it is useful if you do not intend to run the full DAS,
    but want access to code available in one of its modules (e.g., to run HDMM on a single level without looking
    at the data).

    This module contains a reader class that is a subclass of AbstractDASReader.

    The reader class must contain a method called read.
"""

import logging

from das_framework.driver import AbstractDASReader
from programs.rdd_like_list import RDDLikeList
from pyspark.sql import SparkSession

class EmptyRDDReader(AbstractDASReader):
    """
        Empty reader, returnning an empty RDD
    """

    def read(self) -> RDDLikeList:
        """
            This function creates empty RDD
        """

        logging.info("creating empty rdd")

        # make empty rdd
        spark = SparkSession.builder.getOrCreate()
        block_nodes = spark.sparkContext.parallelize([])
        block_nodes.persist()

        return block_nodes


class reader(EmptyRDDReader):
    """ For compatibility with old config files"""
    pass