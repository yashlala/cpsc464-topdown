"""
mdf2020writer.py:  Writes out the MDF in the 2020 format.
"""
# Some Human
# 6/4/19

import logging
from typing import Union, Callable, List
import xml.etree.ElementTree as ET

from pyspark import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from programs.writer.writer import DASDecennialWriter
from programs.writer.rowtools import makeHistRowsFromMultiSparse
from programs.writer.hh2010_to_mdfunit2020 import Household2010ToMDFUnit2020Recoder, H12020MDFHousehold2020Recoder # Household 2010 Recoder (Demonstration Products)
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import DHCPHHGQToMDFPersons2020Recoder # DHCP HHGQ Recoder (Demonstration Products)
from programs.writer.dhcp_to_mdf2020 import DHCPToMDF2020Recoder, PL94ToMDFPersons2020Recoder, CVAP2020Recoder # DHCP Recoder
from programs.writer.mdf2020writer import MDF2020Writer, addEmptyAndGQ, addGroupQuarters
from programs.nodes.nodes import GeounitNode, getNodeAttr
from programs.schema import schema as sk
from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr
from das_framework.ctools.s3 import s3open
from das_constants import CC


class MDF2020WriterRevisedColumnNames(MDF2020Writer):
    def getListOfAllVars(self, only_return_geo_vars=False):
        if (self.setup.geo_bottomlevel is None) or (self.setup.geo_bottomlevel is '') or (self.setup.geo_bottomlevel == CC.GEOLEVEL_BLOCK):
            geo_var_list = ["TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK"]
        else:
            geo_var_list = [geolevel for geoid_len, geolevel in self.das.reader.modified_geocode_dict.items() if geoid_len > 0]
        if only_return_geo_vars:
            return geo_var_list
        return self.var_list[:2] + geo_var_list + self.var_list[2:]

    def saveHeader(self, *, path: str):
        """Saves header to the requested S3 location. This header will then be combined with the contents by the s3cat command"""
        self.annotate(f"writing header to {path}")
        with s3open(path, "w", fsync=True) as f:
            f.write("|".join(self.getListOfAllVars()))
            f.write("\n")


class MDF2020PersonWriterRevisedColumnNames(MDF2020WriterRevisedColumnNames):
    """
    Applies recodes and saves file for the DHCP_HHGQ Demonstration product
    """
    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "EPNUM",
        "RTYPE",
        "GQTYPE",
        "RELSHIP",
        "QSEX",
        "QAGE",
        "CENHISP",
        "CENRACE",
        "CITIZEN",
        "LIVE_ALONE"
    ]

    row_recoder = DHCPHHGQToMDFPersons2020Recoder

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict)
            return persons

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.getListOfAllVars())

        return df


class DHCP_MDF2020_WriterRevisedColumnNames(MDF2020WriterRevisedColumnNames):
    """
    Applies recodes and saves file for the DHC-P product in the 2020 format as requested
    by the MDF Specification for the Person Table.

    Includes the EPNUM variable, which is added via zipWithIndex and a mapper rather than
    through a recode function.
    """
    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "EPNUM",
        "RTYPE",
        "GQTYPE",
        "RELSHIP",
        "QSEX",
        "QAGE",
        "CENHISP",
        "CENRACE",
        "LIVE_ALONE"
    ]

    row_recoder = DHCPToMDF2020Recoder

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        #TODO: Put this in a common location
        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict, microdata_field=None)
            return persons

        def addIndexAsEPNUM(row_index) -> Row:
            row, index = row_index
            rowdict = row.asDict()
            rowdict['EPNUM'] = index
            ordered_cols = self.getListOfAllVars() + [CC.PRTCTD]
            return Row(*ordered_cols)(*[rowdict[col] for col in ordered_cols])

        # rdd = rdd.repartition(20000)
        rdd = rdd.flatMap(node2SparkRows)
        rdd = rdd.flatMap(lambda r: [r] * r[CC.PRTCTD])
        rdd = rdd.repartition(20000)

        # df = rdd.zipWithIndex().map(addIndexAsEPNUM).toDF()
        spark_schema = StructType([StructField(col, (StringType() if col != 'EPNUM' else LongType())) for col in self.getListOfAllVars() + [CC.PRTCTD]])
        rdd = rdd.zipWithIndex().map(addIndexAsEPNUM)
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=spark_schema)
        df = df.select(self.getListOfAllVars())

        return df

    def sort(self, df: DataFrame):
        df = df.orderBy(self.getListOfAllVars(only_return_geo_vars=True) + ["EPNUM"])
        return df


class MDF2020HouseholdWriterRevisedColumnNames(MDF2020WriterRevisedColumnNames):
    """
    Write out the household universe MDF. Applies recodes and adds rows for vacant housing units and groupquarters
    """

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "RTYPE",
        "GQTYPE",
        "TEN",
        "VACS",
        "HHSIZE",
        "HHT",
        "HHT2",
        "CPLT",
        "UPART",
        "MULTG",
        "HHLDRAGE",
        "HHSPAN",
        "HHRACE",
        "PAOC",
        "P18",
        "P60",
        "P65",
        "P75",
        "PAC",
        "HHSEX",
    ]

    row_recoder = Household2010ToMDFUnit2020Recoder

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        #TODO: Put this in a common location
        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))
            households = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            units = addEmptyAndGQ(nodedict, schema, households, row_recoder=self.row_recoder, gqtype_recoder=HHGQUnitDemoProductAttr.das2mdf, geocode_dict=inverted_geodict)
            return units

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.getListOfAllVars())
        return df


class MDF2020PersonPL94HistogramWriterRevisedColumnNames(DHCP_MDF2020_WriterRevisedColumnNames):

    row_recoder = PL94ToMDFPersons2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "EPNUM",
        "RTYPE",
        "GQTYPE_PL",
        "VOTING_AGE",
        "CENHISP",
        "CENRACE"
    ]



class MDF2020H1HistogramWriterRevisedColumnNames(MDF2020PersonWriterRevisedColumnNames):
    # Using inheritance from person writer rather than household, because the main histogram contains all units
    # If GQTYPE and RTYPE are desired to be in the output, it may be more convenient to switch to inheritance from MDF2020HouseholdWriter

    row_recoder = H12020MDFHousehold2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "RTYPE",
        "HH_STATUS",
    ]

    #var_list = ["geocode", "vacant_count", "occupied_count"]

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))
            households = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict, microdata_field=None)
            units = addGroupQuarters(nodedict, schema, households, row_recoder=self.row_recoder, geocode_dict=inverted_geodict, to_microdata=False)
            ordered_cols = self.getListOfAllVars() + [CC.PRTCTD]
            return [Row(*ordered_cols)(*[unit[col] for col in ordered_cols]) for unit in units]

        # rdd = rdd.repartition(min(rdd.count(), 20000))
        rdd = rdd.flatMap(node2SparkRows)
        rdd = rdd.flatMap(lambda r: [r] * r[CC.PRTCTD])
        rdd = rdd.repartition(20000)

        # df = rdd.toDF()
        spark_schema = StructType([StructField(col, StringType()) for col in self.getListOfAllVars() + [CC.PRTCTD]])
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=spark_schema)
        df = df.select(self.getListOfAllVars())

        return df


class CVAPWriterRevisedColumnNames(MDF2020WriterRevisedColumnNames):

    row_recoder = CVAP2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "CVAPRACE",
#        "EPNUM"
    ]

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        import numpy as np
        from programs.sparse import multiSparse

        schema = self.setup.schema_obj

        def conform2PL94(node: GeounitNode):
            DP_counts = node.getDenseSyn()
            PL94_counts = node.invar['pl94counts']
            node.syn = multiSparse(np.where(DP_counts > PL94_counts, PL94_counts, DP_counts))
            return node

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((CC.SYN, CC.INVAR, CC.GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            return persons

        # def addIndexAsEPNUM(row_index):
        #     row, index = row_index
        #     rowdict = row.asDict()
        #     rowdict['EPNUM'] = index
        #     return Row(**rowdict)

        df: DataFrame = rdd.map(conform2PL94).flatMap(node2SparkRows).toDF()  # zipWithIndex().map(addIndexAsEPNUM).toDF()

        df = df.select(self.getListOfAllVars())

        return df

    def sort(self, df: DataFrame):
        df = df.orderBy(self.getListOfAllVars(only_return_geo_vars=True))
        return df


## These classes produce MDF2020 specifications Unit and Person Files from nodes with any histogram schemas, filling with "0". For testing purposes
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import AnyToMDFPersons2020Recoder
from programs.writer.hh2010_to_mdfunit2020 import AnyToMDFHousehold2020Recoder


class MDF2020PersonAnyHistogramWriterRevisedColumnNames(MDF2020PersonWriterRevisedColumnNames):

    row_recoder = AnyToMDFPersons2020Recoder


class MDF2020HouseholdAnyHistogramWriterRevisedColumnNames(MDF2020HouseholdWriterRevisedColumnNames):

    row_recoder = AnyToMDFHousehold2020Recoder
