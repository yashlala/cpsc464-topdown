from typing import Union, Callable, List
from pyspark import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from programs.writer.rowtools import makeHistRowsFromMultiSparse, makeHistRowsFromUnitMultiSparse
from programs.writer.mdf2020writerRevisedColumns import MDF2020HouseholdWriterRevisedColumnNames
from programs.writer.cef_2020.dhch_to_mdf2020_recoders import DHCHToMDF2020HouseholdRecoder, DHCHToMDF2020UnitRecoder, DHCHToMDF2020HouseholdRecoderFullTenure, DHCHToMDF2020HouseholdRecoderTen3Lev, DHCHToMDF2020UnitRecoderTen3Lev
from programs.writer.cef_2020.dhch_to_mdf2020_writer import greedyTenureRecode
from programs.nodes.nodes import GeounitNode, getNodeAttr

from das_constants import CC

from programs.schema.schemas.schemamaker import SchemaMaker


class DHCH_MDF2020_WriterRevisedColumnNames(MDF2020HouseholdWriterRevisedColumnNames):
    """
    Applies recodes and saves file for the DHC-H product in the 2020 format as requested
    by the MDF Specification.
    """

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        CC.ATTR_MDF_RTYPE,
        CC.ATTR_MDF_GQTYPE,
        CC.ATTR_MDF_TEN,
        CC.ATTR_MDF_VACS,
        CC.ATTR_MDF_HHSIZE,
        CC.ATTR_MDF_HHT,
        CC.ATTR_MDF_HHT2,
        CC.ATTR_MDF_CPLT,
        CC.ATTR_MDF_UPART,
        CC.ATTR_MDF_MULTG,
        CC.ATTR_MDF_HHLDRAGE,
        CC.ATTR_MDF_HHSPAN,
        CC.ATTR_MDF_HHRACE,
        CC.ATTR_MDF_PAOC,
        CC.ATTR_MDF_P18,
        CC.ATTR_MDF_P60,
        CC.ATTR_MDF_P65,
        CC.ATTR_MDF_P75,
        CC.ATTR_MDF_PAC,
        CC.ATTR_MDF_HHSEX,
    ]


    row_recoder = DHCHToMDF2020HouseholdRecoder

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}
        ordered_list = self.getListOfAllVars()

        def node2SparkRows(node: GeounitNode):
            households = makeHistRowsFromMultiSparse(node, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict)
            units = makeHistRowsFromUnitMultiSparse(node,
                        SchemaMaker.fromName(name=CC.SCHEMA_UNIT_TABLE_10_TENVACSGQ),
                        households, row_recoder=DHCHToMDF2020UnitRecoder, geocode_dict=inverted_geodict)
            units_tenure_recoded = greedyTenureRecode(node, units, ordered_list)
            return units_tenure_recoded

        # rdd = rdd.repartition(20000)
        rdd = rdd.flatMap(node2SparkRows)
        rdd = rdd.repartition(20000)

        schema = StructType([StructField(col, StringType()) for col in ordered_list])
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=schema)

        df = df.select(self.getListOfAllVars())
        return df

    def sort(self, df: DataFrame):
        df = df.orderBy(self.getListOfAllVars(only_return_geo_vars=True))
        return df

class DHCH_MDF2020_WriterRevisedColumnNamesTen3Lev(MDF2020HouseholdWriterRevisedColumnNames):
    """
    Applies recodes and saves file for the DHC-H product in the 2020 format as requested
    by the MDF Specification.
    """

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        CC.ATTR_MDF_RTYPE,
        CC.ATTR_MDF_GQTYPE,
        CC.ATTR_MDF_TEN,
        CC.ATTR_MDF_VACS,
        CC.ATTR_MDF_HHSIZE,
        CC.ATTR_MDF_HHT,
        CC.ATTR_MDF_HHT2,
        CC.ATTR_MDF_CPLT,
        CC.ATTR_MDF_UPART,
        CC.ATTR_MDF_MULTG,
        CC.ATTR_MDF_HHLDRAGE,
        CC.ATTR_MDF_HHSPAN,
        CC.ATTR_MDF_HHRACE,
        CC.ATTR_MDF_PAOC,
        CC.ATTR_MDF_P18,
        CC.ATTR_MDF_P60,
        CC.ATTR_MDF_P65,
        CC.ATTR_MDF_P75,
        CC.ATTR_MDF_PAC,
        CC.ATTR_MDF_HHSEX,
    ]


    row_recoder = DHCHToMDF2020HouseholdRecoderTen3Lev

    def transformRDDForSaving(self, rdd: RDD):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}
        ordered_list = self.getListOfAllVars()

        def node2SparkRows(node: GeounitNode):
            households = makeHistRowsFromMultiSparse(node, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict)
            units = makeHistRowsFromUnitMultiSparse(node,
                        SchemaMaker.fromName(name=CC.SCHEMA_UNIT_TABLE_10_TENVACSGQ),
                        households, row_recoder=DHCHToMDF2020UnitRecoderTen3Lev, geocode_dict=inverted_geodict)
            units_tenure_recoded = greedyTenureRecode(node, units, ordered_list, is_ten_3lev=True)
            return units_tenure_recoded

        # rdd = rdd.repartition(20000)
        rdd = rdd.flatMap(node2SparkRows)
        rdd = rdd.repartition(20000)

        schema = StructType([StructField(col, StringType()) for col in ordered_list])
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=schema)

        df = df.select(self.getListOfAllVars())
        return df

    def sort(self, df: DataFrame):
        df = df.orderBy(self.getListOfAllVars(only_return_geo_vars=True))
        return df


class MDF2020HouseholdWriterFullTenureRevisedColumnNames(DHCH_MDF2020_WriterRevisedColumnNames):
    row_recoder = DHCHToMDF2020HouseholdRecoderFullTenure

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        inverted_geodict = {v: k for k, v in self.das.reader.modified_geocode_dict.items()}

        def node2SparkRows(node: GeounitNode):
            households = makeHistRowsFromMultiSparse(node, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict)
            units = makeHistRowsFromUnitMultiSparse(node,
                        SchemaMaker.fromName(name=CC.SCHEMA_UNIT_TABLE_10_TENVACSGQ),
                        households, row_recoder=DHCHToMDF2020UnitRecoder, geocode_dict=inverted_geodict)
            # units_tenure_recoded = greedyTenureRecode(node, units)
            # return units_tenure_recoded
            return units

        rdd = rdd.flatMap(node2SparkRows)
        # print(f'rdd2.collect()[0]: {node2SparkRows(rdd.collect()[57])}')
        df: DataFrame = rdd.toDF()
        # print(f'df.count(): {df.count()}')
        df = df.select(self.getListOfAllVars())
        return df
