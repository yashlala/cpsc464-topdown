from typing import Union, Callable, List
from pyspark import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from programs.writer.rowtools import makeHistRowsFromMultiSparse, makeHistRowsFromUnitMultiSparse
from programs.writer.mdf2020writer import MDF2020HouseholdWriter
from programs.writer.cef_2020.dhch_to_mdf2020_recoders import DHCHToMDF2020HouseholdRecoder, DHCHToMDF2020UnitRecoder, DHCHToMDF2020HouseholdRecoderFullTenure
from programs.nodes.nodes import GeounitNode, getNodeAttr

from das_constants import CC

from programs.schema.schemas.schemamaker import SchemaMaker


class DHCH_MDF2020_Writer(MDF2020HouseholdWriter):
    """
    Applies recodes and saves file for the DHC-P product in the 2020 format as requested
    by the MDF Specification for the Person Table.

    Includes the EPNUM variable, which is added via zipWithIndex and a mapper rather than
    through a recode function.
    """

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
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

        def node2SparkRows(node: GeounitNode):
            households = makeHistRowsFromMultiSparse(node, schema, row_recoder=self.row_recoder, geocode_dict=inverted_geodict)
            units = makeHistRowsFromUnitMultiSparse(node,
                        SchemaMaker.fromName(name=CC.SCHEMA_UNIT_TABLE_10_TENVACSGQ),
                        households, row_recoder=DHCHToMDF2020UnitRecoder, geocode_dict=inverted_geodict)
            units_tenure_recoded = greedyTenureRecode(node, units, self.var_list)
            return units_tenure_recoded

        rdd = rdd.repartition(20000)
        rdd = rdd.flatMap(node2SparkRows)

        rdd = rdd.repartition(20000)

        schema = StructType([StructField(col, StringType()) for col in self.var_list])
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(rdd, schema=schema)

        df = df.select(self.var_list)
        return df

    def sort(self, df: DataFrame):
        df = df.orderBy(df.TABBLKST.asc(), df.TABBLKCOU.asc(), df.TABTRACTCE.asc(), df.TABBLK.asc())
        return df


def greedyTenureRecode(node: GeounitNode, rows: List[Row], ordered_cols, is_ten_3lev=False) -> List[Row]:
    # Find the relevant detailed tenure values from the protected unit row
    unit_syn_array = node.unit_syn.sparse_array
    #unit_syn_array = node.raw_housing.sparse_array
    mortgage_remaining = unit_syn_array[0, 0]
    owned_remaining = unit_syn_array[0, 1]
    rented_remaining = unit_syn_array[0, 2]
    no_pay_remaining = unit_syn_array[0, 3]

    #TODO: Validate that the constraints held for this node
    # The number of detailed tenure should matches the coarse tenure values
    # ie. mortgage+owned in unit == number of owned in all rows, and that rented+no_pay in unit == number of rented in all rows

    print(f'geocode: {node.geocode}, mortgage_remaining: {mortgage_remaining}, owned_remaining: {owned_remaining}, rented_remaining: {rented_remaining}, no_pay_remaining: {no_pay_remaining}')

    new_rows = []
    for row in rows:
        new_row_dict = row.asDict()

        rtype: str = row[CC.ATTR_MDF_RTYPE]
        ten: str = row[CC.ATTR_MDF_TEN]
        hhsize: str = row[CC.ATTR_MDF_HHSIZE]

        # Only modify non-vacant households
        if rtype == "2" and hhsize != "0":
            if ten == "1" and not is_ten_3lev:
                # Some mortgage will be changed to owned
                if mortgage_remaining > 0: # Keep the first mortgage rows set to mortgage
                    mortgage_remaining -= 1
                    new_row_dict[CC.ATTR_MDF_TEN] = "1" # Owned with a mortgage
                else:
                    # Once we've kept the right number of mortgage rows, set the remaining mortgage rows to owned
                    owned_remaining -= 1
                    new_row_dict[CC.ATTR_MDF_TEN] = "2" # 2 == Owned free and clear
            elif ten == "3":
                # Some rented will be changed to no pay
                if rented_remaining > 0:
                    rented_remaining -= 1
                    new_row_dict[CC.ATTR_MDF_TEN] = "3" # 3 == Rented
                else:
                    # Once we've kept the right number of rented rows, set the remaining rented rows to no pay
                    no_pay_remaining -= 1
                    new_row_dict[CC.ATTR_MDF_TEN] = "4" # 4 == Occupied without payment of rent
            elif ((not is_ten_3lev) and ten not in ["1", "3"]) or (is_ten_3lev and ten not in ["1", "2", "3"]):
                raise Exception(f'Before greedy Tenure Recoding, rows should only be populated with {["1", "2", "3"] if is_ten_3lev else ["1", "3"]}, found {ten}')

        new_row = Row(*ordered_cols)(*[new_row_dict[col] for col in ordered_cols])
        new_rows.append(new_row)

        print(f'old rows: {row}')

    if is_ten_3lev:
        assert (rented_remaining == 0) and (no_pay_remaining == 0), f'Not all no_pay were set! rented_remaining: {rented_remaining}, no_pay_remaining: {no_pay_remaining}'
    else:
        assert (mortgage_remaining == 0) and (owned_remaining == 0) and (rented_remaining == 0) and (no_pay_remaining == 0), f'Not all owned or no_pay were set! Remaining: mortgage_remaining: {mortgage_remaining}, owned_remaining: {owned_remaining}, rented_remaining: {rented_remaining}, no_pay_remaining: {no_pay_remaining}'

    print(f'Remaining: mortgage_remaining: {mortgage_remaining}, owned_remaining: {owned_remaining}, rented_remaining: {rented_remaining}, no_pay_remaining: {no_pay_remaining}')

    print(f'size of new_rows: {new_rows}')

    return new_rows


class MDF2020HouseholdWriterFullTenure(DHCH_MDF2020_Writer):
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
        df = df.select(self.var_list)
        return df
