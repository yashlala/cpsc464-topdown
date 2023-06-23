#!/usr/bin/env python3
#
"""
Validate the E2E MDF using Spark!
Notes:

1. The parse_cef_unit and parse_cef_per are both built by hand; they should be built by the ETL system.
2. The only thing validated are the invariants.
3. This module can be imported into pyspark with "import e2e_validator" and then you can
   use the cef_df() and the mdf_df() to get dataframes for each.

The invariants are specified here:
https://{HOST_NAME}/div/pco/PDSIntranet/Documents/DSEP%20Meeting%20Record%20FINAL%202018_11_08.pdf

The five items set as invariant were
C1: Total population (invariant at the county level for the 2018 E2E)
C2: Voting-age population (population age 18 and older) (eliminated for the 2018 E2E)
C3: Number of housing units (invariant at the block level)
C4: Number of occupied housing units (invariant at the block level)
C5: Number of group quarters facilities by group quarters type (invariant at the block level)

"""
import logging
import sys
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, Namespace
from tempfile import TemporaryDirectory
from collections.abc import Iterable
from operator import add
from typing import Union

# """
# Section to add pyspark to python path.
# """
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))
# """
# End section to add pyspark.
# """


try:
    import das_framework.ctools.clogging as clogging
    import das_framework.ctools.cspark as cspark
except ImportError:
    sys.path.append( os.path.join( os.path.dirname(__file__), ".."))
    sys.path.append( os.path.join( os.path.dirname(__file__), "../.."))
    import das_framework.ctools.clogging as clogging
    import das_framework.ctools.cspark as cspark

try:
    from ctools.env import census_getenv
except ImportError:
    logging.error('No top level ctools')
    from das_framework.ctools.env import census_getenv

from programs.writer.multi_writer import MultiWriter
from programs.writer.mdf2020writer import MDF2020Writer

# try:
from das_constants import CC
from programs.reader.cef_2020.cef_validator_classes import CEF20_UNIT, CEF20_PER
from programs.validator import constraints_validator
from programs.writer.cef_2020.mdf_validator_classes import MDF_Unit as DHC_MDF_Unit, MDF_Person as DHC_MDF_Person
from programs.writer.cef_2020.mdf_validator_classes_nonstandard_geocodes import MDF_Unit_nonstandard_geocodes as DHC_MDF_Unit_nonstandard_geocodes, MDF_Person_nonstandard_geocodes as DHC_MDF_Person_nonstandard_geocodes
from programs.writer.cef_2020.mdf_validator_classes_pl94 import MDF_Person as PL94_MDF_Person, MDF_Unit as H1_MDF_Unit
from programs.writer.cef_2020.mdf_validator_classes_pl94_nonstandard_geocodes import MDF_Person_nonstandard_geocodes as PL94_MDF_Person_nonstandard_geocodes, MDF_Unit_nonstandard_geocodes as H1_MDF_Unit_nonstandard_geocodes
from das_framework.driver import AbstractDASValidator
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
import pyspark.sql.types as T
from copy import deepcopy
from pyspark.sql import DataFrame
# except ImportError as e:
#     print(e)


#Setup logger
app_logger = logging.getLogger("application_logger")
app_logger.setLevel(logging.DEBUG)

# Upper bound on the number of persons in a unit (including both households and group quarters)
MAX_NUMBER_OF_PERSONS_IN_UNIT = 99999

if os.getenv('DAS_VALIDATE_MDF') and os.getenv('DAS_VALIDATE_MDF') != 'strict':
    strict_validation=False
else:
    strict_validation=True


def build_dataframe(spark: SparkSession, validator, sql_table_name: str, text_file: str) -> DataFrame:
    """
    This function given a spark instance, sql_table_name and text_file location build a dataframe and registers it as
    a spark sql table.
    :param spark: Instance of spark
    :param validator: Validator class
    :param sql_table_name: The name that we are going to register this dataframe as in the Spark SQL
    :param text_file: The file that we are trying to read in.
    :return:
    """
    if isinstance(validator(), (DHC_MDF_Unit, DHC_MDF_Person, PL94_MDF_Person, H1_MDF_Unit)):
        df_test = build_dataframe_csv(spark=spark, sql_table_name=type(validator).__name__, text_file=text_file, csvOptions={
            'header': 'true', 'delimiter': '|', 'comment': "#"
        })
        rdd = df_test.rdd.map(lambda row: '|'.join([str(c) for c in row])).filter(lambda line: line)
        rdd = rdd.map(lambda line: validator.parse_piped_line(line))
    else:
        rdd = spark.sparkContext.textFile(text_file)
        rdd = rdd.map(lambda line: validator.parse_line(line))
    df = spark.createDataFrame(rdd)
    df=df.withColumn("VALIDST",F.reverse(F.split(F.input_file_name(), "_"))[0].substr(0,2))
    df.persist()
    df.registerTempTable(sql_table_name)
    return df


def build_dataframe_csv(spark: SparkSession, sql_table_name: str, text_file: str, csvOptions: str = None) -> DataFrame:
    """

    :param spark: Instance of spark
    :param sql_table_name: The name that we are going to register the new dataframe as in Spark SQL.
    :param text_file: The file that we are trying to read.
    :param csvOptions: The csvOptions that we want to provide to the spark csv reader function.
    :return:
    """
    df = spark.read.format('csv').options(**csvOptions).load(text_file)
    df.persist()
    df.registerTempTable(sql_table_name)
    return df


class E2E:
    """
    A class that is used the create and store all of the pyspark dataframes.
    """
    def __init__(self, grfc_file: str, cef_unit_file: str = None, cef_per_file: str = None, mdf_unit_file: str = None,
                 mdf_per_file: str = None, schema: str = CC.SCHEMA_PL94_2020, standard_geocodes: bool = True) -> None:
        """
        This is the constructor for the E2E Class. This creates the dataframes for all the the CEF and MDF files as well
        as registers those dataframes as Spark SQL tables.
        :param grfc_file: Grfc File Path
        :param cef_unit_file: CEF unit file path
        :param cef_per_file: CEF person file path
        :param mdf_unit_file: MDF Unit file path
        :param mdf_per_file: MDF Person file path
        """
        spark = SparkSession.builder.getOrCreate()

        if schema == CC.SCHEMA_PL94_2020 or schema == CC.SCHEMA_H1_2020:
            if standard_geocodes:
                self.mdf_per_validator = PL94_MDF_Person
                self.mdf_unit_validator = H1_MDF_Unit
            else:
                self.mdf_per_validator = PL94_MDF_Person_nonstandard_geocodes
                self.mdf_unit_validator = H1_MDF_Unit_nonstandard_geocodes
        else:
            if standard_geocodes:
                self.mdf_per_validator = DHC_MDF_Person
                self.mdf_unit_validator = DHC_MDF_Unit
            else:
                self.mdf_per_validator = DHC_MDF_Person_nonstandard_geocodes
                self.mdf_unit_validator = DHC_MDF_Unit_nonstandard_geocodes

        self.grfc, self.grfc_geocoded = self.create_grfc(spark=spark, grfc_file=grfc_file)

        self.cef_unit = self.create_cef_unit(spark=spark, cef_unit_file=cef_unit_file)
        self.cef_per = self.create_cef_per(spark=spark, cef_per_file=cef_per_file)

        self.mdf_unit = self.create_mdf_unit(spark=spark, mdf_unit_data=mdf_unit_file)
        self.mdf_per = self.create_mdf_per(spark=spark, mdf_per_data=mdf_per_file)

    def create_cef_unit(self, spark: SparkSession, cef_unit_file: str) -> DataFrame:
        """
        Creates the dataframe for cef unit as well as adds the geocode.
        :param spark: An instance of spark
        :param cef_unit_file:  The path to the cef unit
        :return: Either None or a pyspark dataframe that contains the cef units.
        """
        cef_unit = None
        if cef_unit_file is not None:
            cef_unit = build_dataframe(spark=spark, validator=CEF20_UNIT, sql_table_name="cef_unit",
                                       text_file=cef_unit_file)
            cef_unit = cef_unit.join(self.grfc_geocoded, self.grfc_geocoded.OIDTABBLK == cef_unit.oidtb)
            cef_unit.registerTempTable("cef_unit")
        return cef_unit

    @staticmethod
    def create_cef_per(spark: SparkSession, cef_per_file: str) -> DataFrame:
        """
        Creates the dataframe for the cef person.
        :param spark: An instance of spark
        :param cef_per_file: The path to the cef per
        :return: Either None or a pyspark dataframe that contains the cef people.
        """
        return build_dataframe(spark=spark, validator=CEF20_PER, sql_table_name="cef_per",
                               text_file=cef_per_file) if cef_per_file is not None else None

    @staticmethod
    def create_grfc(spark: SparkSession, grfc_file: str) -> DataFrame:
        """
        Creates the dataframe for the grfc
        :param spark: An instance of spark
        :param grfc_file: The path to the grfc
        :return: A grfc pyspark dataframe
        """
        grfc = build_dataframe_csv(spark=spark, sql_table_name="grfc", text_file=grfc_file,
                                   csvOptions={'header': 'true', 'inferSechema': 'true', 'delimiter': '|'})
        grfc_geocoded = grfc.select(F.concat(F.col("TABBLKST"), F.col("TABBLKCOU"), F.col("TABTRACTCE"),
                                             F.col("TABBLKGRPCE"), F.col("TABBLK")).alias("geocode"),
                                    F.col("OIDTABBLK"))
        grfc_geocoded.registerTempTable("grfc_geocoded")

        return grfc, grfc_geocoded

    def create_mdf_per(self, spark: SparkSession, mdf_per_data: Union[DataFrame, str]) -> DataFrame:
        """
        Creates the mdf person dataframe as well as added the geocode to the dataframe.
        :param spark: An instance of spark
        :param mdf_per_data: The pth to the mdf person file or the person MDF dataframe
        :return: Either None or a pyspark dataframe with the mdf person.
        """
        mdf_per = None
        if mdf_per_data is not None:
            if not isinstance(mdf_per_data, DataFrame):
                mdf_per = build_dataframe(spark=spark, validator=self.mdf_per_validator, sql_table_name="mdf_per",
                                          text_file=mdf_per_data)
            else:
                mdf_per = mdf_per_data
            mdf_per = mdf_per.toDF(*[c.lower() for c in mdf_per.columns])
            mdf_per = E2E.add_geocode(dataframe=mdf_per)
            mdf_per.registerTempTable("mdf_per")

        return mdf_per

    def create_mdf_unit(self, spark: SparkSession, mdf_unit_data: Union[DataFrame, str]) -> DataFrame:
        """
            Creates the mdf unit dataframe as well as added the geocode to the dataframe.
            :param spark: An instance of spark
            :param mdf_unit_data: mdf person dataframe or path to the textfile.
            :return: Either None or a pyspark dataframe with the mdf unit.
        """
        mdf_unit = None
        if mdf_unit_data is not None:
            if not isinstance(mdf_unit_data, DataFrame):
                mdf_unit = build_dataframe(spark=spark, validator=self.mdf_unit_validator, sql_table_name="mdf_unit",
                                           text_file=mdf_unit_data)
            else:
                mdf_unit = mdf_unit_data
            mdf_unit = mdf_unit.toDF(*[c.lower() for c in mdf_unit.columns])
            mdf_unit.registerTempTable("mdf_unit")
            mdf_unit = E2E.add_geocode(dataframe=mdf_unit)
            mdf_unit.registerTempTable("mdf_unit_geocoded")
        return mdf_unit

    @staticmethod
    def add_geocode(dataframe: DataFrame) -> DataFrame:
        return dataframe.withColumn("geocode", F.concat_ws('', dataframe.tabblkst, dataframe.tabblkcou,
                                                           dataframe.tabtractce, dataframe.tabblkgrpce,
                                                           dataframe.tabblk))


class E2EValidator(AbstractDASValidator):
    """
    This is the Base class of the MDF Validator. You will see that method called validate you will need to create
    a sub-class that implements that function.
    """
    def __init__(self, grfc_file: str = None, schema: str = CC.SCHEMA_PL94_2020, standard_geocodes: bool = True,
                 test_total_pops: bool = False,
                 validate_format_only: bool = False, **kwargs) -> None:

        if kwargs.get("OVERRIDE") is None:
            super().__init__(**kwargs)
        self.e2e = None

        self.errors = 0
        self.failed_invariants = 0
        self.E2E_Unit = None
        self.E2E_Person = None

        self.grfc_file = self.getconfig("grfc_path", section=CC.READER) if grfc_file is None else grfc_file

        self.schema = schema
        self.standard_geocodes = standard_geocodes
        self.validate_format_only = validate_format_only
        self.test_total_pops = test_total_pops

    def validate(self, original_data: DataFrame, written_data_reference, **kwargs) -> None:
        raise NotImplemented(f"No Implemented.")

    @staticmethod
    def select(stmt: str) -> DataFrame:
        """Execute a SQL statement and return the results"""
        # spark = SparkSession.builder.appName('e2e_validator').getOrCreate()
        spark = SparkSession.builder.getOrCreate()
        return spark.sql(stmt).collect()

    @staticmethod
    def select1(stmt: str) -> Row:
        """Execute a SQL statement and return the first element of the first row"""
        # spark = SparkSession.builder.appName('e2e_validator').getOrCreate()
        spark = SparkSession.builder.getOrCreate()
        for row in spark.sql(stmt).collect():
            return row[0]

    @staticmethod
    def verify(msg: str, cond: bool) -> int:
        """Print msg. cond should be true. Return number of errors."""
        import inspect
        caller_lineno = inspect.stack()[1].lineno
        print(f"{caller_lineno}: {msg} {'PASS' if cond else 'FAIL'}")
        return 0 if cond else 1

    @staticmethod
    def select_into(sql_table_name: str, stmt: str) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        df = spark.sql(stmt)
        df.registerTempTable(sql_table_name)
        return df

    @staticmethod
    def show(sql_table_name: str):
        spark = SparkSession.builder.getOrCreate()
        spark.sql(f"SELECT * from {sql_table_name};").show()


class E2EValidatorUnit(E2EValidator):
    """
    This is the MDF validator the the MDF_Unit. This class requires the CEF Unit file, MDF Unit File and the grf file.
    """
    def __init__(self, cef_unit_file: str = None, mdf_unit_file: str = None, **kwargs) -> None:
        """
        :param cef_unit_file: CEF Unit file location
        :param mdf_unit_file:  MDF Unit file location
        :param kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.cef_unit_file = self.getconfig("Household.path", section=CC.READER) if cef_unit_file is None \
            else cef_unit_file
        if kwargs.get("OVERRIDE") is None:
            if isinstance(self.das.writer, MultiWriter):
                for writer in self.das.writer.writers:
                    if isinstance(writer, MDF2020Writer):
                        file_path = os.path.join(writer.output_path, writer.output_datafname)
                        if writer.s3cat:
                            file_path += writer.s3cat_suffix
                        self.mdf_unit_file = file_path
            else:
                self.mdf_unit_file = os.path.join(self.das.writer.output_path, self.das.writer.output_datafname)
                if self.das.writer.s3cat:
                    self.mdf_unit_file += self.das.writer.s3cat_suffix
        else:
            self.mdf_unit_file = mdf_unit_file
        self.mdf_per_file, self.cef_per_file = None, None

    def create_e2e(self, dataframe: DataFrame = None) -> None:
        mdf_unit_file = dataframe if dataframe else self.mdf_unit_file
        self.e2e = E2E(cef_unit_file=self.cef_unit_file,
                       cef_per_file=self.cef_per_file,
                       mdf_unit_file=mdf_unit_file,
                       mdf_per_file=self.mdf_per_file,
                       grfc_file=self.grfc_file,
                       schema=self.schema)

    def validate_data(self, dataframe: DataFrame = None, join_method: str = 'full') -> None:
        self.create_e2e(dataframe=dataframe)
        if not self.validate_format_only:
            self.test_mdf_count_vs_cef_count(join_method=join_method)
            self.validate_cef_unit()
            self.create_temp_tables_cef_unit()
            if self.schema in [CC.SCHEMA_DHCH, CC.SCHEMA_DHCH_TEN_3LEV]:
                self.validate_housing()
            self.check_household_types()

        return True if self.errors + self.failed_invariants == 0 else False

    def validate(self, original_data: DataFrame = None, written_data_reference=None, **kwargs) -> bool:
        return self.validate_data(**kwargs)

    def validate_cef_unit(self) -> None:
        self.cef_unit_total = self.select1(f"SELECT COUNT(*) from cef_unit")
        cef_unit_oidtabblk_valid = self.select1(f"SELECT COUNT(*) from cef_unit where "
                                       f"cef_unit.oidtabblk in (SELECT oidtabblk from grfc)")

        cef_unit_oidtb_valid = self.select1(f"SELECT COUNT(*) from cef_unit where "
                                            f"cef_unit.oidtb in (SELECT oidtabblk from grfc)")

        if self.cef_unit_total == cef_unit_oidtb_valid:
            self.errors += self.verify(f"Number of CEF units: {self.cef_unit_total}  number with valid oidtb: "
                                       f"{cef_unit_oidtb_valid}",self.cef_unit_total == cef_unit_oidtb_valid)
        else:
            self.errors += self.verify(f"Number of CEF units: {self.cef_unit_total}  number with valid oidtb: "
                                       f"{cef_unit_oidtabblk_valid}",self.cef_unit_total == cef_unit_oidtabblk_valid)

    @staticmethod
    def create_temp_tables_cef_unit() -> None:
        spark = SparkSession.builder.getOrCreate()

        cef_unit_geocoded = spark.sql("SELECT cef_unit.mafid AS mafid,grfc_geocoded.geocode AS geocode,ten,qgqtyp "
                                           "FROM cef_unit LEFT JOIN grfc_geocoded ON cef_unit.oidtb=grfc_geocoded.oidtabblk")
        cef_unit_geocoded.registerTempTable("cef_unit_geocoded")

    def validate_housing(self) -> None:
        if self.e2e.mdf_unit is not None:
            mdf_unit_total = self.select1("SELECT COUNT(*) from mdf_unit")
            self.errors += self.verify(f"Total MDF unit: {mdf_unit_total}",
                                       self.cef_unit_total == mdf_unit_total)
            self.occupied_checks()
            self.test_vacant_units_in_mdf_where_none_in_cef()
        else:
            raise RuntimeError(f"A mdf unit file was not provided")

    def test_mdf_count_vs_cef_count(self, join_method: str = 'full') -> None:
        """
        This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
        four.
        join_method: This is the method to use when joining the MDF with the CEF in this function. Full should be used
        when you are running this check on the full completed MDF. If however you are checking only specific levels of
        the MDF you should use inner.
        :return:
        """
        QGQTYPE_EDIT = F.when(
            F.col("QGQTYP").isin(['702', '704', '706', '903', '904']), "997"
        ).otherwise(F.col("QGQTYP"))
        check_cef = self.e2e.cef_unit.withColumn("qgqtype_edit1", QGQTYPE_EDIT)

        QGQTYPE_EDIT = F.when(
            F.col("qgqtype_edit1").isin(['   ']), "000"
        ).otherwise(F.col("qgqtype_edit1"))
        check_cef = check_cef.withColumn("qgqtype_edit", QGQTYPE_EDIT)

        mdf_groupby_geocode = self.e2e.mdf_unit.groupBy("geocode", "gqtype").agg(F.count("*").alias("mdf_unit_count"))
        cef_groupby_geocode = check_cef.groupBy("geocode", "qgqtype_edit").agg(F.count("*").alias("cef_unit_count"))

        join_df = mdf_groupby_geocode.join(cef_groupby_geocode,
                                           (mdf_groupby_geocode.geocode == cef_groupby_geocode.geocode) &
                                           (mdf_groupby_geocode.gqtype == cef_groupby_geocode.qgqtype_edit),
                                           how=join_method)
        join_df = join_df.drop('geocode')
        join_df = join_df.fillna(0, subset=['cef_unit_count', 'mdf_unit_count'])
        filter_count = join_df.filter(join_df.mdf_unit_count != join_df.cef_unit_count).count()
        if filter_count != 0:
            print("Failed test_mdf_count_vs_cef_count")
            self.errors += 1
            return
        print("Passed test_mdf_count_vs_cef_count")

    def occupied_checks(self) -> None:
        """
            This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
            Six.
            :return:
        """
        occupied = self.e2e.mdf_unit.filter(self.e2e.mdf_unit.hhsize > 0)
        test_six_df = occupied.filter((occupied.rtype != 2) & (occupied.gqtype != '000') & (occupied.vacs != 0))
        if test_six_df.count() > 0:
            print(f"Found bad values for RTYPE, GQTYPE, VACS")
            test_six_df.head(10)
            self.errors += 1

    def check_household_types(self) -> None:
        """
            This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
            Seven.
            :return:
        """
        occupied = self.e2e.mdf_unit.filter(self.e2e.mdf_unit.hhsize > 0)
        occupied_greater_1 = self.e2e.mdf_unit.filter(self.e2e.mdf_unit.hhsize > 1)
        occupied_equal_1 = self.e2e.mdf_unit.filter(self.e2e.mdf_unit.hhsize == 1)
        test_hht_0 = occupied.filter((occupied.hht == 0))
        test_hht_greater_1 = occupied_greater_1.filter((occupied_greater_1.hht == 4) | (occupied_greater_1.hht == 6))
        occupied_equal_1 = occupied_equal_1.filter((occupied_equal_1.hht == 1) | (occupied_equal_1.hht == 2) |
                                                   (occupied_equal_1.hht == 3) | (occupied_equal_1.hht == 5) |
                                                   (occupied_equal_1.hht == 7))
        if test_hht_0.count() > 0:
            print(f"Found bad value for htt it equals 0 when HHSIZE > 0")
            self.errors += 1
        if test_hht_greater_1.count() > 0:
            print(f"Found bad value for htt it equals 4 or 6 when HHSIZE > 1")
            self.errors += 1
        if occupied_equal_1.count() > 0:
            print(f"Found bad value for htt it equals 1, 2, 3, 5 or 7 when HHSIZE == 1")
            self.errors += 1

    def test_vacant_units_in_mdf_where_none_in_cef(self) -> None:
        """

        """
        print('======================================')
        # Group Quarters are irrelevant, so filter out only records for housing units.
        # .groupby geocode and find types of tenures of units in that geocode
        # vacant '0' means occupied, others mean vacant, filter to only occupied, no vacant in the block,
        # to get a list of geocodes that don't have vacant housing units
        # It maybe worth to convert 'ten' to T.IntegerType() first
        cef_no_vacant_housing_unit_geocodes = (
            self.e2e.cef_unit
                .filter(self.e2e.cef_unit.rtype == 2)
                .groupby('geocode')
                .agg(F.collect_set('vacs').alias('vacss'))
                .rdd.filter(lambda r: r['vacss'] == ['0'])
                .map(lambda r: (r['geocode'], 1))
        )

        print(f'len(cef_no_vacant_housing_unit_geocodes) {cef_no_vacant_housing_unit_geocodes.count()}')
        valid_vacs=[1,3,4,5,6,7,8]
        mdf_vacant_units = \
            self.e2e.mdf_unit\
                .filter((self.e2e.mdf_unit.vacs.isin(valid_vacs)) & (self.e2e.mdf_unit.rtype == 2))\
                .groupby('geocode')\
                .agg(F.count('*').alias('units'))\
                .rdd.map(lambda r: (r['geocode'], r['units']))

        print(f'len(mdf_vacant_units) {mdf_vacant_units.count()}')

        mdf_vacant_units_in_cef_no_vacant_units = mdf_vacant_units.join(cef_no_vacant_housing_unit_geocodes)

        filter_count = mdf_vacant_units_in_cef_no_vacant_units.count()
        filter_count_units = mdf_vacant_units_in_cef_no_vacant_units.map(lambda d: d[1][0]).reduce(add)
        print(f'filter_count_geocodes:  {filter_count}')
        print(f'filter_count_units:  {filter_count_units}')
        print('======================================')
        if filter_count == 0:
            print(f"Failed test_vacant_units_in_mdf_where_none_in_cef. With count of {filter_count}")
            self.errors += 1
            return
        print("Passed test_vacant_units_in_mdf_where_none_in_cef")


class E2EValidatorPerson(E2EValidator):
    """
    This is the MDF validator the the MDF_Person. This class requires the CEF Unit file, MDF Per File, CEF Person file
    and the grf file.
    """
    def __init__(self, cef_unit_file: str = None, mdf_per_file: str = None, cef_per_file: str = None, **kwargs) -> None:
        """
        E2EValidatorPerson Constructor
        :param cef_unit_file: CEF Unit file location
        :param mdf_per_file:  MDF Unit file location
        :param cef_per_file:  CEF Person file location
        :param kwargs: Keyword arguments.
        """
        super().__init__(**kwargs)

        self.GQTYPE_VARIABLE = "gqtype"

        if kwargs.get("OVERRIDE") is None:
            if isinstance(self.das.writer, MultiWriter):
                for writer in self.das.writer.writers:
                    if isinstance(writer, MDF2020Writer):
                        file_path = os.path.join(writer.output_path, writer.output_datafname)
                        if writer.s3cat:
                            file_path += writer.s3cat_suffix
                        self.mdf_per_file = file_path
            else:
                self.mdf_per_file = os.path.join(self.das.writer.output_path, self.das.writer.output_datafname)
                if self.das.writer.s3cat:
                    self.mdf_per_file += self.das.writer.s3cat_suffix
        else:
            self.mdf_per_file = mdf_per_file
        self.cef_per_file = self.getconfig("Person.path", section=CC.READER) if cef_per_file is None \
            else cef_per_file
        self.cef_unit_file = self.getconfig("Unit.path", section=CC.READER) if cef_unit_file is None \
            else cef_unit_file
        self.mdf_unit_file = None

    def create_e2e(self, dataframe: DataFrame = None) -> None:
        mdf_per_file = dataframe if dataframe else self.mdf_per_file
        self.e2e = E2E(cef_unit_file=self.cef_unit_file,
                       cef_per_file=self.cef_per_file,
                       mdf_unit_file=self.mdf_unit_file,
                       mdf_per_file=mdf_per_file,
                       grfc_file=self.grfc_file,
                       schema=self.schema, standard_geocodes=self.standard_geocodes)

    def validate_data(self, dataframe: DataFrame = None) -> bool:
        self.create_e2e(dataframe=dataframe)
        cef_unit_sum = self.create_cef_unit_sum_df()
        mdf_per_sum = self.create_mdf_per_sum_df()
        self.joined_sum_df = self.create_joined_df(cef_unit_df=cef_unit_sum, mdf_per_df=mdf_per_sum)
        self.cef_per_total = self.select1(f"SELECT COUNT(*) from cef_per")
        if self.schema in [CC.SCHEMA_DHCP]:
            self.validate_persons()
        return True if self.errors + self.failed_invariants == 0 else False

    def validate(self, original_data: DataFrame = None, written_data_reference=None, dataframe: DataFrame = None, **kwargs) -> bool:

        if dataframe is not None:
            return self.validate_data(dataframe=dataframe)

        if written_data_reference is None:
            return self.validate_data(**kwargs)

        if not isinstance(written_data_reference, Iterable):
            written_data_reference = [written_data_reference]
        validated = []
        for written_data in written_data_reference:
            if isinstance(written_data, DataFrame):
                self.log_and_print(f"Validating {written_data}")
                validated.append(self.validate_data(dataframe=written_data))

        return all(validated)

    def create_cef_unit_sum_df(self) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        cef_unit_sum = spark.sql("SELECT * from cef_unit")

        QGQTYPE_EDIT = F.when(
            F.col("QGQTYP").isin(['702', '704', '706', '903', '904']), "997"
        ).when(F.col("QGQTYP").isin(['   ']), "000").otherwise(F.col("QGQTYP"))

        cef_unit_sum = cef_unit_sum.withColumn("qgqtype_edit", QGQTYPE_EDIT)
        cef_distinct_gqtype = cef_unit_sum.select("qgqtype_edit").distinct().rdd.flatMap(lambda x: x).collect()
        for target in cef_distinct_gqtype:
            cef_unit_sum = cef_unit_sum.withColumn(f"cef_level{target}",
                                                   self.generate_udf(target)(cef_unit_sum['qgqtype_edit']))
        cef_unit_sum = cef_unit_sum.groupBy(cef_unit_sum["geocode"]).sum()
        return cef_unit_sum

    def create_mdf_per_sum_df(self) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        mdf_per_sum = spark.sql("SELECT * from mdf_per")
        mdf_per_sum = mdf_per_sum.drop("tabblkst", "tabblkcou", "tabractce", "tabblkgrpce", "tabblk")

        self.mdf_distinct_gqtype = mdf_per_sum.select(self.GQTYPE_VARIABLE).distinct().rdd.flatMap(lambda x: x).collect()
        for target in self.mdf_distinct_gqtype:  # Add binary flag columns for each level of HHGQ
            mdf_per_sum = mdf_per_sum.withColumn(f"mdf_level{target}",
                                                 self.generate_udf(target)(mdf_per_sum[self.GQTYPE_VARIABLE]))

        mdf_per_sum = mdf_per_sum.groupBy(mdf_per_sum["geocode"]).sum()
        return mdf_per_sum

    @staticmethod
    def create_joined_df(cef_unit_df: DataFrame, mdf_per_df: DataFrame) -> DataFrame:
        return mdf_per_df.join(cef_unit_df, cef_unit_df.geocode == mdf_per_df.geocode, how='left_outer').persist()

    def validate_persons(self) -> None:
        spark = SparkSession.builder.getOrCreate()
        if self.e2e.mdf_per is not None:
            mdf_per_total = self.select1("SELECT COUNT(*) from mdf_per")
            self.errors += self.verify(f"Total MDF per: {mdf_per_total}",
                                       self.cef_per_total == mdf_per_total)

            if self.cef_per_total != mdf_per_total:
                print(self.e2e.mdf_per.head(10))
                print("--------")
                print(self.e2e.cef_per.head(10))

            if not self.validate_format_only:
                # Note that the following lines were disabled for dhc because of our use of a HHGQ invariants heuristic for DHCP in 2020
                # This is parent class(which is also used by DHCP directly), and PL94 is in a subclass.
                # TODO this needs to be split into a seperate subclass to handle the differences
                if self.schema not in [CC.SCHEMA_DHCP]:
                    self.find_lb_violations(self.joined_sum_df)
                    self.find_ub_violations(self.joined_sum_df)
                    self.validate_person_in_gq()
                if self.test_total_pops:
                    self.check_state_total_pops(spark=spark)
                self.test_unit_implies_no_person()
                self.test_NIU()

                self.test_people_in_cef_vacant_hus()
                self.test_cef_people_in_mdf_vacant_hus()
        else:
            raise RuntimeError(f"A mdf person file was not provided")

    def validate_person_in_gq(self) -> None:
        """
            This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
            one.
            :return:
        """
        mdf_groupby_geocode = self.e2e.mdf_per.groupBy("geocode", self.GQTYPE_VARIABLE).agg(F.count("*").alias("mdf_per_count"))
        cef_groupby_geocode = self.e2e.cef_unit.groupBy("geocode", "QGQTYP").agg(F.count("*").alias("cef_unit_count"))

        mdf_groupby_geocode = mdf_groupby_geocode.filter(mdf_groupby_geocode[self.GQTYPE_VARIABLE] != '000')
        cef_groupby_geocode = cef_groupby_geocode.filter(cef_groupby_geocode["QGQTYP"] != '000')

        join_df = mdf_groupby_geocode.join(cef_groupby_geocode,
                                           (mdf_groupby_geocode.geocode == cef_groupby_geocode.geocode) &
                                           (mdf_groupby_geocode[self.GQTYPE_VARIABLE] == cef_groupby_geocode.QGQTYP),
                                           how='left')
        join_df = join_df.drop('geocode')
        filter_count = join_df.filter(join_df.mdf_per_count < join_df.cef_unit_count).count()

        if filter_count != 0:
            print(f"Failed validate_person_in_gq. With count of {filter_count}")
            self.errors += 1
            return
        print("Passed validate_person_in_gq")

    def test_unit_implies_no_person(self) -> None:
        """
            This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
            Two.
            :return:
        """
        # it checks whether no Units of GQTYPE in geocode in CEF implies no Persons with GQTYPE in MDF Persons:
        mdf_groupby_geocode = self.e2e.mdf_per.groupBy("geocode", self.GQTYPE_VARIABLE).agg(F.count("*").alias("mdf_per_count"))
        cef_groupby_geocode = self.e2e.cef_unit.groupBy("geocode", "QGQTYP").agg(F.count("*").alias("cef_unit_count"))

        join_df = mdf_groupby_geocode.join(cef_groupby_geocode,
                                           (mdf_groupby_geocode.geocode == cef_groupby_geocode.geocode) &
                                           (mdf_groupby_geocode[self.GQTYPE_VARIABLE] == cef_groupby_geocode.QGQTYP),
                                           how='left')

        join_df = join_df.drop('geocode')
        filter_count = join_df.filter((join_df.mdf_per_count > 0) & (join_df.cef_unit_count == 0)).count()

        if filter_count != 0:
            print(f"Failed test_unit_implies_no_person. With count of {filter_count}")
            self.errors += 1
            return
        print("Passed test_unit_implies_no_person")

    def test_NIU(self) -> None:
        # Filters to just HUs (NIUs / 000s). Then filters to (geocode, 000)'s that have more than MAX_NUMBER_OF_PERSONS_IN_UNIT  persons per HU.
        # Empty if there are no geocodes with too many people per HU:
        mdf_groupby_geocode = self.e2e.mdf_per\
            .filter(self.e2e.mdf_per["rtype"] == '3')\
            .groupBy("geocode").agg(F.count("*").alias("mdf_per_count"))

        cef_groupby_geocode = self.e2e.cef_unit\
            .filter(self.e2e.cef_unit["RTYPE"] == '2')\
            .groupBy("geocode").agg(F.count("*").alias("cef_unit_count"))

        join_df = mdf_groupby_geocode.join(cef_groupby_geocode,
                                           (mdf_groupby_geocode.geocode == cef_groupby_geocode.geocode),
                                           how='left')
        filter_count = join_df.filter(join_df.mdf_per_count > MAX_NUMBER_OF_PERSONS_IN_UNIT * join_df.cef_unit_count).count()

        if filter_count != 0:
            print(f"Failed test_NIU. With count of {filter_count}")
            self.errors += 1
            return
        print("Passed test_NIU")

    def find_lb_violations(self, joined_df: DataFrame) -> None:
        """
        This comes from <xl_flag>phill's preexisting tests and converted to work with mdf and cef.
        :param joined_df:
        :return:
        """
        find_lb_violations = 0

        # We don't want to use 0 in these checks
        local_mdf_check = deepcopy(self.mdf_distinct_gqtype)
        if '0' in local_mdf_check:
            local_mdf_check.remove('0')
        for gq_type in local_mdf_check:
            print(
                f"Checking by block whether # Persons in gqtype {gq_type} in MDF is >= # GQ facilities in CEF of type "
                f"{gq_type}")
            violations = joined_df.filter(joined_df[f"sum(mdf_level{gq_type})"] < joined_df[f"sum(cef_level{gq_type}01)"])
            numViolations = violations.count()
            print(f"# Violations detected in lb_violations gytype: {gq_type}: {numViolations}")
            find_lb_violations += numViolations
        if find_lb_violations > 0:
            print(f"Found {find_lb_violations} violations in ub_violations")
        self.errors += find_lb_violations

    def find_ub_violations(self, joined_df: DataFrame) -> None:
        """
        This comes from <xl_flag>phill's preexisting tests and converted to work with mdf and cef.
        :param joined_df:
        :return:
        """
        ub_violations = 0

        print(f"Checking if {MAX_NUMBER_OF_PERSONS_IN_UNIT} (MAX_NUMBER_OF_PERSONS_IN_UNIT) * #Housing Units >= # Ppl in Housing Units")
        violationsDF = joined_df.filter(
            MAX_NUMBER_OF_PERSONS_IN_UNIT * (joined_df["sum(cef_level000)"]) < joined_df["sum(mdf_level0)"])
        print(f"Found # violations {violationsDF.count()}")
        ub_violations += violationsDF.count()

        print(f"Checking if no Housing Units => no ppl in housing units")
        noHHsDF = joined_df.filter(joined_df["sum(cef_level000)"] == 0)
        print(f"Found # geocodes w/ no housing units: {noHHsDF.count()}")

        noPeopleDF = noHHsDF.filter(joined_df["sum(mdf_level0)"] == 0)
        print(f"Subset of those w/ no ppl of type (should be all): {noPeopleDF.count()}")
        ub_violations += 1 if noPeopleDF.count() != noHHsDF.count() else 0

        # We don't want to use 000 in the checks
        local_mdf_check = deepcopy(self.mdf_distinct_gqtype)
        local_mdf_check.remove('0')
        for gqtype in local_mdf_check:
            print(
                f"Checking by block whether # Persons in gqtype {gqtype} in MDF is 0 if # GQ facilities in Table 10 "
                f"of type {gqtype}")
            noFacilitiesDF = joined_df.filter(joined_df[f"sum(cef_level{gqtype}01)"] == 0)
            print(f"Found # geocodes w/ no facilities of type: {noFacilitiesDF.count()}")
            noPeopleDF = noFacilitiesDF.filter(joined_df[f"sum(mdf_level{gqtype})"] == 0)
            print(f"Subset of those w/ no ppl of type (should be all): {noPeopleDF.count()}")
            ub_violations += 1 if noFacilitiesDF.count() != noPeopleDF.count() else 0
        if ub_violations > 0:
            print(f"Found {ub_violations} violations in ub_violations")
        self.errors += ub_violations

    def check_state_total_pops(self, spark: SparkSession) -> None:
        mdf_per = spark.sql("SELECT * from mdf_per")
        cef_per = spark.sql("SELECT * from cef_per")
        mdf_per_distinct_states = mdf_per.select("tabblkst").distinct().rdd.flatMap(lambda x: x).collect()
        mdf_per_distinct_states = [x for x in mdf_per_distinct_states if x.strip()]
        for fip in mdf_per_distinct_states:
            mdf_count = mdf_per.filter(mdf_per["tabblkst"] == fip).count()
            cef_count = cef_per.filter(cef_per["VALIDST"] == fip).count()
            print(f"{fip} has # ppl in MDF vs CEF: {mdf_count} vs {cef_count}")
            self.errors += 1 if mdf_count != cef_count else 0

    def test_people_in_cef_vacant_hus(self) -> None:
        """
        Validate that there are at least some Housing Units which were vacant in the CEF now contain people in the output MDF
        """
        print('======================================')
        # Group Quarters are irrelevant, so filter out only records for housing units.
        # .groupby geocode and find types of tenures of units in that geocode
        # tenure '0' means vacant, others mean occupied, filter to only vacant, no occupied
        # to get a list of geocodes that don't have occupied housing units (could be '0' in r['tens'] to include blocks without vacant units, but they are irrelevant)
        # It maybe worth to convert 'ten' to T.IntegerType() first
        cef_vacant_housing_unit_geocodes = (
            self.e2e.cef_unit
                .filter(self.e2e.cef_unit.rtype == 2)
                .groupby('geocode')
                .agg(F.collect_set('ten').alias('tens'))
                .rdd.filter(lambda r: r['tens'] == ['0'])
                .map(lambda r: r['geocode'])
                .collect()
        )

        print(f'len(cef_vacant_housing_unit_geocodes) {len(cef_vacant_housing_unit_geocodes)}')

        # Filter only 'persons-in-HU' records (rtype 3) and see if any appeared in MDF where there were none in CEF
        mdf_persons_in_previously_vacant_geocodes = self.e2e.mdf_per.filter((self.e2e.mdf_per.rtype == 3) & (self.e2e.mdf_per.geocode.isin(cef_vacant_housing_unit_geocodes)))

        filter_count = mdf_persons_in_previously_vacant_geocodes.count()
        print(f'filter_count:  {filter_count}')
        print('======================================')
        if filter_count == 0:
            print(f"Failed test_people_in_cef_vacant_hus. With count of {filter_count}")
            self.errors += 1
            return
        print("Passed test_people_in_cef_vacant_hus")

    def test_cef_people_in_mdf_vacant_hus(self) -> None:
        """
        Validate that there are at least some Housing Units which were vacant in the CEF now contain people in the output MDF
        """
        print('======================================')
        cef_occupied_housing_unit_blocks = (
            self.e2e.cef_unit
                .filter(self.e2e.cef_unit.rtype == 2)
                .groupby('geocode')
                .agg(F.collect_set('vacs').alias('vacss'))
                .rdd.filter(lambda r: '0' in r['vacss'])
                .map(lambda r: (r['geocode'],1))
        )

        print(f'len(cef_vacant_housing_unit_geocodes) {cef_occupied_housing_unit_blocks.count()}')

        mdf_people_in_hu = self.e2e.mdf_per.filter((self.e2e.mdf_per.rtype == 3)).rdd.map(lambda r: (r['geocode'], 1))

        cef_mdf = cef_occupied_housing_unit_blocks.leftOuterJoin(mdf_people_in_hu)

        filter_count = cef_mdf.filter(lambda d: d[1][1] is None).count()

        print(f'filter_count:  {filter_count}')
        print('======================================')
        if filter_count == 0:
            print(f"Failed test_cef_people_in_mdf_vacant_hus. With count of {filter_count}")
            self.errors += 1
            return
        print("Passed test_cef_people_in_mdf_vacant_hus")

    @staticmethod
    def generate_udf(target: str) -> DataFrame:
        def level_flag(level: int) -> int:
            return 1 if str(level) == str(target) else 0
        return F.udf(level_flag, T.IntegerType())


def main(args: Namespace) -> None:
    """
    This is the main function that is used to for the MDF validator when you are running in Standalone mode. You can
    also run these tests at the end of a DAS run however this logic is never called in that case.
    :param args: These are the command line arguments that are provided by the user.
    """
    clogging.setup(args.loglevel,
                   syslog=True,
                   filename=args.logfilename,
                   log_format=clogging.LOG_FORMAT,
                   syslog_format=clogging.YEAR + " " + clogging.SYSLOG_FORMAT)
    # Moving import here because it errors when end2end_validator is imported during DAS run.
    schema = args.schema
    standard_geocodes = args.nonstandard_geocodes is None or args.nonstandard_geocodes is False
    validate_format_only = args.format_only is not None and args.format_only is True

    from das2020_driver import DASDelegate
    validation_violations=False
    with TemporaryDirectory() as tempdir:

        delegate = DASDelegate(mission_name="end2end_validator")
        from pyspark.sql import SparkSession
        from das_utils import ship_files2spark

        spark = (SparkSession.builder.appName('DAS 2020 MDF validator')
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.driver.memory", "256g").getOrCreate()
                )
        ship_files2spark(spark, allfiles=True)

        if args.type.lower() == "unit":
            if args.grfc_file is not None and args.cef_unit_file is not None and args.mdf_unit_file is not None:
                validator_class = E2EValidatorUnit
                if schema == CC.SCHEMA_PL94_2020 or schema == CC.SCHEMA_H1_2020:
                    validator_class = E2EValidatorH12020

                validator = validator_class(grfc_file=args.grfc_file, cef_unit_file=args.cef_unit_file, schema=schema,
                                            mdf_unit_file=args.mdf_unit_file, OVERRIDE=True,
                                            standard_geocodes=standard_geocodes,
                                            validate_format_only=validate_format_only)
            else:
                logging.error("You must provide a grfc_file, cef_unit_file and mdf_unit_file to run unit mdf checks.")
                return
        if args.type.lower() == "person":
            if args.grfc_file is not None and args.mdf_per_file is not None and args.cef_per_file is not None \
                    and args.cef_unit_file is not None:
                validator_class = E2EValidatorPerson
                if schema == CC.SCHEMA_PL94_2020 or schema == CC.SCHEMA_H1_2020:
                    validator_class = E2EValidatorPL942020

                validator = validator_class(mdf_per_file=args.mdf_per_file, cef_per_file=args.cef_per_file,
                                            cef_unit_file=args.cef_unit_file, grfc_file=args.grfc_file, schema=schema,
                                            OVERRIDE=True, standard_geocodes=standard_geocodes, validate_format_only=validate_format_only)
            else:
                logging.error("You must provide a grfc_file, mdf_per_file, cef_per_file and cef_unit_file to run "
                              "person mdf checks.")
                return
        try:
            if validator.validate():
                logging.info(f"Passed validation for {args.type.lower()}")
                delegate.log_testpoint(testpoint="003S")
                delegate.log_testpoint(testpoint="004S")
                delegate.log_testpoint(testpoint="005S")
            else:
                error_string = f"Failed validation for {args.type.lower()}. " \
                    f"There where {validator.errors + validator.failed_invariants} violations"
                delegate.log_testpoint(testpoint=f"{'003F' if validator.failed_invariants else '003F'}")
                delegate.log_testpoint(testpoint=f"{'004F' if validator.errors else '004F'}")
                logging.error(error_string)
                validation_violations=True
                if strict_validation:
                    raise Exception(error_string)
            if validation_violations:
                logging.error("See log for validation violations")
            else:
                logging.info(f"Main validate succeed {args.type.lower()}")
                delegate.log_testpoint(testpoint="005S")
        except Exception as ex:
            if args.type.lower() == "unit":
                delegate.log_testpoint(testpoint="005F")
            if args.type.lower() == "person":
                delegate.log_testpoint(testpoint="005F")
            delegate.log_testpoint(testpoint="005F")
            logging.warning(f"Main validate fail {args.type.lower()}")
            logging.error(str(ex))
            raise ex
        constraints_validator.main(args, spark)


class E2EValidatorPL942020(E2EValidatorPerson):

    def __init__(self,  **kwargs) -> None:
        super().__init__(**kwargs)
        self.GQTYPE_VARIABLE = "gqtype_pl"

    def create_cef_unit_sum_df(self) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        cef_unit_sum = spark.sql("SELECT * from cef_unit")

        # Convert '   ' to '000' and take first digit
        cef_unit_sum = cef_unit_sum\
            .withColumn("qgqtype_1dig",
                        F.when(F.col("QGQTYP").isin(['   ']), "000")
                        .otherwise(F.concat(F.substring(F.col("QGQTYP"), 0, 1), F.lit("01")))
                        )

        QGQTYPE_EDIT = F.when(F.col("qgqtype_1dig").isin(['801', '901']), "701").otherwise(F.col("qgqtype_1dig"))


        cef_unit_sum = cef_unit_sum.withColumn("qgqtype_edit", QGQTYPE_EDIT)
        cef_distinct_gqtype = cef_unit_sum.select("qgqtype_edit").distinct().rdd.flatMap(lambda x: x).collect()
        for target in cef_distinct_gqtype:
            cef_unit_sum = cef_unit_sum.withColumn(f"cef_level{target}",
                                                   E2EValidatorPL942020.generate_udf(target)(cef_unit_sum['qgqtype_edit']))
        cef_unit_sum = cef_unit_sum.groupBy(cef_unit_sum["geocode"]).sum()
        return cef_unit_sum


class E2EValidatorH12020(E2EValidatorUnit):

    def create_e2e(self, dataframe: DataFrame = None, schema: str = CC.SCHEMA_PL94_2020, standard_geocodes: bool = True) -> None:
        mdf_unit_file = dataframe if dataframe else self.mdf_unit_file
        self.e2e = E2E(cef_unit_file=self.cef_unit_file,
                       cef_per_file=self.cef_per_file,
                       mdf_unit_file=mdf_unit_file,
                       mdf_per_file=self.mdf_per_file,
                       grfc_file=self.grfc_file,
                       schema=self.schema, standard_geocodes=self.standard_geocodes)

    def validate_data(self, dataframe: DataFrame = None, join_method: str = 'full') -> bool:
        self.create_e2e(dataframe=dataframe)

        if not self.validate_format_only:
            self.test_mdf_count_vs_cef_count(join_method=join_method)
            self.validate_cef_unit()
            self.create_temp_tables_cef_unit()
            if self.schema in [CC.SCHEMA_DHCH, CC.SCHEMA_DHCH_TEN_3LEV]:
                self.validate_housing()

        return True if self.errors + self.failed_invariants == 0 else False

    def test_mdf_count_vs_cef_count(self, join_method: str = 'full') -> None:
        """
        This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
        four.
        join_method: This is the method to use when joining the MDF with the CEF in this function. Full should be used
        when you are running this check on the full completed MDF. If however you are checking only specific levels of
        the MDF you should use inner.
        :return:
        """
        mdf_groupby_geocode = self.e2e.mdf_unit.groupBy("geocode", ).agg(F.count("*").alias("mdf_unit_count"))
        cef_groupby_geocode = self.e2e.cef_unit.groupBy("geocode", ).agg(F.count("*").alias("cef_unit_count"))

        join_df = mdf_groupby_geocode.join(cef_groupby_geocode,
                                           (mdf_groupby_geocode.geocode == cef_groupby_geocode.geocode),
                                           how=join_method)
        join_df = join_df.drop('geocode')
        join_df = join_df.fillna(0, subset=['cef_unit_count', 'mdf_unit_count'])
        filter_count = join_df.filter(join_df.mdf_unit_count != join_df.cef_unit_count).count()
        if filter_count != 0:
            print("Failed test_mdf_count_vs_cef_count")
            self.errors += 1
            return
        print("Passed test_mdf_count_vs_cef_count")

    def occupied_checks(self) -> None:
        """
            This was taken from wills pre existing tests and converted to work with the cef and mdf. It was his test number
            Six.
            :return:
        """
        print(f"Running validation to ensure all Housing Units have HH_STATUS Occupied or Vacant")
        housing_units = self.e2e.mdf_unit.filter(self.e2e.mdf_unit.rtype == 2) # Grab all non-group quarters
        test_six_df = housing_units.filter((housing_units.hh_status != 1) & (housing_units.hh_status != 2))
        if test_six_df.count() > 0:
            print(f"Found bad values for RTYPE and HH_STATUS")
            test_six_df.head(10)
            self.errors += 1

        print(f"Running validation to ensure all Group Quarters have HH_STATUS = NIU")
        group_quarters = self.e2e.mdf_unit.filter(self.e2e.mdf_unit.rtype == 4) # Grab all non-group quarters
        test_six_df = group_quarters.filter(group_quarters.hh_status != 0)
        if test_six_df.count() > 0:
            print(f"Found bad values for RTYPE and HH_STATUS")
            test_six_df.head(10)
            self.errors += 1



if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,
                            description="Validate MDF Files")

    parser.add_argument("--type", type=str, required=True, help="Example unit or person")
    parser.add_argument("--grfc_file", type=str, required=False, help="Path to grfc file this can include regex. "
                                                                      "${DAS_S3INPUTS}/2010-convert/grfc/"
                                                                      "grfc_tab20_[0-9]*.txt")
    parser.add_argument("--cef_unit_file", type=str, required=False, help="Path to cef_unit")
    parser.add_argument("--mdf_unit_file", type=str, required=False, help="Path to mdf_unit")

    parser.add_argument("--mdf_per_file", type=str, required=False, help="Path to mdf_per")
    parser.add_argument("--cef_per_file", type=str, required=False, help="Path to cef_per")

    parser.add_argument("--schema", type=str, default=CC.SCHEMA_PL94_2020, help="Schema to use to obtain appropriate validator classes")
    parser.add_argument("--nonstandard_geocodes", action='store_true', help="Geocodes are non-standard")
    parser.add_argument("--format_only", action='store_true', help="Only include format validation")
    parser.add_argument("--test_total_pops", action='store_true', help="Skip comparison of CEF/MDF total pops")
    parser.add_argument("--run_configuration", default="production/pl94",
                        help="for specifying the path to the config files")
    parser.add_argument("--nohierconfig", action="store_true",
                        help="use normal configparser instead of hierarch one")

    clogging.add_argument(parser)
    args_parse = parser.parse_args()
    main(args=args_parse)
