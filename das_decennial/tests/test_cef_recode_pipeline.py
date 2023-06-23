import pytest
import os
import pandas as pd

from pyspark.sql import Row
from pyspark.sql.functions import lit
from programs.reader.cef_2020.cef_2020_dhcp_reader import DHCP_recoder
from programs.reader.cef_2020.cef_2020_dhch_reader import DHCH_Household_recoder
from programs.writer.cef_2020.dhch_to_mdf2020_recoders import DHCHToMDF2020HouseholdRecoder
from programs.writer.cef_2020.dhch_to_mdf2020_writer import DHCH_MDF2020_Writer
from programs.writer.dhcp_to_mdf2020 import DHCPToMDF2020Recoder
from programs.writer.mdf2020writer import DHCP_MDF2020_Writer
# from programs.reader.cef_2020.cef_validator_classes import CEF20_UNIT, CEF20_PER
# from programs.writer.cef_2020.mdf_validator_classes import MDF_Unit, MDF_Person
from programs.validator.end2end_validator import DHC_MDF_Person, DHC_MDF_Unit
from programs.tests.spark_fixture import spark
from das_constants import CC

DAS_S3INPUTS = os.environ['DAS_S3INPUTS']
PATH = DAS_S3INPUTS + '/unit-test-data/das_decennial/test_cef_recode_pipeline'

def read_and_validate(spark, file, validator):
    rdd = spark.sparkContext.textFile(file)
    rdd = rdd.map(lambda line: validator.parse_piped_line(line))
    df = spark.createDataFrame(rdd)
    return df


@pytest.mark.parametrize(
    "cef_file, mdf_file, mdf_validator, input_recoder, input_recoder_input, output_recoder, output_recoder_input, output_writer",
    [
        (f"{PATH}/cenrace_cef.data",
         f"{PATH}/cenrace_mdf.data", DHC_MDF_Person,
         DHCP_recoder, {"relgq_varname_list": ("relship", "qgqtyp"),
                        "sex_varname_list": ("qsex",),
                        "age_varname_list": ("qage",),
                        "hispanic_varname_list": ("cenhisp",),
                        "cenrace_das_varname_list": ("cenrace",)},
         DHCPToMDF2020Recoder, {}, DHCP_MDF2020_Writer),
        (f"{PATH}/hhtype_cef.data",
         f"{PATH}/hhtype_mdf.data", DHC_MDF_Unit,
         DHCH_Household_recoder, {"hhsex_varname_list": ("hhsex",), "hhage_varname_list": ("hhldrage",),
                                  "hhhisp_varname_list": ("hhspan",), "hhrace_varname_list": ("hhrace",),
                                  "elderly_varname_list": ("p60", "p65", "p75"),
                                  "tenure_varname_list": ("ten",), "hht_varname_list": ("hht",)},
         DHCHToMDF2020HouseholdRecoder, {}, DHCH_MDF2020_Writer
         )
    ]
)
def test_full_recode_pipeline(spark, cef_file, mdf_file, mdf_validator, input_recoder, input_recoder_input,
                              output_recoder, output_recoder_input, output_writer):
    # Read in the final comparison dataframe.
    readin_mdf = read_and_validate(spark=spark, file=mdf_file, validator=mdf_validator)

    # Read in test CEF File. This is slightly modified input from the standard cef it is CSV with geocode and final_pop.
    dhcp_df_input = spark.read.csv(os.path.expandvars(cef_file), header="true")
    dhcp_df_input = dhcp_df_input.toDF(*[c.lower() for c in dhcp_df_input.columns])
    # Make sure the test CEF File is the same size as the read in final comparison df.
    assert dhcp_df_input.count() == readin_mdf.count()

    # Create the input Recoder with provided variables.
    dhcp_input_recoder = input_recoder(**input_recoder_input)
    # Convert to rdd and recode the rows then convert back to DF.
    dhcp_df_input_recoded = dhcp_df_input.rdd.map(lambda row: dhcp_input_recoder.recode(row=row))
    dhcp_df_input_recoded = spark.createDataFrame(dhcp_df_input_recoded)
    # Make sure the recode is the same size as the read in final comparison df.
    assert dhcp_df_input_recoded.count() == readin_mdf.count()

    dhcp_df_input_recoded = dhcp_df_input_recoded.drop(CC.ATTR_CENRACE)
    dhcp_df_input_recoded = dhcp_df_input_recoded.withColumnRenamed(CC.ATTR_CENRACE_DAS, CC.ATTR_CENRACE)

    # Create the output Recoder with provided variables.
    dhcp_output_recoder = output_recoder(**output_recoder_input)

    # Select out geocode and the schema dimnames.
    to_select = ["geocode"]
    to_select.extend(dhcp_output_recoder.schema.dimnames)
    dhcp_df_input_recoded = dhcp_df_input_recoded.select(to_select)

    # Go throught and mangle the schema column names and recode the data for output.
    z = {name: f"{name}_{dhcp_output_recoder.schema.name}" for name in dhcp_output_recoder.schema.dimnames}
    dhcp_df_input_recoded = dhcp_df_input_recoded.toDF(*[z.get(colName) if z.get(colName) is not None else colName for colName in dhcp_df_input_recoded.columns])
    dhcp_df_input_recoded_for_output = dhcp_df_input_recoded.rdd.map(lambda row: Row(**dhcp_output_recoder.recode(row=row.asDict())))
    assert dhcp_df_input_recoded_for_output.count() == readin_mdf.count()

    # Add EPNUM which in the case is all zeros. TODO: This should be changed to really generate EPNUM
    dhcp_df_input_recoded_for_output = spark.createDataFrame(dhcp_df_input_recoded_for_output)
    dhcp_df_input_recoded_for_output = dhcp_df_input_recoded_for_output.withColumn("EPNUM", lit(0))
    dhcp_df_input_recoded_for_output = dhcp_df_input_recoded_for_output.select(output_writer.var_list)
    dhcp_df_input_recoded_for_output = dhcp_df_input_recoded_for_output.toDF(*[c.lower() for c in dhcp_df_input_recoded_for_output.columns])
    assert dhcp_df_input_recoded_for_output.count() == readin_mdf.count()

    # This is the dataframe that is read in from the S3 to do final comparison. The reason there is a select is so that
    # the columns for both dataframes are in the same order which is required for the exceptAll Call.
    readin_mdf = readin_mdf.select(output_writer.var_list)
    no_matching = dhcp_df_input_recoded_for_output.exceptAll(readin_mdf)  # This should be empty
    assert no_matching.count() == 0  # If dhcp_df_input_recoded_for_output is the same as the readin mdf df this should be zero.
