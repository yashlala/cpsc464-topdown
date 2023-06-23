from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import LongType

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

A_US = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-cathe-1811exps-1/DAS-REL-MASTER-cathe-1811exps-1/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-cathe-1811exps-1/DAS-REL-MASTER-cathe-1811exps-1/mdf/us/unit/MDF10_UNIT_US.txt/MDF10_UNIT_US-MDF2020H1.txt",
    "A_US"
)
A_PR = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-cathe-1811exps-1/DAS-REL-MASTER-cathe-1811exps-1/mdf/pr/per/MDF10_PER_PR.txt/MDF10_PER_PR-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-cathe-1811exps-1/DAS-REL-MASTER-cathe-1811exps-1/mdf/pr/unit/MDF10_UNIT_PR.txt/MDF10_UNIT_PR-MDF2020H1.txt",
    "A_PR"
)
B_US = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-lubit-1811exps-2/DAS-REL-MASTER-lubit-1811exps-2/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-lubit-1811exps-2/DAS-REL-MASTER-lubit-1811exps-2/mdf/us/unit/MDF10_UNIT_US.txt/MDF10_UNIT_US-MDF2020H1.txt",
    "B_US"
)
B_PR = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-lubit-1811exps-2/DAS-REL-MASTER-lubit-1811exps-2/mdf/pr/per/MDF10_PER_PR.txt/MDF10_PER_PR-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-lubit-1811exps-2/DAS-REL-MASTER-lubit-1811exps-2/mdf/pr/unit/MDF10_UNIT_PR.txt/MDF10_UNIT_PR-MDF2020H1.txt",
    "B_PR"
)
C_US = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-miske-1811exps-3/DAS-REL-MASTER-miske-1811exps-3/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-miske-1811exps-3/DAS-REL-MASTER-miske-1811exps-3/mdf/us/unit/MDF10_UNIT_US.txt/MDF10_UNIT_US-MDF2020H1.txt",
    "C_US"
)
C_PR = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-miske-1811exps-3/DAS-REL-MASTER-miske-1811exps-3/mdf/pr/per/MDF10_PER_PR.txt/MDF10_PER_PR-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-MASTER-miske-1811exps-3/DAS-REL-MASTER-miske-1811exps-3/mdf/pr/unit/MDF10_UNIT_PR.txt/MDF10_UNIT_PR-MDF2020H1.txt",
    "C_PR"
)
D_US = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-019-thirs-1811exps-4/DAS-REL-019-thirs-1811exps-4/mdf/us/per/MDF10_PER_US.txt/MDF10_PER_US-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-019-thirs-1811exps-4/DAS-REL-019-thirs-1811exps-4/mdf/us/unit/MDF10_UNIT_US.txt/MDF10_UNIT_US-MDF2020H1.txt",
    "D_US"
)
D_PR = (
    "${DAS_S3ROOT}/runs/tests/DAS-REL-019-thirs-1811exps-4/DAS-REL-019-thirs-1811exps-4/mdf/pr/per/MDF10_PER_PR.txt/MDF10_PER_PR-MDFPL942020.txt",
    "${DAS_S3ROOT}/runs/tests/DAS-REL-019-thirs-1811exps-4/DAS-REL-019-thirs-1811exps-4/mdf/pr/unit/MDF10_UNIT_PR.txt/MDF10_UNIT_PR-MDF2020H1.txt",
    "D_PR"
)
# for person in [A_US, B_US, C_US, D_US]:
#     for unit in [A_US, B_US, C_US, D_US]:
for person in [A_PR, B_PR, C_PR, D_PR]:
    for unit in [A_PR, B_PR, C_PR, D_PR]:
    #for pair in [D_US, D_PR]:
        if person == unit:
            continue

        du = spark.read.csv(unit[1], sep='|', comment='#', header=True)
        dp = spark.read.csv(person[0], sep='|', comment='#', header=True)
        name = f"Per{person[2]}_Un{unit[2]}"
        # du = du.filter('TABBLKST=44')
        # dp = dp.filter('TABBLKST=44')

        du = du.withColumn("geocode", F.concat(F.col('TABBLKST'), F.col('TABBLKCOU'), F.col('TABTRACTCE'), F.col('TABBLKGRPCE'), F.col('TABBLK')))
        dp = dp.withColumn("geocode", F.concat(F.col('TABBLKST'), F.col('TABBLKCOU'), F.col('TABTRACTCE'), F.col('TABBLKGRPCE'), F.col('TABBLK')))

        ###
        # Analyze

        # Count units with HH_STATUS=1 within every geocode
        occupied = du.filter("HH_STATUS=1").groupby("geocode").count().withColumnRenamed("count", "occupied")
        # vacant = du.filter("HH_STATUS=2").groupby("geocode").count().withColumnRenamed("count", "vacant")

        # Count people in households within every geocode
        hh_pop = dp.filter("GQTYPE_PL=0").groupby("geocode").count().withColumnRenamed("count", "hh_pop")

        # Count all people within every geocode
        tot_pop = dp.groupby("geocode").count().withColumnRenamed("count", "tot_pop")

        # Join finding only the blocks that are absent in occupied (this will include blocks with no units)
        joined_hh = hh_pop.join(occupied, hh_pop.geocode == occupied.geocode, 'left_anti')
        joined = tot_pop.join(occupied, tot_pop.geocode == occupied.geocode, 'left_anti')


        def count2bin(count):
            " 0, 1, 2, ..., 9, 10 - 49, 50 - 99, 100 - 249, 250 - 499, 500 - 999, 1000 +"
            if count is None:
                return 0
            if count < 10:
                return count
            elif count < 50:
                return 10
            elif count < 100:
                return 50
            elif count < 250:
                return 100
            elif count < 500:
                return 250
            elif count < 1000:
                return 500
            else:
                return 1000

        def binstr(bin):
            " 0, 1, 2, ..., 9, 10 - 49, 50 - 99, 100 - 249, 250 - 499, 500 - 999, 1000 +"
            if bin < 10:
                return " " + str(bin)
            return {10: " 10-49",
                    50: " 50-99",
                    100: "100-249",
                    250: "250-499",
                    500: "500-999",
                    1000: "1000+",}[bin]

        binstr_udf = F.udf(binstr)
        bin_udf = F.udf(count2bin, LongType())
        binned_hh = joined_hh.select([bin_udf("hh_pop").alias("hhpopbin"), F.substring("geocode", 0, 2).alias("State")])
        binned = joined.select([bin_udf("tot_pop").alias("totpopbin"), F.substring("geocode", 0, 2).alias("State")])


        print("Number of blocks in each bin (hh pop)")
        nb = hh_pop.select([bin_udf("hh_pop").alias("hhpopbin"), F.substring("geocode", 0, 2).alias("State")])\
                .groupBy("State", 'hhpopbin').count().withColumnRenamed("count", "num_blocks")\
                .orderBy(["State", 'hhpopbin']).withColumn('hhpopbin_str', binstr_udf(F.col('hhpopbin')))\

        nb.show()
        nb.toPandas().to_csv(f'{name}_numblocks_by_state.csv', header=True, index=False)
        nb = nb.groupBy('hhpopbin').agg(F.sum('num_blocks').alias('num_blocks')).orderBy('hhpopbin').withColumn('hhpopbin_str', binstr_udf(F.col('hhpopbin')))
        nb.show()
        nb.toPandas().to_csv(f'{name}_numblocks.csv', header=True, index=False)

        hist_hh = binned_hh.groupBy("State", 'hhpopbin').count().withColumnRenamed("count", "blocks_wo_occupied_units").orderBy(["State", 'hhpopbin'])
        hist = binned.groupBy('State','totpopbin').count().withColumnRenamed("count", "blocks_wo_occupied_units").orderBy(["State",'totpopbin'])

        hist = hist.withColumn('totpopbin_str', binstr_udf(F.col('totpopbin')))
        hist_hh = hist_hh.withColumn('hhpopbin_str', binstr_udf(F.col('hhpopbin')))


        print("Household population; blocks include blocks with no vacant or occupied units")
        hist_hh.show()
        print("Total population; blocks only include blocks with no vacant or occupied units")
        hist.show()

        hist_hh.toPandas().to_csv(f'{name}_hist_hh_str_by_state.csv', header=True, index=False)
        hist.toPandas().to_csv(f'{name}_hist_str_by_state.csv', header=True, index=False)

        hist_hh = hist_hh.groupBy('hhpopbin').agg(F.sum('blocks_wo_occupied_units').alias('blocks_wo_occupied_units')).orderBy('hhpopbin')
        hist = hist.groupBy('totpopbin').agg(F.sum('blocks_wo_occupied_units').alias('blocks_wo_occupied_units')).orderBy('totpopbin')

        hist = hist.withColumn('totpopbin_str', binstr_udf(F.col('totpopbin')))
        hist_hh = hist_hh.withColumn('hhpopbin_str', binstr_udf(F.col('hhpopbin')))

        print("Household population; blocks include blocks with no vacant or occupied units")
        hist_hh.show()
        print("Total population; blocks only include blocks with no vacant or occupied units")
        hist.show()

        hist_hh.toPandas().to_csv(f'{name}_hist_hh_str.csv', header=True, index=False)
        hist.toPandas().to_csv(f'{name}_hist_str_by.csv', header=True, index=False)
