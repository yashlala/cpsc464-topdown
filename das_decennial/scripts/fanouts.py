from pyspark.sql.functions import *
df = spark.read.csv('${DAS_S3INPUTS}/title13_input_data/table10_20190610', sep='\t', header=True)


# Will show what is maximum fanout of lower_split level units in upper_split. Lower_split and upper_split are digits of geocode (1-starting)
# To decide where to put boundary you need 4 numbers: maximal split on either side of the boundary for both options of where to put it.
# E.g. if you're deciding whether the digit for Tract_Group should be 8 or 9:
# 1.1) How many TractGroup9 in a County maximum? upper_split = 5 (County), lower_split = 9 (TractGroup9)
# 1.2) How many Tracts in a TractGroup9 maximum? upper_split = 9 (TractGroup9), lower_split = 11 (Tract)
# 2.1) How many TractGroup8 in a County maximum? upper_split = 5 (County), lower_split = 8 (TractGroup9)
# 2.2) How many Tracts in a TractGroup8 maximum? upper_split = 8 (TractGroup8), lower_split = 11 (Tract)
# Then compare maximal of 1.1 and 1.2 with maximal of 2.1 and 2.2. If (2) is smaller than (1), then TractGroup8 wins.
upper_split = 2
lower_split = 5
df.select(
    [substring('geocode', 1, upper_split).alias('upper'), substring('geocode', upper_split+1, lower_split-upper_split).alias('lower')])\
    .distinct()\
    .groupby('upper')\
    .count()\
    .select(max('count'))\
    .show()
