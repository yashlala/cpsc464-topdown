# This script produces a small subset of data to run with DHCP and DHCH schemas
# To be run from ipyspark interactively (not bothering with setting up imports and spark session for now)
# As in run
# PYSPARK_DRIVER_PYTHON=ipython pyspark
# in shell
# and then in ipython pyspark:
# run -i scripts/make_test_dataset.py

das_s3inputs = os.environ["DAS_S3INPUTS"]
das_s3outputs = os.environ["DAS_S3ROOT"]
samplerate = 0.0001
#output_path = f"{das_s3inputs}/title13_input_data/test"
output_path = f"{das_s3outputs}/{username}/testdata"

dfp = spark.read.csv(f'{das_s3inputs}/title13_input_data/table1a/ri44.txt', sep='\t', header=True)
dfu = spark.read.csv(f'{das_s3inputs}/title13_input_data/table10/ri44.txt', sep='\t', header=True)

# Join persons and units, so that we have the same units in the same places as the persons
dfpu = dfp.join(dfu, ["#MAFID", "geocode"],'outer').sample(False, samplerate)

persons = dfpu.filter('hhgq!=1').select('#MAFID qage geocode white black aian asian nhopi other hispanic sex citizen relation'.split(" "))

units = dfpu.select('#MAFID geocode hhgq'.split(' '))

vacant_units = dfu.filter('hhgq=1').select('*').limit(20)

units = units.union(vacant_units)

persons.coalesce(1).write.csv(f'{output_path}/table1a.txt', header=True, sep='\t', mode='overwrite')
units.coalesce(1).write.csv(f'{output_path}/table10simple.txt', header=True, sep='\t', mode='overwrite')


## Now the same for households

dfh = spark.read.csv(f'{das_s3inputs}/title13_input_data/table12a/ri44.txt', sep='\t', header=True)
dfu = spark.read.csv(f'{das_s3inputs}/title13_input_data/table10_20190610/ri44.txt', sep='\t', header=True)

# Join households and units, so that we have the same units in the same places as the persons
dfhu = dfh.join(dfu, ["#MAFID", "geocode", "ten"],'outer').sample(False, samplerate)

households = dfhu.filter('vacs=0').select('#MAFID qage hispanic sex ten multigeneration geocode hhsize race elderly household_type'.split(" "))
units = dfhu.select('#MAFID ten vacs gqtype geocode'.split(' '))
vacant_units = dfu.filter('vacs!=0').select('*').limit(20)
gqs = dfu.filter(dfu.gqtype>0).select('*').limit(10)
units = units.union(vacant_units).union(gqs)

households.coalesce(1).write.csv(f'{output_path}/table12a.txt', header=True, sep='\t', mode='overwrite')
units.coalesce(1).write.csv(f'{output_path}/table10.txt', header=True, sep='\t', mode='overwrite')

# The same for DHCP2020 tables

dfp = spark.read.csv(f'{das_s3inputs}/title13_input_data/table13/ri44.txt', sep='\t', header=True)
dfu = spark.read.csv(f'{das_s3inputs}/title13_input_data/table10_20190610/ri44.txt', sep='\t', header=True)

# Join persons and units, so that we have the same units in the same places as the persons
dfpu = dfp.join(dfu, ["#MAFID", "geocode"],'outer').sample(False, samplerate)

persons = dfpu.filter('VACS==0').select('#MAFID geocode hispanic sex QAGE CENRACE relgq'.split(" "))

units = dfpu.select('#MAFID ten vacs gqtype geocode'.split(' '))

vacant_units = dfu.filter('VACS!=0').filter('gqtype==000').select('*').limit(20)
#gqs = dfu.filter(dfu.gqtype>0).select('*').limit(10)  # Don't need GQs, they'll break constraints, since there probably won't be people on those blocks

units = units.union(vacant_units)  #.union(gqs)

persons.coalesce(1).write.csv(f'{output_path}/table13.txt', header=True, sep='\t', mode='overwrite')
units.coalesce(1).write.csv(f'{output_path}/table10gowith13.txt', header=True, sep='\t', mode='overwrite')
