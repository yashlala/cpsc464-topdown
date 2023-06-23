import sys
import os
import analysis.constants as AC


if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

DAS_DIR = '/mnt/users/user007/das_decennial'
sys.path.append('/mnt/users/user007/das_decennial')
#sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

#DAS_DIR    = os.path.dirname( os.path.abspath( __file__ ))
#sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

#os.environ["PYTHONPATH"] = "/mnt/users/user007/das_decennial"
import numpy as np

''' Test the age hierarchy queries
if __name__=="__main__":
    #print(len(list(range(0,116))))

    import programs.schema.schemas.Schema_DHCP_HHGQ as Schemap
    schemap = Schemap.buildSchema()

    #print(schemap.getQuerySeed("prefix_agecats"))
    #print(schemap.getQuerySeed("range_agecats"))
    #print(schemap.getQuerySeed("binarysplit_agecats"))

    import programs.schema.table_building.building_DHCP_tables as tbp
    tbp = tbp.getTableBuilder()

    datap = np.ones(schemap.shape) #Generate fake data to play with

    print(tbp.getTable('prefixquery', data=datap).to_string())
    #print(tbp.getTable('rangequery', data=datap).tail(5).to_string())
    #print(tbp.getTable('binarysplitquery', data=datap).to_string())

'''

''' Create tables with data of 1s
if __name__=="__main__":

    #import programs.schema.schemas.Schema_PL94_CVAP as Schema
    import programs.schema.schemas.Schema_DHCP_HHGQ as Schemap
    import programs.schema.schemas.Schema_Household2010 as Schemah
    #importlib.reload(Schema)
    schemap = Schemap.buildSchema()
    schemah = Schemah.buildSchema()

    #import programs.schema.table_building.building_PL94_CVAP_tables as tb
    import programs.schema.table_building.building_DHCP_tables as tbp
    import programs.schema.table_building.building_DHCH_tables as tbh

    tbp = tbp.getTableBuilder()
    tbh = tbh.getTableBuilder()

    #Generate fake data
    datap = np.ones(schemap.shape) #Generate fake data to play with
    datah = np.ones(schemah.shape) #Generate fake data to play with

    #print(data)
    #print(schema.getQuery("gqlevels"))
    #print(tb.getCustomTable(['gqTotal', 'institutionalized'],data=data).to_string())


    table_data = []
    for table in tbp.tablenames:
        print(table)
        df=tbp.getTable(table,data=datap)
        df['table']=table
        table_data.append(df)

    table_data=pd.concat(table_data)
    table_data.to_csv('/mnt/users/user007/dhcp_tables.csv')

    table_data = []
    for table in tbh.tablenames:
        print(table)
        df=tbh.getTable(table,data=datah)
        df['table']=table
        table_data.append(df)

    table_data=pd.concat(table_data)
    table_data.to_csv('/mnt/users/user007/dhch_tables.csv')
'''




    #print(tb.getTable("P3",data=data).to_string())

"""

tables=tb.getTableDict()
tables.keys()



schema.getQuery('blackAny').answer(data)


name = "asianAny"

# Find the list of cenrace categories that include the function
racetxt = name.replace('Any', '')
labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

idx = [racetxt in label for label in labels]
idx = [idx.index(i) for i in idx if i == True]
label = labels[idx]
cats = groups[idx]

groupings = {CENRACE: {label: cats}}

tb.getWorkloadByTable("P6")
#tb.getCustomTable(['numraces', 'citizen', 'numraces * institutionalized'])
tb.getTable("P6")
print(tb.getTable("P6").to_string())


import programs.schema.table_building.tablebuilder as tablebuilder
import programs.cenrace as cenrace

### WORKING WITH SCHEMAS ####

schema.getQuery("numraces").answer(data) # Answers the query. Counts number of races
#schema.getQuerySeed("numraces") # Shows groupings of number of races. Shows how the measure was created
#Schema.getQuerySeed("institutionalized")



#sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import programs.schema.schemas.Schema_DHCP_HHGQ as Schema
import das_utils as du
import numpy as np


### WORKING WITH SCHEMAS ####

schema
#Schema: PL94
#Dimnames: ['hhgq', 'votingage', 'hispanic', 'cenrace']
#Shape: (8, 2, 2, 63)
#Recodes: ['household', 'gqTotal', 'gqlevels', 'institutionalized', 'numraces', 'voting', 'nonvoting', 'nurse']
#Queries: ['hhgq', 'votingage', 'hispanic', 'cenrace', 'household', 'gqTotal', 'gqlevels', 'institutionalized', 'numraces', 'voting', 'nonvoting', 'nurse', 'detailed', 'total']

data = np.ones(schema.shape) #Generate fake data to play with
print(data)

print(schema)
print(du.pretty(schema.base_queries))

schema.getQuery("numraces").answer(data) # Answers the query. Counts number of races
schema.getQuerySeed("numraces") # Shows groupings of number of races. Shows how the measure was created

schema.getQuerySeed("institutionalized")
schema.getQuery("institutionalized") # This showst the query

#------------- query looks like -----------------
#name: institutionalized
#array_dims: (8, 2, 2, 63)
#groupings: {0: [[1, 2, 3, 4], [5, 6, 7]]}
#add_over_margins: (1, 2, 3)
#-----------------------------------------------------

schema.getQuery("institutionalized").answer(data) # This answers the query
schema.getQueryLevel("institutionalized") # Convenient way to view the levels of variable

#Working with Tables
import programs.schema.table_building.building_PL94_CVAP_tables as tb
tb = tb.getTableBuilder()
tb.getWorkloadByTable("P1_CVAP")
tb.getCustomTable(['numraces', 'citizen', 'numraces * institutionalized'])
tb.getTable("P4")
print(tb.getTable("P4").to_string())
"""


"""
import sys
import os

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

os.environ["PYTHONPATH"] = "/mnt/users/user007/das_decennial"

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import das_utils

#export PYTHONPATH=$PYTHONPATH:/mnt/users/user007/das_decennial/

# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python', 'lib', 'py4j-src.zip'))
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import analysis.sdftools as sdftools
import analysis.treetools as treetools
import analysis.aggtools as aggtools

import programs.datadict as datadict
import das_utils as du

import pandas
import numpy
import json


spark = SparkSession.builder.appName('Testing spark').getOrCreate()

das_utils.ship_files2spark(spark,tempdir='/mnt/users/user007/')

myCollection = [{'raw': 'first row', 'syn': 'kljwe', 'geocode': "0398023"} ,
                {'raw': "second row with more lines", 'syn': 'kljwe', 'geocode': "0393233"}]
rdd = spark.sparkContext.parallelize(myCollection, 2)
df = rdd.toDF()

define

newwords=words.filter(lambda word: startsWithS(word))
y=newwords.flatMap(lambda newwords: list(newwords)).collect()



#path="${DAS_S3ROOT}/experiments/convertedFromGeounitNodeToDict/PL94_SF1_c1state3/td001_1/run_0/data/"
#rdd = spark.sparkContext.pickleFile(path)
#raw = rdd.map(lambda x: rdd[0]).take(1)
#raw2 = rdd['raw']
#print(rdd)
#print(raw)
#print(raw2)
#[{'raw': <programs.sparse.multiSparse object at 0x7f72cff229e8>, 'syn': <programs.sparse.multiSparse object at 0x7f72cff22668>, 'geocode': '4400701450133026'}]


#print(type(rdd))
#print(type(rdd))


### Need to change the schema_name to match the schema of the data ###
#schema_name = "PL94_P12"
schema = SchemaMaker.fromName(name=schema_name)
print(schema)


# this is where the pandas data frame of accuracy results will be saved (as a csv file)
accuracy_saveloc = f"/mnt/users/user007/testing.csv"

# define the queries to look at / answer
querynames = ["detailed"]

# and the geolevels to analyze
geolevels = ["State", "County", "Tract_Group", "Tract", "Block_Group", "Block"]


experiment="${DAS_S3ROOT}/experiments/convertedFromGeounitNodeToDict/PL94_SF1_c1state/td10_1/"



runtree = treetools.RunTree(experiment)
runs = runtree.runs
print(type(runs))
runs=runs[0:2]
print(runs)

algset = runs[0].algset # Pulls the algset, e.g., td10_1


spark = SparkSession.builder.appName('Testing spark').getOrCreate()

das_utils.ship_files2spark(spark,subdirs=('programs','das_framework','analysis'),tempdir='/mnt/users/user007/')

# build the Spark DataFrame (note that a row only exist if that cell's orig count > 0 OR priv count > 0)
df = sdftools.getExperimentSparseDF(spark, runs, schema).persist()

column_order = ['geocode', 'algset', 'run_id', 'plb', AC.ORIG, AC.PRTCTD] + schema.dimnames
df = df.select(column_order)
print("Print the DF")
df.show()
df.printSchema()

crosswalkdict = {geolevel: aggtools.getGeodict()[geolevel] for geolevel in geolevels}
print(crosswalkdict)

#Runs is a list of dictionaries, with a dictionary for each run. The PLB is incorrect in the list

def main(experiments, schema_name, accuracy_saveloc, querynames, geolevels):
    # each experiment is a single algset path (algsets are equivalent to the config's "budget_groups")
    accuracy_tuples = []
    for e, experiment in enumerate(experiments):


        # this accommodates for the issues with td025_1... shouldn't bother "normal" algsets, like td2
        plb = du.algset2plb(du.findallSciNotationNumbers(algset)[0])



        # reorder to the df so it's easier to look at manually
        column_order = ['geocode', 'algset', 'run_id', 'plb', AC.ORIG, AC.PRTCTD] + schema.dimnames
        df = df.select(column_order)
        print("Print the DF")
        df.show()
        df.printSchema()

        crosswalkdict = {geolevel: aggtools.getGeodict()[geolevel] for geolevel in geolevels}
        print(crosswalkdict)

        for geolevel, mapping in crosswalkdict.items():
            geodf = sdftools.getGeolevelSumDF(spark, df, geolevel, mapping, replace_geocode=False).persist()
            print("Print the GeoDF")
            geodf.show()
            geodf.printSchema()
            for queryname in querynames:
                # answers the query using Spark DataFrame
                querydf = sdftools.getQueryDF(geodf, queryname, schema, basegroup=["run_id", geolevel, "plb"]).persist()
                print("Print the QueryDF")
                querydf.show()
                querydf.printSchema()
                ### Start of 1-TVD analysis

                # Calculate L1 for each cell in the query
                querydf = querydf.withColumn("L1", F.abs(querydf.priv - querydf.orig)).persist()

                # Sum over the L1, as well as orig, and priv counts, grouping by geocode and trial (aka run_id)
                querydf = querydf.select(geolevel, "orig", "priv", "L1", "run_id").groupBy(geolevel, "run_id").sum().persist()

                # Calculate the 1-TVD statistic for each geounit and trial
                querydf = querydf.withColumn("1-TVD", 1 - (F.col("sum(L1)") / (2 * F.col("sum(orig)")))).persist()

                # Find the Average 1-TVD across geounits, grouped by trial and convert to Pandas DataFrame
                accuracy = querydf.groupBy("run_id").agg(F.avg("1-TVD")).toPandas()

                ### End of TVD

                # Printing the pandas dataframe to the output file / log file
                print(f"---Pandas start | {geolevel} | {algset}---")
                print(accuracy.to_string())
                print(f"---Pandas end | {geolevel} | {algset}---")

                # Print the accuracy tuple (including experiment "metadata") to output / log file
                # also keep the tuples to be aggregated into a single pandas dataframe after all experiments have been analyzed
                for x in accuracy.values:
                    accuracy_tuple = (algset, x[0], plb, geolevel, schema.name, queryname, x[1])
                    print(f"---Accuracy Tuple--- | {accuracy_tuple}")
                    accuracy_tuples.append(accuracy_tuple)

    accuracy_df = pandas.DataFrame(accuracy_tuples, columns=['algset', 'run_id', 'plb', 'geolevel', 'schema', 'query', '1-tvd'])
    accuracy_df.to_csv(accuracy_saveloc, index=False)


"""
