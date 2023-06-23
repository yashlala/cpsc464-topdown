######################################################
# To Run this script:
#
# cd into das_decennial/analysis/
# analysis=[path to analysis script] bash run_analysis.sh
#
# More info on analysis can be found here:
# https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/analysis/readme.md
######################################################

import das_utils as du
import pandas
import numpy as np
import os
import analysis.tools.datatools as datatools
from analysis.tools.initializetools import initializeAnalysis
import analysis.tools.sdftools as sdftools
import analysis.constants as AC
from pyspark.sql import functions as sf
from pyspark.sql import Row
from programs.schema.schema import Schema
import analysis.vignettes.toytools as toytools


if __name__ == "__main__":
    """
    It's entirely possible to take advantage of Analysis's Spark DataFrame-based functionality within the DAS.
    The error_metrics module is the spot that makes the most sense for accessing Analysis.

    The steps to prepare the data for use is:
    1. Use analysis.tools.datatools.rdd2df to transform the RDD of GeounitNode objects into the histogram form
       required by Analysis
    2. Use the tools to analyze the data
        - analysis.tools.crosswalk is used to access and join the GRFC to the data for geolevel aggregation support
        - analysis.tools.sdftools is the location of most of the Spark DataFrame operations for answering queries
          and calculating a wide variety of metrics
    """
    # Note that this script is not running the DAS; it's only used to demostrate that it's possible to
    # take an RDD of GeounitNodes (as is typically found in the DAS) and use Analysis to calculate metrics

    # since we're not in the DAS in this script, we will use setuptools to extract the SparkSession object
    ################################################################
    # Set the save_location to your own JBID (and other folder(s))
    # it will automatically find your JBID
    # if something different is desired, just pass what is needed
    # into the setuptools.setup function.
    ################################################################
    jbid = os.environ.get('JBID', 'temp_jbid')
    save_folder = "analysis_results/"

    save_location = du.addslash(f"{jbid}/{save_folder}")

    spark_loglevel = "ERROR"
    analysis = initializeAnalysis(save_location=save_location, spark_loglevel=spark_loglevel)

    # save the analysis script?
    # toggle to_linux=True|False to save|not save this analysis script locally
    # toggle to_s3=True|False to save|not save this analysis script to s3
    analysis.save_analysis_script(to_linux=False, to_s3=False)

    # save/copy the log file?
    analysis.save_log(to_linux=False, to_s3=False)

    # zip the local results to s3?
    analysis.zip_results_to_s3(flag=False)

    spark = analysis.spark




    # build an example schema
    schema = Schema("example", ['a', 'b', 'c'], (2,3,5))
    sdftools.print_item(schema, "Toy example Schema")

    # build a set of GeounitNodes to use
    geocodes = ['000', '001', '002', '003', '010', '011', '012', '020', '022']
    geocode_dict = {3: 'block', 2: 'county'}

    # build geounits
    geounits = toytools.getToyGeounitData_GeounitNode(schema, geocodes, geocode_dict, raw_params={'low': 0, 'high': 100})

    rdd = spark.sparkContext.parallelize(geounits).persist()

    sdftools.print_item(rdd.take(1), "One of the toy example geounits")

    # use Analysis to transform the rdd of geounitnodes into a spark dataframe
    df = datatools.rdd2df(rdd, schema)
    sdftools.print_item(df, "Toy example DF", 300)


    # aggregate geolevels
    df = df.withColumn("block", sf.col(AC.GEOCODE)[0:3]).persist()
    df = df.withColumn("county", sf.col(AC.GEOCODE)[0:2]).persist()
    df = df.withColumn("nation", sf.col(AC.GEOCODE)[0:1]).persist()
    sdftools.show(df, "df with geolevel crosswalk columns")
    df = sdftools.aggregateGeolevels(spark, df, ['block', 'county', 'nation'])
    sdftools.show(df, "df after geolevel aggregation", 1000)

    # answer total query
    qdf = sdftools.answerQuery(df, schema, "total", labels=False, merge_dims=False)
    sdftools.show(qdf, "Query df with the query 'total'", 1000)

    # select geounits by quantile bins
    rdd = sdftools.getRowGroupsAsRDD(qdf, groupby=[AC.GEOLEVEL, AC.QUERY])
    sdftools.show(rdd.collect(), "Row groups")


    def row_selection_mapper(rows, selection_function, **selection_kwargs):
        pandas_df = pandas.DataFrame(rows)
        pandas_df = selection_function(pandas_df, **selection_kwargs)
        rows = pandas_df.to_dict('records')
        return rows



    def qbin(data_column, bins):
        q = np.quantile(data_column, bins)
        qlist = list(zip(bins, q))
        df = pandas.DataFrame(qlist)
        qmin = df.groupby(1).min().values.flatten().tolist()
        qmax = df.groupby(1).max().values.flatten().tolist()
        qlab = list(zip(qmin, qmax))
        qlabels = [x[0] for x in qlab]
        qlabels += [qlab[-1][1]]
        qlabels = np.unique(qlabels).tolist()
        labels = [f"({qlabels[i]}, {b}]" for i,b in enumerate(qlabels[1:])]
        return qlist, qlab, qlabels, labels


    def create_pandas_bins(pandas_df, columns, bins, btype="quantile"):
        quantile_dict = {}
        columns = du.aslist(columns)
        quantile_bins = du.aslist(bins)
        for column in columns:
            if btype == "quantile":
                labels = [f"'({bins[i]}, {bins[i+1]}]'" for i,b in enumerate(bins[1:])]
                pandas_df[f'{column}_bins'], retbins = pandas.qcut(pandas_df[column], bins, duplicates='drop', retbins=True)
                print(f"Bins: {retbins}")
            else:
                pandas_df[f'{column}_bins'] = pandas.cut(x=pandas_df[column], bins=quantiles)
        return pandas_df


    def make_pandas_qcut_bin_labels(bins):
        bins = du.aslist(bins)
        binstr = [f"'(-Inf, {bins[0]}]'"]
        if len(bins) >= 2:
            for i,b in enumerate(bins[1:]):
                binstr += [f"'({bins[i]}, {bins[i+1]})'"]
        return binstr


    def row_selection_mapper_exclude_top_and_bottom_percent(rows, column, quantile_threshold):
        pandas_df = pandas.DataFrame(rows)
        q = quantile_threshold
        p = 1 - q
        if q <= p:
            bottom_quantile = q
            top_quantile = p
        else:
            bottom_quantile = p
            top_quantile = q

        bottom, top = np.quantile(pandas_df[column], [bottom_quantile, top_quantile])
        pandas_df['keep'] = (pandas_df[column] >= bottom) & (pandas_df[column] <= top)
        #pandas_df = pandas_df[(pandas_df[column] >= bottom) & (pandas_df[column] <= top)]
        rows = pandas_df.to_dict('records')
        return rows


    rdd = rdd.flatMapValues(lambda rows: row_selection_mapper_exclude_top_and_bottom_percent(rows, AC.PRTCTD, 0.05)).persist()
    sdftools.show(rdd.collect(), "Row group dfs")

    #rdd = rdd.map(lambda key_row: { k: v for (k,v) in list(zip([AC.GEOLEVEL, AC.QUERY, key_row[0]])) + list(key_row[1].items()) })
    rdd = rdd.map(lambda row: Row(**row[1]))
    df = rdd.toDF().persist()
    sdftools.show(df, "Filtered DF", 1000)
