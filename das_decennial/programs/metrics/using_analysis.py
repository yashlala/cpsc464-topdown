from das_framework.driver import AbstractDASErrorMetrics
# include these modules to use analysis
from analysis.tools import datatools, sdftools
import analysis.constants as AC
from pyspark.sql import SparkSession
from pyspark import SparkContext


class AccuracyMetrics(AbstractDASErrorMetrics):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def run(self, engine_tuple) -> None:
        block_nodes = engine_tuple

        # access the SparkSession (needed for aggregating geolevels)
        spark = SparkSession(SparkContext.getOrCreate())

        # transform the rdd of block-level nodes into a 'sparse histogram' spark df
        df = datatools.rdd2df(block_nodes, self.setup.schema_obj)
        sdftools.show(df, "The Block-level Geounit Nodes as Sparse Histogram DF", 1000)

        # read the geolevels from the error_metrics section of the config file
        geolevels = self.setup.config['error_metrics']['geolevels'].split(", ")
        #geolevels = self.setup.levels

        # aggregate blocks to get the different geolevels
        df = sdftools.aggregateGeolevels(spark, df, geolevels)
        sdftools.show(df, f"DF with all Geolevels in {geolevels}", 1000)

        # access the queries from the error_metrics section of the config file
        queries = self.setup.config['error_metrics']['queries'].split(", ")
        # and answer the queries
        df = sdftools.answerQueries(df, self.setup.schema_obj, queries)
        sdftools.show(df, f"DF with all Queries in {queries}", 1000)

        # compute the Geolevel 1-TVD metric
        geolevel_tvd = sdftools.getGeolevelTVD(df, groupby=[AC.GEOLEVEL, AC.QUERY])
        geolevel_tvd = geolevel_tvd.orderBy([AC.QUERY, AC.GEOLEVEL])
        sdftools.show(geolevel_tvd, f"Geolevel 1-TVD per geolevel per query", 1000)

        # calculate sparsity change
        sparsity_df = sdftools.getCellSparsityByGroup(df, self.setup.schema_obj, groupby=[AC.GEOCODE, AC.GEOLEVEL, AC.QUERY])
        sdftools.show(sparsity_df, f"Query and Geolevel DF with Sparsity per group", 1000)

        # geolevel_sparsity = sdftools.getCellSparsityAcrossGeolevelSuperHistogram(sparsity_df, groupby=[AC.GEOLEVEL, AC.QUERY])
        # sdftools.show(geolevel_sparsity, f"Cell Sparsity Across Geolevel Super Histogram", 1000)
