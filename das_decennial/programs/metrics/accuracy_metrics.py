from operator import add
from typing import List, Dict, TextIO, Union, Tuple
import warnings

import numpy as np
import os
# import traceback
import datetime
from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
from programs.queries.querybase import AbstractLinearQuery
from programs.sparse import multiSparse
from programs.nodes.nodes import GeounitNode
from das_framework.driver import AbstractDASErrorMetrics
from analysis.tools import sdftools
from analysis.tools import datatools
import analysis.constants as AC
from das_constants import CC
from das_framework.ctools.paths import substvars, mkpath
from programs.geographic_spines.define_spines import make_grfc_ids

EMPTY_TUPLE = ()

def infNoneFloatToStr(d):
    if d is None:
        return "None"
    if np.isinf(d):
        return str(d)
    if np.isnan(d):
        raise ValueError("Bin limit is np.NaN")
    if abs(d - int(d)) > 1e-7:
        warnings.warn("Float converted to int with difference > 1e-7", UserWarning)
    return str(int(d))


def binEndsToStr(bin_ends: tuple):
    return f"{infNoneFloatToStr(bin_ends[0])} to {infNoneFloatToStr(bin_ends[1])}"


def getTreeDepthFromNumPart(rdd):
    # Number of partitions
    x = rdd.getNumPartitions()

    # Get number of levels, one level per power of two, no reason to split 16 = 2^4, hence start from -4
    depth2 = -4
    while x:
        x >>= 1
        depth2 += 1
    return max(1, depth2)


class AccuracyMetrics(AbstractDASErrorMetrics):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.levels_reversed = None
        self.geolevels = None

        self.geolevel_node_counts = {}
        self.geolevel_node_counts_zero_adjusted = {}

        return_all_levels = self.getboolean(CC.RETURN_ALL_LEVELS, default=False)

        self.analysis_df_dict = None

        # Check if the geocodeDict in the nodes corresponds to the AIAN spine at county and above. This requires that the spine type is
        # not non-AIAN spine, and either all levels were returned or short-circuiting was used to stop topdown before block but after county:
        # TODO: The following if condition and the elif condition do assume the county geolevel was included on the spine. Is this always the case?
        if return_all_levels:
            aian_or_opt_spine_at_county = self.setup.spine_type != CC.NON_AIAN_SPINE
        elif (self.setup.geo_bottomlevel is not None) and (self.setup.geo_bottomlevel != '') and (self.setup.geo_bottomlevel != self.setup.levels[0]):
            # short circuiting was used. Make sure the geolevel that was stopped at is before block and is County or after:
            at_least_county_and_before_block = self.setup.geo_bottomlevel in (CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_BLOCK_GROUP)
            aian_or_opt_spine_at_county = (self.setup.spine_type != CC.NON_AIAN_SPINE) and at_least_county_and_before_block
        else:
            aian_or_opt_spine_at_county = False

        self.print_county_total_and_votingage = self.getboolean(CC.PRINT_COUNTY_TOTAL_AND_VOTINGAGE, section=CC.ERROR_METRICS, default=False)
        msg = f"The option {CC.PRINT_COUNTY_TOTAL_AND_VOTINGAGE} requires that county geounit IDs are in the format of either the AIAN or the optimized spine."
        assert (self.print_county_total_and_votingage and aian_or_opt_spine_at_county) or (not self.print_county_total_and_votingage), msg

        self.print_blau_quintile_errors = self.getboolean(CC.PRINT_BLAU_QUINTILE_ERRORS, section=CC.ERROR_METRICS, default=False)
        self.print_8_cell_cenrace_hisp_errors = self.getboolean(CC.PRINT_8_CELL_CENRACE_HISP_ERRORS, section=CC.ERROR_METRICS, default=False)

        # Check to see if spine was an AIAN spine or opt-spine and topdown was short circuited at the state geolevel
        aian_or_opt_spine_state_sc = (self.setup.geo_bottomlevel == CC.GEOLEVEL_STATE) and (self.setup.spine_type != CC.NON_AIAN_SPINE)
        self.print_aian_state_total_L1_errors = self.getboolean(CC.PRINT_AIAN_STATE_TOTAL_L1_ERRORS, section=CC.ERROR_METRICS, default=False)
        msg = f"The option {CC.PRINT_AIAN_STATE_TOTAL_L1_ERRORS} requires that top-down was short-circuited at the state geolevel and that an AIAN or optimized spine is used."
        assert (self.print_aian_state_total_L1_errors and aian_or_opt_spine_state_sc) or (not self.print_aian_state_total_L1_errors), msg

        # TODO: Add an assert for the schema being H1
        self.print_H1_county_metrics = self.getboolean(CC.PRINT_H1_COUNTY_METRICS, section=CC.ERROR_METRICS, default=False)
        self.print_aians_l1_total_pop = self.getboolean(CC.PRINT_AIANS_L1_ERROR_ON_TOTAL_POP, section=CC.ERROR_METRICS, default=False)
        self.print_place_mcd_ose_bg_l1_total_pop = self.getboolean(CC.PRINT_PLACE_MCD_OSE_BG_L1_ERROR_ON_TOTAL_POP, section=CC.ERROR_METRICS, default=False)
        self.print_block_and_county_total_pop_errors = self.getboolean(CC.PRINT_BLOCK_AND_COUNTY_TOTAL_POP_ERRORS, section=CC.ERROR_METRICS, default=True)
        aian_or_opt_spine_county_sc = (self.setup.geo_bottomlevel == CC.GEOLEVEL_COUNTY) and (self.setup.spine_type != CC.NON_AIAN_SPINE)
        msg = f"The option {CC.PRINT_H1_COUNTY_METRICS} requires that top-down was short-circuited at the county geolevel and that an AIAN or optimized spine is used."
        assert (self.print_H1_county_metrics and aian_or_opt_spine_county_sc) or (not self.print_H1_county_metrics), msg

        self.quantile_error_file_started = False
        self.query_l1_file_started = False
        # self.total_pop_errors_and_quantiles_file_started = False  # Not needed, because we're fine to add the header row again
        self.l1_total_error_by_bin_file_started = False
        self.l1_relative_query_file_started = False
        logfilename = os.getenv('LOGFILE_NAME')
        if logfilename is None:
            # If LOGFILE_NAME isn't defined, default to the previous csv format, which uses date and timestamp
            dtnow = datetime.datetime.now()
            date = str(dtnow.date())
            timestamp = int(dtnow.timestamp())
            self.quantile_error_file_name = f"query_err_quantiles_{date}_result-{timestamp}.csv"
            self.query_l1_file_name = f"query_l1_{date}_result-{timestamp}.csv"
            self.total_pop_errors_and_quantiles_file = f"total_pop_errors_and_quantiles_{date}_result-{timestamp}.csv"
            self.l1_total_error_by_bin_file = f"l1_total_error_by_bin_{date}_result-{timestamp}.csv"
            self.relative_total_pop_file = f"relative_total_pop_{date}_result-{timestamp}.csv"
            self.l1_relative_query_file = f"l1_relative_query_metrics_{date}_result-{timestamp}.csv"
        else:
            self.quantile_error_file_name = logfilename.replace(".log", "_query_err_quantiles.csv")
            self.query_l1_file_name = logfilename.replace(".log", "_query_l1.csv")
            self.total_pop_errors_and_quantiles_file = logfilename.replace(".log", "_total_pop_errors_and_quantiles.csv")
            self.l1_total_error_by_bin_file = logfilename.replace(".log", "_l1_total_error_by_bin.csv")
            self.relative_total_pop_file = logfilename.replace(".log", "_relative_total_pop.csv")
            self.l1_relative_query_file = logfilename.replace(".log", "_l1_relative_query_metrics.csv")
        self.quantile_errors = self.getboolean("calculate_per_query_quantile_errors", default=True)
        self.quantile_signed_errors = self.getboolean("calculate_per_query_quantile_signed_errors", default=True)

    def run(self, engine_tuple):
        """ Perform all accuracy metrics calculations"""
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")
        all_levels_reversed = tuple(self.das.reader.modified_geocode_dict[k] for k in sorted(list(self.das.reader.modified_geocode_dict.keys())))

        self.levels_reversed = []
        for level in all_levels_reversed:
            self.levels_reversed.append(level)
            if level == self.setup.geo_bottomlevel:
                break
        self.geolevels = tuple(reversed(self.levels_reversed))

        print(f'self.all_levels_reversed: {all_levels_reversed}')
        print(f'self.geolevels: {self.geolevels}')
        print(f'self.levels_reversed: {self.levels_reversed}')

        skip_levels = self.gettuple("skip_levels", default=())
        levels2calc = tuple(filter(lambda gl: gl not in skip_levels, self.levels_reversed))

        block_df = self.makeBlockDF(engine_tuple)

        if self.print_aian_state_total_L1_errors:
            self.printAIANStateTotalL1Errors(engine_tuple)

        if self.print_H1_county_metrics:
            self.printH1CountyMetrics(engine_tuple)

        self.annotate("TOTALS FROM ANALYSIS BINNED BY POP")
        if self.print_block_and_county_total_pop_errors:
            if "Block" not in skip_levels:
                self.compute_total_pop_errors_and_quantiles(block_df, "Block", False, [0, 9, 99, 999])
            self.compute_total_pop_errors_and_quantiles(block_df, "County", True, [1000, 9999, 99999, 999999])
            self.compute_total_pop_errors_and_quantiles(block_df, CC.GEOLEVEL_PRIM, False, [100, 999, 9999, 99999, 999999])

        if self.print_aians_l1_total_pop:
            population_bin_starts = np.array([0, 100, 1000, 10000])
            self.annotate("TOTALS FROM ANALYSIS FOR FED_AIRS")
            self.calculate_L1_total_error_by_bin(block_df, "FED_AIRS", population_bin_starts)
            self.annotate("TOTALS FROM ANALYSIS FOR AIAN")
            self.calculate_L1_total_error_by_bin(block_df, "AIAN_AREAS", population_bin_starts)

        if self.print_place_mcd_ose_bg_l1_total_pop:
            population_bin_starts = np.arange(51, dtype=int) * 50
            for entity in ["MCD", "OSE", "Place", "Block_Group", CC.GEOLEVEL_PRIM]:
                self.annotate(f"TOTALS FROM ANALYSIS FOR {entity}")
                self.calculate_L1_total_error_by_bin(block_df, entity, population_bin_starts)

        print("CALCULATING REL_TP_POP")
        rel_tp_error = self.calculate_relative_total_pop_error(block_df)

        l1_relative = [self.calculate_l1_relative_errors(block_df, population_cutoff=0, use_bins=True)]
        population_cutoff = self.getint(CC.POPULATION_CUTOFF, section=CC.ERROR_METRICS, default=0)
        if population_cutoff > 0:
            l1_relative_pc_config = self.calculate_l1_relative_errors(block_df, population_cutoff=population_cutoff, use_bins=False)
            l1_relative = l1_relative + [l1_relative_pc_config]

        if self.analysis_df_dict is not None:
            for df in self.analysis_df_dict.values():
                df.unpersist()
            self.analysis_df_dict = None

        block_df.unpersist()

        if self.getboolean(CC.DELETERAW, section=CC.ENGINE, default=True):
            self.log_and_print("[{}] {} is true, so accuracyMetrics will not be computed.".format(CC.ENGINE, CC.DELETERAW))
            return

        # In case some custom aggregation levels needed
        geolevels = self.geolevels

        if not isinstance(engine_tuple, dict):
            block_nodes = engine_tuple
            #### REAGGREGATE block_nodes
            self.annotate("Reaggregating optimized block data")
            nodes_dict = self.aggregateNodes(geolevels, block_nodes)
            if self.getboolean(CC.COMPUTE_PRIM_ERROR_METRICS, section=CC.ERROR_METRICS, default=True):
                levels2calc = levels2calc + ("Prim",)
                self.geolevels = ("Prim",) + self.geolevels
                nodes_dict["Prim"] = self.makePrimGeolevel(block_nodes)
        else:
            self.log_and_print("Optimizer returned all levels, skipping reaggregation")
            nodes_dict = engine_tuple


        # # For aian-spine with return_all_levels: also get the errors for Full States (i.e. AIAN+non-AIAN parts)
        # self.log_and_print("Aggregating non-AIAN and AIAN areas (denoted as 'States') into FullStates")
        # nodes_dict['FullState'] = nodes_dict['State']\
        #             .map(lambda aian_non_node: (aian_non_node.geocode[1:], aian_non_node)) \
        #             .reduceByKey(lambda x, y: x.addInReduce(y, inv_con=False)) \
        #             .map(lambda geocode_node: geocode_node[1]) \
        #             .persist()

        toplevel = levels2calc[0]
        self.annotate(f"Finding total population by summing the {toplevel} level")
        total_population = int(nodes_dict[toplevel].map(GeounitNode.fromZipped).map(lambda node: node.raw.sum()).reduce(add))
        # self.annotate(f"Total population: {total_population}")
        # print(f"Total households raw: {int(nodes_dict[toplevel].collect()[0].getDenseRaw().sum())}")
        # print(f"Total households protected: {int(nodes_dict[toplevel].collect()[0].getDenseSyn().sum())}")

        # Define set of queries for which to calculate and print errors
        # Start with the DPQueries measured

        MAIN = "main"  # Query is for the main histogram using main schema_obj, .raw and .syn
        UNIT = "unit"  # Query is for the unit histogram using unit_schema_obj, .raw_housing and .unit_syn

        qdict = {}
        for qname, q in self.das.engine.getQueriesDict().items():
            try:
                qdict[qname] = (self.setup.schema_obj.getQuery(qname), MAIN)
            except (ValueError, AssertionError):
                qdict[qname] = (self.setup.unit_schema_obj.getQuery(qname), UNIT)

        # Add marginals on each dimension of the schema
        qdict.update({qname: (self.setup.schema_obj.getQuery(qname), MAIN) for qname in self.setup.schema_obj.dimnames})
        # Could add for unit hist too
        # Add specifically asked for queries
        qdict.update({qname: (self.setup.schema_obj.getQuery(qname), MAIN) for qname in self.gettuple(CC.QUERIES2MEASURE, sep=",", default=[])})
        # Could add for unit hist too

        error_geoleveldict = {}
        # for geolevel, rdd in nodes_dict.items():
        for geolevel in levels2calc:
            rdd = nodes_dict[geolevel].persist()
            self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")
            # rdd.repartition(min(5000, rdd.count()))
            self.geolevel_node_counts[geolevel] = rdd.count()
            self.annotate(f"Calculating {geolevel} L1 error")
            raw_syn_rdd = rdd.map(GeounitNode.fromZipped).map(lambda node: (node.raw, node.syn)).persist()
            depth = getTreeDepthFromNumPart(raw_syn_rdd)
            total_L1_error = int(raw_syn_rdd.map(lambda rs: self.L1Sum(rs[0], rs[1])).treeReduce(add, depth=depth))
            max_l1_err_rdd = raw_syn_rdd.map(lambda rs: self.L1Max(rs[0], rs[1]))
            max_L1_error = int(max_l1_err_rdd.treeReduce(add, depth=depth))
            max_of_max_L1_error = int(max_l1_err_rdd.treeReduce(max, depth=depth))
            self.log_and_print(f"{geolevel} max of max L1 errors: {max_of_max_L1_error}")
            num_zero_matches = int(raw_syn_rdd.map(lambda rs: 1 if int(rs[0].max()) == 0 and int(rs[1].max()) == 0.0 else 0).treeReduce(add, depth=depth))
            num_raw_zeros = int(raw_syn_rdd.map(lambda rs: 1 if int(rs[0].max()) == 0 else 0).treeReduce(add, depth=depth))
            num_syn_zeros = int(raw_syn_rdd.map(lambda rs: 1 if int(rs[1].max()) == 0 else 0).treeReduce(add, depth=depth))
            self.annotate(f"In {geolevel}, # geounits: {self.geolevel_node_counts[geolevel]}, # 0-pop raw geounits: {num_raw_zeros}, "
                            + f"# 0-pop syn geounits: {num_syn_zeros}, # 0-pop raw & syn geounits: {num_zero_matches}")

            self.geolevel_node_counts_zero_adjusted[geolevel] = self.geolevel_node_counts[geolevel] - num_zero_matches

            # self.annotate(f"Calculating {geolevel} sparsity error")
            # sparsity = float(rdd.map(lambda node: self.sparsityChange(node.raw, node.syn)).reduce(add))

            self.annotate(f"Calculating {geolevel} total change")

            def sign(x):
                return 0 if x == 0 else 2 * int(x > 0) - 1

            totals = (
                raw_syn_rdd
                    .map(lambda rs: rs[1].sum() - rs[0].sum())
                    .map(lambda vc: np.array((vc, abs(vc), sign(vc)), dtype=int))
                    .treeReduce(add, depth=depth)
            )

            def answerAndElimZeros(q: AbstractLinearQuery, x):
                ans = q.answerSparse(x.transpose())
                ans.eliminate_zeros()
                return ans

            queries = {}
            queries_binned_py_pop = {}
            queries_L1_quantiles = {}
            # pop_diff_rdd = raw_syn_rdd.map(lambda rs: (self.popLowBound(rs[0]), (rs[0] - rs[1]).toDense())).persist()
            # pop_diff_rdd_unit = rdd.map(GeounitNode.fromZipped).map(lambda node: (self.popLowBound(node.raw), (node.raw_housing - node.unit_syn).toDense() if (node.raw_housing is not None and node.unit_syn is not None) else None)).persist()
            pop_diff_rdd = raw_syn_rdd.map(lambda rs: (self.popLowBound(rs[0]), (rs[1] - rs[0]).sparse_array)).persist()
            pop_diff_rdd_unit = rdd.map(GeounitNode.fromZipped).map( lambda node: (self.popLowBound(node.raw), (node.unit_syn - node.raw_housing).sparse_array if (node.raw_housing is not None and node.unit_syn is not None) else None)).persist()
            if self.print_county_total_and_votingage and geolevel == CC.GEOLEVEL_COUNTY:
                county_diff_with_geocode = rdd.map(lambda node: (node.geocode, (node.syn - node.raw).toDense())).persist()
            rdd.unpersist()

            if self.print_blau_quintile_errors:
                self.printByBlauQuintileErrors(geolevel, raw_syn_rdd)
            if self.print_8_cell_cenrace_hisp_errors:
                self.print8CellCenraceHispErrors(geolevel, raw_syn_rdd)

            raw_syn_rdd.unpersist()

            for qname, (q, sk) in qdict.items():
                n_bins = 20
                if sk == MAIN:
                    main_or_unit_rdd = pop_diff_rdd
                elif sk == UNIT:
                    main_or_unit_rdd = pop_diff_rdd_unit
                else:
                    raise ValueError(f"In accuracy metrics query dict the indicated schema is neither '{MAIN}' nor '{UNIT}'")
                # qL1rddsigned = main_or_unit_rdd.map(lambda d: (d[0], q.answer(d[1]))).persist()
                qL1rddsigned = main_or_unit_rdd.map(lambda d: (d[0], answerAndElimZeros(q, d[1]))).persist()

                if ((qname == "total") or (qname == "votingage")) and self.print_county_total_and_votingage and (geolevel == CC.GEOLEVEL_COUNTY):
                    if qname == "votingage":
                        county_query_diff = list(county_diff_with_geocode.map(lambda row: (row[0], q.answer(row[1])[1])).collect())
                    else:
                        county_query_diff = list(county_diff_with_geocode.map(lambda row: (row[0], q.answer(row[1]))).collect())
                    self.printCountyTotalAndVotingage(county_query_diff, qname)

                qL1rdd = qL1rddsigned.map(lambda d: (d[0], abs(d[1]))).persist()
                self.annotate(f"Calculating {geolevel} {qname} query errors")

                # Total L1 error of the query over the query cells, summed over geounits in geolevel
                # l1 = float(qL1rdd.map(lambda d: np.sum(d[1])).reduce(add))
                depth2 = getTreeDepthFromNumPart(qL1rdd)
                l1 = float(qL1rdd.map(lambda d: d[1].sum()).treeReduce(add, depth=depth2))
                queries[qname + "_L1"] = l1

                # Max L1 error of the query over the query cells, averaged over geounits in geolevel
                # l1max = float(qL1rdd.map(lambda d: np.max(d[1])).reduce(add))
                q_max_l1_rdd = qL1rdd.map(lambda d: d[1].max()).persist()
                l1max = float(q_max_l1_rdd.treeReduce(add, depth=depth2))
                queries[qname + "_max"] = l1max

                if not self.query_l1_file_started:
                    self.query_l1_file_started = True
                    with open(self.query_l1_file_name, "w") as f:
                        f.write(",".join(["Geolevel", "Query", "L1", "L1_mean_over_nodes", "L1max", "L1max_mean_over_nodes\n"]))
                with open(self.query_l1_file_name, "a") as f:
                    f.write(f"{geolevel},{qname},{l1},{l1 / self.geolevel_node_counts[geolevel]},{l1max},{l1max / self.geolevel_node_counts[geolevel]}\n")


                # # This commented out block is an alternative to the below approach to per-query quantile errors
                # import pyspark.sql.types as T
                # import pyspark.sql.functions as F
                # import pyspark.sql.Window as W
                #
                # def node2errors(node):
                #      errors = (node.syn - node.raw).sparse_array
                #      errors_dict = dict(Counter(np.abs(errors.data).tolist()))
                #      num_zero_errors = int(np.prod(node.raw.shape) - errors.count_nonzero())
                #      errors_dict[0] = num_zero_errors
                #      return T.Row(err_dict=errors_dict)
                #
                # df = rdd.map(node2errors).toDF()
                # df = df.select(F.explode(df.err_dict).alias("err"))
                # df_ordered = df.groupBy("err").agg({"num": "sum"}).orderBy("err")
                # windowval = (W.orderBy('err').rangeBetween(W.unboundedPreceding, 0))
                # total = df_ordered.agg({"sum(num)":"sum"}).rdd.collect()[0]['sum(sum(num))']
                # dfo_w_cumsum = df_ordered.withColumn('cum_sum', F.sum('sum(num)').over(windowval)/total)

                if (self.quantile_errors or self.quantile_signed_errors) and not self.quantile_error_file_started:
                    # Write out the CSV header
                    with open(self.quantile_error_file_name, "w") as f:
                        f.write("Geolevel,Query,ErrorType,Quantile,Error\n")
                    self.quantile_error_file_started = True

                if self.quantile_errors:
                    with open(self.quantile_error_file_name, "a") as f:
                        self.quantileErrors(qL1rdd, q, geolevel, lambda d: d[1], "absolute", f)

                if self.quantile_signed_errors:
                    with open(self.quantile_error_file_name, "a") as f:
                        self.quantileErrors(qL1rddsigned, q, geolevel, lambda d: d[1], "signed", f)

                qL1rddsigned.unpersist()

                if self.getboolean("calculate_binned_query_errors", default=True):
                    # # Bin the error by value
                    # Find bins based on maximal value of error (minimal is 0)
                    max_error = int(q_max_l1_rdd.treeReduce(max, depth=getTreeDepthFromNumPart(q_max_l1_rdd)))
                    bin_size = max(1, max_error // n_bins)

                    # Histogram the values. Each scalar in the query answer in each node is a data point
                    # hist = qL1rdd.flatMap(lambda d: [(erbin, 1) for erbin in (d[1] // bin_size + 1) * bin_size]).reduceByKey(add).collect()
                    hist = qL1rdd.flatMap(lambda d: [(erbin, 1) for erbin in (d[1].data // bin_size + 1) * bin_size] + [(bin_size, d[1].shape[0] - len(d[1].data))]).reduceByKey(add).collect()
                    hist = np.array(list(zip(*sorted(hist))))
                    # print(f"Average cell error: {queries[qname]/q.numAnswers()/rdd.count()}; from hist: {np.sum(hist[0]*hist[1])/np.sum(hist[1])}")

                    # Replace upper bound of the last bin by the maximal error value
                    hist[0, -1] = max_error
                    queries_L1_quantiles[qname] = hist

                    # # Repeat the same with binning by population before that

                    # Bin sizes for each population bin
                    bin_sizes = dict(qL1rdd.mapValues(np.max).reduceByKey(max).mapValues(lambda d: max(1, d // n_bins)).collect())

                    # Make 2D histogram and convert to dict (keys are popbins) of dicts (keys are error values bins)
                    # .flatMap(lambda d: [((d[0], erbin), 1) for erbin in (d[1] // bin_sizes[d[0]] + 1) * bin_sizes[d[0]]])\
                    pop_hist = qL1rdd\
                        .flatMap(lambda d: [((d[0], erbin), 1) for erbin in (d[1].data // bin_sizes[d[0]] + 1) * bin_sizes[d[0]]] + [((d[0], bin_sizes[d[0]]), d[1].shape[0] - len(d[1].data))])\
                        .reduceByKey(add) \
                        .map(lambda d: (d[0][0], (d[0][1], d[1])))\
                        .groupByKey()\
                        .mapValues(sorted).mapValues(dict)\
                        .collect()

                    pop_hist = dict(sorted(pop_hist))

                    # # Fill empty bins with zero values (may be neater printing, but then have to be filtered for plotting on
                    # # log scale of counts

                    # for popbin in pop_hist:
                    #     for erbin in range(0, n_bins * bin_sizes[popbin], bin_sizes[popbin]):
                    #         if erbin not in pop_hist[popbin]:
                    #             pop_hist[popbin][erbin] = 0

                    queries_binned_py_pop[qname] = pop_hist
                q_max_l1_rdd.unpersist()
                qL1rdd.unpersist()

            rdd.unpersist()
            pop_diff_rdd.unpersist()

            # pop_diff_rdd_unit.unpersist()
            if self.print_county_total_and_votingage and geolevel == CC.GEOLEVEL_COUNTY:
                county_diff_with_geocode.unpersist()

            error_geoleveldict[geolevel] = {'detailed_L1': total_L1_error,
                                            'detailed_max': max_L1_error}, totals, queries, queries_L1_quantiles, queries_binned_py_pop

        self.printErrors(error_geoleveldict, total_population, l1_relative, population_cutoff, rel_tp_error)

        block_df.unpersist()

        # self.printAndComputeQueryAccuracies(qdict, total_population,  error_geoleveldict, nodes_dict)
        if self.das.experiment:
            return error_geoleveldict, total_population

    def printByBlauQuintileErrors(self, geolevel: str, raw_syn_rdd):
        # cenrace_major_query = self.setup.schema_obj.getQuery(CC.CENRACE_MAJOR)
        hisp_cenrace_major_query = self.setup.schema_obj.getQuery(("*".join((CC.CENRACE_MAJOR, CC.ATTR_HISP)),))
        bi_rdd = raw_syn_rdd.map(lambda rs: (self.BlauIndexCEF(rs[0], hisp_cenrace_major_query), rs))  # .repartition(5000)
        quantiles = np.quantile(bi_rdd.map(lambda d: float(d[0])).collect(), [0.2, 0.4, 0.6, 0.8], interpolation='nearest')

        def quintileNum(d, quintiles: List[float]) -> int:
            for i in range(4):
                if d <= quintiles[i]:
                    return i + 1
            return 5

        biq_rdd = bi_rdd.map(lambda d: (quintileNum(d[0], quantiles), (d[1][1].sum() - d[1][0].sum(), 1)))
        # biq_sig_errors = biq_rdd.reduceByKey(add).collect()
        # biq_abs_errors = biq_rdd.reduceByKey(lambda x, y: np.abs(x) + np.abs(y)).collect()
        biq_sig_errors = biq_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).collect()
        biq_abs_errors = biq_rdd.reduceByKey(lambda x, y: (np.abs(x[0]) + np.abs(y[0]), x[1] + y[1])).collect()

        # print(f"{geolevel} Blau index quintiles: {quantiles}")
        blau_mse = [f"{qi}: {(aggerr / count):.3f}" for qi, (aggerr, count) in biq_sig_errors]
        blau_mae = [f"{qi}: {(aggerr / count):.3f}" for qi, (aggerr, count) in biq_abs_errors]
        print(f"{geolevel} Mean signed errors by Blau index quintile: {blau_mse}, aggregate: ", [f"{qi}: {aggerr}" for qi, (aggerr, count) in biq_sig_errors])
        print(f"{geolevel} MAE errors by Blau index quintile: {blau_mae}, aggregate: ",  [f"{qi}: {aggerr}" for qi, (aggerr, count) in biq_abs_errors])

    def print8CellCenraceHispErrors(self, geolevel: str, raw_syn_rdd):
        hisp_cenrace_major_query = self.setup.schema_obj.getQuery(("*".join((CC.CENRACE_MAJOR, CC.ATTR_HISP)),))
        qlevels = ['Hispanic', 'WhiteANH', 'BlackANH', 'AIAN_ANH', 'AsianANH', 'NHOPI_ANH', 'SOR_ANH', '2ormoreNH']

        def bin10_100(n: int) -> str:
            if n < 10:
                return "0-9"
            if n < 100:
                return "10-99"
            return "100+"

        if geolevel not in ['Tract', "Block_Group"]:
            return
        hmr_rs_tuple_rdd = raw_syn_rdd.map(lambda rs: self.cenraceHisp8cells(rs, hisp_cenrace_major_query))
        for i, cat in enumerate(qlevels):
            cat_hmr_rdd_binned = hmr_rs_tuple_rdd.map(lambda d: (bin10_100(d[0][i]), (np.abs(d[1][i] - d[0][i]), 1)))
            # print(cat_hmr_rdd_binned.take(5))
            cat_hmr = sorted(cat_hmr_rdd_binned.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).collect())
            print(f"{geolevel} {cat} MAE: ", {qi: f"{(aggerr / count):.3f}" for qi, (aggerr, count) in cat_hmr}, "(Absolute: ", {qi: aggerr for qi, (aggerr, count) in cat_hmr}, ")")
        print(f"{geolevel} MAEs (total over pop bins) by race cat:")
        totals, count = hmr_rs_tuple_rdd.map(lambda d: (np.abs(d[1] - d[0]), 1)).treeReduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), depth=getTreeDepthFromNumPart(hmr_rs_tuple_rdd))
        for cat, total in zip(qlevels, totals.tolist()):
            print(f"{cat}: MAE {(total/count):.3f} (Absolute {total})")

    def compute_total_pop_errors_and_quantiles(self, block_df, geolevel: str, compute_rel: bool, pop_cutoffs: List[Union[int, None]]) -> None:
        df = self.answerQueriesUsingAnalysis(block_df, [geolevel], ["total"])
        errors = df.withColumn("error", F.col(AC.PRTCTD) - F.col(AC.ORIG)).select(["error", AC.ORIG]).withColumn("l1_error", F.abs(F.col("error"))).persist()
        # These quantiles should match with the quants_errors in the compute_metrics_in_bin method
        if compute_rel:
            header_row = ["count", "l1_mean", "pct_rel_mean", "q(0.005)", "q(0.025)", "q(0.05)", "q(0.25)", "q(0.5)", "q(0.75)", "q(0.95)", "q(0.975)", "q(0.995)"]
        else:
            header_row = ["count", "l1_mean", "q(0.005)", "q(0.025)", "q(0.05)", "q(0.25)", "q(0.5)", "q(0.75)", "q(0.95)", "q(0.975)", "q(0.995)"]
        pop_cutoffs = [None] + pop_cutoffs + [None]
        all_bin_ends = [(None, None)] + list(zip(pop_cutoffs[:-1], pop_cutoffs[1:]))
        res = []
        with open(self.total_pop_errors_and_quantiles_file, "a") as f:
            f.write(",".join(["Metric/Bin", "Geolevel"] + header_row) + "\n")
        for bin_ends in all_bin_ends:
            res_bin = self.compute_metrics_in_bin(bin_ends, errors, compute_rel)
            assert len(res_bin) == len(header_row)
            with open(self.total_pop_errors_and_quantiles_file, "a") as f:
                f.write(f"{binEndsToStr(bin_ends)}, {geolevel},{','.join(map(str, res_bin))}\n")
            res.append([bin_ends] + list(zip(header_row, res_bin)))
        with open(self.total_pop_errors_and_quantiles_file, "a") as f:
            f.write("\n\n\n")
        self.log_and_print("########################################")
        self.log_and_print(f"Total population errors and quantiles for {geolevel}, with format [population_bounds_of_bin, (statistic_1_name, statistic_1), (statistic_2_name, statistic_2), ...]:", cui=True)
        for res_k in res:
            self.log_and_print(f"{res_k}", cui=True)
        self.log_and_print("########################################")
        errors.unpersist()


    @staticmethod
    def compute_metrics_in_bin(bin_ends: Union[list, tuple], df: DataFrame, compute_rel: bool) -> List[Union[int, None]]:
        if bin_ends[0] is not None and bin_ends[1] is not None:
            df_filtered = df.filter((F.col(AC.ORIG) <= bin_ends[1]) & (F.col(AC.ORIG) > bin_ends[0]))
        elif bin_ends[0] is not None:
            df_filtered = df.filter(F.col(AC.ORIG) > bin_ends[0])
        elif bin_ends[1] is not None:
            df_filtered = df.filter(F.col(AC.ORIG) <= bin_ends[1])
        else:
            df_filtered = df
        if df_filtered.count() == 0:
            final_len = 11 + compute_rel
            return [None] * final_len
        quants_errors = df_filtered.approxQuantile("error", [0.005, 0.025, 0.05, 0.25, .5, 0.75, 0.95, 0.975, 0.995], 0)
        l1_mean = [np.round(list(df_filtered.agg({"l1_error": "avg"}).collect())[0]["avg(l1_error)"], 3)]
        num = df_filtered.count()
        if compute_rel:
            df_filtered = df_filtered.withColumn("pct_rel", F.col("l1_error") / F.col(AC.ORIG) * 100.)
            pct_rel_mean = [np.round(list(df_filtered.agg({"pct_rel": "avg"}).collect())[0]["avg(pct_rel)"], 3)]
            return [num] + l1_mean + pct_rel_mean + quants_errors
        return [num] + l1_mean + quants_errors

    @staticmethod
    def printH1CountyMetrics(engine_tuple):
        nodes = engine_tuple
        county_data = list(nodes.map(lambda node: (node.geocode, node.getDenseRaw().ravel(), node.getDenseSyn().ravel())).collect())
        county_query_diff = [(row[0][1:3] + row[0][5:8], row[2][1] - row[1][1], np.sum(row[2])) for row in county_data]
        # Format: (county geoid (from geocode16 format), diff in occupied units, total units)

        l1_errors = AccuracyMetrics.l1FromUniqueCounties(county_query_diff)
        np.set_printoptions(threshold=50000)
        print(f"The MAE of occupied counts for Counties is {np.mean(l1_errors)}")
        print(f"County_Occupied_Counts_L1_Errors_Are:{np.array(l1_errors)}")

    @staticmethod
    def printAIANStateTotalL1Errors(engine_tuple) -> None:
        nodes = engine_tuple
        aian_states = nodes.filter(lambda node: node.geocode[0] == "1")
        l1_errors = np.array(aian_states.map(lambda node: np.abs(node.syn.sum() - node.raw.sum())).collect())
        print(f"AIAN_State_Total_L1_Errors_Are:{l1_errors}")

    @staticmethod
    def printCountyTotalAndVotingage(county_query_diff, qname: str) -> None:
        # To find State + County fixed geocode16 ID, defined as first 5 digits of geocode16 block IDs:
        county_query_diff = [(row[0][1:3] + row[0][5:8], row[1]) for row in county_query_diff]
        l1_errors = AccuracyMetrics.l1FromUniqueCounties(county_query_diff)
        thresh = 20 if qname == "total" else 15
        proportion = np.mean([x <= thresh for x in l1_errors])
        np.set_printoptions(threshold=50000)
        print(f"The MAE of query {qname} for Counties is {np.mean(l1_errors)}")
        print(f"The proportion of Counties with query {qname} that satisfy our accuracy goal is {proportion}")
        print(f"County_{qname}_L1_Errors_Are:{np.array(l1_errors)}")

    @staticmethod
    def l1FromUniqueCounties(county_query_diff):
        l1_errors = []
        unique_counties = np.unique([row[0] for row in county_query_diff]).tolist()
        for county in unique_counties:
            included_l1s = [row[1] for row in county_query_diff if row[0] == county]
            l1_errors.append(np.abs(np.sum(included_l1s)))
        return l1_errors

    def makeBlockDF(self, engine_tuple):
        block_nodes = engine_tuple
        block_nodes_mapped = block_nodes.map(lambda node: node.redefineGeocodes(self.setup.geocode_dict))
        block_df = datatools.rdd2df(block_nodes_mapped, self.setup.schema_obj).persist()
        return block_df

    def answerQueriesUsingAnalysis(self, block_df, geolevels: List[str], queries: List[str]) -> DataFrame:
        cur_path = os.path.abspath(os.path.curdir)
        os.chdir(os.path.join(cur_path, 'analysis'))

        if self.analysis_df_dict is None:
            spark = SparkSession.builder.getOrCreate()
            df_dict = sdftools.aggregateGeolevels(spark, block_df, self.geolevels + ("OSE", "Place", "MCD", "AIAN_AREAS", "FED_AIRS", CC.GEOLEVEL_PRIM), grfc_path=self.setup.grfc_path,
                                         strong_mcd_states=self.setup.strong_mcd_states, aian_areas=self.setup.aian_areas,
                                         aian_ranges_path=self.setup.aian_ranges_path, verbose=False, return_dict=True)
            self.analysis_df_dict = df_dict

        df_dict = self.analysis_df_dict

        union_df = None
        for geolevel in geolevels:
            df_trimmed = sdftools.remove_not_in_area(df_dict[geolevel], geolevels)
            union_df = df_trimmed if union_df is None else union_df.unionByName(df_trimmed)

        df_out = sdftools.answerQueries(union_df, self.setup.schema_obj, queries, verbose=False)
        os.chdir(cur_path)
        return df_out

    def calculate_L1_total_error_by_bin(self, block_df, geolevel: str, population_bin_starts: np.ndarray) -> None:
        df = self.answerQueriesUsingAnalysis(block_df, [geolevel], ["total"])
        if df.count() == 0:
            return None
        df_l1 = df.withColumn("L1_error", F.abs(F.col(AC.PRTCTD) - F.col(AC.ORIG)))
        rdd_l1 = df_l1.rdd.map(lambda row: (float(row["L1_error"]), int(np.digitize(row[AC.ORIG], population_bin_starts))))
        df_l1_w_bins = rdd_l1.toDF(["L1_error", "pop_bin"])
        avg_over_all_bins = np.round(list(df_l1_w_bins.agg({"L1_error": "avg"}).collect())[0]["avg(L1_error)"], 5)
        df_l1_grouped = df_l1_w_bins.groupBy("pop_bin").agg({"L1_error": "avg", "*": "count"})

        df_collect = df_l1_grouped.collect()
        n_bins = len(population_bin_starts) + 1
        population_bin_starts = np.concatenate(([-np.inf], population_bin_starts, [np.inf]))
        ranges = list(zip(population_bin_starts[:-1], population_bin_starts[1:] - 1))
        tmp_res = [None] * n_bins
        for row in df_collect:
            tmp_res[int(row["pop_bin"])] = np.round(row["avg(L1_error)"], 5)
        assert len(tmp_res) == (len(population_bin_starts) - 1)
        final_res = list(zip(ranges, tmp_res))[1:]
        if not self.l1_total_error_by_bin_file_started:
            with open(self.l1_total_error_by_bin_file, "w") as f:
                f.write("Geolevel,Bin,Error\n")
            self.l1_total_error_by_bin_file_started = True
        with open(self.l1_total_error_by_bin_file, "a") as f:
            for binstr, err in  final_res:
                f.write(f"{geolevel},{binEndsToStr(binstr)},{err}\n")
        self.log_and_print("########################################")
        self.log_and_print(f"Total Query mean L1 Error in {geolevel} overall entities is {avg_over_all_bins}, and Binned by CEF Total Population:\n{final_res}", cui=True)
        self.log_and_print("########################################")

    def calculate_relative_total_pop_error(self, block_df, quantiles: List[float] = None, threshold: float = 0.05):
        geolevels = list(self.gettuple(CC.TOTAL_POP_RELATIVE_ERROR_GEOLEVELS, section=CC.ERROR_METRICS, sep=CC.REGEX_CONFIG_DELIM, default=()))
        quantiles = [xi / 20. for xi in np.arange(20)] + [.975, .99, 1.] if quantiles is None else quantiles
        population_bin_starts = np.arange(51, dtype=int) * 50
        if len(geolevels) == 0:
            return EMPTY_TUPLE
        df = self.answerQueriesUsingAnalysis(block_df, geolevels, ["total"])

        df_l1 = df.withColumn("L1_error", F.abs(F.col(AC.PRTCTD) - F.col(AC.ORIG)))

        rdd_rel = df_l1.rdd.map(lambda row: (float(row["L1_error"] / np.maximum(1., row[AC.ORIG])), int(np.digitize(row[AC.ORIG], population_bin_starts)), row[AC.GEOLEVEL]))
        rdd_rel = rdd_rel.map(lambda row: (row[0], row[1], row[2], 1. if row[0] <= threshold else 0.))

        df_prop_lt = rdd_rel.toDF(["tp_rel", "pop_bin", AC.GEOLEVEL, "prop_lt"])
        df_prop_lt_grouped = df_prop_lt.groupBy(AC.GEOLEVEL, "pop_bin").agg({"tp_rel": "avg", "prop_lt": "avg", "*": "count"})
        # The column format at this point is: (AC.GEOLEVEL, "pop_bin", "avg(tp_rel)", "avg(prop_lt)", "count(1)")

        prop_lt_binned = df_prop_lt_grouped.collect()
        n_bins = len(population_bin_starts) + 1
        tp_rel_dict = {geolevel: [None] * n_bins for geolevel in geolevels}
        prop_lt_dict = {geolevel: [None] * n_bins for geolevel in geolevels}
        prop_lt_binned_final = {}
        tp_rel_dict_final = {}
        bin_counts = {}
        prop_lt_counts = {geolevel: [0] * n_bins for geolevel in geolevels}
        for row in prop_lt_binned:
            tp_rel_dict[row[AC.GEOLEVEL]][int(row["pop_bin"])] = np.round(row["avg(tp_rel)"], 5)
            prop_lt_dict[row[AC.GEOLEVEL]][int(row["pop_bin"])] = np.round(row["avg(prop_lt)"], 5)
            prop_lt_counts[row[AC.GEOLEVEL]][int(row["pop_bin"])] = int(row["count(1)"])

        population_bin_starts = np.concatenate(([-np.inf], population_bin_starts, [np.inf]))
        ranges = list(zip(population_bin_starts[:-1], population_bin_starts[1:] - 1))
        for geolevel in geolevels:
            assert len(tp_rel_dict[geolevel]) == (len(population_bin_starts) - 1)
            tp_rel_dict_final[geolevel] = list(zip(ranges, tp_rel_dict[geolevel]))
            prop_lt_binned_final[geolevel] = list(zip(ranges, prop_lt_dict[geolevel]))
            bin_counts[geolevel] = list(zip(ranges, prop_lt_counts[geolevel]))

        cur_path = os.path.abspath(os.path.curdir)
        os.chdir(os.path.join(cur_path, 'analysis'))
        n_quants = len(quantiles)
        # Find unbinned results:
        # Recall df_prop_lt has columns ["tp_rel", "pop_bin", AC.GEOLEVEL, "prop_lt"]
        quantiles_df = sdftools.getGroupQuantiles(df_prop_lt, columns=["tp_rel"], groupby=[AC.GEOLEVEL], quantiles=quantiles).collect()

        quantiles_dict_final = {geolevel: [None] * n_quants for geolevel in geolevels}
        for row in quantiles_df:
            quantiles_dict_final[row[AC.GEOLEVEL]][np.digitize(float(row["quantile"]), quantiles) - 1] = (float(row["quantile"]), np.round(row["tp_rel"], 5))
        os.chdir(cur_path)

        with open(self.relative_total_pop_file, "a") as f:
            f.write("Geolevel,Bin,Proportion with rel error < 0.05,Count,Average of total population relative error \n")
            for geolevel in geolevels:
                for (bin_ends, perc_err), cnt, tp_rel in zip(prop_lt_binned_final[geolevel], bin_counts[geolevel], tp_rel_dict_final[geolevel]):
                    f.write(f"{geolevel},{binEndsToStr(bin_ends)},{perc_err},{cnt[1]},{tp_rel[1]}\n")

            f.write("\n\nGeolevel,Quantile,Relative error\n")
            for geolevel in geolevels:
                for quantile, rel_err in quantiles_dict_final[geolevel]:
                    f.write(f"{geolevel},{quantile},{rel_err}\n")

        self.log_and_print(f"Proportion of geounits with total population relative error less than 0.05, the count of geounits and average of total population relative error binned by CEF total population:\n",cui=False)
        self.log_and_print("Geolevel\tBin\tProportion with rel error < 0.05\tCount\tAverage of total population relative error")
        for geolevel in geolevels:
            for (bin_ends, perc_err), cnt, tp_rel in zip(prop_lt_binned_final[geolevel], bin_counts[geolevel], tp_rel_dict_final[geolevel]):
                self.log_and_print(f"{geolevel}\t{binEndsToStr(bin_ends)}\t{perc_err}\t{cnt[1]}\t{tp_rel[1]}", cui=True)
        self.log_and_print("########################################")

        self.log_and_print(f"Total population relative error quantiles:", cui=False)
        for geolevel in geolevels:
            for quantile, rel_err in quantiles_dict_final[geolevel]:
                self.log_and_print(f"{geolevel}\t{quantile}\t{rel_err}", cui=True)
        self.log_and_print("########################################")
        return bin_counts, prop_lt_binned_final, tp_rel_dict_final, quantiles_dict_final

    def calculate_l1_relative_errors(self, block_df, quantiles: List[float] = None, threshold: float = 0.05,
                                     population_cutoff: int = None, use_bins: bool = False):
        geolevels = list(self.gettuple(CC.L1_RELATIVE_ERROR_GEOLEVELS, section=CC.ERROR_METRICS, sep=CC.REGEX_CONFIG_DELIM, default=()))
        queries = list(self.gettuple(CC.L1_RELATIVE_ERROR_QUERIES, section=CC.ERROR_METRICS, sep=CC.REGEX_CONFIG_DELIM, default=()))
        quantiles = [xi / 20. for xi in np.arange(20)] + [.975, .99, 1.] if quantiles is None else quantiles
        population_bin_starts = np.arange(51, dtype=int) * 50

        if len(geolevels) == 0 or len(queries) == 0:
            return EMPTY_TUPLE
        denom_query = self.getconfig(CC.L1_RELATIVE_DENOM_QUERY, section=CC.ERROR_METRICS, default="total")
        denom_level = self.getconfig(CC.L1_RELATIVE_DENOM_LEVEL, section=CC.ERROR_METRICS, default="total")

        df = self.answerQueriesUsingAnalysis(block_df, geolevels, queries + [denom_query])
        cur_path = os.path.abspath(os.path.curdir)
        os.chdir(os.path.join(cur_path, 'analysis'))
        df = sdftools.getL1Relative(df, colname="L1Relative", denom_query=denom_query, denom_level=denom_level).persist()

        if use_bins:
            # Find the proportion of geounits that have L1Relative errors less than threshold for each query, geolevel, and total population bin:
            rdd_prop_lt = df.rdd.map(lambda row: (row[AC.QUERY], row[AC.GEOCODE], row[AC.GEOLEVEL], int(np.digitize(row["orig"], population_bin_starts)), 1. if row["L1Relative"] <= threshold else 0.))
            df_prop_lt = rdd_prop_lt.toDF([AC.QUERY, AC.GEOCODE, AC.GEOLEVEL, "pop_bin", "prop_lt"])
            df_prop_lt_grouped = df_prop_lt.groupBy(AC.QUERY, AC.GEOLEVEL, "pop_bin").agg({"prop_lt": "avg", "*": "count"})
            # The column format at this point is: (AC.QUERY, AC.GEOLEVEL, "pop_bin", "avg(prop_lt)", "count(1)")

            prop_lt_binned = df_prop_lt_grouped.collect()
            n_bins = len(population_bin_starts) + 1
            prop_lt_dict = {geolevel: {query: [None] * n_bins for query in queries} for geolevel in geolevels}
            prop_lt_binned_final = {geolevel: {query: [None] * n_bins for query in queries} for geolevel in geolevels}
            # Note that the geounit counts do not depend on the query:
            prop_lt_counts = {geolevel: [0] * n_bins for geolevel in geolevels}
            bin_counts = {}
            for row in prop_lt_binned:
                prop_lt_dict[row[AC.GEOLEVEL]][row[AC.QUERY]][int(row["pop_bin"])] = np.round(row["avg(prop_lt)"], 5)
                prop_lt_counts[row[AC.GEOLEVEL]][int(row["pop_bin"])] = int(row["count(1)"])

            population_bin_starts = np.concatenate(([-np.inf], population_bin_starts, [np.inf]))
            ranges = list(zip(population_bin_starts[:-1], population_bin_starts[1:] - 1))
            for query in queries:
                for geolevel in geolevels:
                    assert len(prop_lt_dict[geolevel][query]) == (len(population_bin_starts) - 1)
                    prop_lt_binned_final[geolevel][query] = list(zip(ranges, prop_lt_dict[geolevel][query]))
                    bin_counts[geolevel] = list(zip(ranges, prop_lt_counts[geolevel]))
        else:
            bin_counts = EMPTY_TUPLE
            prop_lt_binned_final = EMPTY_TUPLE

        df = df.filter(df.orig >= population_cutoff)

        prop_lt_rdd = df.rdd.map(lambda row: ((row[AC.QUERY], row[AC.GEOLEVEL]), row["L1Relative"]))
        ## Find the proportion of geounits that have L1Relative errors less than threshold:
        prop_lt_rdd = prop_lt_rdd.groupByKey().mapValues(lambda row: np.mean([1. if x <= threshold else 0. for x in row]))

        ## After this line, the column format will be (AC.QUERY, AC.GEOLEVEL, prop_lt):
        prop_lt_rdd = prop_lt_rdd.map(lambda row: (*row[0], row[1]))
        prop_lt = prop_lt_rdd.collect()

        counts = df.groupBy([AC.QUERY, AC.GEOLEVEL]).count().collect()
        df = df.filter(df.L1Relative != 2.)
        counts_correct_sign = df.groupBy([AC.QUERY, AC.GEOLEVEL]).count().collect()

        quantiles_df = sdftools.getGroupQuantiles(df, columns=["L1Relative"], groupby=[AC.QUERY, AC.GEOLEVEL], quantiles=quantiles).collect()
        avg = df.groupBy([AC.QUERY, AC.GEOLEVEL]).avg("L1Relative").collect()
        df.unpersist()

        quantiles_dict = {geolevel: {query: {quant: None for quant in quantiles} for query in queries} for geolevel in geolevels}
        quantiles_dict_final = {geolevel: {query: None for query in queries} for geolevel in geolevels}
        avg_dict = {geolevel: {query: None for query in queries} for geolevel in geolevels}
        lt_prop_dict = {geolevel: {query: None for query in queries} for geolevel in geolevels}
        counts_dict = {geolevel: {query: None for query in queries} for geolevel in geolevels}
        counts_correct_sign_dict = {geolevel: {query: None for query in queries} for geolevel in geolevels}

        for row in quantiles_df:
            quantiles_dict[row[AC.GEOLEVEL]][row[AC.QUERY]][float(row["quantile"])] = np.round(row["L1Relative"], 5)
        for query in queries:
            for geolevel in geolevels:
                quantiles_dict_final[geolevel][query] = list(zip(quantiles, [quantiles_dict[geolevel][query][quant] for quant in quantiles]))

        for row in avg:
            avg_dict[row[AC.GEOLEVEL]][row[AC.QUERY]] = np.round(row["avg(L1Relative)"], 5)

        for row in prop_lt:
            lt_prop_dict[row[1]][row[0]] = np.round(row[2], 5)

        for row in counts:
            counts_dict[row[AC.GEOLEVEL]][row[AC.QUERY]] = row["count"]

        for row in counts_correct_sign:
            counts_correct_sign_dict[row[AC.GEOLEVEL]][row[AC.QUERY]] = row["count"]
        os.chdir(cur_path)

        geos = list(avg_dict.keys())
        qns = list(avg_dict[geos[0]].keys())
        if len(prop_lt_binned_final) > 0:
            with open(self.l1_relative_query_file, "a") as f:
                f.write(f"Geolevel,Query,CEF_pop_bin,Prop<0.05,Geounit Count\n")
                for qn in qns:
                    for geolevel in geos:
                        for (binends, propl1les0p5), (binends1, bin_count) in zip(prop_lt_binned_final[geolevel][qn], bin_counts[geolevel]):
                            f.write(f"{geolevel},{qn},{binEndsToStr(binends)},{propl1les0p5},{bin_count}\n")
        if not self.l1_relative_query_file_started:
            with open(self.l1_relative_query_file, "a") as f:
                f.write(f"\n\nGeolevel,Query,PopulationCutoff,Average rel L1,Prop<0.05\n")
            self.l1_relative_query_file_started = True
        with open(self.l1_relative_query_file, "a") as f:
            for qn in qns:
                for geolevel in geos:
                    f.write(f"{geolevel},{qn},{population_cutoff},{avg_dict[geolevel][qn]},{lt_prop_dict[geolevel][qn]}\n")

        return avg_dict, lt_prop_dict, quantiles_dict_final, counts_dict, counts_correct_sign_dict, bin_counts, prop_lt_binned_final

    def quantileErrors(self, rdd, q, geolevel: str, fun, err_name_string: str, f: TextIO = None) -> None:
        """
        Calculated error quantiles for a query q
        :param rdd: rdd, which constains query errors which can be taken out by mapping operation with a function :fun:
        :param q: query, the errors to which are quantilized
        :param geolevel: geolevel
        :param fun: function to map the query errors out of the rdd
        :param err_name_string: error description string for printing
        :param f: file to write a quantile CVS string to
        :return: None
        """
        quantiles_desired = sorted([i / 20 for i in range(21)] + [.975, .99, .999])
        # errors_rdd = rdd.map(fun).flatMap(lambda csr: csr.data.tolist() if csr.nnz > 0 else [])  # Note, we did .eliminate_zeros after answering the query
        # # errors_rdd = rdd.map(fun).flatMap(lambda err_arr: err_arr[np.where(err_arr != 0)].tolist())
        # # num_errors = errors_rdd.count()
        # # if num_errors > 0:
        # quantiles_rdd = errors_rdd.repartition(500)
        # #quantiles_rdd = quantiles_rdd.map(lambda err: Row(**{"val": float(err)}))
        # quantiles_rdd = quantiles_rdd.map(Row)
        spark = SparkSession.builder.getOrCreate()
        # try:
        #     quantiles = spark.createDataFrame(quantiles_rdd, T.StructType([T.StructField('val', T.LongType())]))
        #     quantiles = quantiles.approxQuantile("val", quantiles_desired, 0.01)
        # # pylint: disable=W0703
        # except Exception as e:
        #     self.log_and_print(f"Error in {geolevel} for {q.name} with {err_name_string}! {e}. ", cui=True)
        #     quantiles = [None] * len(quantiles_desired)
        #     self.log_and_print(f"Quantiles set to: {quantiles}", cui=True)
        #     traceback.print_exc()
        #
        #     t = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        #     file_loc = f"${DAS_S3ROOT}/users/user007/tmp/calculate_per_query_quantile_signed_errors_exception/{geolevel}-{q.name}-{t}-Exception"
        #     self.log_and_print(f"In calculate_per_query_quantile_signed_errors, saving quantiles_rdd to: {file_loc}")
        #     errors_rdd.saveAsPickleFile(file_loc)

        # Note: T.LongType has to become T.FloatType if doing no rounding (e.g. with OLS -- although with OLS there's not much point in using sparse matrices)
        quantiles_df = spark.createDataFrame(
            rdd.map(fun).map(lambda csr: T.Row(err_arr=csr.data.tolist() if csr.nnz > 0 else [])), T.StructType([T.StructField('err_arr', T.ArrayType(T.LongType()))])  # Note, we did .eliminate_zeros after answering the qu
        )
        num_errors = quantiles_df.select(F.sum(F.size(quantiles_df.err_arr)).alias('count')).collect()[0]['count']
        if num_errors > 0:
            err_df = quantiles_df.select(F.explode(quantiles_df.err_arr).alias('val'))
            if num_errors < 5000:
                quantiles = np.quantile(err_df.rdd.map(lambda r: r['val']).collect(), quantiles_desired, interpolation='nearest').tolist()
            else:
                quantiles = err_df.approxQuantile("val", quantiles_desired, 0.01)
            output_str = f"{geolevel} {q.name} {err_name_string} L1 error quantiles"
            output_str += " [[CEF=MDF counts excluded]]"
            output_str += f": {list(zip(quantiles_desired, quantiles))}"
            self.log_and_print(output_str, cui=True)
            if f is not None:
                for quantile, error in zip(quantiles_desired, quantiles):
                    f.write(f"{geolevel},{q.name},{err_name_string},{quantile},{error}\n")
        else:
            self.log_and_print(f"All errors in {geolevel} for {q.name} identically 0.", cui=True)
        num_geounits = self.geolevel_node_counts[geolevel]
        query_size = np.prod(q.queryShape())
        num_zero_equals_zero = num_geounits * query_size - num_errors
        self.log_and_print(f"# scalars in {geolevel} for {q.name} with {err_name_string} error!=0: {num_errors}", cui=True)
        self.log_and_print(f"# scalars in {geolevel} for {q.name} with {err_name_string} error==0: {num_zero_equals_zero}", cui=True)
        self.log_and_print(f"# scalars in {geolevel} for {q.name}, total: {num_geounits * query_size}", cui=True)

    def printErrors(self, error_geoleveldict: Dict, total_population: int, l1_relatives, population_cutoff, rel_tp_error):
        # levels_to_print = list(reversed(self.geolevels))
        levels_to_print = list(error_geoleveldict.keys())
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")
        self.log_and_print("########################################")
        self.log_and_print("1 - TVD for each geolevel (detailed):")
        for geolevel in levels_to_print:
            L1 = error_geoleveldict[geolevel][0]['detailed_L1']
            self.log_and_print(f"{geolevel}: {1. - L1 / (2. * total_population)}", cui=True)
        self.log_and_print("########################################")

        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")
        self.log_and_print("Max L1 error for each geolevel (detailed) (avg'd over geounits):")
        self.log_and_print(f"self.geolevels: {self.geolevels}")
        for geolevel in levels_to_print:
            L1_max_err = error_geoleveldict[geolevel][0]['detailed_max']
            self.log_and_print(f"{geolevel}: {L1_max_err:10d} \t{L1_max_err/self.geolevel_node_counts[geolevel]:5.5f}", cui=True)
        self.log_and_print("########################################")

        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")
        self.log_and_print("Zero-adjusted max L1 error for each geolevel (detailed) (avg'd over geounits, 0=0 nodes removed):")
        self.log_and_print(f"self.geolevels: {self.geolevels}")
        for geolevel in levels_to_print:
            L1_max_err_zero_adjusted = error_geoleveldict[geolevel][0]['detailed_max']
            self.log_and_print(f"{geolevel}: {L1_max_err_zero_adjusted:10d} "
                                + f"\t{L1_max_err_zero_adjusted/self.geolevel_node_counts_zero_adjusted[geolevel]:5.5f}", cui=True)
        self.log_and_print("########################################")

        # self.log_and_print("########################################")
        # self.log_and_print("Total sparsity change (in relative units) for each geolevel:")
        # for geolevel in levels_to_print:
        #     sparsity = error_geoleveldict[geolevel][1]
        #     self.log_and_print(f"{geolevel}: {sparsity}", cui=True)
        # self.log_and_print("########################################")
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")

        self.log_and_print("########################################")
        self.log_and_print("Total change and L1 error for each geolevel:")
        self.log_and_print(f"{'Geolevel':20}: \t{'change':10} \t{'L1 error':10} \t{'change sign count':10}", cui=True)

        for geolevel in levels_to_print:
            total = error_geoleveldict[geolevel][1]
            self.log_and_print(f"{geolevel:20}: \t{total[0]:10d} \t{total[1]:10d} \t{total[2]:10d}", cui=True)
        self.log_and_print("########################################")
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")

        # Print L1 average over geolevel nodes for each query
        for qn in error_geoleveldict[levels_to_print[0]][2]:
            self.log_and_print("########################################")
            self.log_and_print(f"{qn} query L1 error for each geolevel:")
            for geolevel in levels_to_print:
                q = error_geoleveldict[geolevel][2][qn]
                if "_L1" in qn:
                    self.log_and_print(f"{geolevel:20}: \t{q:10.1f} \t{q/self.geolevel_node_counts[geolevel]:>5.5f}"
                                        + f"\t{1. - q / (2. * total_population)}", cui=True)
                elif "_max" in qn:
                    self.log_and_print(f"{geolevel:20}: \t{q:10.1f} \t{q/self.geolevel_node_counts[geolevel]:>5.5f}", cui=True)
                else:
                    raise NotImplementedError(f"query {qn} has unrecognized type/suffix.")
            self.log_and_print("########################################")
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")

        if len(error_geoleveldict[levels_to_print[0]]) > 2:
            # Print distribution (histogram, quantiles) of L1 error of each query answer cells.
            # (e.g. for hhgq query, that has 8 scalars as the answer, that is going to be distribution over those 8 cells,
            #  with L1 in each cell being a sum of L1 of that cell over all the nodes in the level )
            for qn in error_geoleveldict[levels_to_print[0]][3]:
                # n_bins = min(error_geoleveldict[self.setup.levels[0]][2][qn].ravel().shape[0], 5)
                self.log_and_print("########################################")
                self.log_and_print(f"{qn} query L1 error histogram for each geolevel:")
                # self.log_and_print((" "*34 + ":\t" + " "*(n_bins*10//2) + "Bins / Quantiles"))
                bins_and_counts = {}
                for geolevel in levels_to_print:
                    h = error_geoleveldict[geolevel][3][qn]
                    # h = np.histogram(q.ravel(), bins=n_bins )
                    counts_string = ",".join(f"{int(h):10d}" for h in h[1])
                    bins_string = ",".join(f"{int(h):10d}" for h in h[0])
                    # self.log_and_print(f"{geolevel:20}: \t[{bins_string}]", cui=True)
                    self.log_and_print(f"{geolevel:20}: \t{'Bins          '}{bins_string}", cui=True)
                    self.log_and_print(f"{'':20}: \t{'Counts'}\t{counts_string}", cui=True)
                    bins_and_counts[geolevel] = list(zip(h[0], h[1]))
                    # with np.printoptions(precision=0, linewidth=1000, threshold=1000):
                    #     print(h)
                self.log_and_print("Bins and counts, zipped: ", cui=False)
                for geolevel in levels_to_print:
                    self.log_and_print(f"{geolevel:20}: \t {bins_and_counts[geolevel]}", cui=True)
                self.log_and_print("########################################")
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")

        if len(error_geoleveldict[levels_to_print[0]]) > 3:
            for qn in error_geoleveldict[levels_to_print[0]][4]:
                # n_bins = min(error_geoleveldict[self.setup.levels[0]][3][qn][0][1].ravel().shape[0], 5)
                self.log_and_print("########################################")
                self.log_and_print(f"{qn} query L1 error histogram for each geolevel binned by node population:")
                # self.log_and_print((" "*34 + ":\t" + " "*(n_bins*10//2) + "Counts\t" + " "*(n_bins*(10+8)//2)) + "Bins / Quantiles")
                bins_and_counts = {}
                for geolevel in levels_to_print:
                    qbinned = error_geoleveldict[geolevel][4][qn]
                    self.log_and_print(f"{geolevel:20}", cui=True)
                    # self.log_and_print("Popbin\tError Bin\t Count")
                    bins_and_counts[geolevel] = {}
                    for popbin in sorted(qbinned.keys()):
                        bins = sorted(qbinned[popbin].keys())
                        # bins_string = str(int(bins[0])) + "," + ",".join(f"{int(b):10d}" for b in bins[1:])
                        bins_string = ",".join(f"{b:10d}" for b in bins)
                        self.log_and_print(f"{'':20}: \t{'            Bins      '}{bins_string}", cui=True)
                        counts_string = ",".join(f"{qbinned[popbin][b]:10d}" for b in bins)
                        self.log_and_print(f"{'':20}: \t{popbin:10d}+\t{counts_string}", cui=True)
                        bins_and_counts[geolevel][popbin] = tuple((b, qbinned[popbin][b]) for b in bins)
                self.log_and_print("Bins and counts, zipped: ", cui=False)
                for geolevel, bc in bins_and_counts.items():
                    self.log_and_print(f"{geolevel:20}", cui=True)
                    for popbin, counts in bc.items():
                        self.log_and_print(f"{'':20}: \t{popbin:10d}+\t{counts}", cui=True)
                self.log_and_print("########################################")
        self.log_and_print(f"VINTAGE OF INPUT DATA: {self.setup.input_data_vintage}")

        if len(l1_relatives[0]) > 0:
            cutoffs = [0] if len(l1_relatives) == 1 else [0, population_cutoff]
            for cutoff, l1_relative in zip(cutoffs, l1_relatives):
                avg_dict, lt_prop_dict, quantiles_dict, counts_dict, counts_correct_sign_dict, bin_counts, prop_lt_binned_final = l1_relative
                geos = list(avg_dict.keys())
                qns = list(avg_dict[geos[0]].keys())
                if len(prop_lt_binned_final) > 0:
                    self.log_and_print("########################################")
                    for qn in qns:
                        for geolevel in geos:
                            prop_lt_binned_i = prop_lt_binned_final[geolevel][qn]
                            self.log_and_print(f"{geolevel} {qn} geounit proportion with L1 relative error less than 0.05, binned by CEF total population: {prop_lt_binned_i}", cui=True)
                            self.log_and_print("########################################")

                    self.log_and_print("########################################")
                    for geolevel in geos:
                        bin_count = bin_counts[geolevel]
                        self.log_and_print(f"{geolevel} geounit counts in each total population bin: {bin_count}", cui=True)
                        self.log_and_print("########################################")

                for qn in qns:
                    self.log_and_print("########################################")
                    self.log_and_print(f"Average {qn} query L1 relative error over all geounits with population at least {cutoff}:")
                    for geolevel in geos:
                        error = avg_dict[geolevel][qn]
                        self.log_and_print(f"{geolevel}: {error}", cui=True)

                for qn in qns:
                    self.log_and_print("########################################")
                    self.log_and_print(f"Of all geounits with population at least {cutoff}, proportion that have L1 relative error less than 0.05 for {qn} query:")
                    for geolevel in geos:
                        error = lt_prop_dict[geolevel][qn]
                        self.log_and_print(f"{geolevel}: {error}", cui=True)

        if len(rel_tp_error) > 0:
            self.log_and_print("########################################")
            bin_counts, prop_lt_binned_final, tp_rel_dict_final, quantiles_dict_final = rel_tp_error
            geos = quantiles_dict_final.keys()

            for geolevel in geos:
                prop_lt_binned_i = prop_lt_binned_final[geolevel]
                self.log_and_print(f"{geolevel} proportion with total population relative error less than 0.05, binned by CEF total population: {prop_lt_binned_i}", cui=True)
                self.log_and_print("########################################")

            for geolevel in geos:
                bin_count = bin_counts[geolevel]
                self.log_and_print(f"{geolevel} geounit counts in each total population bin: {bin_count}", cui=True)
                self.log_and_print("########################################")

            for geolevel in geos:
                tp_rel = tp_rel_dict_final[geolevel]
                self.log_and_print(f"{geolevel} average of total population relative error, binned by CEF total population: {tp_rel}", cui=True)
                self.log_and_print("########################################")

            for geolevel in geos:
                quant = quantiles_dict_final[geolevel]
                self.log_and_print(f"{geolevel} total population relative error quantiles: {quant}", cui=True)
                self.log_and_print("########################################")

    def printAndComputeQueryAccuracies(self, queries_dict: Dict, total_pop: int, error_geoleveldict: Dict, nodes_dict: Dict) -> None:
        """
        Another error printing layout
        :param queries_dict:
        :param total_pop:
        :param error_geoleveldict:
        :param nodes_dict:
        :return:
        """
        qdict = queries_dict.copy()
        self.log_and_print(f"CEF total pop: {total_pop}", cui=True)
        self.log_and_print("dpq".rjust(20)+" | "+"nnls".rjust(20)+" | "+"mdf".rjust(20) + "  1-TVD", cui=True)
        for geolevel, rdd in nodes_dict.items():
            for qname, q in qdict.items():
                def getQueryCEF(node):
                    cef_hist = node.getDenseRaw()
                    return node.geocode, q.answer(cef_hist)

                def getQueryMDF(node):
                    mdf_hist = node.getDenseSyn()
                    return node.geocode, q.answer(mdf_hist)
                #mdf_err = rdd.map(lambda node: np.sum(self.qL1(node, q))).reduce(add)
                mdf_err = error_geoleveldict[geolevel][2][qname]
                dpq_err = "-".rjust(20, ' ')
                nnls_err = "-".rjust(20, ' ')
                mdf_err = str(1. - mdf_err/(2. * total_pop)).rjust(20, ' ')
                self.log_and_print(f"{dpq_err} | {nnls_err} | {mdf_err}" + f"   {qname}, {geolevel}", cui=True)
                if geolevel == "County" and qname == "total":
                    cef = {gc: val for gc, val in rdd.map(getQueryCEF).collect()}
                    mdf = {gc: val for gc, val in rdd.map(getQueryMDF).collect()}
                    for gc in sorted(cef.keys()):
                        self.log_and_print(f"{geolevel}-level CEF {qname} counts: {(gc, cef[gc])}", cui=True)
                    for gc in sorted(mdf.keys()):
                        self.log_and_print(f"{geolevel}-level MDF {qname} counts: {(gc, mdf[gc])}", cui=True)

    def L1Sum(self, orig: multiSparse, protected: multiSparse) -> int:
    #def L1Sum(self, orig: np.ndarray, protected: Union[np.ndarray]) -> int:
        """
        returns the sum of the cell-wise L1 errors
        """
        # return int(np.sum(np.abs(protected - orig)))
        return int(np.abs(protected.sparse_array - orig.sparse_array).sum())

    @staticmethod
    def L1Max(orig, protected) -> int:
        """
        returns the max of the cell-wise L1 errors
        """
        # return int(np.max(np.abs(protected - orig)))
        return int(np.abs(protected.sparse_array - orig.sparse_array).max())

    @staticmethod
    def BlauIndexCEF(raw, hisp_cenrace_major_query: AbstractLinearQuery):
        # hisp_major_races = hisp_cenrace_major_query.answer(raw.toDense())  # CEF based
        hisp_major_races = hisp_cenrace_major_query.answerSparse(raw.sparse_array.transpose()).toarray()
        # return 1 - np.sum(major_races * major_races) / np.sum(major_races) ** 2
        return 1 - (np.sum(hisp_major_races[7:] ** 2) + np.sum(hisp_major_races[:7] ** 2)) / np.sum(hisp_major_races) ** 2

    @staticmethod
    def cenraceHisp8cells(rs, hisp_cenrace_major_query: AbstractLinearQuery):
        # hmr_raw = hisp_cenrace_major_query.answer(rs[0].toDense())
        # hmr_syn = hisp_cenrace_major_query.answer(rs[1].toDense())
        hmr_raw = hisp_cenrace_major_query.answerSparse(rs[0].sparse_array.transpose()).toarray()[:, 0]
        hmr_syn = hisp_cenrace_major_query.answerSparse(rs[1].sparse_array.transpose()).toarray()[:, 0]
        return tuple(map(lambda d: np.array([np.sum(d[7:]), ] + list(d[:7])), [hmr_raw, hmr_syn]))

    @staticmethod
    def qL1(node, q: AbstractLinearQuery, sparse=False):
        """Function that calculates signed L1 error of a main histogram query on a node"""
        if sparse:
            # return q.answerSparse(node.syn.sparse_array.transpose()) - q.answerSparse(node.raw.sparse_array.transpose())
            return q.answerSparse((node.syn - node.raw).sparse_array.transpose())
        return q.answer(node.getDenseSyn()) - q.answer(node.getDenseRaw())

    @staticmethod
    def qL1unit(node, q: AbstractLinearQuery, sparse=False):
        """Function that calculates signed  L1 error of a unit histogram query on a node"""
        if sparse:
            # return q.answerSparse(node.unit_syn.sparse_array.transpose()) - q.answerSparse(node.raw_housing.sparse_array.transpose())
            return q.answerSparse((node.unit_syn - node.raw_housing).sparse_array.transpose())
        return q.answer(node.getDenseSynHousing()) - q.answer(node.getDenseRawHousing())

    @staticmethod
    def popLowBound(raw) -> int:
        """
        Calculates lower bound of population bin, with bins defined as 10-based orders of magnitude, i.e returns:
            0 for 0 - 1
            1 for 1 - 9
            10 for 10 - 99
            100 for 100 - 999
            1000 for 1000 - 9999
            etc.
        """
        total_pop = raw.sum()
        if total_pop < .5:
            return 0
        ans = 10 ** int(np.floor(np.log10(total_pop)))
        return ans

    @staticmethod
    def sparsityChange(orig: multiSparse, protected: multiSparse):
        """
        By how much the sparsity (i.e. fraction of zeros, "foz") changed in a node:
        foz_protected - foz_original = (1 - fnz_protected) - (1 - fnz_original) =
                                   = fnz_original - fnz_protected =
                                   = nnz_original/hist_size - nnz_protected/hist_size

        Will be positive if protected data is more sparse than original
        :param orig:
        :param protected:
        :return:
        """
        return float(orig.sparse_array.count_nonzero() - protected.sparse_array.count_nonzero()) / np.prod(orig.shape)

    def makePrimGeolevel(self, block_nodes):

        spine_type = self.setup.spine_type
        prim_spine = self.getboolean(CC.PRIM_SPINE, section=CC.GEODICT, default=False)
        redefine_counties = self.getconfig(key=CC.REDEFINE_COUNTIES, section=CC.GEODICT, default='nowhere')
        use_prim_crosswalk = self.getboolean("use_prim_crosswalk", section=CC.GEODICT, default=True)
        prim_crosswalk = self.setup.make_prim_crosswalk_dict(self.setup.prim_geo_s3_path) if prim_spine else None

        grfc = make_grfc_ids(self.setup.aian_areas, redefine_counties, self.setup.grfc_path, self.setup.aian_ranges_path, self.setup.strong_mcd_states, prim_crosswalk, use_prim_crosswalk)
        # Format: (geocode16, (state, aian, ['0'][4 digit AIANNHCE] or ['10']+[3 digit county], [5 digit MCD] or [5 digit Place], tract, block, DAS AIAN area code, SD, prim_geocode))
        grfc = grfc.map(lambda row: (row[0], row[1][-1]))
        # Format: (geocode16, prim_geocode)

        rdd2save = block_nodes.map(GeounitNode.fromZipped).map(lambda block_node: (block_node.geocode, block_node)).join(grfc)
        # Format: (geocode16, (block_node, prim_geocode))
        rdd2save = rdd2save.map(lambda row: (row[1][1], row[1][0])).reduceByKey(lambda x, y: x.__add__(y, perform_attr_checks=False)).map(lambda row: row[1])

        import das_utils
        spark = SparkSession.builder.getOrCreate()
        path = mkpath(self.getconfig(key=CC.OUTPUT_PATH, section=CC.WRITER), 'aggregated4errmetr')
        prim_crosswalk = self.setup.make_prim_crosswalk_dict(substvars(self.setup.prim_geo_s3_path))

        savepath = os.path.join(path, "Prim.pickle")
        rdd_elements = rdd2save.count()
        if rdd_elements < rdd2save.getNumPartitions():
            rdd2save = rdd2save.coalesce(rdd_elements)
        rdd2save = rdd2save.map(GeounitNode.clsToZipped)
        print(f"Saving re-aggregated Prim RDD")
        das_utils.savePickledRDD(savepath, rdd2save, dvs_singleton=None)
        rdd2save.unpersist()
        print(f"Reloading re-aggregated Prim RDD")
        rdd = spark.sparkContext.pickleFile(savepath)
        return rdd

    def aggregateNodes(self, levels: Union[Tuple, List], block_nodes):
        """
        """
        import das_utils
        spark = SparkSession.builder.getOrCreate()
        path = mkpath(self.getconfig(key=CC.OUTPUT_PATH, section=CC.WRITER), 'aggregated4errmetr')
        nodes_dict = {levels[0]: block_nodes}
        for level, upper_level in zip(levels[:-1], levels[1:]):
            rdd2save = \
                nodes_dict[level] \
                    .map(GeounitNode.fromZipped) \
                    .map(lambda block_node: (block_node.parentGeocode, block_node)) \
                    .reduceByKey(lambda x, y: x.addInReduce(y, inv_con=False)) \
                    .map(lambda geocode_node: geocode_node[1].shiftGeocodesUp())
            savepath = os.path.join(path, f"{upper_level}.pickle")
            rdd_elements = rdd2save.count()
            if rdd_elements < rdd2save.getNumPartitions():
                rdd2save = rdd2save.coalesce(rdd_elements)
            rdd2save = rdd2save.map(GeounitNode.clsToZipped)
            print(f"Saving re-aggregated {upper_level} RDD")
            das_utils.savePickledRDD(savepath, rdd2save, dvs_singleton=None)
            rdd2save.unpersist()
            print(f"Reloading re-aggregated {upper_level} RDD")
            nodes_dict[upper_level] = spark.sparkContext.pickleFile(savepath)
            # nodes_dict[upper_level] = rdd2save
        return nodes_dict
