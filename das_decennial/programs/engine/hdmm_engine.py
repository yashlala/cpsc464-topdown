"""
High Dimensional Matrix Mechanism engine implementation
NOTE (March 11, 2021): INCOMPATIBLE WITH CURRENT BUDGET PARSING AND CALCULATIONS IN PARENT CLASSES
"""
# python imports
import sys
import os
from typing import List, Dict
from configparser import NoOptionError, NoSectionError
import numpy as np
# das-created imports
from programs.engine.topdown_engine import TopdownEngine
from programs.nodes.nodes import GeounitNode
import programs.queries.querybase as querybase
import programs.workload.make_workloads as make_workloads
import programs.engine.primitives as primitives
from programs.queries.constraints_dpqueries import DPquery
from programs.queries.querybase import AbstractLinearQuery
import das_utils
from exceptions import DASValueError, DASConfigError
from das_constants import CC

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "hdmm", "src"))

import hdmm.workload as workload
import hdmm.templates as templates

# TODO: Move these to das_constants.py
SAVE_HDMM_STRATEGY = "save_strategy"
LOAD_HDMM_STRATEGY = "load_strategy"
LOAD_HDMM_STRATEGY_PATH = "load_strategy_path"
SAVED_STRATEGY_NAME = "hdmm_strategy.pickle"

class HDMMEngine(TopdownEngine):
    """High Dimensional Matrix Mechanism engine implementation"""

    workload: List[str]
    workload_queries_dict: Dict[str, AbstractLinearQuery]  # Dictionary with query objects

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.mechanism = primitives.basic_dp_answer
        self.dp_queries = False
        try:
            self.strategy_type: str = self.getconfig(CC.HDMM_STRATEGY_TYPE, section=CC.HDMM)
        except NoSectionError as err:
            raise DASConfigError(f"HDMM engine requires '{CC.HDMM_STRATEGY_TYPE}' to be set up in [{err.section}] section", None, err.section)
        except NoOptionError as err:
            raise DASConfigError(f"MHDMM engine requires '{err.option}' to be set up in [{err.section}] section", err.option, err.section)
        print("HDMM Strategy type: ", self.strategy_type)

    def setWorkload(self) -> None:
        try:
            self.workload = list(self.gettuple(CC.WORKLOAD, section=CC.WORKLOAD, sep=CC.REGEX_CONFIG_DELIM))
        except NoSectionError as err:
            raise DASConfigError(f"HDMM engine requires '{CC.WORKLOAD}' to be set up in [{err.section}] section", None, err.section)
        except NoOptionError as err:
            raise DASConfigError(f"HDMM engine requires '{err.option}' to be set up in [{err.section}] section", err.option, err.section)
        print("workload: ", self.workload)

        self.workload_queries_dict = make_workloads.WorkloadQueriesCreator(self.setup.schema_obj, self.workload).workload_queries_dict

        self.queries_dict = {}

    def makeDPNode(self, node: GeounitNode):
        """"
        This function takes a GeounitNode with "raw" data and generates noisy DP query answers depending the specifications in the config object.
        This is specific to topdown - the specifics of how budget is divided up

        Inputs:
            config: configuration object
            GeounitNode: a Node object with "raw" data
            schema_name: str giving the keyword name of a schema
        Outputs:
            dp_geounit_node: a Node object with selected DP measurements
        """

        # index relative to topdown
        index = self.levels_reversed.index(node.geolevel)

        node_hist: np.ndarray = node.getDenseRaw()
        dp_budget = self.budget.total_budget * self.getNodePLB(node)

        query: AbstractLinearQuery = self.queries_dict["hdmm_strategy"]
        dp_query: DPquery = self.makeDPQuery(hist=node_hist, query=query, inverse_scale=dp_budget)

        dp_geounit_node = node
        dp_geounit_node.dp_queries = {"hdmm_strategy": dp_query}
        return dp_geounit_node

    @staticmethod
    def dict2workload(workload_dict: Dict[str, AbstractLinearQuery]):
        """ Convert a dict of queries into HDMM Workload"""
        workload_list: List[workload.Kron] = []
        for query in workload_dict.values():
            kron_factors = [workload.EkteloMatrix(x) for x in query.kronFactors()]
            workload_list.append(workload.Kronecker(kron_factors))

        return workload.VStack(workload_list)

    # pylint: disable=invalid-name
    def findPIdentityStrategy(self) -> templates.TemplateStrategy:
        """find a Kronecker product strategy by optimizing the workload"""
        ps_parameters = self.gettuple_of_ints(CC.PS_PARAMETERS, section=CC.HDMM, sep=CC.REGEX_CONFIG_DELIM)
        print("pidentity parameters: ", ps_parameters)
        return templates.KronPIdentity(ps_parameters, self.hist_shape)

    def findMarginalStrategy(self):
        return templates.Marginals(self.hist_shape)

    def strategy2query(self, strategy: templates.TemplateStrategy) -> AbstractLinearQuery:
        """ Convert HDMM strategy into query via Kronecker factors"""

        if self.strategy_type == CC.PIDENTITY_STRATEGY:
            kron_factors = [A.sparse_matrix() for A in strategy.strategy().matrices]
            query = querybase.QueryFactory.makeKronQuery(kron_factors, "hdmm_strategy")
        elif self.strategy_type == CC.MARGINAL_STRATEGY:
            #make sure to normalize the strategy (sum over rows <=1)
            #dense_strategy = np.array(strategy.strategy().sparse_matrix().todense())
            #print("strategy shape: ", dense_strategy.shape)
            #sensitivity = np.max(dense_strategy.sum(0))
            #norm_strategy = strategy.strategy().sparse_matrix().tocsr() / sensitivity
            hdmm_strategy = strategy.strategy().sparse_matrix().tocsr()
            print("strategy shape: ", hdmm_strategy.shape)
            print("There are ", (hdmm_strategy < 0).size, "negative coefficients and ", (hdmm_strategy > 0).size, " positive ones." )
            query = querybase.QueryFactory.makeKronQuery((hdmm_strategy, ), "hdmm_strategy")
        else:
            raise DASValueError(f"Strategy type '{self.strategy_type}' not implemented in das_decennial HDMM engine", self.strategy_type)
        return query

    def optimizeWorkload(self) -> None:
        """
        Gathers the workload and optimizes to find the strategy
        """

        # Load previously calculated strategy if requested
        if self.getboolean(LOAD_HDMM_STRATEGY, section=CC.HDMM, default=False):
            saved_strategy_path = self.getconfig(LOAD_HDMM_STRATEGY_PATH, section=CC.HDMM, default='')
            if not saved_strategy_path:
                try:
                    saved_strategy_path = self.das.writer.output_path
                except (NoOptionError, NoSectionError) as err:
                    raise DASConfigError(
                        f"Load HDMM strategy is requested but neither [{CC.HDMM}]/{LOAD_HDMM_STRATEGY_PATH}, nor [{err.section}]/{err.option} are provided to find the saved strategy",
                        err.option, err.section)
            load_path = os.path.join(saved_strategy_path, SAVED_STRATEGY_NAME)
            self.log_and_print(f"Loading HDMM strategy from {load_path}...")
            try:
                strategy = das_utils.loadPickleFile(load_path)
                self.queries_dict["hdmm_strategy"] = self.strategy2query(strategy)
                return
            except (FileNotFoundError, EOFError):
                self.log_and_print(f"File with saved HDMM strategy {load_path} not found, re-calculating strategy...", cui=False)

        # Calculated strategy
        W = self.dict2workload(self.workload_queries_dict)
        if self.strategy_type == CC.PIDENTITY_STRATEGY:
            strategy = self.findPIdentityStrategy()
        elif self.strategy_type == CC.MARGINAL_STRATEGY:
            strategy = self.findMarginalStrategy()
        else:
            raise DASValueError(f"Strategy type '{self.strategy_type}' not implemented in das_decennial HDMM engine", self.strategy_type)

        # run optimization
        ans = strategy.optimize(W)

        # Save strategy if requested
        if self.getboolean(SAVE_HDMM_STRATEGY, section=CC.HDMM):
            try:
                output_path = self.das.writer.output_path
                save_path = os.path.join(output_path, SAVED_STRATEGY_NAME)
            except (NoOptionError, NoSectionError) as err:
                raise DASConfigError(
                    f"Save HDMM strategy is requested but saving path [{err.section}]/{err.option} is not provided", err.option, err.section)
            das_utils.savePickleFile(save_path, strategy)

        self.queries_dict["hdmm_strategy"] = self.strategy2query(strategy)
