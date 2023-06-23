from configparser import ConfigParser
from typing import Dict, List

import pytest
from copy import deepcopy
import numpy as np
import warnings

from programs.engine.bottomup_engine import BottomUpEngine
from programs.engine.topdown_engine import TopdownEngine
from programs.engine.topdown_noiseless_engine import TopdownNoiselessEngine
from programs.engine.engine_utils import DASEngineHierarchical

from programs.schema.schemas.schemamaker import SchemaMaker
from programs.schema.attributes.sex import SexAttr

from programs.nodes.nodes import GeounitNode
from programs.queries.constraints_dpqueries import Constraint
from programs.queries.querybase import QueryFactory, MultiHistQuery, StubQuery
import programs.das_setup as ds
from programs.sparse import multiSparse
from fractions import Fraction
from programs.fake_rdd import FakeRDD
from programs.rdd_like_list import RDDLikeList

from das_constants import CC

from programs.tests.spark_fixture import spark  # Might show up as not used, but it is a fixture, it's used in the function arguments. Do not remove.
spark_fixture = spark  # This line is so that the previous import is formally used
from programs.engine.budget import Budget

class TestEngines:

    class MFURSetup:
        """
        Setup module just for this test. Only what is needed.
        """
        setup_sconfig = """
        [geodict]
        geolevel_names = Block, County, State

        [budget]
        privacy_framework= zcdp
        dp_mechanism= discrete_gaussian_mechanism
        print_per_attr_epsilons= True
        global_scale: 1/1
        bun_steinke_eps_delta_conversion: False

        #budget in topdown order (e.g. US+PR, State, .... , Block)
        geolevel_budget_prop= 204/512, 204/512, 104/512

        strategy: DetailedOnly
        """
        def __init__(self, engine: DASEngineHierarchical, das_stub):
            self.config = ConfigParser()
            self.config.read_string(self.setup_sconfig)
            self.privacy_framework = "zcdp"
            self.schema = "TEST"
            self.hist_shape = (2,)
            self.unit_hist_shape = (2,)
            self.hist_vars = ("sex",)
            self.schema_obj = SchemaMaker.fromAttlist("justsex", [SexAttr])
            self.unit_schema_obj = SchemaMaker.fromAttlist("justsex", [SexAttr])
            self.validate_input_data_constraints = False
            self.spine_type = 'non_aian_spine'
            self.plb_allocation = None
            self.part_size_optimized = 0
            self.part_size_optimized_block = 0
            self.geocode_dict = {4: "Block", 3: "County", 1: "State"}
            self.dp_mechanism_name = CC.GEOMETRIC_MECHANISM
            self.inv_con_by_level = {
                'Block': {
                    'invar_names': ('tot',) if engine == BottomUpEngine else (),
                    'cons_names': ('total',) if engine == BottomUpEngine else (),
                },
                'County': {
                    'invar_names': (),
                    'cons_names': ()
                },
                'State': {
                    'invar_names': ('tot',),
                    'cons_names': ('total',)
                }
            }
            self.budget = Budget(self, config=self.config, das=das_stub)
            self.levels = list(self.inv_con_by_level.keys())
            self.geo_bottomlevel = 'Block'
            self.geolevel_prop_budgets = (Fraction(1 / 5), Fraction(1 / 5), Fraction(3 / 5))
            self.postprocess_only = False
            self.only_dyadic_rationals = False

        @staticmethod
        def makeInvariants(raw, raw_housing, invariant_names) -> Dict:
            inv_dict = {}
            if 'tot' in invariant_names:
                inv_dict.update({'tot': np.sum(raw.toDense())})
            return inv_dict

        @staticmethod
        def makeConstraints(hist_shape, invariants, constraint_names) -> Dict:
            cons_dict = {}
            if 'total' in constraint_names:
                cons_dict.update(
                    {
                        'total': Constraint(
                                    MultiHistQuery(
                                            (QueryFactory.makeTabularGroupQuery((2,), add_over_margins=(0,)), StubQuery((2,1), "stub")),
                                            (1, 0)
                                    ),
                                    np.array(invariants['tot']), "=", "total")
                    })
            return cons_dict

    def mfUrData(self, setup_instance) -> List[GeounitNode]:
        """
        Data in the shape of histograms for 3 Rural Blocks in 1 Rural county and 3 Urban blocks in 1 Urban county, all in 1 states
        Histogram is shape (2,) for sex, i.e. each block provides number of male and number of female.
        This is the same test example as in JavaScript simulator.
        """
        rb1 = multiSparse(np.array([1, 2]))
        rb2 = multiSparse(np.array([3, 4]))
        rb3 = multiSparse(np.array([5, 6]))
        ub1 = multiSparse(np.array([101, 102]))
        ub2 = multiSparse(np.array([103, 104]))
        ub3 = multiSparse(np.array([105, 106]))
        block_nodes = []
        for block, geocode in zip([rb1, rb2, rb3, ub1, ub2, ub3], ['1RB1', '1RB2', '1RB3', '1UB1', '1UB2', '1UB3']):
            invariants = setup_instance.makeInvariants(raw=block, raw_housing=block,
                                                       invariant_names=setup_instance.inv_con_by_level['Block']['invar_names'])
            constraints = setup_instance.makeConstraints(hist_shape=(2,), invariants=invariants,
                                                         constraint_names=setup_instance.inv_con_by_level['Block']['cons_names'])
            block_nodes.append(GeounitNode(geocode, raw=block, raw_housing=block,
                                           invar=invariants,
                                           cons=constraints,
                                           geocode_dict={4: "Block", 3: "County", 1: "State"}))
        return block_nodes

    def getEngine(self, engine: DASEngineHierarchical, setup_instance, use_spark, das_stub) -> DASEngineHierarchical:
        engine_s_config = """
                  [environment]
                  DAS_FRAMEWORK_VERSION: 0.0.1
                  GRB_ISV_NAME: Census
                  GRB_APP_NAME: DAS
                  GRB_Env3: 0
                  GRB_Env4:
                  das_run_uuid: TEST_UUID

                  [gurobi]
                  gurobi_path: /mnt/apps5/gurobi900/linux64/lib/${PYTHON_VERSION}_utf32/
                  gurobi_lic: /apps/gurobi752/gurobi_client.lic
                  gurobi_logfile_name: gurobi.log
                  OutputFlag: 1
                  OptimalityTol: 1e-9
                  BarConvTol: 1e-8
                  BarQCPConvTol: 0
                  BarIterLimit: 1000
                  FeasibilityTol: 1e-9
                  Threads: 1
                  Presolve: -1
                  NumericFocus: 3
                  gurobi_lic_create=true
                  TOKENSERVER=$GRB_TOKENSERVER
                  PORT=$GRB_TOKENSERVER_PORT

                  # Do we explicitly run presolve in Python?  1 or 0
                  python_presolve: 1

                  [budget]
                  query_ordering: DetailedOnly_Ordering

                  [geodict]
                  geolevel_num_part: 0,0,0
                  geolevel_leng: 1
                  # detailed query proportion of budget (a float between 0 and 1)

                  [engine]
                  save_noisy: off
                  save_optimized: off
                  geolevel_num_Part: 0,0,0
                  aggregated_checkpoint = False

                  [constraints]
                  minimalSchema: sex
                  """
        engine_config = ConfigParser()
        engine_config.read_string(engine_s_config)
        engine_config.set(section='engine', option='spark', value=str(use_spark))
        engine_instance = engine(config=engine_config, setup=setup_instance, name='engine', das=das_stub)
        return engine_instance

    @pytest.mark.skip(reason="Skipping to allow Jenkins to run pipeline. To be removed.")
    @pytest.mark.parametrize("engine", [BottomUpEngine, TopdownEngine, TopdownNoiselessEngine])
    @pytest.mark.parametrize("use_spark", [False, True])
    def test_engines(self, spark, use_spark, engine, dd_das_stub) -> None:
        if use_spark:  # -- with Spark on, doesn't run in Jenkins pipeline, can't import some _backport module of matplotlib.cbook
            pytest.skip()
        setup_instance = self.MFURSetup(engine, dd_das_stub)
        setup_instance.use_spark = use_spark

        block_nodes_input = self.mfUrData(setup_instance)
        block_nodes = spark.sparkContext.parallelize(block_nodes_input) if use_spark else RDDLikeList(deepcopy(block_nodes_input))
        # block_nodes = FakeRDD(spark.sparkContext.parallelize(block_nodes, numSlices=1))
        # block_nodes = RDDLikeList(deepcopy(block_nodes_input))

        use_spark = False
        engine_instance = self.getEngine(engine, setup_instance, use_spark, dd_das_stub)
        if engine == TopdownNoiselessEngine:
            engine_instance.config.set(CC.ENGINE, CC.CHECK_BUDGET, "off")
        protected_nodes = engine_instance.run(block_nodes)
        assert protected_nodes.count() == len(block_nodes_input)
        assert np.sum(list(map(lambda node: node.getDenseSyn(), protected_nodes.collect()))) == np.sum(list(map(lambda node: np.sum(node.getDenseRaw()), block_nodes_input)))
