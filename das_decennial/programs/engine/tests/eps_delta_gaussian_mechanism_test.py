import os, sys
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname( os.path.dirname(__file__)))))

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'python','lib', 'py4j-src.zip'))

from configparser import ConfigParser
from copy import deepcopy
from typing import List

import numpy as np
from fractions import Fraction

from programs.engine.engine_utils import DASEngineHierarchical
from programs.engine.topdown_engine import TopdownEngine
from programs.nodes.nodes import GeounitNode
from programs.engine.budget import Budget
from programs.queries.querybase import QueryFactory
from programs.sparse import multiSparse
from programs.schema.schemas.schemamaker import SchemaMaker
from programs.schema.attributes.sex import SexAttr
from das_framework.das_stub import DASStub

from das_constants import CC


from programs.tests.spark_fixture import spark  # Might show up as not used, but it is a fixture, it's used in the function arguments. Do not remove.
spark_fixture = spark  # This line is so that the previous import is formally used


class TestEngines:

    class Setup:
        """
            Setup module just for this test. Only what is needed.
        """
        # Note this config doesn't matter for actual budget calculations that are tests. It is just for objects to be able to be created
        # PLBs are overridden prior tp checking variances
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
        def __init__(self, das_stub) -> None:
            self.config = ConfigParser()
            self.config.read_string(self.setup_sconfig)
            self.privacy_framework = "zcdp"
            self.schema = "TEST"
            self.hist_shape = (2,)
            self.unit_hist_shape = (2,)
            self.hist_vars = ("sex",)
            self.schema_obj = SchemaMaker.fromAttlist("justsex", [SexAttr])
            self.unit_schema_obj = SchemaMaker.fromAttlist("justsex", [SexAttr])
            empty_dict = {
                            'invar_names': (),
                            'cons_names': (),
                        }
            self.inv_con_by_level = {
                            'Block': empty_dict,
                        }
            self.budget = Budget(self, config=self.config, das=das_stub)
            self.levels =  CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, "Tract_Group", CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE, CC.GEOLEVEL_US
            self.geo_bottomlevel = 'Block'
            self.spine_type = 'non_aian_spine'
            self.plb_allocation = None
            self.geocode_dict = {1: '2', 3: '4'}
            self.dp_mechanism_name = CC.DISCRETE_GAUSSIAN_MECHANISM
            self.postprocess_only = False
            self.use_spark = False
            self.geolevel_prop_budgets = (Fraction(1,5), Fraction(1,5), Fraction(3,25),
                                            Fraction(3,25), Fraction(3,25), Fraction(3,25), Fraction(3,25))
            self.only_dyadic_rationals = True  # Doesn't matter true or false because the budgets are assigned directly

    def Data(self) -> List[GeounitNode]:
        """
            Data in the shape of histograms for 1 Block. Hist shape (2,) (for, e.g., Male, Female).
        """
        b1 = multiSparse(np.array([1, 2]))
        block_nodes = [GeounitNode('b1', raw=b1, raw_housing=b1,
                                       invar={},
                                       cons={},
                                       geocode_dict={2: "Block"}), ]
        return block_nodes

    def getEngine(self, engine: DASEngineHierarchical, setup_instance, das_stub) -> DASEngineHierarchical:
        # Note this config doesn't matter for actual budget calculations that are tests. It is just for objects to be able to be created
        # PLBs are overridden prior tp checking variances
        engine_config_str = """
                  [budget]
                  dp_mechanism = discrete_gaussian_mechanism
                  global_scale = 1/1
                  epsilon_budget_total: 1/1
                  approx_dp_delta: 1/10000000000
                  bounded_dp_multiplier: 2.0
                  geolevel_budget_prop: 1/5, 1/5, 3/25, 3/25, 3/25, 3/25, 3/25
                  strategy: DetailedOnly
                  query_ordering: DetailedOnly_Ordering
                  # DPqueries: detailed
                  # queriesprop: 1/1
                  [engine]
                  save_optimized: False
                  """
        engine_config = ConfigParser()
        engine_config.read_string(engine_config_str)
        engine_instance = engine(config=engine_config, setup=setup_instance, name='engine', das=das_stub)
        return engine_instance

    def test_engines(self, dd_das_stub) -> None:
        engine = TopdownEngine
        das_stub = DASStub()
        setup_instance = self.Setup(dd_das_stub)
        geounit_nodes = self.Data()
        engine_instance = self.getEngine(engine, setup_instance, das_stub)
        engine_instance.geolevel_prop_budgets = (Fraction(1,5), Fraction(1,5), Fraction(3,25), Fraction(3,25),
                                                    Fraction(3,25), Fraction(3,25), Fraction(3,25))
        # engine_instance.budget.dp_query_prop = {'Level:': (Fraction(1,5), Fraction(1,2), Fraction(1,20), Fraction(1,20),
        #                                     Fraction(1,20), Fraction(1,20))}
        engine_instance.initializeAndCheckParameters()

        detailed_q = QueryFactory.makeTabularGroupQuery([2], groupings=None, add_over_margins=(), name="detailed")
        total_q = QueryFactory.makeTabularGroupQuery([2], groupings=None, add_over_margins=(0,), name="total")
        qs = []
        for i in range(3):
            qs.append(deepcopy(detailed_q))
            qs[-1].name += f"{i}"
        for i in range(3):
            qs.append(deepcopy(total_q))
            qs[-1].name += f"{i}"
        engine_instance.dp_query_names = [f"total{i}" for i in range(3)] + [f"detailed{i}" for i in range(3)]

        for level in engine_instance.levels:
            engine_instance.budget.query_budget.dp_query_names[level] = [f"total{i}" for i in range(3)] + [f"detailed{i}" for i in range(3)]
            engine_instance.budget.query_budget.dp_query_prop[level] = (Fraction(1,5), Fraction(1,2), Fraction(1,20), Fraction(1,20), Fraction(1,20), Fraction(1,20))
        engine_instance.budget.query_budget.queries_dict = dict([(q.name, q) for q in qs])
        # engine_instance.dp_queries = True
        # engine_instance.dp_query_prop = (Fraction(1,5), Fraction(1,2), Fraction(1,20), Fraction(1,20), Fraction(1,20), Fraction(1,20))
        # #engine_instance.setGlobalScale()
        engine_instance.budget.computeTotal()

        dpqs, unit_dpqs = engine_instance.nodeDPQueries(geolevel_prop=Fraction(1,5), main_hist=geounit_nodes[0].getDenseRaw(),
                                                            unit_hist=geounit_nodes[0].getDenseRawHousing(), geolevel='Block')
        # When limiting global_scale denominator to 1048576
        # correct_variances = [430.4296188961136, 68.8687390233782, 6886.8739023378175, 6886.8739023378175, 6886.8739023378175, 6886.8739023378175]
        # When limiting global_scale denominator to 1024
        #correct_variances = [430.7986497879028, 68.92778396606445, 6892.778396606444, 6892.778396606444, 6892.778396606444, 6892.778396606444]
        # With upper/lower limit_denom bounds and LIMIT_DENOMINATOR 1024 for global_scale, inverse_scale, and sigma_sq
        #correct_variances = [430.43062381852553, 68.8688998109641, 6886.8899810964085, 6886.8899810964085, 6886.8899810964085, 6886.8899810964085]
        # After DGM change to specify global_scale, not epsilon, & expect squared units in input proportions
        correct_variances = [25.000000000000004, 10.000000000000002, 99.99999999999999, 99.99999999999999, 99.99999999999999, 99.99999999999999]
        for correct_variance, dpq_name, dpq in zip(correct_variances, dpqs.keys(), dpqs.values()):
            print(f"dpq {dpq_name} variance: {dpq.Var} correct_variance: {correct_variance}")
            assert np.isclose(correct_variance, dpq.Var, atol=0.01)


if __name__ == "__main__":

    test_cls = TestEngines()
    test_cls.test_engines()
