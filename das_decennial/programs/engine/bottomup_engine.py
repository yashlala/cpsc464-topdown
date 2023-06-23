"""
Bottom Up Engine
Noise infused only at the bottom level and then all the upper levels are constructed from the bottom level
"""
# python imports

# das-created imports
from programs.engine.engine_utils import DASEngineHierarchical, __NodeRDD__
from programs.nodes.nodes import GeounitNode
import programs.nodes.manipulate_nodes as manipulate_nodes
from fractions import Fraction
from das_constants import CC


class BottomUpEngine(DASEngineHierarchical):
    """
    Implements Bottom Up engine
    """
    def initializeAndCheckParameters(self) -> None:
        """
        Set per query budgets, no per-geolevel budgeting
        """
        super().initializeAndCheckParameters()

        # Set budgets of each query
        # self.setOptimizersAndQueryOrderings()

        # Shares of budget designated to each geolevel. Should be None since bottomup is not doing by-level bugdet
        # (it governs the behavior of self.makeDPNode)
        self.geolevel_prop_budgets = (1.0,)
        self.geolevel_prop_budgets_dict: dict = {self.all_levels[0]: 1.0}

        # Because global_scale is set in config file, total budget is now computed as a consequence of global_scale.
        # self.computeTotalBudget()

    def run(self, original_data: __NodeRDD__) -> __NodeRDD__:
        """
        Inputs:
        Output:
        """

        super().run(original_data)

        block_nodes: __NodeRDD__ = self.makeOrLoadNoisy(original_data)

        # TODO: These privacy checks should be invoked automatically, either in super().run() or in super().didRun()
        if self.getboolean(CC.CHECK_BUDGET, default=True):
            self.checkTotalBudget({self.levels[0]: block_nodes})
        else:
            self.log_and_print("Skipping privacy checks")

        block_nodes = self.bottomUp(block_nodes)

        # ## commented out the following since we don't need the nodes_dict anymore (at least with respect to what gets returned) ###
        # nodes_dict = self.getNodesAllGeolevels(block_nodes, additional_ic=False)
        # self.freeMemRDD(block_nodes)

        return block_nodes

    def getNodePLB(self, node: GeounitNode):
        return Fraction(1, 1)

    def noisyAnswers(self, nodes: __NodeRDD__, **kwargs) -> __NodeRDD__:
        """
        Infuse noise
        """
        nodes = nodes.map(self.makeDPNode).persist()
        self.logInvConsCheckPLB(nodes)

        return nodes

    def bottomUp(self, block_nodes: __NodeRDD__) -> __NodeRDD__:
        """
        Construct the upper levels
        """
        block_nodes = block_nodes.map(
            lambda node: manipulate_nodes.geoimp_wrapper_root(config=self.config, parent_shape=(self.hist_shape, self.unit_hist_shape),
                                                              root_node=node, optimizers=self.optimization_query_ordering.optimizers)
        ).persist()

        return block_nodes

    def getNodesAllGeolevels(self, block_nodes: __NodeRDD__, **kwargs):
        return block_nodes

    def saveNoisyAnswers(self, nodes: __NodeRDD__, repart_by_parent=True, postfix="") -> None:
        """ Subclass to call with a dictionary of RDDs (single entry in this case)"""
        super().saveNoisyAnswers({self.levels[0]: nodes}, repart_by_parent=False, postfix=postfix)

    def loadNoisyAnswers(self, application_id, postfix="", levels2load=None) -> __NodeRDD__:
        """ Subclass to return a single RDD rather than a dictionary of RDDs"""
        return super().loadNoisyAnswers(application_id, postfix, levels2load=(self.levels[0],))[self.levels[0]]

    def freeMemNodes(self, nodes: __NodeRDD__) -> None:
        self.freeMemRDD(nodes)
