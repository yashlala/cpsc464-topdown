"""
Bootstrap Engine

"""
# python imports
import math
import numpy
import scipy.sparse as ss
from collections import OrderedDict
from operator import add
# das-created imports
from programs.engine.engine_utils import DASEngineHierarchical, __NodeRDD__, __NodeDict__
from programs.nodes.nodes import GeounitNode, __HistData__
from programs.sparse import multiSparse

class BootstrapEngine(DASEngineHierarchical):
    """
    The purpose of the Bootstrap Engine is to create a sample with replacement of the input data.
    See the create_sampled_rdd method for more implementation details.
    """

    def initializeAndCheckParameters(self) -> None:
        """
        The BootstrapEngine requires no additional configuration parameters
        """
        pass

    def run(self, original_data: __NodeRDD__) -> __NodeRDD__:
        """
        This takes the input data RDD and, for each state, samples a new histogram using multinomial distributions,
        and saves the resulting histogram to the each Node's syn attribute.

        :param original_data: An RDD of the original data passed in to the BootstrapEngine for processing.
        :return: A new RDD with each block node's syn attribute set to the newly sampled histogram
        """

        super().run(original_data)

        self.log_and_print("In Bootstrap Engine, running the run method.")

        # Compute a dictionary that maps all the nodes to their respective levels
        all_geo_levels: __NodeDict__ = self.getNodesAllGeolevels(original_data, additional_ic=False)

        # Get all the State level nodes
        state_rdd: __NodeRDD__ = all_geo_levels.get('State')
        # Get all the Block level nodes
        blocks_rdd = all_geo_levels.get('Block')

        # Sum all of the state Nodes, and return a dictionary of geocode to state population sum
        summed_state_dict = dict(state_rdd.map(lambda node: (node.geocode, node.raw.sparse_array.sum())).collect())

        # This will be the final_rdd to be returned
        final_rdd: __NodeRDD__ = None

        for state_geocode, state_sum in summed_state_dict.items():
            # For this state, get the new sampled blocks and return it as an RDD
            new_blocks_rdd = BootstrapEngine.create_sampled_rdd(state_geocode, state_sum, blocks_rdd)

            # Ensure the sum of the new synthetic sampled histograms
            assert state_sum == new_blocks_rdd.map(lambda node: node.syn.sum()).reduce(add), \
                'Total synthetic population from all blocks does not match the original total population from all blocks!'

            # Combine this state's new blocks rdd values into the final RDD to be returned
            if final_rdd is None:
                final_rdd = new_blocks_rdd
            else:
                final_rdd = final_rdd.union(new_blocks_rdd)
                self.freeMemRDD(new_blocks_rdd)

        self.freeMemRDD(state_rdd)
        self.freeMemRDD(blocks_rdd)


        return final_rdd

    def freeMemNodes(self, nodes: __NodeRDD__) -> None:
        self.freeMemRDD(nodes)

    @staticmethod
    def create_sampled_rdd(state_geocode: str, state_sum: int, blocks_rdd: __NodeRDD__) -> __NodeRDD__:
        """
        Given a state S, sample a new population as follows:

        Let n_s be the state's population.
        Let b1 ... bk be the blocks in the state and m1 ... mk be their corresponding populations.
        We resample new population totals as a multinomial distribution with n=n_s and the vector p is (m1/n_s, ..., m_k/n_s).
        Call the sampled population totals r1, ... , rk

        Now that the state has a sampled population r1, we will sample records.
        For a given block, let H be its histogram and let b be its actual total population.
        We create a probability vector p = H/b and we sample r1 records from the multinomial distribution.
        This histogram is saved in the syn attribute of each node.

        # TODO: This is very nearly generalized to any parent/child relationship (Tract vs Block, for instance)
        # It may already work as such. This TODO is to verify and test for other relationships specifically,
        # and then rename parameters appropriately

        :param state_geocode: The geocode of the state to be sampled
        :param state_sum: The total population of the state. This could be calculated from the block nodes, but passing
                          it in assists with verification that the calculated sums are correct
        :param blocks_rdd: RDD representing all the blocks GeounitNodes (not just for this particular state)
        :return: A new RDD with newly sampled block GeounitNodes, where the syn attribute of each node represents the
                 newly sampled histogram.
        """

        print(f'state_geocode: {state_geocode}, sum: {state_sum}')

        # Filter: Get all the block nodes whose geocodes start with this state's geocode,
        # indicating that they are a part of the current state
        state_blocks_rdd = blocks_rdd.filter(lambda block: block.geocode.startswith(state_geocode))

        # Map: Sum all the nodes in this block, and associate with the block's geocode
        # Collect: Return it as a local dict, to be used with the multinomial
        summed_block_dict = OrderedDict(state_blocks_rdd
                                        .map(lambda block: (block.geocode, block.raw.sparse_array.sum())) \
                                        .collect())

        # summed_block_dict = blocks_rdd.map(lambda block: (block.geocode, block.raw.sparse_array.sum())).collect()
        total_blocks_pop = sum(summed_block_dict.values())

        # Ensure the sum of the blocks populations is equivalent to the current state's population
        assert state_sum == total_blocks_pop, 'Total state population does not match the total population from all blocks'

        block_distributions = OrderedDict((geocode, block_sum / float(state_sum)) for geocode, block_sum in summed_block_dict.items())

        # Ensure the total probabilities sum to 1.0
        assert math.isclose(sum(block_distributions.values()), 1.0,
                            rel_tol=1e-7), 'The total of the block probability distributions should be 1.0'

        # produce r1 ... rk
        block_distr_keys = block_distributions.keys()
        block_distr_values = list(block_distributions.values())
        sampled_population_totals = numpy.random.multinomial(state_sum, block_distr_values)

        sampled_pop_dict = dict(zip(block_distr_keys, sampled_population_totals))
        # Sample each block's histogram and set the syn attribute
        new_blocks_rdd = state_blocks_rdd.map(
            lambda node: BootstrapEngine.sample_histogram(node, sampled_pop_dict.get(node.geocode))).persist()

        return new_blocks_rdd

    @staticmethod
    def compute_pval(node: GeounitNode):
        # H is the histogram (node.raw). b is the total actual population for the block (node.raw.sum()).
        # probability vector = H/b
        total_in_node = node.raw.sparse_array.sum()

        node_data = node.raw.sparse_array.data

        # Get the probability vector
        pval = node_data / float(total_in_node)
        #import logging
        #logging.warn(f"sum of probability vector is {pval.sum()}\n")


        return pval

    @staticmethod
    def sample_histogram(node: GeounitNode, sample_target: int):
        """
        :param node: The input GeounitNode which will receive a new sampled histogram
        :param sample_target: The size of the target sample population
        :return: The input node with its syn attribute set to the sampled histogram
        """
        assert all([node.raw is not None,
                    isinstance(node.raw, multiSparse),
                    node.raw.sparse_array is not None,
                    node.raw.sparse_array.data is not None])

        # Grab the sparse data array from the node to do work on directly
        # This is in the format of a 1D array
        data_shape = node.raw.shape

        # Get the shape and indices of populated values in the sparse matrix to be able
        # to recreate a new one
        csr_shape = node.raw.sparse_array.shape
        indices = node.raw.sparse_array.indices
        indptr = node.raw.sparse_array.indptr

        # Get the probability vector
        pval = BootstrapEngine.compute_pval(node)

        # Sample from a multinomial of the pval
        sampled_data = numpy.random.multinomial(sample_target, pval)

        # Produce the new CSR matrix and histogram
        new_matrix = ss.csr_matrix((sampled_data, indices, indptr), shape=csr_shape)
        new_histogram: __HistData__ = multiSparse(new_matrix, shape=data_shape)

        # Set the node's syn attribute
        node.syn = new_histogram
        return node

    def noisyAnswers(self, nodes: __NodeRDD__, **kwargs) -> __NodeRDD__:
        """
        No noisy answers in this engine, so just pass this required abstract method.
        """
        pass
