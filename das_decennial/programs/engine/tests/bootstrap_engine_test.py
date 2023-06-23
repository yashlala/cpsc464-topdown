import pytest
import numpy as np
import scipy.sparse as ss
import math

from programs.engine.bootstrap_engine import BootstrapEngine
from programs.nodes.nodes import GeounitNode
from programs.nodes.tests.nodes_test import geocodeDict, makeNode
from programs.sparse import multiSparse
from programs.rdd_like_list import RDDLikeList

# Sample database with 9 people
d1 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 0, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 3],
                [3, 0, 0, 15, 4],
                [3, 0, 0, 15, 5],
                [3, 0, 0, 15, 6],
                [3, 0, 0, 15, 7],
                [1, 1, 0, 15, 8]
            ])

# Sample database with a single person
d2 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0]
            ])


def test_compute_pval() -> None:
    node: GeounitNode = makeNode(d2, '1234567890abcdef', geocodeDict)

    pval = BootstrapEngine.compute_pval(node)

    assert math.isclose(sum(pval), 1.0, rel_tol=1e-5), 'The total pval must be 1.0'
    assert isinstance(pval, np.ndarray), 'pval should be a numpy array'


def test_sample_histogram() -> None:
    node: GeounitNode = makeNode(d1, '1234567890abcdef', geocodeDict)
    assert node.syn is None, 'Original node should start with no syn attribute'

    # Run sample_histogram. This modifies the input geounitnode
    sample_size = 10
    BootstrapEngine.sample_histogram(node, sample_size)

    expected_node: GeounitNode = makeNode(d1, '1234567890abcdef', geocodeDict)
    assert node.raw == expected_node.raw, 'Sampled node should not change the input raw data at all'

    assert node.syn is not None, 'Original node should now have a populated syn attribute'
    assert isinstance(node.syn, multiSparse), 'Populated syn attribute should be a multiSparse'
    assert isinstance(node.syn.sparse_array, ss.csr_matrix), 'Populated sparse_array should be of tpe csr_matrix'

    assert sum(node.syn.sparse_array.data) == sample_size, 'Populated sparse_array data should be equal to the requested sample_size'

    # Test that all populated syn sparse array indices are a subset of the raw sparse array indices
    assert all([a == b for a, b in zip(node.raw.sparse_array.indices, node.syn.sparse_array.indices)]), \
           'The indices for the sparse array data should not have changed'
    assert all([a == b for a, b in zip(node.raw.sparse_array.indptr, node.syn.sparse_array.indptr)]), \
           'The indptrs for the sparse array data should not have changed'

    # Ensure that the shapes of the resulting histogram objects are all identical to the originals
    assert node.syn.shape == node.raw.shape
    assert node.syn.sparse_array.shape == node.raw.sparse_array.shape
    assert node.getDenseRaw().shape == node.getDenseSyn().shape, 'The syn histogram is incorrectly configured, it converts to a histogram of incorrect shape'


def test_create_sampled_rdd() -> None:
    node1: GeounitNode = makeNode(d1, '1234567890abcdef', geocodeDict)
    node2: GeounitNode = makeNode(d2, '1234567890abcdeg', geocodeDict)
    other_state_node: GeounitNode = makeNode(d1, '9934567890abcdeg', geocodeDict)
    fake_rdd = RDDLikeList([node1, node2, other_state_node])

    total_population = 10

    returned_rdd = BootstrapEngine.create_sampled_rdd('12', total_population, fake_rdd)

    assert sum([sum(node.syn.sparse_array.data) for node in returned_rdd.list]) == total_population, \
           'The total population of all blocks should stay the same'
