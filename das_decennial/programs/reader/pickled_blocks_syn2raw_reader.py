"""
Reader module which can read protected data saved as pickled RDD in a previous run, and treat it
as raw data for the new run.
At this point (03/06/19), i t needs raw_housing to be saved, to recalculate invariants for upper geolevels.
If housing histogram is protected it would work too. It recalculates constraints and invariants with right hand sides
from the protected data, but those should be the same since they were held invariant.
"""
from operator import and_
import warnings
import logging
from typing import Dict, Any, Union
import numpy as np
from pyspark.sql import SparkSession
import programs.reader.table_reader
import das_utils
from programs.nodes.nodes import GeounitNode, getNodeAttr, hasNodeAttr
from das_constants import CC

class PickledBlockSyn2RawReader(programs.reader.table_reader.DASDecennialReader):
    """
    Implements a reader module which reads pickled RDDs, checks that it has synthetic data and geocodes,
    checks, that invariants can be reconstructed for (needs housing histogram), reconstructs constraints
    and invariants for the bottom level and compares to the saved ones if any.
    """
    def read(self):
        spark = SparkSession.builder.getOrCreate()
        pickled_path = self.getconfig(CC.PICKLEDPATH)

        self.annotate("Reading protected data")
        nodes_dict_rdd = spark.sparkContext.pickleFile(pickled_path)

        self.annotate("Validating the input data")

        for key, key_name in zip((CC.CONS, CC.INVAR, CC.GEOCODE, CC.SYN, CC.RAW_HOUSING),
                                 ("constaints", "invariants", "geocodes", "synthetic data", "raw_housing data")):

            self.validateNodeRDD(nodes_dict_rdd,
                                 failed_node_function=lambda rec, k=key: k not in rec,
                                 failed_msg=f"have no {key_name} saved",
                                 sample_function=lambda rec: getNodeAttr(rec, "geocode"),
                                 error=False)

        self.annotate("Converting the input data to geonodes, treating syn data as raw, possibly reconstructing constraints or invariants")
        nodes_rdd = nodes_dict_rdd.map(self.dict2node)

        das_utils.freeMemRDD(nodes_dict_rdd)

        self.annotate("Validating the geonodes")
        self.checkConsInvGeolevel(nodes_rdd)
        return nodes_rdd

    def checkConsInvGeolevel(self, nodes_rdd):
        """ Check that every node has constraints, invariants and all are on the bottom geolevel"""
        cons_exist = nodes_rdd.map(lambda node: bool(node.cons)).reduce(and_)
        if not cons_exist:
            msg = "Not all nodes have constraints!"
            warnings.warn(msg)
            self.log_warning_and_print(msg)

        inv_exist = nodes_rdd.map(lambda node: bool(node.invar)).reduce(and_)
        if not inv_exist:
            msg = "Not all nodes have invariants!"
            logging.error(msg)
            raise ValueError(msg)

        bottom_geolevel = nodes_rdd.map(lambda node: self.geocode_dict[len(node.geocode)] == self.setup.levels[0]).reduce(and_)
        if not bottom_geolevel:
            msg = f"Not all nodes geocode length corresponds to bottom geolevel {self.setup.levels[0]}!"
            warnings.warn(msg)
            self.log_warning_and_print(msg)

    def dict2node(self, s_node: Union[Dict[str, Any]]) -> GeounitNode:
        """
        Convert dictionary of node fields into geo node, substituting synthetic data as raw
        :rtype: nodes.GeounitNode
        :param s_node: saved node, either a Geounit node object or a GeounitNode constructor arguments dict, read from file, has node field values
        :return: geoNode
        """

        # These fields are needed for nodes to be reconstructed. Geocode is obvious, syn is to use as raw data, raw_housing to recreate invariants
        for key, msg_pref in zip((CC.GEOCODE, CC.SYN, CC.RAW_HOUSING),
                                 ("Geocodes", "Synthetic data", "Raw housing")):
            if not hasNodeAttr(s_node, key):
                raise ValueError(f"{msg_pref} should be present in the saved pickled nodes. Missing from geocode {getNodeAttr(s_node, CC.GEOCODE)}")

        # If geocodedict is saved, it should be the same. Otherwise, it's probably wrong config used
        if hasNodeAttr(s_node, CC.GEOCODEDICT):
            assert getNodeAttr(s_node, CC.GEOCODE) == self.geocode_dict, f"Saved geocode_dict ({getNodeAttr(s_node, CC.GEOCODE)}) is different from the one in config ({self.geocode_dict})!"

        # Check the shape of the saved syn histogram. If it is different, most likely it's the wrong config file
        assert getNodeAttr(s_node, CC.SYN).shape == self.setup.hist_shape, f"Saved histogram shape {getNodeAttr(s_node, CC.SYN).shape} does not correspond to schema in config {self.setup.schema}:{self.setup.hist_shape}!"

        # Create the node
        node = GeounitNode(geocode=getNodeAttr(s_node, CC.GEOCODE), raw=getNodeAttr(s_node, CC.SYN), raw_housing=getNodeAttr(s_node, CC.RAW_HOUSING), geocode_dict=self.geocode_dict)

        # Recreate invariants and constraints. Note, it is done from raw housing histogram (which is also needed further on when
        # in topdown procedure we add invariants pertaining to each level

        # Invariants
        recr_inv = self.setup.makeInvariants(raw=node.raw, raw_housing=node.raw_housing, invariant_names=self.invar_names)
        if hasNodeAttr(s_node, CC.INVAR):
            # invar is dictionary of numpy arrays. Have to use all() to get single boolean
            msg = "Saved invariants are different from reconstructed!"
            assert getNodeAttr(s_node, CC.INVAR).keys() == recr_inv.keys(), msg
            for inv_name in getNodeAttr(s_node, CC.INVAR).keys():
                assert np.array_equal(getNodeAttr(s_node, CC.INVAR)[inv_name], recr_inv[inv_name]), msg
        node.invar = recr_inv

        # Constraints
        recr_cons = self.setup.makeConstraints(hist_shape=(self.setup.hist_shape, self.setup.unit_hist_shape), invariants=recr_inv, constraint_names=self.cons_names)
        if hasNodeAttr(s_node, CC.CONS):
            assert getNodeAttr(s_node, CC.CONS) == recr_cons, "Saved constraints are different from reconstructed!"

        node.cons = recr_cons

        return node
