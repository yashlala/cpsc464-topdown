import pickle
import os, sys
import numpy as np
from fractions import Fraction

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.."))

import programs.constraints.Constraints_PL94 as cPL94
from programs.invariants.invariantsmaker import InvariantsMaker
from programs.nodes.nodes import GeounitNode
from programs.schema.schemas.schemamaker import SchemaMaker
from das_utils import table2hists
from programs.queries.querybase import QueryFactory
from programs.queries.constraints_dpqueries import DPquery
from programs.engine.primitives import RationalGeometricMechanism as GeometricMechanism
from das_constants import CC


d1 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 0, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 2],
                [1, 1, 0, 15, 3]
            ])


d2 = np.array([   # columns: 'hhgq', 'votingage', 'hispanic', 'cenrace', 'unique unitid' (shape 8,2,2,63 + unitUID)
                # each row is a person
                [0, 1, 1, 20, 0],
                [1, 1, 0, 1, 1],
                [3, 1, 0, 10, 2],
                [3, 0, 0, 15, 4],
                [1, 1, 0, 15, 3]
            ])

geocodeDict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State', 1: 'Root Geography'}
invn = ('tot', 'va', 'gqhh_vect', 'gqhh_tot', 'gq_vect')
consn = ("total", "hhgq_total_lb", "hhgq_total_ub", "voting_age", "hhgq_va_ub", "hhgq_va_lb", "nurse_nva_0")

def convertPL94(d):
    return table2hists(d, SchemaMaker.fromName(CC.SCHEMA_PL94), CC.ATTR_HHGQ)

def makeInvs(ph, invnames):
    return InvariantsMaker.make(schema=CC.SCHEMA_PL94, raw=ph[0], raw_housing=ph[1], invariant_names=invnames)

def makeCons(ph, consnames, inv_dict):
    return cPL94.ConstraintsCreator(hist_shape=(ph[0].shape, ph[1].shape), invariants=inv_dict,
                                    constraint_names=consnames).calculateConstraints().constraints_dict

def makeNode(d, geocode, geocode_dict, addsyn=False, dpq_make=False, querydict=None, consn=consn, invn=invn):
    ph = convertPL94(d)
    syn = ph[0].toDense() + np.ones(np.prod(ph[0].shape)).reshape(ph[0].shape) if addsyn else None
    dpqdict = {CC.DETAILED: DPquery(QueryFactory.makeTabularGroupQuery(array_dims=ph[0].shape), GeometricMechanism(.1, 2, ph[0].toDense()))} if dpq_make else {}
    if querydict:
        dpqdict.update({
                name: DPquery(query, GeometricMechanism(Fraction(1,10), 2, query.answer(ph[0].toDense())))
                for name, query in SchemaMaker.fromName(CC.SCHEMA_PL94).getQueries(querydict).items()
            })
    inv_dict = makeInvs(ph, invn)
    return GeounitNode(geocode, raw=ph[0], raw_housing=ph[1], syn=syn, cons=makeCons(ph, consn, inv_dict), invar=inv_dict,
                       geocode_dict=geocode_dict, dp_queries=dpqdict)

if __name__ == "__main__":
    node = makeNode(d1, "12345", geocodeDict)
    with open("geounitnode2.pickle", 'wb') as f:
        pickle.dump(node, f)
