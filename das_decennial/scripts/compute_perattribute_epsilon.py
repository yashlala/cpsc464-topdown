"""
    This file is a stand-alone script for computing the per-attribute epsilon (in an epsilon, delta sense) achieved by a given run of the DAS. Use of this script should rapidly be deprecated in favor of automatic calculation and printing of these values within each DAS run.
"""

from collections import defaultdict
import sys
sys.path.append("..")

import programs.engine.curve as curve
from programs.engine.budget import Budget
from programs.schema.schemas.schemamaker import SchemaMaker


queries = defaultdict(dict)
queries["2b"]['default'] = ["total", "hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq", "hispanic * cenrace11cats * votingage", "detailed"]
queries["2b"]['US'] = ["hispanic * cenrace11cats", "votingage", "hhinstlevels", "hhgq", "hispanic * cenrace11cats * votingage", "detailed"]

qprops = defaultdict(dict)
qprops["2b"]['default']    = [1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1018/1024]
qprops["2b"]['US']         = [1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1019/1024]
qprops["2b"]['State']      = [815/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 51/256]
qprops["2b"]['County']     = [171/512, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 677/1024]

attrs_to_queries = {}
attrs_to_queries["cenrace"] = ["cenrace", "hispanic * cenrace", "votingage * cenrace", "votingage * hispanic * cenrace", "detailed"]
attrs_to_queries["cenrace"] += ["hispanic * cenrace11cats", "hispanic * cenrace11cats * votingage"]
attrs_to_queries["hispanic"] = ["hispanic", "hispanic * cenrace", "votingage * hispanic", "votingage * hispanic * cenrace"]
attrs_to_queries["hispanic"] += ["detailed", "hispanic * cenrace11cats", "hispanic * cenrace11cats"]
attrs_to_queries["votingage"] = ["votingage", "votingage * cenrace", "votingage * hispanic", "votingage * hispanic * cenrace"]
attrs_to_queries["votingage"] += ["detailed", "hispanic * cenrace11cats * votingage"]
attrs_to_queries["hhgq"] = ["hhinstlevels", "hhgq", "detailed"]

geolevels = ["US", "State", "County", "Tract", "Block_Group", "Block"]
geolevel_props = [27/512, 67/256, 5/64, 27/512, 99/1024, 469/1024]

global_scale = 222/205

MODE = "attrs" # 'attrs' or 'geolevels'

def get_q_props_for_attr(strat_name, attr):
    this_attr_q_props = defaultdict(list)
    if attr != "*":
        for level in geolevels:
            cur_qnames = queries[strat_name][level] if level in queries[strat_name].keys() else queries[strat_name]['default']
            cur_qprops = qprops[strat_name][level] if level in qprops[strat_name].keys() else qprops[strat_name]['default']
            for qname, q_prop in zip(cur_qnames, cur_qprops):
                if qname in attrs_to_queries[attr]:
                    this_attr_q_props[level].append(q_prop)
    else:
        for level in geolevels:
            cur_qprops = qprops[strat_name][level] if level in qprops[strat_name].keys() else qprops[strat_name]['default']
            this_attr_q_props[level] = cur_qprops
    return this_attr_q_props

def get_q_props_for_geolevels(strat_name, cur_list_of_geolevels):
    these_geolevels_qprops = defaultdict(list)
    for level in cur_list_of_geolevels:
        cur_qprops = qprops[strat_name][level] if level in qprops[strat_name].keys() else qprops[strat_name]['default']
        these_geolevels_qprops[level] = cur_qprops
    return these_geolevels_qprops

if __name__ == "__main__":
    strat_name = "2b"
    delta = 1e-10
    MODE = "attrs" # Calculate semantics for geolevels or for demographic attrs?

    if MODE == "attrs":
        list_of_attrs = list(attrs_to_queries.keys())
        list_of_geolevels = ["*"] # Asterisk is treated as wildcard geolevel; all geolevels match it
    elif MODE == "geolevels":
        list_of_attrs = ["*"] # Asterisk is treated as wildcard attr; all queries (within a geolevel) match it
        list_of_geolevels = geolevels
    else:
        raise ValueError(f"MODE {MODE} not recognized.")

    print(f"------\nComputing epsilon implied by global_scale, delta\n------")
    for i in range(len(list_of_geolevels)):
        cur_list_of_geolevels = list_of_geolevels[-1 * (i+1):] # Grab last i geolevels: [B], then [BG, B], then [TR, BG, B]...
        for attr in list_of_attrs:
            if MODE == "attrs":
                gl4dict = geolevels
                q_props = get_q_props_for_attr(strat_name, attr)
            else:
                gl4dict = cur_list_of_geolevels
                q_props = get_q_props_for_geolevels(strat_name, cur_list_of_geolevels)
            print(f"\nFor {attr}, relevant query proportions: {q_props}")
            geolevel_props_tmp = [p if (gn in cur_list_of_geolevels or '*' in cur_list_of_geolevels) else -1 for gn, p in zip(geolevels, geolevel_props)]
            geolevel_props_tmp = [p for p in geolevel_props_tmp if p != -1] # Retains geolevel proportions for the current subset
            print(f"For {cur_list_of_geolevels}, relevant geolevel proportions: {geolevel_props_tmp}")
            geo_allocations_dict = {}
            assert len(gl4dict) == len(geolevel_props_tmp)
            for geolevel, gprop in zip(gl4dict, geolevel_props_tmp):  # TODO: Check this, line 80 may make lengths unequal
                geo_allocations_dict[geolevel] = gprop, q_props[geolevel]
            gaussian_mechanism_curve = curve.zCDPEpsDeltaCurve(geo_allocations_dict, verbose=False)
            computed_epsilon = gaussian_mechanism_curve.get_epsilon(delta, global_scale, bounded=True,
                                                                                    tol=1e-7, zcdp=True)
            eps_msg = f"strat: {strat_name}, delta: {delta}, global_scale: {global_scale}"
            eps_msg += f"\ngeolevel props: {geolevel_props_tmp}"
            eps_msg += f"\nquery props: {q_props}"
            eps_msg += f"\nattribute: {attr}"
            eps_msg += f"\ncomputed epsilon: {computed_epsilon}"
            print(eps_msg)

    print(f"----\nComputing using functions in production code\n ----")

    geolevel_prop_budgets_dict = dict(zip(geolevels, geolevel_props))
    qps = qprops[strat_name]
    qns = queries[strat_name]
    dp_query_prop = dict((gl, qps[gl]) if gl in qps else (gl, qps['default']) for gl in geolevels)
    query_names   = dict((gl, qns[gl]) if gl in qns else (gl, qns['default']) for gl in geolevels)
    levels = list(reversed(list(geolevel_prop_budgets_dict)))

    schema = SchemaMaker.fromName("PL94")

    def query_iter(gl):
        for qname, qprop in zip(query_names[gl], dp_query_prop[gl]):
            yield schema.getQuery(qname), qprop

    attr_query_props = Budget.getAttrQueryProps(levels, schema.dimnames, query_iter)

    def getEpsFromGeoAlloc(geo_all_dict):
        return curve.zCDPEpsDeltaCurve(geo_all_dict, verbose=False).get_epsilon(float(delta), global_scale, bounded=True, tol=1e-7,zcdp=True)

    per_attr_epsilons, per_geolevel_epsilons = Budget.getPerAttrEpsilonFromProportions(attr_query_props, getEpsFromGeoAlloc, levels,
                                                                                           geolevel_prop_budgets_dict, dp_query_prop)

    for attr, eps in per_attr_epsilons.items():
        print(f"For single attr/dim {attr} semantics, zCDP-implied epsilon: {eps} (approx {float(eps):.2f})")
    for level, eps in per_geolevel_epsilons.items():
        print(f"For geolevel semantics protecting {levels[0]} within {level}, zCDP-implied epsilon: {eps} (approx {float(eps):.2f})")