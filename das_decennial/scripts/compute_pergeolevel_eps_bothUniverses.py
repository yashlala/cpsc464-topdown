"""
    This file is a stand-alone script for computing the per-attribute epsilon (in an epsilon, delta sense) achieved by a given run of the DAS. Use of this script should rapidly be deprecated in favor of automatic calculation and printing of these values within each DAS run.
"""
from collections import defaultdict
from math import sqrt
import sys
sys.path.append("..")
import programs.engine.curve as curve
from programs.engine.budget import Budget
from programs.schema.schemas.schemamaker import SchemaMaker

queries = defaultdict(dict)
queries["NA"]['default']    = ["detailed"] # Just a placeholder, irrelevant to geolevels epsilon calculations

qprops = defaultdict(lambda: defaultdict(lambda: [1/1]))
qprops["NA"]['default']     = [1/1]

if __name__ == "__main__":
    strat_name = "NA"
    delta = 1e-10

    print(f"----\nComputing using functions in production code\n ----")
    # NOTE: to use script, uncomment & fill in the below
    #persons_geolevel_props = [104/4099, 1440/4099, 447/4099, 687/4099, 1256/4099, 165/4099] # PATCHED_COMFORT / US Persons [4 of 6]
    #persons_global_scale = 339/542 # PATCHED_COMFORT / US Persons [4 of 6]
    #units_geolevel_props = [20/4100, 20/4100, 350/4100, 1456/4100, 1759/4100, 495/4100] # SALTY_PICTURE / US Units [1 of 6]
    #units_global_scale = 12343/3379 # SALTY_PICTURE / US Units [1 of 6]
    #geolevels = ["US", "State", "County", "Tract", "Block_Group", "Block"] # US

    persons_geolevel_props = [689/4099, 695/4099, 772/4099, 1778/4099, 165/4099] # FIRM_FRIEND / PR Persons [4 of 6]
    persons_global_scale = 339/542 # FIRM_FRIEND / PR Persons [4 of 6]
    units_geolevel_props = [15141/2629740, 224952/2629740, 944510/2629740,
                                1127644/2629740, 317493/2629740] # CLANGING_FUNNY / PR Units [1 of 6]
    units_global_scale = 12343/3379 # CLANGING_FUNNY / PR Units [1 of 6]
    geolevels = ["State", "County", "Tract", "Block_Group", "Block"] # PR

    assert len(units_geolevel_props)==len(persons_geolevel_props), f"Units, Persons geolevel lists of unequal lengths."
    units_rho = 1/units_global_scale**2
    persons_rho = 1/persons_global_scale**2
    units_geolevel_rhos = [units_rho * geolevel_prop for geolevel_prop in units_geolevel_props]
    persons_geolevel_rhos = [persons_rho * geolevel_prop for geolevel_prop in persons_geolevel_props]
    geolevel_props = [(urho + prho) / (units_rho + persons_rho) for urho, prho in zip(units_geolevel_rhos, persons_geolevel_rhos)]
    global_rho = units_rho + persons_rho
    global_scale = 1/sqrt(global_rho)

    print(f"units_rho: {units_rho}, persons_rho: {persons_rho}, global_rho: {global_rho}")
    print(f"units_geolevel_rhos: {units_geolevel_rhos} sum: {sum(units_geolevel_rhos)}")
    print(f"geolevel_props: {geolevel_props}, sum -> {sum(geolevel_props)}")
    print(f"persons_geolevel_rhos: {persons_geolevel_rhos} sum: {sum(persons_geolevel_rhos)}")

    geolevel_prop_budgets_dict = dict(zip(geolevels, geolevel_props))
    qps = qprops[strat_name]
    qns = queries[strat_name]
    dp_query_prop = dict((gl, qps[gl]) if gl in qps else (gl, qps['default']) for gl in geolevels)
    query_names   = dict((gl, qns[gl]) if gl in qns else (gl, qns['default']) for gl in geolevels)
    levels = list(reversed(list(geolevel_prop_budgets_dict)))
    print(f"dp_query_prop: {dp_query_prop}")
    for k, vals in dp_query_prop.items():
        print(f"{k} : sum -> {sum(vals)}, len -> {len(vals)}")
    print(f"query_names: {query_names}")
    for k, qnames in query_names.items():
        print(f"{k} : len -> {len(qnames)}")

    schema = SchemaMaker.fromName("PL94") # NOTE: units is actually H1, but irrelevant for geolevel calculations

    def query_iter(gl):
        for qname, qprop in zip(query_names[gl], dp_query_prop[gl]):
            yield schema.getQuery(qname), qprop

    attr_query_props = Budget.getAttrQueryProps(levels, schema.dimnames, query_iter)
    print(f"attr_query_props: {attr_query_props}")

    def getEpsFromGeoAlloc(geo_all_dict):
        return curve.zCDPEpsDeltaCurve(geo_all_dict, verbose=False).get_epsilon(float(delta), global_scale,
                                        bounded=True, tol=1e-7, zcdp=True)

    per_attr_epsilons, per_geolevel_epsilons = Budget.getPerAttrEpsilonFromProportions(attr_query_props, getEpsFromGeoAlloc, levels,
                                                                                        geolevel_prop_budgets_dict, dp_query_prop)

    #for attr, eps in per_attr_epsilons.items():
    #    print(f"For single attr/dim {attr} semantics, zCDP-implied epsilon: {eps} (approx {float(eps):.2f})")
    for level, eps in per_geolevel_epsilons.items():
        print(f"For geolevel semantics protecting {levels[0]} within {level}, zCDP-implied epsilon: {eps} (approx {float(eps):.2f})")

    geo_allocations_dict = {}
    for level, geoprop in geolevel_prop_budgets_dict.items():
        geo_allocations_dict[level] = (geoprop, qps[level])
        print(f"geo_allocations_dict[{level}] = ({geoprop}, {qps[level]})")
    global_eps = curve.zCDPEpsDeltaCurve(geo_allocations_dict, verbose=False).get_epsilon(float(delta), global_scale,
                                        bounded=True, tol=1e-7, zcdp=True)
    print(f"Global Epsilon over all levels: {global_eps}")
