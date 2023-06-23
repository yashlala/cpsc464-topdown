"""
    3/23/21: JMA requested re-runs of final tuned Redistricting Experiments DAS runs, with all Total Budget (epsilon)
    values increased to ~10.3 (global_scale 429/439), assigning in Type B strats the extra to BG-level Total:
    https://{GIT_HOST_NAME}/CB-DAS/das_decennial/issues/1256#issuecomment-25774
    Given a new global_scale(=429/439), old global_scale(>429/439), and extant strategy, this script solves for adjusted query
    and geolevel allocations that assign all excess rho from the new global_scale to the BG-level Total query.
"""
import numpy as np
from fractions import Fraction
from copy import deepcopy
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from das_constants import CC
import programs.strategies.strategies as strategies

ACTIVE_STRATEGY_NAME = "Strategy2b_St_Cty_isoTot"
GEOLEVELS = ["US", "State", "County", "Tract", "Block_Group", "Block"]
#GEOPROPS = [27/512, 67/256, 5/64, 27/512, 99/1024, 469/1024]
#OLD_GLOBAL_SCALE = 222/205
#NEW_GLOBAL_SCALE = 429/439
#GEOPROPS = [56/1024, 168/1024, 85/1024, 56/1024, 84/1024, 575/1024] # 1b opt
GEOPROPS = [54/1024, 268/1024, 80/1024, 54/1024, 99/1024, 469/1024] # 2b opt
#GEOPROPS = [95/1024, 189/1024, 59/512, 95/1024, 27/256, 419/1024] # 2b aian
OLD_GLOBAL_SCALE = 738/719
NEW_GLOBAL_SCALE = 429/439
#COUNTY_QPROPS = [342/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 3/1024, 3/1024, 1/1024, 11/1024, 659/1024] # 1b opt
COUNTY_QPROPS =  [342/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 677/1024] # 2b opt
#COUNTY_QPROPS = [205/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 407/512] # 2b aian
#STATE_QPROPS = [678/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 2/1024, 2/1024, 1/1024, 6/1024, 330/1024] # 1b opt
STATE_QPROPS = [815/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 204/1024] # 2b opt
#STATE_QPROPS = [509/1024, 1/1024, 1/1024, 1/1024, 1/1024, 1/1024, 255/512] # 2b aian
#CTY_ST_QNAMES = ['total', 'cenrace', 'hispanic', 'votingage', 'hhinstlevels', 'hhgq', 'hispanic * cenrace','votingage * cenrace','votingage * hispanic','votingage * hispanic * cenrace','detailed'] # 1b
CTY_ST_QNAMES = ["total", "hispanic * cenrace11cats", "votingage", "hhinstlevels",
                                "hhgq", "hispanic * cenrace11cats * votingage", "detailed"] # 2b

def safeZip(iterable1, iterable2):
    assert len(iterable1)==len(iterable2), f"len({iterable1})!=len({iterable2}). ({len(iterable1)} != {len(iterable2)})"
    return zip(iterable1, iterable2)

def to1024Fracs(values):
    """
     values expected to be either
             list[float, float, ..., float]
    or
        Fraction
    Returns list of Fractions or single Fraction converted to 1024 denominator, and
    slightly hacked to display with denominator exactly 1024.
    """
    if isinstance(values, list):
        fracs = [Fraction(1, 1) for value in values]
        for i, value in enumerate(values):
            new_value = Fraction(value)
            new_value._numerator = int(np.ceil(value * 1024))
            new_value._denominator = 1024
            fracs[i] = new_value
        return fracs
    else:
        assert isinstance(values, Fraction), f"{values} has unexpected type {type(values)}"
        assert np.floor(1024/values._denominator) - 1024/values._denominator == 0.0
        new_frac = Fraction(1, 1)
        new_frac._numerator = int((1024/values._denominator) * values._numerator)
        new_frac._denominator = 1024
        return new_frac

def makeQpropsByGeoLevsNested(qprops_by_geolevs_unnested, geolevels):
    """
        Converts dict mapping [data_structure_type][level] -> tuples (of qprops or qnames)
        to dict mapping [level][query_name] -> query_proportion_in_level
    """
    qprops_by_geolevs_nested = {}
    for level in geolevels:
        lev_qnames = qprops_by_geolevs_unnested[CC.DPQUERIES][level]
        lev_qprops = qprops_by_geolevs_unnested[CC.QUERIESPROP][level]
        qprops_by_geolevs_nested[level] = dict(safeZip(lev_qnames, lev_qprops))
    return qprops_by_geolevs_nested

def findNewAllocations(qprops_by_geolevs_dict, old_global_scale, new_global_scale, geolevels, geoprops,
                        target_query="total", target_geolevel="Block_Group"):
    """

    """
    ## Convert strategy dict to more convenient format: level -> qname -> qprop
    qprops_by_geolevels_dict = makeQpropsByGeoLevsNested(qprops_by_geolevs_dict, geolevels)
    print(f"qprops_by_geolevels_dict: ")
    for k, v in qprops_by_geolevels_dict.items():
        print(f"{k} --> {v}")

    ## Compute increase in global_rho
    old_global_rho = 1./(old_global_scale**2.)
    new_global_rho = 1./(new_global_scale**2.)
    extra_global_rho = new_global_rho - old_global_rho # Find extra rho to be added to target_query in target_geolevel
    assert extra_global_rho > 0., f"Additional rho is non-positive: {extra_global_rho}"
    print(f"extra_global_rho: {extra_global_rho}")

    ## Do allocations calculations for all geolevels: compute old rhos, update target_geolevel rho, re-normalize
    old_per_geolevel_rhos = {}
    for geolev, old_geoprop in safeZip(geolevels, geoprops):
        old_per_geolevel_rhos[geolev] = old_global_rho * old_geoprop

    new_target_geolevel_rho = old_per_geolevel_rhos[target_geolevel] + extra_global_rho # Extra rho to add to target level rho
    new_per_geolevel_rhos = deepcopy(old_per_geolevel_rhos)
    new_per_geolevel_rhos[target_geolevel] = new_target_geolevel_rho

    # Re-compute per-geolevel proportions, using new per-geolevel rho values
    new_geoprops = [per_geolev_rho/new_global_rho for per_geolev_rho in new_per_geolevel_rhos.values()]
    print(f"new_geoprops, float: {new_geoprops}")

    ## Do allocations calculations for queries in target_geolevel: compute old rhos, update target_query rho, re-normalize
    old_target_geolev_perq_rhos = {}
    for qname, old_qprop in qprops_by_geolevels_dict[target_geolevel].items():
        old_target_geolev_perq_rhos[qname] = old_per_geolevel_rhos[target_geolevel] * old_qprop

    old_target_query_in_target_geolev_rho = (old_per_geolevel_rhos[target_geolevel] *
                                                        qprops_by_geolevels_dict[target_geolevel][target_query])
    new_target_query_in_target_geolev_rho = old_target_query_in_target_geolev_rho + extra_global_rho

    new_target_geolev_perq_rhos = {}
    new_target_geolev_perq_rhos = deepcopy(old_target_geolev_perq_rhos)
    new_target_geolev_perq_rhos[target_query] = new_target_query_in_target_geolev_rho
    print(f"old geolev perq rhos: {old_target_geolev_perq_rhos}")
    print(f"new geolev perq rhos: {new_target_geolev_perq_rhos}")

    # New query proportions for each query in target_geolevel
    new_qprops_in_target_geolev = [new_perq_rho/new_target_geolevel_rho for new_perq_rho in new_target_geolev_perq_rhos.values()]
    assert np.isclose(sum(new_geoprops), 1.0, atol=1e-4)
    assert np.isclose(sum(new_qprops_in_target_geolev), 1.0, atol=1e-4)

    ## Convert to Fractions; force them to display with denominator 1024, for convenience
    new_geoprops_as_fracs = to1024Fracs(new_geoprops)
    new_qprops_in_target_geolev_as_fracs = to1024Fracs(new_qprops_in_target_geolev)
    assert np.all(np.array([float(f) for f in new_geoprops_as_fracs]) >= 1/1024), f"Min geoprop < 1/1024: {new_geoprops_as_fracs}"
    assert np.all(np.array([float(f) for f in new_qprops_in_target_geolev_as_fracs]) >= 1/1024), f"Min qprop < 1/1024: {new_qprops_in_target_geolev_as_fracs}"

    print(f"Sum of new (frac, unnormalized) geoprops: {sum(new_geoprops_as_fracs)}, i.e. {to1024Fracs(sum(new_geoprops_as_fracs))}")
    print(f"Sum of new (frac, unnormalized) qprops in {target_geolevel}: {sum(new_qprops_in_target_geolev_as_fracs)}, i.e. {to1024Fracs(sum(new_qprops_in_target_geolev_as_fracs))}")
    assert np.isclose(float(sum(new_geoprops_as_fracs)), 1.0, atol=1e-1) # Weak checks; can only guarantee close to 1+len/1024?
    assert np.isclose(float(sum(new_qprops_in_target_geolev_as_fracs)), 1.0, atol=1e-1)

    qnames_in_target_geolev = new_target_geolev_perq_rhos.keys()
    new_geoprops = dict(safeZip(geolevels, new_geoprops_as_fracs))
    new_qprops_in_target_geolev = dict(safeZip(qnames_in_target_geolev, new_qprops_in_target_geolev_as_fracs))
    print(f"RETURN: New geolevel proportions: {new_geoprops}")
    print(f"RETURN: New qprops for {target_geolevel}: {new_qprops_in_target_geolev}")

if __name__ == "__main__":
    geos_qs_props_dict = strategies.StrategySelector.strategies[ACTIVE_STRATEGY_NAME].make(GEOLEVELS)
    if COUNTY_QPROPS != []:
        geos_qs_props_dict["County"] = dict(zip(COUNTY_QPROPS, CTY_ST_QNAMES))
    if STATE_QPROPS != []:
        geos_qs_props_dict["State"] = dict(zip(STATE_QPROPS, CTY_ST_QNAMES))
    findNewAllocations(geos_qs_props_dict, OLD_GLOBAL_SCALE, NEW_GLOBAL_SCALE, GEOLEVELS, GEOPROPS, target_geolevel="Block_Group")
