"""
    Simple script for computing the global_scale equivalent to a given epsilon at a given delta.
"""

import sys
sys.path.append("..")
import programs.engine.curve as curve

queries = {}
queries["H1"] = ["occupied_vacant"]
query_props = [1.0]

if __name__ == "__main__":
    strat_name = "H1"
    epsilon = 0.5
    delta = 1e-10

    geolevels = ["US", "State", "County", "Tract", "Block_Group", "Block"]
    geolevel_props = [20/100, 16/100, 16/100, 16/100, 16/100, 16/100]
    geolevel_props = [g**2 for g in geolevel_props] # Needed if computing for DGM with old accounting
    geo_allocations_dict = {}
    for geolevel, gprop in zip(geolevels, geolevel_props):
        geo_allocations_dict[geolevel] = gprop, query_props
    gaussian_mechanism_curve = curve.zCDPEpsDeltaCurve(geo_allocations_dict, verbose=False)
    computed_global_scale = gaussian_mechanism_curve.get_scale(epsilon, delta, bounded=True,
                                                                                    tol=1e-7, zcdp=True)
    gs_msg = f"strat: {strat_name}, epsilon: {epsilon}, delta: {delta}"
    gs_msg += f"\nquery_props: {query_props}"
    gs_msg += f"\ngeolevel_props: {geolevel_props}"
    gs_msg += f"\ncomputed global_scale: {computed_global_scale}"
    print(gs_msg)
