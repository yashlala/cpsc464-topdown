import numpy as np
import scipy.optimize

TARGET_PRECISION = 0.026465028355387527

geolevels = ["US", "State", "County", "Tract"]
s3_1a = [1/11]*11
s3_1b = [1/1024, 25/1024, 1/1024, 1/1024, 1/1024, 4/1024, 50/1024, 50/1024, 2/1024, 100/1024, 789/1024]
s3_2a = [1/7]*7
s3_2b = [1/1024, 11/1024, 1/1024, 1/1024, 4/1024, 22/1024, 984/1024]
strat_query_props = [s3_1a, s3_1b, s3_2a, s3_2b]
strat_names = ["3_1a", "3_1b", "3_2a", "3_2b"]
strats_dict = dict(zip(strat_names, strat_query_props))

def computeTwiceRho_singleGeolevel_fromGlobalScale(strat_name, global_scale):
    """
        For a run in which a single geolevel had geoprop = 1.0, for a fixed strategy with a known global_scale,
        compute and return 2 * rho, where rho is the zCDP per-geolevel rho for this geolevel (which is the same
        as the sum of the per-query rho for that geolevel; see Theorem 14, Cannone et al).

        NOTE: this function is used with runs that used config-file inputs predating #1490, i.e., this is for use
        when q**2 is the appropriate term for a proportion in computing rho, not just q.
    """
    query_props = strats_dict[strat_name]
    geolevel_prop = 1.0
    twice_rho = 2 * sum([q**2 for q in query_props]) * (geolevel_prop**2) / (global_scale**2)
    return twice_rho

def findGeopropsGlobalScaleFromTwiceRho(strat_name, num_levels, twiceRho, gScale_guess):
    """
        strat_name      : string indicating which strategy is in use (assumed symmetric across geolevels)
        num_levels      : [num geolevels of type1, num geolevels of type2, ... ]
        twiceRho        : [2*rho per geolevel of t1, 2*rho per geolevel of t2, ... ]

        (if twiceRho is not known, use helper function above when providing inputs)

        returns:        None (prints solved-for geolevel proportions and global_scale)
    """
    print(f"Finding geoprops and global_scale given num_levels {num_levels} and twiceRho {twiceRho}")
    query_props = strats_dict[strat_name]
    def root_fxns(geoprops_gScale):
        exprs = []
        for i, geoprop in enumerate(geoprops_gScale[:-1]):
            global_scale = geoprops_gScale[-1]
            exprs.append(2. * geoprop - twiceRho[i] * (global_scale**2))
        exprs.append(1. - sum([num * geoprop for num, geoprop in zip(num_levels, geoprops_gScale[:-1])]))
        return exprs
    x0 = [1./sum(num_levels) for i in range(len(num_levels))] + [gScale_guess]
    sol = scipy.optimize.root(root_fxns, x0) # TODO: consider replacing with method for constraining solution to (n-1)-simplex X R
    print(f"solved for geoprops, gScale: {sol.x}")
    assert np.isclose(sum([n*xi for n, xi in zip(num_levels, sol.x[:-1])]), 1.0), "Solved-for geoprops do not sum to 1.0."
    for i, g in enumerate(sol.x[:-1]):
        assert 0. <= g <= 1., f"Geolevel prop # {i}, with value {g}, is not in [0,1]."

if __name__ == "__main__":
    strat_name = "3_1a"
    twice_rho1 = 0.02646502892261148
    twice_rho2 = computeTwiceRho_singleGeolevel_fromGlobalScale(strat_name, 1214/1007)
    num_geolevels_of_type = [4, 1]
    findGeopropsGlobalScaleFromTwiceRho(strat_name, num_geolevels_of_type, [twice_rho1, twice_rho2], 1.)
