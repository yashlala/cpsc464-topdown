import numpy as np

from fractions import Fraction as Fr
from collections import defaultdict
from copy import deepcopy

from das_constants import CC

from programs.strategies.dhch_strategies import DecompTestStrategyDHCH_20220623

# This script file is intended to load the strategy DecompTestStrategyDHCH_20220623, compute the rho allocations for each geolevel, define new
# target geolevel rho allocations defined by reallocating 0.17694285714285712 from the state geolevel to the county geolevel, define a new
# strategy with these rho allocation targets by rescaling the rho allocations of each query group in each geolevel, and print this final
# output strategy. This is repeated to make a PR strategy as well. The printed output strategies are used to define DecompTestStrategyDHCH_20220713
# and DecompTestStrategyDHCH_20220713_PR in programs/strategies/dhch_strategies.py.

strat = DecompTestStrategyDHCH_20220623()
res = strat.make(None)
geolevels = res[CC.GEODICT_GEOLEVELS]

level_total_init = {lev:0 for lev in geolevels}
for level in geolevels:
    for key in [CC.QUERIESPROP, CC.UNITQUERIESPROP]:
        res[key][level] = np.array([float(xi) for xi in res[key][level]])
        level_total_init[level] += np.sum(res[key][level])

print(geolevels)
print(level_total_init)
print(np.sum(np.array([level_total_init[level] for level in geolevels])))

# Note: the code below was used to generate target geolevel precision allocations for 17.1. For the purposes of generating
# the strategy for DHC experiment iteration 19.1, the geolevel precision allocations from experiment iteration 17.1, i.e.,
# the array 'trgt' below, can be viewed as exogenous inputs. (In short the allocations below are intended to satisfy a few
# constraints such as trgt[0] = 0.016, trgt[k] > np.maximum(intended, actual)  for each k > 0, and np.sum(trgt) = 6.14.
# Note that the additive, rather than multiplicative, renormalization is used to ensure the last constraint holds to promote
# geolevel precision allocations with a lower variance. This was done because the standard deviation of the noise for a given
# primitive mechanism is approximately sqrt(1/rho_allocated_to_query), so there is decreasing returns (in terms of the
# inverse scale of the noise in the primitive mechanisms) to increasing rho allocated to particular query. This appears to
# be one of the few choices in the code below.

## intended/actual per geolevel total PLB allocations for first demonstration data product:
#intended = [0.0101, 0.4301, 0.78  , 0.4301, 0.4301, 0.4301, 1.1001, 0.265 ]
#actual = deepcopy(intended)
#actual.reverse()
## This vector will be our new per geolevel total PLB allocations:
#trgt = np.maximum(intended, actual)
## Set block to 0.0101 * 6.14/3.875  (ie: PLB of block in experiment iteration 13.1):
#trgt[0] = 0.016
## Add new plb to geolevels above block to make sure total PLB is 6.14 (ie: total PLB of experiment iteration 13.1):
#trgt[1:] = trgt[1:] + (6.14 - np.sum(trgt))/7

# Note that, the final term in the final line of the code above, i.e., (6.14 - np.sum(trgt))/7, is equal to 0.17694285714285712.
# The purpose of this experiment iteration is to move this precision from state to county geolevel:

trgt = np.array([level_total_init[level] for level in geolevels])

trgt[-2] -= 0.17694285714285712
trgt[-3] += 0.17694285714285712

print(f"New per geolevel total PLB allocations: {trgt}, and sum: {np.sum(trgt)}")

# New total rho for each geolevel:
level_total_new = {lev: lev_rho for lev, lev_rho in zip(geolevels, trgt)}
# Ratios of old to new query allocations for each geolevel:
ratios = {lev: level_total_new[lev] / level_total_init[lev] for lev in geolevels}

res1 = deepcopy(res)
res2 = deepcopy(res)

level_total_rounded = {lev:0 for lev in geolevels}
for level in geolevels:
    for key in [CC.QUERIESPROP, CC.UNITQUERIESPROP]:
        # The final allocations will all be fractions with denominator given by 10000:
        res1[key][level] = np.round(res1[key][level] * ratios[level] * 10000).astype(int)
        level_total_rounded[level] += np.sum(res1[key][level])
        res1[key][level] = tuple(Fr(xi, 10000) for xi in res1[key][level])

new_total_rho = np.sum([level_total_rounded[level] for level in geolevels])

keys = [CC.DPQUERIES, CC.QUERIESPROP, CC.UNITDPQUERIES, CC.UNITQUERIESPROP]
# Print US strategy and adjustment for TENVACGQ US allocation to ensure global rho is exactly 6.14:
print("\n\n US strategy (modulo small adjustment of US PLBs to ensure total PLB of 6.14): \n\n")
for key in keys:
    for level in geolevels:
        print(f"strategy[CC.{key}][\"{level}\"] = {res1[key][level]}")

print(f"\nNote: Remove a total of {new_total_rho - 61400}/10000 precision from US geolevel")

trgt_pr = deepcopy(trgt)
# zero out US and then reallocate PLB to other levels other than block to define Puerto Rico (PR) strategy:
trgt_pr[-1] = 0
trgt_pr[1:-1] = trgt_pr[1:-1] + trgt[-1]/6
print(f"New per geolevel total PLB allocations for PR: {trgt_pr}")

level_total_new = {lev: lev_rho for lev, lev_rho in zip(geolevels, trgt_pr)}
ratios = {lev: level_total_new[lev] / level_total_init[lev] for lev in geolevels}
level_total_rounded = {lev:0 for lev in geolevels}
for level in geolevels:
    for key in [CC.QUERIESPROP, CC.UNITQUERIESPROP]:
        res2[key][level] = np.round(res2[key][level] * ratios[level] * 10000).astype(int)
        level_total_rounded[level] += np.sum(res2[key][level])
        res2[key][level] = tuple(Fr(xi, 10000) for xi in res2[key][level])
new_total_rho = np.sum([level_total_rounded[level] for level in geolevels])

# Print PR strategy and adjustment for TENVACGQ in state geolevel to ensure global rho is exactly 6.14:
print("\n\n PR strategy (modulo small adjustment of State PLBs to ensure total PLB of 6.14): \n\n")
for key in keys:
    for level in geolevels[:-1]:
        # Note that the bug in this file was that 'res2' in the next line was replaced with 'res1'.
        # The result was that PR did not have the same global rho as the US.
        print(f"strategy[CC.{key}][\"{level}\"] = {res2[key][level]}")
print(f"\nNote: Remove a total of {new_total_rho - 61400}/10000 precision from State geolevel")
