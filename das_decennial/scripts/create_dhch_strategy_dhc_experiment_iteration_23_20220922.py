import numpy as np
from copy import deepcopy
from collections import defaultdict
from fractions import Fraction as Fr
from das_constants import CC


# The change parameters for this experiment iteration are listed below. Parameters that begin with "basis_point_multiplier" multiply the previous rho allocation, and those that start with "init_basis_points" are initial allocations for new query groups.

# DHCP change parameters:
basis_point_multiplier_popSehsdTargetsRelship_tract_and_county = 4
basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg = 4

# DHCH change parameters:
init_basis_points_coupled_hh_type_hisp_hhtenshort_2lev_tract = 1500
init_basis_points_partner_type_detailed_own_child_status_sex_hhtenshort_2lev_tract = 1500
init_basis_points_hisp_hhtenshort_2lev_race_tract = 1500
init_basis_points_hisp_hhtenshort_2lev_race_county_prim_tsg = 1500
basis_point_multiplier_coupled_hh_type_hisp_hhtenshort_2lev_county = 4
basis_point_multiplier_partner_type_detailed_own_child_status_sex_hhtenshort_2lev = 2

tenk = 10000

# The next two strategies were created by running the US DHC experiment iteration 22 strategies through scripts/strat2df.py, to make all
# denominators of precision allocations equal to 10000 for readability and also, in the case of DHCH only, updating/adding query names
# to correspond to those used in DHC experiment iteration 23.

class DecompTestStrategyDHCP_20220922_exp_iteration_23_1:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = {}
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES] = {
            "Block": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "Block_Group": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "Tract_Subset": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "Tract_Subset_Group": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "Prim": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "County": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "State": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
            "US": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_38_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_40_groups * hispanic * cenrace * sex", "detailed", ),
        }
        strategy[CC.QUERIESPROP] = {
            "Block": (Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), ),
            "Block_Group": (Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), ),
            "Tract_Subset": (Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868 * basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg, 10000), Fr(868 * basis_point_multiplier_popSehsdTargetsRelship_tract_and_county, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), ),
            "Tract_Subset_Group": (Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478 * basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), ),
            "Prim": (Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478 * basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), ),
            "County": (Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310 * basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg, 10000), Fr(310 * basis_point_multiplier_popSehsdTargetsRelship_tract_and_county, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), ),
            "State": (Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999 * basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), ),
            "US": (Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73 * basis_point_multiplier_gq_constr_groups_age_10_groups_above_bg, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), ),
        }
        strategy[CC.UNITDPQUERIES] = {
            "Block": (),
            "Block_Group": (),
            "Tract_Subset": (),
            "Tract_Subset_Group": (),
            "Prim": (),
            "County": (),
            "State": (),
            "US": (),
        }
        strategy[CC.UNITQUERIESPROP] = {
            "Block": (),
            "Block_Group": (),
            "Tract_Subset": (),
            "Tract_Subset_Group": (),
            "Prim": (),
            "County": (),
            "State": (),
            "US": (),
        }
        return strategy


class DecompTestStrategyDHCH_20220922_exp_iteration_23_1:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels

        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Block_Group"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'coupled_hh_type * hisp * hhtenshort_2lev', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["US"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')

        strategy[CC.QUERIESPROP] = {
            "Block": (Fr(40, 10000), Fr(40, 10000), Fr(40, 10000), ),
            "Block_Group": (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), ),
            "Tract_Subset": (Fr(3090, 10000), Fr(3090, 10000), Fr(3090, 10000), Fr(init_basis_points_coupled_hh_type_hisp_hhtenshort_2lev_tract, 10000), Fr(init_basis_points_partner_type_detailed_own_child_status_sex_hhtenshort_2lev_tract, 10000), Fr(init_basis_points_hisp_hhtenshort_2lev_race_tract, 10000), ),
            "Tract_Subset_Group": (Fr(973, 10000), Fr(214, 10000), Fr(init_basis_points_hisp_hhtenshort_2lev_race_county_prim_tsg, 10000), Fr(973 * basis_point_multiplier_partner_type_detailed_own_child_status_sex_hhtenshort_2lev, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), ),
            "Prim": (Fr(973, 10000), Fr(214, 10000), Fr(init_basis_points_hisp_hhtenshort_2lev_race_county_prim_tsg, 10000), Fr(973 * basis_point_multiplier_partner_type_detailed_own_child_status_sex_hhtenshort_2lev, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), ),
            "County": (Fr(986, 10000), Fr(266, 10000), Fr(init_basis_points_hisp_hhtenshort_2lev_race_county_prim_tsg, 10000), Fr(986 * basis_point_multiplier_partner_type_detailed_own_child_status_sex_hhtenshort_2lev, 10000), Fr(986 * basis_point_multiplier_coupled_hh_type_hisp_hhtenshort_2lev_county, 10000), Fr(1380, 10000), Fr(1380, 10000), Fr(266, 10000), Fr(1380, 10000), ),
            "State": (Fr(714, 10000), Fr(193, 10000), Fr(191, 10000), Fr(714 * basis_point_multiplier_partner_type_detailed_own_child_status_sex_hhtenshort_2lev, 10000), Fr(714, 10000), Fr(3216, 10000), Fr(3216, 10000), Fr(193, 10000), Fr(3931, 10000), ),
            "US": (Fr(792, 10000), Fr(212, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(257, 10000), Fr(257, 10000), Fr(212, 10000), Fr(1049, 10000), ),
        }
        strategy[CC.UNITDPQUERIES] = {
            "Block": ("tenvacgq", ),
            "Block_Group": ("tenvacgq", ),
            "Tract_Subset": ("tenvacgq", ),
            "Tract_Subset_Group": ("tenvacgq", ),
            "Prim": ("tenvacgq", ),
            "County": ("tenvacgq", ),
            "State": ("tenvacgq", ),
            "US": ("tenvacgq", ),
        }
        strategy[CC.UNITQUERIESPROP] = {
            "Block": (Fr(40, 10000), ),
            "Block_Group": (Fr(1065, 10000), ),
            "Tract_Subset": (Fr(3090, 10000), ),
            "Tract_Subset_Group": (Fr(973, 10000), ),
            "Prim": (Fr(973, 10000), ),
            "County": (Fr(1380, 10000), ),
            "State": (Fr(3216, 10000), ),
            "US": (Fr(259, 10000), ),
        }
        return strategy


dhcp_strat = DecompTestStrategyDHCP_20220922_exp_iteration_23_1().make(None)
dhch1_strat = DecompTestStrategyDHCH_20220922_exp_iteration_23_1().make(None)
geolevels = dhch1_strat[CC.GEODICT_GEOLEVELS]
prop_keys = [CC.QUERIESPROP, CC.UNITQUERIESPROP]

def make_float_strat_and_levels_total(strat):
    strat = deepcopy(strat)
    level_total_init = {lev:0 for lev in geolevels}
    for level in geolevels:
        for key in prop_keys:
            strat[key][level] = np.array([float(xi) for xi in strat[key][level]])
            level_total_init[level] += np.sum(strat[key][level])
    level_totals = np.array([level_total_init[level] for level in geolevels])
    return strat, level_total_init, level_totals

dhcp_strat, level_total_init_dhcp, dhcp_level_totals = make_float_strat_and_levels_total(dhcp_strat)

# US DHCH strategy 23.1 is equal to 23.2 but with the strategy at tract_subset and below redefined to be equal to the strategy for county in strategy 23.2,
# after renormalizing so that the total allocation of each geolevel is not changed modulo rounding differences:
dhch1_strat, level_total_init_dhch1, dhch1_level_totals = make_float_strat_and_levels_total(dhch1_strat)
dhch2_strat, level_total_init_dhch2, dhch2_level_totals = make_float_strat_and_levels_total(dhch1_strat)

for key in prop_keys:
    for level in ["Block","Block_Group","Tract_Subset"]:
        dhch1_strat[key][level] = dhch1_strat[key]["County"] * np.sum([np.sum(dhch2_strat[key_in][level]) for key_in in prop_keys]) / np.sum([np.sum(dhch2_strat[key_in]["County"]) for key_in in prop_keys])

for key in [CC.DPQUERIES, CC.UNITDPQUERIES]:
    for level in ["Block","Block_Group","Tract_Subset"]:
        dhch1_strat[key][level] = dhch1_strat[key]["County"]


def print_strat(strat, strat_name, new_total_rho, skip_us=False):
    print(f"\n{strat_name} total rho is {new_total_rho} and strat is:")
    for key in [CC.DPQUERIES, CC.UNITDPQUERIES]:
        for level in geolevels:
            if level == geolevels[-1] and skip_us:
                continue
            print(f"strategy[CC.{key}][\"{level}\"] = {strat[key][level]}")

    for key in prop_keys:
        for level in geolevels:
            if level == geolevels[-1] and skip_us:
                continue

            if len(strat[key][level]) > 1:
                tuple_props = "(" + f", {tenk}), ".join([ f"Fr({xi}" for xi in strat[key][level]]) + f", {tenk}))"
            elif len(strat[key][level]) == 1:
                tuple_props = f"(Fr({strat[key][level][0]}, {tenk}), )"
            else:
                tuple_props = "()"
            print(f"strategy[CC.{key}][\"{level}\"] = " + tuple_props)


def print_us(strat, strat_name):
    strat = deepcopy(strat)
    level_total_rounded = {lev:0 for lev in geolevels}
    for level in geolevels:
        for key in prop_keys:
            # The final allocations will all be fractions with denominator given by 10000:
            strat[key][level] = np.round(strat[key][level] * tenk).astype(int)
            level_total_rounded[level] += np.sum(strat[key][level])
    new_total_rho = np.sum([level_total_rounded[level] for level in geolevels])
    print_strat(strat, strat_name, new_total_rho)
    return new_total_rho


def print_pr(us_strat, level_totals, strat_name):
    trgt_pr = deepcopy(level_totals)
    pr_strat = deepcopy(us_strat)
    # zero out US and then reallocate PLB to other levels other than block to define Puerto Rico (PR) strategy:
    trgt_pr[1:-1] = trgt_pr[1:-1] + trgt_pr[-1] / 6
    trgt_pr[-1] = 0
    ratios = np.array([trgt_pr[k]/np.sum([np.sum(pr_strat[key_in][level]) for key_in in prop_keys]) for k, level in enumerate(geolevels)])
    level_total_rounded = {lev:0 for lev in geolevels}
    for level_ind, level in enumerate(geolevels):
        for key in prop_keys:
            pr_strat[key][level] = np.round(pr_strat[key][level] * ratios[level_ind] * tenk).astype(int)
            level_total_rounded[level] += np.sum(pr_strat[key][level])
    new_total_rho = np.sum([level_total_rounded[level] for level in geolevels])
    print_strat(pr_strat, strat_name, new_total_rho, skip_us=True)
    return new_total_rho


pr_dhcp_tot = print_pr(dhcp_strat, dhcp_level_totals, "PR DHCP")
pr_dhch1_tot = print_pr(dhch1_strat, dhch1_level_totals, "PR DHCH1")
pr_dhch2_tot = print_pr(dhch2_strat, dhch2_level_totals, "PR DHCH2")

us_dhcp_tot = print_us(dhcp_strat, "US DHCP")
us_dhch1_tot = print_us(dhch1_strat, "US DHCH1")
us_dhch2_tot = print_us(dhch2_strat, "US DHCH2")

print("pr global rhos:")
print(pr_dhcp_tot, pr_dhch1_tot, pr_dhch2_tot)

print("us global rhos:")
print(us_dhcp_tot, us_dhch1_tot, us_dhch2_tot)

# These final print statements are:
# pr global rhos:
# 49624 77009 77008
# us global rhos:
# 49622 77005 77004

# Change final geolevel and final query group allocation in print output of
# this script so that final precision basis points are:
# DHCH1: 77005
# DHCH2: 77005
# DHCP: 49622
