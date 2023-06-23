from das_constants import CC
from fractions import Fraction as Fr
from collections import defaultdict
import numpy as np


class USLevelStrategy:
    """
    Generic parent class for strategies that can be used with US-level runs. It has the levels attribute, but at this point
    it is only used by the unit tests. In actual DAS runs, levels are supplied from reading the config file
    """
    levels = CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE, CC.GEOLEVEL_US


class DHCHStrategy(USLevelStrategy):
    schema = CC.SCHEMA_DHCH


class DHCHFullTenureStrategy(USLevelStrategy):
    schema = CC.SCHEMA_DHCH_FULLTENURE


class DHCHStrategyTen3Level(USLevelStrategy):
    schema = CC.SCHEMA_DHCH_TEN_3LEV


class TestStrategy(DHCHStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))  # So as to avoid having to specify empty dicts and defaultdicts
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                "sex * hhage",
                "sex * hisp * race * hhtype_dhch",
                "elderly * sex * hhtype_dhch",
                "hisp * race",
                "hhage * hhtype_dhch * sex",
                "detailed"),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 100) for num in (20, 20, 20, 15, 10, 10))),
            # CC.DPQUERIES: {},
            # CC.QUERIESPROP: {},
            # CC.UNITDPQUERIES: {},
            # CC.UNITQUERIESPROP: {},
            # CC.VACANCYDPQUERIES: {},
            # CC.VACANCYQUERIESPROP: {},
        })

        def queries(level):
            return test_strategy[CC.DPQUERIES + "default"]

        def allocation(level):
            if level == CC.GEOLEVEL_COUNTY:
                return tuple(Fr(num, 100) for num in (10, 30, 20, 15, 10, 10))
            if level == CC.GEOLEVEL_TRACT:
                return tuple(Fr(num, 100) for num in ( 5, 35, 20, 15, 10, 10))
            if level == CC.GEOLEVEL_BLOCK_GROUP:
                return tuple(Fr(num, 100) for num in ( 1, 39, 20, 15, 10, 10))
            if level == CC.GEOLEVEL_BLOCK:
                return tuple(Fr(num, 100) for num in (39,  1, 20, 15, 10, 10))
            return test_strategy[CC.QUERIESPROP + "default"]

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = queries(level)
            test_strategy[CC.QUERIESPROP][level] = allocation(level)
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq", "vacantoccupied", )
            test_strategy[CC.UNITQUERIESPROP][level] = (Fr(4, 100), Fr(1, 100), )

        return test_strategy


class SingleStateTestStrategyRegularOrdering:
    @staticmethod
    def make(levels):
        # levels = (CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE)
        ordering = {

            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ("hisp * race", "vacantoccupied", "tenvacgq"),
                    1: ("sex * hisp * race * hhtype_dhch",),
                    2: ("elderly * sex * hhtype_dhch",),
                    3: ("sex * hhage",)
                },
                1: {
                    0: ("hhage * hhtype_dhch * sex",),
                    1: ("detailed",)
                }
            },

            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ("hisp * race", "vacantoccupied", "tenvacgq"),
                    1: ("sex * hisp * race * hhtype_dhch",),
                    2: ("elderly * sex * hhtype_dhch",),
                    3: ("sex * hhage",)
                },
                1: {
                    0: ("hhage * hhtype_dhch * sex",),
                    1: ("detailed",)
                }

            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ("hisp * race",)
                },
                1: {
                    0: ("hhage * hhtype_dhch * sex", "detailed",)
                }
            }

        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        return query_ordering


class TestStrategyFullTenure(DHCHFullTenureStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))  # So as to avoid having to specify empty dicts and defaultdicts
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                "sex * hhage",
                "sex * hisp * race * hhtype_dhch",
                "elderly * sex * hhtype_dhch",
                "hisp * race",
                "hhage * hhtype_dhch * sex",
                "sex * hhage * hisp * race * elderly * tenure_2lev * hhtype_dhch"),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 100) for num in (20, 20, 20, 15, 10, 10))),
            # CC.DPQUERIES: {},
            # CC.QUERIESPROP: {},
            # CC.UNITDPQUERIES: {},
            # CC.UNITQUERIESPROP: {},
        })

        def queries(level):
            return test_strategy[CC.DPQUERIES + "default"]

        def allocation(level):
            if level == CC.GEOLEVEL_COUNTY:
                return tuple(Fr(num, 100) for num in (10, 30, 20, 15, 10, 10))
            if level == CC.GEOLEVEL_TRACT:
                return tuple(Fr(num, 100) for num in ( 5, 35, 20, 15, 10, 10))
            if level == CC.GEOLEVEL_BLOCK_GROUP:
                return tuple(Fr(num, 100) for num in ( 1, 39, 20, 15, 10, 10))
            if level == CC.GEOLEVEL_BLOCK:
                return tuple(Fr(num, 100) for num in (39,  1, 20, 15, 10, 10))
            return test_strategy[CC.QUERIESPROP + "default"]

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = queries(level)
            test_strategy[CC.QUERIESPROP][level] = allocation(level)
            test_strategy[CC.UNITDPQUERIES][level] = ("vacant", "vacs",)   # These are separate budget, so impact is not going to be checked
            test_strategy[CC.UNITQUERIESPROP][level] = (Fr(4, 100), Fr(1, 100),)


        return test_strategy


class SingleStateTestStrategyRegularOrderingFullTenure:
    @staticmethod
    def make(levels):
        # levels = (CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE)
        ordering = {

            CC.L2_QUERY_ORDERING: {
                0: {0: ("vacant",)},
                1: {
                    0: ("hisp * race", "vacant", "vacs",),
                    1: ("sex * hisp * race * hhtype_dhch",),
                    2: ("elderly * sex * hhtype_dhch",),
                    3: ("sex * hhage",)
                },
                2: {
                    0: ("hhage * hhtype_dhch * sex",),
                    1: ("sex * hhage * hisp * race * elderly * tenure_2lev * hhtype_dhch",)
                }
            },

            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("vacant",)},
                1: {
                    0: ("hisp * race", "vacant", "vacs",),
                    1: ("sex * hisp * race * hhtype_dhch",),
                    2: ("elderly * sex * hhtype_dhch",),
                    3: ("sex * hhage",)
                },
                2: {
                    0: ("hhage * hhtype_dhch * sex",),
                    1: ("sex * hhage * hisp * race * elderly * tenure_2lev * hhtype_dhch",)
                }

            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("total",)},
                1: {
                    0: ("hisp * race",)
                },
                2: {
                    0: ("hhage * hhtype_dhch * sex", "sex * hhage * hisp * race * elderly * tenure_2lev * hhtype_dhch",)
                }
            }

        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }
        return query_ordering


class DecompTestStrategyDHCH(DHCHStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,

            CC.DPQUERIES + "default": ("sex * hisp * hhtenshort", "sex * hisp * hhtenshort * race * hhage", "detailed"),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 100) for num in (30, 30, 25))),

            CC.UNITDPQUERIES + "default": ("tenvacgq",),
            CC.UNITQUERIESPROP + "default": (Fr(15, 100),)
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
            test_strategy[CC.UNITDPQUERIES][level] = test_strategy[CC.UNITDPQUERIES + "default"]
            test_strategy[CC.UNITQUERIESPROP][level] = test_strategy[CC.UNITQUERIESPROP + "default"]
        return test_strategy


class DecompTestStrategyRegularOrderingDHCH(TestStrategy):
    @staticmethod
    def make(levels):
        # levels = (CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE)
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {
                    0: ("tenvacgq",)
                },
                1: {
                    0: ("sex * hisp * hhtenshort",)
                },
                2: {
                    0: ("sex * hisp * hhtenshort * race * hhage", "detailed")
                }
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: ("tenvacgq",)
                },
                1: {
                    0: ("sex * hisp * hhtenshort",)
                },
                2: {
                    0: ("sex * hisp * hhtenshort * race * hhage", "detailed")
                }
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: ("tenvacgq",)
                },
                1: {
                    0: ("sex * hisp * hhtenshort",)
                },
                2: {
                    0: ("sex * hisp * hhtenshort * race * hhage", "detailed")
                }
            }
        }

        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = {
                CC.L2_QUERY_ORDERING: ordering[CC.L2_QUERY_ORDERING],
                CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                CC.ROUNDER_QUERY_ORDERING: ordering[CC.ROUNDER_QUERY_ORDERING]
            }

        return query_ordering


class DecompTestStrategyDHCH_20220217(TestStrategy):
    # This strategy is approximately the same as DecompTestStrategyDHCH_20220217_omit_with_relative_add_05, but fraction denominators are rounded to 10,000
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"

    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "US":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.05, 0.05, 0.0162, 0.0162, 0.0662])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0162])
            elif level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.05, 0.05, 0.225, 0.225, 0.275])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.225])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.05, 0.05, 0.07, 0.07, 0.07])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.07])
            elif level in ("Prim", "Tract_Subset_Group", "Block_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.1075, 0.1075, 0.1075])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.1075])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.195, 0.195, 0.195])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.195])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0025, 0.0025, 0.0025])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0025])
        return test_strategy


class DecompTestStrategyDHCH_20220217_PR(TestStrategy):
    # This strategy is approximately the same as DecompTestStrategyDHCH_20220217_omit_with_relative_add_05_PR, but fraction denominators are rounded to 10,000 and
    # 1/10,000 is subtracted from detailed query at the state level to ensure the global precision is the same as the corresponding US strategy above
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0527, 0.0527, 0.242, 0.242, 0.2946])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.242])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0527, 0.0527, 0.0753, 0.0753, 0.0753])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0753])
            elif level in ("Prim", "Tract_Subset_Group", "Block_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.1156, 0.1156, 0.1156])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.1156])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.2097, 0.2097, 0.2097])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.2097])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0027, 0.0027, 0.0027])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0027])
        return test_strategy

class DecompTestStrategyDHCH_20220323_pt1(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_county_prim_tsg = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "US":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0134, 0.0134, 0.05, 0.05, 0.0162, 0.0162, 0.0134, 0.0662])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0162])
            elif level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0135, 0.0134, 0.05, 0.05, 0.225, 0.225, 0.0135, 0.275])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.225])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0135, 0.05, 0.05, 0.07, 0.07, 0.0135, 0.07])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.07])
            elif level in ("Prim", "Tract_Subset_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0614, 0.0135, 0.0614, 0.0614, 0.0614, 0.0614, 0.0135, 0.0614])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0614])
            elif level == "Block_Group":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0672, 0.0672, 0.0672])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0672])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.195, 0.195, 0.195])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.195])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0025, 0.0025, 0.0025])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0025])
        return test_strategy

class DecompTestStrategyDHCH_20220323_pt2(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_county_prim_tsg = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "US":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0134, 0.0134, 0.05, 0.05, 0.0162, 0.0296, 0.0662])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0162])
            elif level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0135, 0.0134, 0.05, 0.05, 0.225, 0.2385, 0.275])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.225])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0135, 0.05, 0.05, 0.07, 0.0835, 0.07])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.07])
            elif level in ("Prim", "Tract_Subset_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0614, 0.0135, 0.0614, 0.0614, 0.0614, 0.0749, 0.0614])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0614])
            elif level == "Block_Group":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0672, 0.0672, 0.0672])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0672])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.195, 0.195, 0.195])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.195])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0025, 0.0025, 0.0025])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0025])
        return test_strategy


class DecompTestStrategyDHCH_20220323_pt3(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        dp_queries_county_prim_tsg = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "US":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0134, 0.0134, 0.05, 0.05, 0.0162, 0.0162, 0.0134, 0.0662])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0162])
            elif level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0135, 0.0134, 0.05, 0.05, 0.225, 0.225, 0.0135, 0.275])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.225])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.05, 0.0135, 0.05, 0.05, 0.07, 0.07, 0.0135, 0.07])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.07])
            elif level in ("Prim", "Tract_Subset_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0614, 0.0135, 0.0614, 0.0614, 0.0614, 0.0614, 0.0135, 0.0614])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0614])
            elif level == "Block_Group":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0672, 0.0672, 0.0672])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0672])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.195, 0.195, 0.195])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.195])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0025, 0.0025, 0.0025])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0025])
        return test_strategy


class DecompTestStrategyDHCH_20220323_PR_pt1(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                                  'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize',
                                  'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_county_prim_tsg = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0105, 0.0105, 0.0527, 0.0527, 0.242, 0.242, 0.0106, 0.2946])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.242])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0106, 0.0527, 0.0527, 0.0753, 0.0753, 0.0106, 0.0753])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0753])
            elif level in ("Prim", "Tract_Subset_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.066, 0.0106, 0.066, 0.066, 0.066, 0.066, 0.0106, 0.066])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.1156])
            elif level == "Block_Group":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0672, 0.0672, 0.0672])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0672])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.2097, 0.2097, 0.2097])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.2097])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0027, 0.0027, 0.0027])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0027])
        return test_strategy


class DecompTestStrategyDHCH_20220323_PR_pt2(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                                  'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize',
                                  'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_county_prim_tsg = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0105, 0.0105, 0.0527, 0.0527, 0.242, 0.2526, 0.2946])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.242])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0106, 0.0527, 0.0527, 0.0753, 0.0859, 0.0753])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0753])
            elif level in ("Prim", "Tract_Subset_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.066, 0.0106, 0.066, 0.066, 0.066, 0.0766, 0.066])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.1156])
            elif level == "Block_Group":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0672, 0.0672, 0.0672])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0672])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.2097, 0.2097, 0.2097])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.2097])
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0027, 0.0027, 0.0027])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0027])
        return test_strategy

class DecompTestStrategyDHCH_20220323_PR_pt3(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        to_frac = lambda y: tuple(Fr(int(np.round(x * 10000)), 10000) for x in y)
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        dp_queries_upper_level = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                                  'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize',
                                  'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        dp_queries_county_prim_tsg = ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort', 'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        dp_queries_lower_level = ('sex * hisp * hhtenshort * race * family_nonfamily_size', 'sex * hisp * hhtenshort * race * hhage * family_nonfamily_size', 'detailed')
        for level in levels2make:
            test_strategy[CC.UNITDPQUERIES][level] = ("tenvacgq",)
            if level == "State":
                test_strategy[CC.DPQUERIES][level] = dp_queries_upper_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0105, 0.0105, 0.0527, 0.0527, 0.242, 0.242, 0.0106, 0.2946])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.242])
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0527, 0.0106, 0.0527, 0.0527, 0.0753, 0.0753, 0.0106, 0.0753])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0753])
            elif level in ("Prim", "Tract_Subset_Group"):
                test_strategy[CC.DPQUERIES][level] = dp_queries_county_prim_tsg
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.066, 0.0106, 0.066, 0.066, 0.066, 0.066, 0.0106, 0.066])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.1156])
            elif level == "Block_Group":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0672, 0.0672, 0.0672])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0672])
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.2097, 0.2097, 0.2097])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.2097])
            else:
                #assert level == "Block"
                assert level == "Block", f"{level}"
                test_strategy[CC.DPQUERIES][level] = dp_queries_lower_level
                test_strategy[CC.QUERIESPROP][level] = to_frac([0.0027, 0.0027, 0.0027])
                test_strategy[CC.UNITQUERIESPROP][level] = to_frac([0.0027])
        return test_strategy


class DecompTestStrategyRegularOrderingDHCH_20220323_pt1(TestStrategy):
    @staticmethod
    def make(levels):
        ordering_us_state = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                        'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort * DetailedCoupleTypeMultGenOwnChildSize", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_county_prim_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                        'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort * DetailedCoupleTypeMultGenOwnChildSize", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_below_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_us_state[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_us_state[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_us_state[CC.ROUNDER_QUERY_ORDERING]}
            elif geolevel in ("County", "Prim", "Tract_Subset_Group"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_county_prim_tsg[CC.ROUNDER_QUERY_ORDERING]}
            else:
                # @Ryan, added Tract here to pass a self-test. Is this right?
                assert geolevel in ("Block_Group", "Tract", "Tract_Subset", "Block"), f"{geolevel}"
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_below_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_below_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_below_tsg[CC.ROUNDER_QUERY_ORDERING]}
        return query_ordering

class DecompTestStrategyRegularOrderingDHCH_20220323_pt2(TestStrategy):
    @staticmethod
    def make(levels):
        ordering_us_state = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                        'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort * DetailedCoupleTypeMultGenOwnChildSize", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_county_prim_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                        'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort * DetailedCoupleTypeMultGenOwnChildSize", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_below_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_us_state[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_us_state[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_us_state[CC.ROUNDER_QUERY_ORDERING]}
            elif geolevel in ("County", "Prim", "Tract_Subset_Group", "Tract"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_county_prim_tsg[CC.ROUNDER_QUERY_ORDERING]}
            else:
                assert geolevel in ("Block_Group", "Tract_Subset", "Block")
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_below_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_below_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_below_tsg[CC.ROUNDER_QUERY_ORDERING]}
        return query_ordering

class DecompTestStrategyRegularOrderingDHCH_20220323_pt3(TestStrategy):
    @staticmethod
    def make(levels):
        ordering_us_state = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'hisp * hhtenshort * race', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                        'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_county_prim_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort', 'hisp * hhtenshort', 'partner_type_own_child_status * sex * hhtenshort', 'coupled_hh_type * hisp * hhtenshort',
                        'sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_below_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_us_state[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_us_state[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_us_state[CC.ROUNDER_QUERY_ORDERING]}
            elif geolevel in ("County", "Prim", "Tract_Subset_Group", "Tract"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_county_prim_tsg[CC.ROUNDER_QUERY_ORDERING]}
            else:
                assert geolevel in ("Block_Group", "Tract_Subset", "Block")
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_below_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_below_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_below_tsg[CC.ROUNDER_QUERY_ORDERING]}
        return query_ordering


class DecompTestStrategyDHCH_20220217_omit_with_relative_add_05(TestStrategy):
    schema = CC.SCHEMA_DHCH
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"

    def make(self, levels):
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        def return_upper_level_dict(precision_from_experiment_iteration1, add_to_detailed=0):
            level_dict = defaultdict(lambda: defaultdict(dict))
            level_dict.update({
                CC.DPQUERIES: ("multig * hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize", "detailed"),
                CC.QUERIESPROP: (tuple(Fr(1, 20) for _ in range(3)) + tuple(precision_from_experiment_iteration1 for _ in range(2)) + (precision_from_experiment_iteration1 + add_to_detailed,)),
                CC.UNITDPQUERIES: ("tenvacgq",),
                CC.UNITQUERIESPROP: (precision_from_experiment_iteration1,)
            })
            return level_dict
        us = return_upper_level_dict(Fr(13, 800), add_to_detailed=Fr(1, 20))
        state = return_upper_level_dict(Fr(9, 40), add_to_detailed=Fr(1, 20))
        county = return_upper_level_dict(Fr(7, 100), add_to_detailed=0)
        def return_lower_level_dict(precision_from_experiment_iteration1):
            level_dict = defaultdict(lambda: defaultdict(dict))
            level_dict.update({
                CC.DPQUERIES: ("sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed"),
                CC.QUERIESPROP: tuple(precision_from_experiment_iteration1 for _ in range(3)),
                CC.UNITDPQUERIES: ("tenvacgq",),
                CC.UNITQUERIESPROP: (precision_from_experiment_iteration1,)
            })
            return level_dict
        prim_tsg_bg = return_lower_level_dict(Fr(76, 707))
        ts = return_lower_level_dict(Fr(109, 559))
        block = return_lower_level_dict(Fr(1, 400))
        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            if level == "US":
                test_strategy[CC.DPQUERIES][level] = us[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = us[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = us[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = us[CC.UNITQUERIESPROP]
            elif level == "State":
                test_strategy[CC.DPQUERIES][level] = state[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = state[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = state[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = state[CC.UNITQUERIESPROP]
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = county[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = county[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = county[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = county[CC.UNITQUERIESPROP]
            elif level in ("Prim", "Tract_Subset_Group", "Block_Group"):
                test_strategy[CC.DPQUERIES][level] = prim_tsg_bg[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = prim_tsg_bg[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = prim_tsg_bg[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = prim_tsg_bg[CC.UNITQUERIESPROP]
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = ts[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = ts[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = ts[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = ts[CC.UNITQUERIESPROP]
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = block[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = block[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = block[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = block[CC.UNITQUERIESPROP]
        return test_strategy


class DecompTestStrategyDHCH_20220217_omit_with_relative_add_05_PR(TestStrategy):
    schema = CC.SCHEMA_DHCH
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"

    def make(self, levels):
        levels2make = levels if levels else self.levels
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        def return_upper_level_dict(precision_for_det_qg, precision_for_other_prior_qgs, precision_for_new_qgs):
            level_dict = defaultdict(lambda: defaultdict(dict))
            level_dict.update({
                CC.DPQUERIES: ("multig * hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize", "detailed"),
                CC.QUERIESPROP: (tuple(precision_for_new_qgs for _ in range(3)) + tuple(precision_for_other_prior_qgs for _ in range(2)) + (precision_for_det_qg,)),
                CC.UNITDPQUERIES: ("tenvacgq",),
                CC.UNITQUERIESPROP: (precision_for_other_prior_qgs,)
            })
            return level_dict
        state = return_upper_level_dict(Fr(80770308274514121, 274084362390966400), Fr(2652801061962357, 10963374495638656), Fr(22163008781373, 420374788943200))
        county = return_upper_level_dict(Fr(294755673551373, 3915490891299520), Fr(294755673551373, 3915490891299520), Fr(22163008781373, 420374788943200))
        def return_lower_level_dict(precision_from_experiment_iteration1):
            level_dict = defaultdict(lambda: defaultdict(dict))
            level_dict.update({
                CC.DPQUERIES: ("sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed"),
                CC.QUERIESPROP: tuple(precision_from_experiment_iteration1 for _ in range(3)),
                CC.UNITDPQUERIES: ("tenvacgq",),
                CC.UNITQUERIESPROP: (precision_from_experiment_iteration1,)
            })
            return level_dict
        prim_tsg_bg = return_lower_level_dict(Fr(294755673551373, 2549621975729920))
        ts = return_lower_level_dict(Fr(884267020654119, 4216682498322560))
        block = return_lower_level_dict(Fr(294755673551373, 109633744956386560))
        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            if level == "State":
                test_strategy[CC.DPQUERIES][level] = state[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = state[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = state[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = state[CC.UNITQUERIESPROP]
            elif level == "County":
                test_strategy[CC.DPQUERIES][level] = county[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = county[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = county[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = county[CC.UNITQUERIESPROP]
            elif level in ("Prim", "Tract_Subset_Group", "Block_Group"):
                test_strategy[CC.DPQUERIES][level] = prim_tsg_bg[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = prim_tsg_bg[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = prim_tsg_bg[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = prim_tsg_bg[CC.UNITQUERIESPROP]
            elif level == "Tract_Subset":
                test_strategy[CC.DPQUERIES][level] = ts[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = ts[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = ts[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = ts[CC.UNITQUERIESPROP]
            else:
                assert level == "Block"
                test_strategy[CC.DPQUERIES][level] = block[CC.DPQUERIES]
                test_strategy[CC.QUERIESPROP][level] = block[CC.QUERIESPROP]
                test_strategy[CC.UNITDPQUERIES][level] = block[CC.UNITDPQUERIES]
                test_strategy[CC.UNITQUERIESPROP][level] = block[CC.UNITQUERIESPROP]
        return test_strategy


class DecompTestStrategyRegularOrderingDHCH_20220217_omit_with_relative(TestStrategy):
    @staticmethod
    def make(levels):
        ordering_us_state_county = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("multig * hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize")},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize", "detailed")}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenOwnChildSize", "detailed")}
            }
        }
        ordering_below_county = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", "detailed")}
            }
        }

        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State", "County"):
                query_ordering[geolevel] = {
                    CC.L2_QUERY_ORDERING: ordering_us_state_county[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_us_state_county[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_us_state_county[CC.ROUNDER_QUERY_ORDERING]
                }
            else:
                # assert geolevel in ("Prim", "Tract_Subset_Group", "Block_Group", "Tract_Subset", "Block")
                query_ordering[geolevel] = {
                    CC.L2_QUERY_ORDERING: ordering_below_county[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_below_county[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_below_county[CC.ROUNDER_QUERY_ORDERING]
                }

        return query_ordering


class DecompTestStrategyDHCH_20220520_pt3(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(lambda: defaultdict(dict))
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ("detailed", "sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Block_Group"] = ("detailed", "sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset"] = ("detailed", "sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["Prim"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["County"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["State"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "hisp * hhtenshort * race", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["US"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "hisp * hhtenshort * race", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.QUERIESPROP]["Block"] = (Fr(40, 10000), Fr(40, 10000), Fr(40, 10000), )
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), Fr(3090, 10000), Fr(3090, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), )
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), )
        strategy[CC.QUERIESPROP]["County"] = (Fr(792, 10000), Fr(214, 10000), Fr(792, 10000), Fr(792, 10000), Fr(1109, 10000), Fr(1109, 10000), Fr(214, 10000), Fr(1109, 10000), )
        strategy[CC.QUERIESPROP]["State"] = (Fr(792, 10000), Fr(214, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(3565, 10000), Fr(3565, 10000), Fr(214, 10000), Fr(4358, 10000), )
        strategy[CC.QUERIESPROP]["US"] = (Fr(792, 10000), Fr(212, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(257, 10000), Fr(257, 10000), Fr(212, 10000), Fr(1049, 10000), )
        strategy[CC.UNITDPQUERIES]["Block"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Prim"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["County"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["State"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["US"] = ("tenvacgq", )
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(40, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1109, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3565, 10000), )
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(257, 10000),)
        return strategy


class DecompTestStrategyDHCH_20220520_PR_pt3(TestStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(lambda: defaultdict(dict))
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ("detailed", "sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Block_Group"] = ("detailed", "sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset"] = ("detailed", "sex * hisp * hhtenshort * race * family_nonfamily_size", "sex * hisp * hhtenshort * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["Prim"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["County"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["State"] = ("multig * hisp * hhtenshort", "hisp * hhtenshort", "hisp * hhtenshort * race", "partner_type_own_child_status * sex * hhtenshort", "coupled_hh_type * hisp * hhtenshort", "sex * hisp * hhtenshort * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.QUERIESPROP]["Block"] = (Fr(43, 10000), Fr(43, 10000), Fr(43, 10000), )
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3323, 10000), Fr(3323, 10000), Fr(3323, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), )
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), )
        strategy[CC.QUERIESPROP]["County"] = (Fr(835, 10000), Fr(168, 10000), Fr(835, 10000), Fr(835, 10000), Fr(1193, 10000), Fr(1193, 10000), Fr(168, 10000), Fr(1193, 10000), )
        strategy[CC.QUERIESPROP]["State"] = (Fr(834, 10000), Fr(165, 10000), Fr(165, 10000), Fr(834, 10000), Fr(835, 10000), Fr(3835, 10000), Fr(3835, 10000), Fr(168, 10000), Fr(4668, 10000), )
        strategy[CC.UNITDPQUERIES]["Block"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Prim"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["County"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["State"] = ("tenvacgq", )
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(43, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3323, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(1832, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(1832, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1193, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3835, 10000), )
        return strategy


class DecompTestStrategyRegularOrderingDHCH_20220613_pt3(DHCHStrategyTen3Level):
    @staticmethod
    def make(levels):
        ordering_us_state = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev * race', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_county_prim_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_below_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_us_state[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_us_state[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_us_state[CC.ROUNDER_QUERY_ORDERING]}
            elif geolevel in ("County", "Prim", "Tract_Subset_Group", "Tract"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_county_prim_tsg[CC.ROUNDER_QUERY_ORDERING]}
            else:
                assert geolevel in ("Block_Group", "Tract_Subset", "Block")
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_below_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_below_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_below_tsg[CC.ROUNDER_QUERY_ORDERING]}
        return query_ordering


class DecompTestStrategyDHCH_20220613_pt3(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(lambda: defaultdict(dict))
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Block_Group"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["Prim"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["County"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["State"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev * race", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["US"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev * race", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.QUERIESPROP]["Block"] = (Fr(40, 10000), Fr(40, 10000), Fr(40, 10000), )
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), Fr(3090, 10000), Fr(3090, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), )
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), )
        strategy[CC.QUERIESPROP]["County"] = (Fr(792, 10000), Fr(214, 10000), Fr(792, 10000), Fr(792, 10000), Fr(1109, 10000), Fr(1109, 10000), Fr(214, 10000), Fr(1109, 10000), )
        strategy[CC.QUERIESPROP]["State"] = (Fr(792, 10000), Fr(214, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(3565, 10000), Fr(3565, 10000), Fr(214, 10000), Fr(4358, 10000), )
        strategy[CC.QUERIESPROP]["US"] = (Fr(792, 10000), Fr(212, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(257, 10000), Fr(257, 10000), Fr(212, 10000), Fr(1049, 10000), )
        strategy[CC.UNITDPQUERIES]["Block"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Prim"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["County"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["State"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["US"] = ("tenvacgq", )
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(40, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1109, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3565, 10000), )
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(257, 10000),)
        return strategy


class DecompTestStrategyDHCH_20220613_PR_pt3(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(lambda: defaultdict(dict))
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Block_Group"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["Prim"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["County"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["State"] = ("multig * hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev", "hisp * hhtenshort_3lev * race", "partner_type_own_child_status * sex * hhtenshort_3lev", "coupled_hh_type * hisp * hhtenshort_3lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.QUERIESPROP]["Block"] = (Fr(43, 10000), Fr(43, 10000), Fr(43, 10000), )
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3323, 10000), Fr(3323, 10000), Fr(3323, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), )
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), )
        strategy[CC.QUERIESPROP]["County"] = (Fr(835, 10000), Fr(168, 10000), Fr(835, 10000), Fr(835, 10000), Fr(1193, 10000), Fr(1193, 10000), Fr(168, 10000), Fr(1193, 10000), )
        strategy[CC.QUERIESPROP]["State"] = (Fr(834, 10000), Fr(165, 10000), Fr(165, 10000), Fr(834, 10000), Fr(835, 10000), Fr(3835, 10000), Fr(3835, 10000), Fr(168, 10000), Fr(4668, 10000), )
        strategy[CC.UNITDPQUERIES]["Block"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Prim"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["County"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["State"] = ("tenvacgq", )
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(43, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3323, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(1832, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(1832, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1193, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3835, 10000), )
        return strategy


class DecompTestStrategyRegularOrderingDHCH_20220623(DHCHStrategyTen3Level):
    @staticmethod
    def make(levels):
        ordering_us_state = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_county_prim_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_below_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_us_state[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_us_state[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_us_state[CC.ROUNDER_QUERY_ORDERING]}
            elif geolevel in ("County", "Prim", "Tract_Subset_Group", "Tract"):
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_county_prim_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_county_prim_tsg[CC.ROUNDER_QUERY_ORDERING]}
            else:
                assert geolevel in ("Block_Group", "Tract_Subset", "Block")
                query_ordering[geolevel] = {CC.L2_QUERY_ORDERING: ordering_below_tsg[CC.L2_QUERY_ORDERING],
                    CC.L2_CONSTRAIN_TO_QUERY_ORDERING: ordering_below_tsg[CC.L2_CONSTRAIN_TO_QUERY_ORDERING],
                    CC.ROUNDER_QUERY_ORDERING: ordering_below_tsg[CC.ROUNDER_QUERY_ORDERING]}
        return query_ordering

class DecompTestStrategyDHCH_20220713(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Block_Group"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["US"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.QUERIESPROP]["Block"] = (Fr(1, 250), Fr(1, 250), Fr(1, 250))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(213, 2000), Fr(213, 2000), Fr(213, 2000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(309, 1000), Fr(309, 1000), Fr(309, 1000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), Fr(107, 5000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(107, 5000), Fr(973, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(973, 10000), Fr(107, 5000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(107, 5000), Fr(973, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(493, 5000), Fr(133, 5000), Fr(493, 5000), Fr(493, 5000), Fr(69, 500), Fr(69, 500), Fr(133, 5000), Fr(69, 500))
        strategy[CC.QUERIESPROP]["State"] = (Fr(357, 5000), Fr(193, 10000), Fr(191, 10000), Fr(357, 5000), Fr(357, 5000), Fr(201, 625), Fr(201, 625), Fr(193, 10000), Fr(3931, 10000))
        strategy[CC.QUERIESPROP]["US"] = (Fr(99, 1250), Fr(53, 2500), Fr(53, 2500), Fr(99, 1250), Fr(99, 1250), Fr(257, 10000), Fr(257, 10000), Fr(53, 2500), Fr(1049, 10000))
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["US"] = ('tenvacgq',)
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(1, 250),)
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(213, 2000),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(309, 1000),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000),)
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(973, 10000),)
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(69, 500),)
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(201, 625),)
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(259, 10000),)
        return strategy


class DecompTestStrategyDHCH_20220713_PR(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Block_Group"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.QUERIESPROP]["Block"] = (Fr(1, 250), Fr(1, 250), Fr(1, 250))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(633, 5000), Fr(633, 5000), Fr(633, 5000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3291, 10000), Fr(3291, 10000), Fr(3291, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(1081, 10000), Fr(119, 5000), Fr(1081, 10000), Fr(1081, 10000), Fr(1081, 10000), Fr(1081, 10000), Fr(119, 5000), Fr(1081, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(1081, 10000), Fr(119, 5000), Fr(1081, 10000), Fr(1081, 10000), Fr(1081, 10000), Fr(1081, 10000), Fr(119, 5000), Fr(1081, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(537, 5000), Fr(29, 1000), Fr(537, 5000), Fr(537, 5000), Fr(1503, 10000), Fr(1503, 10000), Fr(29, 1000), Fr(1503, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(3, 40), Fr(203, 10000), Fr(201, 10000), Fr(3, 40), Fr(3, 40), Fr(27, 80), Fr(27, 80), Fr(203, 10000), Fr(33, 80))
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(1, 250),)
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(633, 5000),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3291, 10000),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(1081, 10000),)
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(1081, 10000),)
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1503, 10000),)
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3380, 10000),)

        return strategy


class DecompTestStrategyDHCH_20220623(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Block_Group"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["Prim"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["County"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["State"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev * race", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["US"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev * race", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.QUERIESPROP]["Block"] = (Fr(40, 10000), Fr(40, 10000), Fr(40, 10000), )
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), Fr(3090, 10000), Fr(3090, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), )
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000), )
        strategy[CC.QUERIESPROP]["County"] = (Fr(792, 10000), Fr(214, 10000), Fr(792, 10000), Fr(792, 10000), Fr(1109, 10000), Fr(1109, 10000), Fr(214, 10000), Fr(1109, 10000), )
        strategy[CC.QUERIESPROP]["State"] = (Fr(792, 10000), Fr(214, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(3565, 10000), Fr(3565, 10000), Fr(214, 10000), Fr(4358, 10000), )
        strategy[CC.QUERIESPROP]["US"] = (Fr(792, 10000), Fr(212, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(257, 10000), Fr(257, 10000), Fr(212, 10000), Fr(1049, 10000), )
        strategy[CC.UNITDPQUERIES]["Block"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Prim"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["County"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["State"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["US"] = ("tenvacgq", )
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(40, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1109, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3565, 10000), )
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(257, 10000),)
        return strategy


class DecompTestStrategyDHCH_20220623_PR(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Block_Group"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset"] = ("detailed", "sex * hisp * hhtenshort_3lev * race * family_nonfamily_size", "sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", )
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["Prim"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["County"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.DPQUERIES]["State"] = ("multig * hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev", "hisp * hhtenshort_2lev * race", "partner_type_own_child_status * sex * hhtenshort_2lev", "coupled_hh_type * hisp * hhtenshort_2lev", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed", )
        strategy[CC.QUERIESPROP]["Block"] = (Fr(43, 10000), Fr(43, 10000), Fr(43, 10000), )
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3323, 10000), Fr(3323, 10000), Fr(3323, 10000), )
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), )
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(1046, 10000), Fr(168, 10000), Fr(1046, 10000), )
        strategy[CC.QUERIESPROP]["County"] = (Fr(835, 10000), Fr(168, 10000), Fr(835, 10000), Fr(835, 10000), Fr(1193, 10000), Fr(1193, 10000), Fr(168, 10000), Fr(1193, 10000), )
        strategy[CC.QUERIESPROP]["State"] = (Fr(834, 10000), Fr(165, 10000), Fr(165, 10000), Fr(834, 10000), Fr(835, 10000), Fr(3835, 10000), Fr(3835, 10000), Fr(168, 10000), Fr(4668, 10000), )
        strategy[CC.UNITDPQUERIES]["Block"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["Prim"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["County"] = ("tenvacgq", )
        strategy[CC.UNITDPQUERIES]["State"] = ("tenvacgq", )
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(43, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3323, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(1832, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(1832, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1193, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3835, 10000), )
        return strategy

class DecompTestStrategyDHCH_20220615(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Block_Group"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev * race', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["US"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev * race', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.QUERIESPROP]["Block"] = (Fr(1, 250), Fr(1, 250), Fr(1, 250))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(3193, 10000), Fr(3193, 10000), Fr(3193, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(299, 1250), Fr(299, 1250), Fr(299, 1250))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(51, 625), Fr(179, 10000), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(179, 10000), Fr(51, 625))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(51, 625), Fr(179, 10000), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(179, 10000), Fr(51, 625))
        strategy[CC.QUERIESPROP]["County"] = (Fr(1047, 10000), Fr(283, 10000), Fr(1047, 10000), Fr(1047, 10000), Fr(733, 5000), Fr(733, 5000), Fr(283, 10000), Fr(733, 5000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(7, 125), Fr(151, 10000), Fr(3, 200), Fr(7, 125), Fr(7, 125), Fr(63, 250), Fr(63, 250), Fr(151, 10000), Fr(77, 250))
        strategy[CC.QUERIESPROP]["US"] = (Fr(180, 2500), Fr(97, 5000), Fr(97, 5000), Fr(181, 2500), Fr(181, 2500), Fr(47, 2000), Fr(47, 2000), Fr(97, 5000), Fr(959, 10000))
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["US"] = ('tenvacgq',)
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(1, 250),)
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(3193, 10000),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(299, 1250),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(51, 625),)
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(51, 625),)
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(733, 5000),)
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(63, 250),)
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(47, 2000),)
        return strategy


class DecompTestStrategyDHCH_20220615_PR(DHCHStrategyTen3Level):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})

        strategy[CC.DPQUERIES]["Block"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Block_Group"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('detailed', 'sex * hisp * hhtenshort_3lev * race * family_nonfamily_size', 'sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev', 'hisp * hhtenshort_3lev * race', 'partner_type_own_child_status * sex * hhtenshort_3lev', 'coupled_hh_type * hisp * hhtenshort_3lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.QUERIESPROP]["Block"] = (Fr(1, 250), Fr(1, 250), Fr(1, 250))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(3193, 10000), Fr(3193, 10000), Fr(3193, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(299, 1250), Fr(299, 1250), Fr(299, 1250))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(51, 625), Fr(179, 10000), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(179, 10000), Fr(51, 625))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(51, 625), Fr(179, 10000), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(51, 625), Fr(179, 10000), Fr(51, 625))
        strategy[CC.QUERIESPROP]["County"] = (Fr(1047, 10000), Fr(283, 10000), Fr(1047, 10000), Fr(1047, 10000), Fr(733, 5000), Fr(733, 5000), Fr(283, 10000), Fr(733, 5000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(7, 125), Fr(151, 10000), Fr(3, 200), Fr(7, 125), Fr(7, 125), Fr(63, 250), Fr(63, 250), Fr(151, 10000), Fr(77, 250))
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(1, 250),)
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(3193, 10000),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(299, 1250),)
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(51, 625),)
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(51, 625),)
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(733, 5000),)
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(63, 250),)
        return strategy

class DecompTestStrategyDHCH_PR_20220922_exp_iteration_23_1:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Block_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.QUERIESPROP]["Block"] = (Fr(11, 10000), Fr(3, 10000), Fr(17, 10000), Fr(22, 10000), Fr(44, 10000), Fr(15, 10000), Fr(15, 10000), Fr(3, 10000), Fr(15, 10000))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(346, 10000), Fr(93, 10000), Fr(526, 10000), Fr(691, 10000), Fr(1382, 10000), Fr(484, 10000), Fr(484, 10000), Fr(93, 10000), Fr(484, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(1205, 10000), Fr(325, 10000), Fr(1833, 10000), Fr(2410, 10000), Fr(4820, 10000), Fr(1687, 10000), Fr(1687, 10000), Fr(325, 10000), Fr(1687, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(1054, 10000), Fr(232, 10000), Fr(1624, 10000), Fr(2107, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(232, 10000), Fr(1054, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(1054, 10000), Fr(232, 10000), Fr(1624, 10000), Fr(2107, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(232, 10000), Fr(1054, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(1041, 10000), Fr(281, 10000), Fr(1584, 10000), Fr(2082, 10000), Fr(4164, 10000), Fr(1457, 10000), Fr(1457, 10000), Fr(281, 10000), Fr(1457, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(748, 10000), Fr(202, 10000), Fr(200, 10000), Fr(1496, 10000), Fr(748, 10000), Fr(3368, 10000), Fr(3368, 10000), Fr(202, 10000), Fr(4117, 10000))
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(15, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(484, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(1687, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(1054, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(1054, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1457, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3364, 10000), )
        return strategy



class DecompTestStrategyDHCH_PR_20220922_exp_iteration_23_2:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
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
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.QUERIESPROP]["Block"] = (Fr(40, 10000), Fr(40, 10000), Fr(40, 10000))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1266, 10000), Fr(1266, 10000), Fr(1266, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3238, 10000), Fr(3238, 10000), Fr(3238, 10000), Fr(1572, 10000), Fr(1572, 10000), Fr(1572, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(1054, 10000), Fr(232, 10000), Fr(1624, 10000), Fr(2107, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(232, 10000), Fr(1054, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(1054, 10000), Fr(232, 10000), Fr(1624, 10000), Fr(2107, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(1054, 10000), Fr(232, 10000), Fr(1054, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(1041, 10000), Fr(281, 10000), Fr(1584, 10000), Fr(2082, 10000), Fr(4164, 10000), Fr(1457, 10000), Fr(1457, 10000), Fr(281, 10000), Fr(1457, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(748, 10000), Fr(202, 10000), Fr(200, 10000), Fr(1496, 10000), Fr(748, 10000), Fr(3368, 10000), Fr(3368, 10000), Fr(202, 10000), Fr(4117, 10000))
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(40, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1266, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3238, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(1054, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(1054, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1457, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3365, 10000), )
        return strategy


class DecompTestStrategyDHCH_20220922_exp_iteration_23_1:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Block_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.DPQUERIES]["US"] = ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev', 'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize', 'sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["US"] = ('tenvacgq',)
        strategy[CC.QUERIESPROP]["Block"] = (Fr(11, 10000), Fr(3, 10000), Fr(17, 10000), Fr(22, 10000), Fr(44, 10000), Fr(15, 10000), Fr(15, 10000), Fr(3, 10000), Fr(15, 10000))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(291, 10000), Fr(78, 10000), Fr(442, 10000), Fr(581, 10000), Fr(1162, 10000), Fr(407, 10000), Fr(407, 10000), Fr(78, 10000), Fr(407, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(1150, 10000), Fr(310, 10000), Fr(1750, 10000), Fr(2300, 10000), Fr(4601, 10000), Fr(1610, 10000), Fr(1610, 10000), Fr(310, 10000), Fr(1610, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), Fr(214, 10000), Fr(1500, 10000), Fr(1946, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(973, 10000), Fr(214, 10000), Fr(1500, 10000), Fr(1946, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(986, 10000), Fr(266, 10000), Fr(1500, 10000), Fr(1972, 10000), Fr(3944, 10000), Fr(1380, 10000), Fr(1380, 10000), Fr(266, 10000), Fr(1380, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(714, 10000), Fr(193, 10000), Fr(191, 10000), Fr(1428, 10000), Fr(714, 10000), Fr(3216, 10000), Fr(3216, 10000), Fr(193, 10000), Fr(3931, 10000))
        strategy[CC.QUERIESPROP]["US"] = (Fr(792, 10000), Fr(212, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(257, 10000), Fr(257, 10000), Fr(212, 10000), Fr(1049, 10000))
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(15, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(407, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(1610, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1380, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3216, 10000), )
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(259, 10000), )
        return strategy


class DecompTestStrategyDHCH_20220922_exp_iteration_23_2:
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
        strategy[CC.UNITDPQUERIES]["Block"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["Prim"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["County"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["State"] = ('tenvacgq',)
        strategy[CC.UNITDPQUERIES]["US"] = ('tenvacgq',)
        strategy[CC.QUERIESPROP]["Block"] = (Fr(40, 10000), Fr(40, 10000), Fr(40, 10000))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(1065, 10000), Fr(1065, 10000), Fr(1065, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), Fr(3090, 10000), Fr(3090, 10000), Fr(1500, 10000), Fr(1500, 10000), Fr(1500, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), Fr(214, 10000), Fr(1500, 10000), Fr(1946, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(973, 10000), Fr(214, 10000), Fr(1500, 10000), Fr(1946, 10000), Fr(973, 10000), Fr(973, 10000), Fr(973, 10000), Fr(214, 10000), Fr(973, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(986, 10000), Fr(266, 10000), Fr(1500, 10000), Fr(1972, 10000), Fr(3944, 10000), Fr(1380, 10000), Fr(1380, 10000), Fr(266, 10000), Fr(1380, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(714, 10000), Fr(193, 10000), Fr(191, 10000), Fr(1428, 10000), Fr(714, 10000), Fr(3216, 10000), Fr(3216, 10000), Fr(193, 10000), Fr(3931, 10000))
        strategy[CC.QUERIESPROP]["US"] = (Fr(792, 10000), Fr(212, 10000), Fr(212, 10000), Fr(792, 10000), Fr(792, 10000), Fr(257, 10000), Fr(257, 10000), Fr(212, 10000), Fr(1049, 10000))
        strategy[CC.UNITQUERIESPROP]["Block"] = (Fr(40, 10000), )
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = (Fr(1065, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = (Fr(3090, 10000), )
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["Prim"] = (Fr(973, 10000), )
        strategy[CC.UNITQUERIESPROP]["County"] = (Fr(1380, 10000), )
        strategy[CC.UNITQUERIESPROP]["State"] = (Fr(3216, 10000), )
        strategy[CC.UNITQUERIESPROP]["US"] = (Fr(260, 10000), )
        return strategy


class DecompTestStrategyRegularOrderingDHCH_20220922_exp_iteration_23_1(DHCHStrategyTen3Level):
    @staticmethod
    def make(levels):
        ordering_default = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            query_ordering[geolevel] = ordering_default
        return query_ordering


class DecompTestStrategyRegularOrderingDHCH_20220922_exp_iteration_23_2(DHCHStrategyTen3Level):
    @staticmethod
    def make(levels):
        ordering_us_state = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev * race', 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_county_prim_tsg = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ('multig * hisp * hhtenshort_2lev', 'hisp * hhtenshort_2lev', "hisp * hhtenshort_2lev * race", 'partner_type_detailed_own_child_status * sex * hhtenshort_2lev', 'coupled_hh_type * hisp * hhtenshort_2lev',
                        'sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize')},
                2: {0: ('sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'hhtenshort_3lev * hhage * DetailedCoupleTypeMultGenDetOwnChildSize', 'detailed')}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",) },
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("hhtenshort_3lev * DetailedCoupleTypeMultGenDetOwnChildSize", "sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeMultGenDetOwnChildSize")},
                2: {0: ("detailed",)}
            }
        }
        ordering_tract_subset = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeDetOwnChildSize", "coupled_hh_type * hisp * hhtenshort_2lev", "partner_type_detailed_own_child_status * sex * hhtenshort_2lev", "hisp * hhtenshort_2lev * race",)},
                2: {0: ("sex * hisp * hhtenshort_3lev * race * hhage * DetailedCoupleTypeMultGenDetOwnChildSize", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeDetOwnChildSize",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * DetailedCoupleTypeDetOwnChildSize",)},
                2: {0: ("detailed",)}
            }
        }
        ordering_below_ts = {
            CC.L2_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("sex * hisp * hhtenshort_3lev * race * hhage * family_nonfamily_size", "detailed")}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: ("tenvacgq",)},
                1: {0: ("sex * hisp * hhtenshort_3lev * race * family_nonfamily_size",)},
                2: {0: ("detailed",)}
            }
        }
        query_ordering = {}
        for geolevel in levels:
            if geolevel in ("US", "State"):
                query_ordering[geolevel] = ordering_us_state
            elif geolevel in ("County", "Prim", "Tract_Subset_Group", "Tract"):
                query_ordering[geolevel] = ordering_county_prim_tsg
            elif geolevel == "Tract_Subset":
                query_ordering[geolevel] = ordering_tract_subset
            else:
                assert geolevel in ("Block_Group", "Block")
                query_ordering[geolevel] = ordering_below_ts
        return query_ordering
