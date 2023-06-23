from das_constants import CC
from fractions import Fraction as Fr
from collections import defaultdict

CR = CC.SCHEMA_CROSS_JOIN_DELIM


class USLevelStrategy:
    """
    Generic parent class for strategies that can be used with US-level runs. It has the levels attribute, but at this point
    it is only used by the unit tests. In actual DAS runs, levels are supplied from reading the config file
    """
    levels = CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE, CC.GEOLEVEL_US


class DHCPStrategy(USLevelStrategy):
    schema = CC.SCHEMA_DHCP


class TestStrategy(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))  # So as to avoid having to specify empty dicts and defaultdicts
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.HHGQ_8LEV,  CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_SEX, CC.AGE_AGEPYRAMID23GROUPS)),
                CR.join((CC.ATTR_SEX, CC.AGE_AGEPYRAMID23GROUPS, CC.CENRACE_MAJOR, CC.ATTR_HISP)),
                CR.join((CC.ATTR_RELGQ, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE,)),
                CR.join((CC.CENRACE_MAJOR, CC.ATTR_RELGQ)),
                CC.DETAILED),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 100) for num in (20, 20, 20, 20, 10, 10))),
        })

        def queries(level):
            return test_strategy[CC.DPQUERIES + "default"]

        def allocation(level):
            if level == CC.GEOLEVEL_COUNTY:
                return tuple(Fr(num, 100) for num in (10, 30, 20, 20, 10, 10))
            if level == CC.GEOLEVEL_TRACT:
                return tuple(Fr(num, 100) for num in ( 5, 35, 20, 20, 10, 10))
            if level == CC.GEOLEVEL_BLOCK_GROUP:
                return tuple(Fr(num, 100) for num in ( 1, 39, 20, 20, 10, 10))
            if level == CC.GEOLEVEL_BLOCK:
                return tuple(Fr(num, 100) for num in (39,  1, 20, 20, 10, 10))
            return test_strategy[CC.QUERIESPROP + "default"]

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = queries(level)
            test_strategy[CC.QUERIESPROP][level] = allocation(level)

        return test_strategy


class SingleStateTestStrategyRegularOrdering:
    @staticmethod
    def make(levels):
        # levels = (CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE)
        ordering = {

            CC.L2_QUERY_ORDERING: {
                0: {
                    0: (CR.join((CC.ATTR_SEX, CC.AGE_AGEPYRAMID23GROUPS)),),
                    1: (CR.join((CC.HHGQ_8LEV,  CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),),
                    2: (CR.join((CC.ATTR_RELGQ, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE,)),),
                    3: (CR.join((CC.CENRACE_MAJOR, CC.ATTR_RELGQ)),)
                },
                1: {
                    0: (CR.join((CC.ATTR_SEX, CC.AGE_AGEPYRAMID23GROUPS, CC.CENRACE_MAJOR, CC.ATTR_HISP)),),
                    1: (CC.DETAILED,)
                }
            },

            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: (CR.join((CC.ATTR_SEX, CC.AGE_AGEPYRAMID23GROUPS)),),
                    1: (CR.join((CC.HHGQ_8LEV,  CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),),
                    2: (CR.join((CC.ATTR_RELGQ, CC.ATTR_AGE, CC.ATTR_HISP, CC.ATTR_CENRACE,)),),
                    3: (CR.join((CC.CENRACE_MAJOR, CC.ATTR_RELGQ)),)
                },
                1: {
                    0: (CR.join((CC.ATTR_SEX, CC.AGE_AGEPYRAMID23GROUPS, CC.CENRACE_MAJOR, CC.ATTR_HISP)),),
                    1: (CC.DETAILED,)
                }

            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: (CR.join((CC.HHGQ_8LEV,  CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),)
                },
                1: {
                    0: (CC.DETAILED,)

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


class DecompTestStrategyDHCP(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))  # So as to avoid having to specify empty dicts and defaultdicts
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.HHGQ_8LEV, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_SEX, CC.HHGQ_8LEV, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_SEX, CC.ATTR_RELGQ, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                CC.DETAILED),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 100) for num in (30, 30, 20, 20))),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        return test_strategy


class DecompTestStrategyRegularOrderingDHCP(TestStrategy):
    @staticmethod
    def make(levels):
        # levels = (CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE)
        ordering = {

            CC.L2_QUERY_ORDERING: {
                0: {
                    0: (CR.join((CC.HHGQ_8LEV, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_SEX, CC.HHGQ_8LEV, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_SEX, CC.ATTR_RELGQ, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)))
                },
                1: {
                    0: (CC.DETAILED,)
                }
            },

            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {
                    0: (CR.join((CC.ATTR_SEX, CC.ATTR_RELGQ, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),)
                },
                1: {
                    0: (CC.DETAILED,)
                }

            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {
                    0: (CR.join((CC.HHGQ_8LEV, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_SEX, CC.HHGQ_8LEV, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_SEX, CC.ATTR_RELGQ, CC.AGE_AGEPYRAMID23GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE)))
                },
                1: {
                    0: (CC.DETAILED,)
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


class DecompTestStrategyRegularOrderingDHCP_20220310(TestStrategy):
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {0: (CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                        CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)))},
                1: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                        CC.DETAILED,)}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),)},
                1: {0: (CC.DETAILED,)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_VOTINGLEVELS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_18_64_116, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)))},
                1: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                        CC.DETAILED,)}
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


class DecompTestStrategyDHCP_20220310(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                CC.DETAILED),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 8) for num in (1, 1, 1, 1, 1, 1, 1, 1))),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        return test_strategy


class DecompTestStrategyRegularOrderingDHCP_20220413(TestStrategy):
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {0: (CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                        CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CC.POPSEHSDTARGETS_RELSHIP,
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)))},
                1: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                        CC.DETAILED,)}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),)},
                1: {0: (CC.DETAILED,)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_VOTINGLEVELS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_18_64_116, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)))},
                1: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                        CC.DETAILED,)}
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


class DecompTestStrategyDHCP_20220413(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                CC.POPSEHSDTARGETS_RELSHIP,
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                CC.DETAILED),
            CC.QUERIESPROP + "default": (tuple(Fr(num, 9) for num in (1, 1, 1, 1, 1, 1, 1, 1, 1))),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        return test_strategy

class DecompTestStrategyRegularOrderingDHCP_20220511(TestStrategy):
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {0: (CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                        CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.GQ_CONSTR_GROUPS, CC.AGE_10_GROUPS)),
                        CC.POPSEHSDTARGETS_RELSHIP,
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)))},
                1: {0: (CC.DETAILED,)}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),)},
                1: {0: (CC.DETAILED,)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_VOTINGLEVELS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_18_64_116, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)))},
                1: {0: (CC.DETAILED,)}
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


class DecompTestStrategyDHCP_20220511(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                CR.join((CC.GQ_CONSTR_GROUPS, CC.AGE_10_GROUPS)),
                CC.POPSEHSDTARGETS_RELSHIP,
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_26_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_RELGQ, CC.AGE_26_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                CC.DETAILED),
                CC.QUERIESPROP + "default": (tuple(Fr(1, 10) for _ in range(10))),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        return test_strategy

class DecompTestStrategyRegularOrderingDHCP_20220518(TestStrategy):
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {0: (CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                        CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_29_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.GQ_CONSTR_GROUPS, CC.AGE_10_GROUPS)),
                        CC.POPSEHSDTARGETS_RELSHIP,
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_29_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_RELGQ, CC.AGE_29_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)))},
                1: {0: (CC.DETAILED,)}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_29_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),)},
                1: {0: (CC.DETAILED,)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_VOTINGLEVELS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_18_64_116, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_29_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_RELGQ, CC.AGE_29_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)))},
                1: {0: (CC.DETAILED,)}
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


class DecompTestStrategyDHCP_20220518(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                CR.join((CC.AGE_26_GROUPS, CC.ATTR_SEX)),
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                CR.join((CC.GQ_CONSTR_GROUPS, CC.AGE_10_GROUPS)),
                CC.POPSEHSDTARGETS_RELSHIP,
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_29_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_RELGQ, CC.AGE_29_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                CC.DETAILED),
                CC.QUERIESPROP + "default": (tuple(Fr(1, 10) for _ in range(10))),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        return test_strategy

class DecompTestStrategyRegularOrderingDHCP_20220811_exp_21_1(TestStrategy):
    @staticmethod
    def make(levels):
        ordering = {
            CC.L2_QUERY_ORDERING: {
                0: {0: (CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                        CR.join((CC.AGE_38_GROUPS, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                        CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_40_GROUPS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.GQ_CONSTR_GROUPS, CC.AGE_10_GROUPS)),
                        CC.POPSEHSDTARGETS_RELSHIP,
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_40_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_RELGQ, CC.AGE_40_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)))},
                1: {0: (CC.DETAILED,)}
            },
            CC.L2_CONSTRAIN_TO_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_RELGQ, CC.AGE_40_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),)},
                1: {0: (CC.DETAILED,)}
            },
            CC.ROUNDER_QUERY_ORDERING: {
                0: {0: (CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_VOTINGLEVELS, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_18_64_116, CC.HHGQ_8LEV, CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_40_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                        CR.join((CC.ATTR_RELGQ, CC.AGE_40_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)))},
                1: {0: (CC.DETAILED,)}
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


class DecompTestStrategyDHCP_20220811_exp_21_1(DHCPStrategy):
    @staticmethod
    def make(levels):
        test_strategy = defaultdict(lambda: defaultdict(dict))
        test_strategy.update({
            CC.GEODICT_GEOLEVELS: levels,
            CC.DPQUERIES + "default": (
                CR.join((CC.AGE_18_64_116, CC.RELGQ_4_GROUPS)),
                CR.join((CC.AGE_18_64_116, CC.ATTR_SEX)),
                CR.join((CC.AGE_38_GROUPS, CC.ATTR_SEX)),
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX)),
                CR.join((CC.ATTR_SEX, CC.RELGQ_4_GROUPS)),
                CR.join((CC.GQ_CONSTR_GROUPS, CC.AGE_10_GROUPS)),
                CC.POPSEHSDTARGETS_RELSHIP,
                CR.join((CC.ATTR_HISP, CC.ATTR_SEX, CC.AGE_40_GROUPS, "relship_and_eight_level_GQ", CC.ATTR_CENRACE)),
                CR.join((CC.ATTR_RELGQ, CC.AGE_40_GROUPS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_SEX)),
                CC.DETAILED),
                CC.QUERIESPROP + "default": (tuple(Fr(1, 10) for _ in range(10))),
        })

        for level in test_strategy[CC.GEODICT_GEOLEVELS]:
            test_strategy[CC.DPQUERIES][level] = test_strategy[CC.DPQUERIES + "default"]
            test_strategy[CC.QUERIESPROP][level] = test_strategy[CC.QUERIESPROP + "default"]
        return test_strategy


class DecompTestStrategyDHCP_20220811_exp_21_2_US(DHCPStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"

    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = {CC.GEODICT_GEOLEVELS: levels2make}
        strategy[CC.DPQUERIES] = {
            "Block": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Block_Group": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Tract_Subset": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Tract_Subset_Group": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Prim": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "County": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "State": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "US": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", )
        }
        strategy[CC.QUERIESPROP] = {
            "Block": (Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), ),
            "Block_Group": (Fr(1110262, 25806400), Fr(1110262, 25806400), Fr(1110262, 25806400) * 3, Fr(1110262, 25806400), Fr(1110262, 25806400), Fr(1110262, 25806400), Fr(1110262, 25806400), Fr(1110262, 25806400), Fr(1110262, 25806400), Fr(1110262, 25806400) * 4, ),
            "Tract_Subset": (Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), Fr(2239342, 25806400), ),
            "Tract_Subset_Group": (Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), ),
            "Prim": (Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), Fr(1232579, 25806400), ),
            "County": (Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), Fr(799765, 25806400), ),
            "State": (Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), ),
            "US": (Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), Fr(188180, 25806400), )
        }
        strategy[CC.UNITDPQUERIES] = {
            "Block": (),
            "Block_Group": (),
            "Tract_Subset": (),
            "Tract_Subset_Group": (),
            "Prim": (),
            "County": (),
            "State": (),
            "US": ()
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


class DecompTestStrategyDHCP_20220811_exp_21_2_PR(DHCPStrategy):
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"

    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = {CC.GEODICT_GEOLEVELS: levels2make}
        strategy[CC.DPQUERIES] = {
            "Block": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Block_Group": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Tract_Subset": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Tract_Subset_Group": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "Prim": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "County": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", ),
            "State": ("age_18_64_116 * relgq_4_groups", "age_18_64_116 * sex", "age_26_groups * sex", "hispanic * sex", "sex * relgq_4_groups", "gq_constr_groups * age_10_groups", "popSehsdTargetsRelship", "hispanic * sex * age_29_groups * relship_and_eight_level_GQ * cenrace", "relgq * age_29_groups * hispanic * cenrace * sex", "detailed", )
        }
        strategy[CC.QUERIESPROP] = {
            "Block": (Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), Fr(28227, 25806400), ),
            "Block_Group": (Fr(1147898, 25806400), Fr(1147898, 25806400), Fr(1147898, 25806400) + Fr(1110262, 25806400) * 2, Fr(1147898, 25806400), Fr(1147898, 25806400), Fr(1147898, 25806400), Fr(1147898, 25806400), Fr(1147898, 25806400), Fr(1147898, 25806400), Fr(1147898, 25806400) + Fr(1110262, 25806400) * 3, ),
            "Tract_Subset": (Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), Fr(2276978, 25806400), ),
            "Tract_Subset_Group": (Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), ),
            "Prim": (Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), Fr(1270215, 25806400), ),
            "County": (Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), Fr(837401, 25806400), ),
            "State": (Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), Fr(2578066, 25806400), )
        }
        strategy[CC.UNITDPQUERIES] = {
            "Block": (),
            "Block_Group": (),
            "Tract_Subset": (),
            "Tract_Subset_Group": (),
            "Prim": (),
            "County": (),
            "State": ()
        }
        strategy[CC.UNITQUERIESPROP] = {
            "Block": (),
            "Block_Group": (),
            "Tract_Subset": (),
            "Tract_Subset_Group": (),
            "Prim": (),
            "County": (),
            "State": ()
        }
        return strategy


class DecompTestStrategyDHCP_20220922_exp_iteration_23_1:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State","US"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Block_Group"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["US"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.UNITDPQUERIES]["Block"] = ()
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ()
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ()
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ()
        strategy[CC.UNITDPQUERIES]["Prim"] = ()
        strategy[CC.UNITDPQUERIES]["County"] = ()
        strategy[CC.UNITDPQUERIES]["State"] = ()
        strategy[CC.UNITDPQUERIES]["US"] = ()
        strategy[CC.QUERIESPROP]["Block"] = (Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000), Fr(430, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868, 10000), Fr(3472, 10000), Fr(3472, 10000), Fr(868, 10000), Fr(868, 10000), Fr(868, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(1912, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(1912, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000), Fr(478, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310, 10000), Fr(1240, 10000), Fr(1240, 10000), Fr(310, 10000), Fr(310, 10000), Fr(310, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(3996, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000), Fr(999, 10000))
        strategy[CC.QUERIESPROP]["US"] = (Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(292, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000), Fr(73, 10000))
        strategy[CC.UNITQUERIESPROP]["Block"] = ()
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = ()
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = ()
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = ()
        strategy[CC.UNITQUERIESPROP]["Prim"] = ()
        strategy[CC.UNITQUERIESPROP]["County"] = ()
        strategy[CC.UNITQUERIESPROP]["State"] = ()
        strategy[CC.UNITQUERIESPROP]["US"] = ()
        return strategy


class DecompTestStrategyDHCP_PR_20220922_exp_iteration_23_1:
    levels = "Block","Block_Group","Tract_Subset","Tract_Subset_Group","Prim","County","State"
    def make(self, levels):
        levels2make = levels if levels else self.levels
        strategy = defaultdict(dict)
        strategy.update({CC.GEODICT_GEOLEVELS: levels2make})
        strategy[CC.DPQUERIES]["Block"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Block_Group"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Tract_Subset_Group"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["Prim"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["County"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.DPQUERIES]["State"] = ('age_18_64_116 * relgq_4_groups', 'age_18_64_116 * sex', 'age_38_groups * sex', 'hispanic * sex', 'sex * relgq_4_groups', 'gq_constr_groups * age_10_groups', 'popSehsdTargetsRelship', 'hispanic * sex * age_40_groups * relship_and_eight_level_GQ * cenrace', 'relgq * age_40_groups * hispanic * cenrace * sex', 'detailed')
        strategy[CC.UNITDPQUERIES]["Block"] = ()
        strategy[CC.UNITDPQUERIES]["Block_Group"] = ()
        strategy[CC.UNITDPQUERIES]["Tract_Subset"] = ()
        strategy[CC.UNITDPQUERIES]["Tract_Subset_Group"] = ()
        strategy[CC.UNITDPQUERIES]["Prim"] = ()
        strategy[CC.UNITDPQUERIES]["County"] = ()
        strategy[CC.UNITDPQUERIES]["State"] = ()
        strategy[CC.QUERIESPROP]["Block"] = (Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000), Fr(11, 10000))
        strategy[CC.QUERIESPROP]["Block_Group"] = (Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000), Fr(446, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset"] = (Fr(878, 10000), Fr(878, 10000), Fr(878, 10000), Fr(878, 10000), Fr(878, 10000), Fr(3512, 10000), Fr(3512, 10000), Fr(878, 10000), Fr(878, 10000), Fr(878, 10000))
        strategy[CC.QUERIESPROP]["Tract_Subset_Group"] = (Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(1961, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000))
        strategy[CC.QUERIESPROP]["Prim"] = (Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(1961, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000), Fr(490, 10000))
        strategy[CC.QUERIESPROP]["County"] = (Fr(320, 10000), Fr(320, 10000), Fr(320, 10000), Fr(320, 10000), Fr(320, 10000), Fr(1280, 10000), Fr(1280, 10000), Fr(320, 10000), Fr(320, 10000), Fr(320, 10000))
        strategy[CC.QUERIESPROP]["State"] = (Fr(1009, 10000), Fr(1011, 10000), Fr(1011, 10000), Fr(1011, 10000), Fr(1011, 10000), Fr(4045, 10000), Fr(1011, 10000), Fr(1011, 10000), Fr(1011, 10000), Fr(1011, 10000))
        strategy[CC.UNITQUERIESPROP]["Block"] = ()
        strategy[CC.UNITQUERIESPROP]["Block_Group"] = ()
        strategy[CC.UNITQUERIESPROP]["Tract_Subset"] = ()
        strategy[CC.UNITQUERIESPROP]["Tract_Subset_Group"] = ()
        strategy[CC.UNITQUERIESPROP]["Prim"] = ()
        strategy[CC.UNITQUERIESPROP]["County"] = ()
        strategy[CC.UNITQUERIESPROP]["State"] = ()
        return strategy
