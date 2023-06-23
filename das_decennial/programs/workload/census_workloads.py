import numpy as np
import das_utils
from das_constants import DHCP, CC

def marginal(*margstrings):
    """ Given a sequence of recode strings, return a string that denotes their marginal """
    return " * ".join(margstrings)


def getWorkload(workload_keywords):
    """
    returns a list of unique queries based on one or more workloads

    Inputs:
        workload_keywords: a list of strings associated with the workloads desired

    Outputs:
        a list of strings/query names

    Notes:
        Since some workloads share queries (e.g. "total"), this function allows us to
        concatenate lists of queries (from multiple workloads) and remove any duplicates,
        as duplicates cause HDMM to not work as we might want/expect.
    """
    keys = das_utils.aslist(workload_keywords)
    querynames = []
    for key in keys:
        querynames += getWorkloadByKey(key)

    unique_querynames = np.unique(querynames).tolist()

    return unique_querynames


class WeightedWorkloads:
    <xl_flag>phil_manual_weighted_workload = {
        CC.GENERIC_RECODE_DETAILED: 0.05/0.8,
        CC.ATTR_HHGQ: 0.15/0.8,
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_CITIZEN): 0.4/0.8,
        marginal(CC.ATTR_AGE, CC.ATTR_SEX): 0.2/0.8,
    }

def getWorkloadByKey(workload_keyword):
    """
    returns a list of workload query names associated with the workload_keyword

    Inputs:
        workload_keyword (string): a name given to a set of queries (aka workload)

    Outputs:
        a list of query names
    """

    ###############################################################################
    # Full Household2010 Workload (without "H" tables)
    ###############################################################################
    SF1_household = [
        "total",
        "hisp",
        "hisp * hhrace",
        "family",
        "nonfamily",
        "marriedfamily",
        "otherfamily",
        "otherfamily * hhsex",
        "alone",
        "notalone",
        "size1",
        "size2plus",
        "hhsex * size1",
        "married_with_children_indicator",
        "hhsex * other_with_children_indicator",
        "hhsex * nonfamily",
        "hhsex * notalone",
        "cohabiting",
        "cohabiting_with_children_indicator",
        "hhsex * no_spouse_or_partner",
        "hhsex * no_spouse_or_partner_levels",
        "hhage * family",
        "hhage * nonfamily",
        "presence60",
        "family * presence60",
        "marriedfamily * presence60",
        "otherfamily * presence60",
        "hhsex * otherfamily * presence60",
        "nonfamily * presence60",
        "size1 * presence60",
        "size2plus * presence60",
        "notalone * presence60",
        "presence65",
        "presence65 * size1",
        "presence65 * size2plus",
        "presence65 * notalone",
        "presence65 * family",
        "presence75",
        "presence75 * size1",
        "presence75 * size2plus",
        "presence75 * notalone",
        "presence75 * family",
        "family * sizex01",
        "nonfamily * sizex0",
        "married_with_children_levels",
        "hhsex * other_with_children_levels",
        "hhrace",
        "hhrace * family",
        "hhrace * nonfamily",
        "hhrace * marriedfamily",
        "hhrace * otherfamily",
        "hhrace * otherfamily * hhsex",
        "hhrace * alone",
        "hhrace * notalone",
        "hisp * family",
        "hisp * nonfamily",
        "hisp * marriedfamily",
        "hisp * otherfamily",
        "hisp * otherfamily * hhsex",
        "hisp * alone",
        "hisp * notalone",
        "whiteonly * hisp",
        "whiteonly * hisp * family",
        "whiteonly * hisp * nonfamily",
        "whiteonly * hisp * marriedfamily",
        "whiteonly * hisp * otherfamily",
        "whiteonly * hisp * otherfamily * hhsex",
        "whiteonly * hisp * alone",
        "whiteonly * hisp * notalone",
        "hhrace * family * sizex01",
        "hhrace * nonfamily * sizex0",
        "hisp * family * sizex01",
        "hisp * nonfamily * sizex0",
        "whiteonly * hisp * family * sizex01",
        "whiteonly * hisp * nonfamily * sizex0",
        "hhrace * married_with_children_indicator",
        "hhrace * married_with_children_levels",
        "hhrace * hhsex * other_with_children_indicator",
        "hhrace * hhsex * other_with_children_levels",
        "hisp * married_with_children_indicator",
        "hisp * married_with_children_levels",
        "hisp * hhsex * other_with_children_indicator",
        "hisp * hhsex * other_with_children_levels",
        "whiteonly * hisp * married_with_children_indicator",
        "whiteonly * hisp * married_with_children_levels",
        "whiteonly * hisp * hhsex * other_with_children_indicator",
        "whiteonly * hisp * hhsex * other_with_children_levels",
        "multi",
        "couplelevels",
        "oppositelevels",
        "samelevels",
        "hhsex * samelevels",
        "hhsex * alone * hhover65",
        "hhsex * notalone * hhover65",
        "hhrace * multi",
        "hisp * multi",
        "whiteonly * hisp * multi"
        ]

    ###############################################################################
    # Full Household2010 Workload (with "H" tables)
    ###############################################################################
    SF1_household_with_tenure = [
        "total",
        "hisp",
        "hisp * hhrace",
        "family",
        "nonfamily",
        "marriedfamily",
        "otherfamily",
        "otherfamily * hhsex",
        "alone",
        "notalone",
        "size1",
        "size2plus",
        "hhsex * size1",
        "married_with_children_indicator",
        "hhsex * other_with_children_indicator",
        "hhsex * nonfamily",
        "hhsex * notalone",
        "cohabiting",
        "cohabiting_with_children_indicator",
        "hhsex * no_spouse_or_partner",
        "hhsex * no_spouse_or_partner_levels",
        "hhage * family",
        "hhage * nonfamily",
        "presence60",
        "family * presence60",
        "marriedfamily * presence60",
        "otherfamily * presence60",
        "hhsex * otherfamily * presence60",
        "nonfamily * presence60",
        "size1 * presence60",
        "size2plus * presence60",
        "notalone * presence60",
        "presence65",
        "presence65 * size1",
        "presence65 * size2plus",
        "presence65 * notalone",
        "presence65 * family",
        "presence75",
        "presence75 * size1",
        "presence75 * size2plus",
        "presence75 * notalone",
        "presence75 * family",
        "family * sizex01",
        "nonfamily * sizex0",
        "married_with_children_levels",
        "hhsex * other_with_children_levels",
        "hhrace",
        "hhrace * family",
        "hhrace * nonfamily",
        "hhrace * marriedfamily",
        "hhrace * otherfamily",
        "hhrace * otherfamily * hhsex",
        "hhrace * alone",
        "hhrace * notalone",
        "hisp * family",
        "hisp * nonfamily",
        "hisp * marriedfamily",
        "hisp * otherfamily",
        "hisp * otherfamily * hhsex",
        "hisp * alone",
        "hisp * notalone",
        "whiteonly * hisp",
        "whiteonly * hisp * family",
        "whiteonly * hisp * nonfamily",
        "whiteonly * hisp * marriedfamily",
        "whiteonly * hisp * otherfamily",
        "whiteonly * hisp * otherfamily * hhsex",
        "whiteonly * hisp * alone",
        "whiteonly * hisp * notalone",
        "hhrace * family * sizex01",
        "hhrace * nonfamily * sizex0",
        "hisp * family * sizex01",
        "hisp * nonfamily * sizex0",
        "whiteonly * hisp * family * sizex01",
        "whiteonly * hisp * nonfamily * sizex0",
        "hhrace * married_with_children_indicator",
        "hhrace * married_with_children_levels",
        "hhrace * hhsex * other_with_children_indicator",
        "hhrace * hhsex * other_with_children_levels",
        "hisp * married_with_children_indicator",
        "hisp * married_with_children_levels",
        "hisp * hhsex * other_with_children_indicator",
        "hisp * hhsex * other_with_children_levels",
        "whiteonly * hisp * married_with_children_indicator",
        "whiteonly * hisp * married_with_children_levels",
        "whiteonly * hisp * hhsex * other_with_children_indicator",
        "whiteonly * hisp * hhsex * other_with_children_levels",
        "multi",
        "couplelevels",
        "oppositelevels",
        "samelevels",
        "hhsex * samelevels",
        "hhsex * alone * hhover65",
        "hhsex * notalone * hhover65",
        "hhrace * multi",
        "hisp * multi",
        "whiteonly * hisp * multi"
        "sizex0",
        "rent",
        "rent * hhrace",
        "rent * hisp",
        "rent * sizex0",
        "rent * hhage",
        "rent * family",
        "rent * marriedfamily",
        "rent * marriedfamily * hhageH18",
        "rent * otherfamily",
        "rent * otherfamily * hhsex",
        "rent * otherfamily * hhsex * hhageH18",
        "rent * nonfamily",
        "rent * nonfamily * hhsex",
        "rent * alone * hhsex * hhageH18",
        "rent * notalone * hhsex * hhageH18",
        "hhrace * rent * sizex0",
        "hisp * rent * sizex0",
        "whiteonly * hisp * rent",
        "whiteonly * hisp * rent * sizex0",
        "whiteonly * hisp * rent * hhage",
        "hisp * rent * hhage",
        "hhrace * rent * hhage",
        "rent * hisp * hhrace"
        ]

    ###############################################################################
    # Full SF1 Person Workload
    ###############################################################################
    SF1_person = [
        "agecat",
        "agecatPCT12",
        "gqCollegeTotal",
        "gqCorrectionalTotal",
        "gqJuvenileTotal",
        "gqMilitaryTotal",
        "gqNursingTotal",
        "gqOtherInstTotal",
        "gqOtherNoninstTotal",
        "gqTotal",
        "hhgq",
        "hispanic",
        "instTotal",
        "institutionalized",
        "majorGQs",
        "majorRaces",
        "minorGQs",
        "noninstTotal",
        "over64yearsTotal",
        "racecomb",
        "relgq",
        "sex",
        "total",
        "under18years",
        "under18yearsTotal",
        "under20years",
        "under20yearsTotal",
        "gqCollegeTotal * agecatPCO8",
        "gqCollegeTotal * sex",
        "gqCorrectionalTotal * agecatPCO1",
        "gqCorrectionalTotal * sex",
        "gqJuvenileTotal * agecatPCO1",
        "gqJuvenileTotal * sex",
        "gqMilitaryTotal * agecatPCO8",
        "gqMilitaryTotal * sex",
        "gqNursingTotal * agecatPCO5",
        "gqNursingTotal * sex",
        "gqOtherInstTotal * agecatPCO1",
        "gqOtherInstTotal * sex",
        "gqOtherNoninstTotal * agecatPCO7",
        "gqOtherNoninstTotal * sex",
        "gqTotal * agecat43",
        "gqTotal * agecatPCO1",
        "gqTotal * sex",
        "hispanic * agecat",
        "hispanic * gqTotal",
        "hispanic * hhgq",
        "hispanic * institutionalized",
        "hispanic * majorGQs",
        "hispanic * majorRaces",
        "hispanic * over64yearsTotal",
        "hispanic * racecomb",
        "hispanic * relgq",
        "hispanic * sex",
        "householder * sex",
        "instTotal * agecatPCO1",
        "instTotal * sex",
        "institutionalized * agecat43",
        "institutionalized * sex",
        "majorGQs * agecat43",
        "majorGQs * sex",
        "majorRaces * agecat",
        "majorRaces * gqTotal",
        "majorRaces * hhgq",
        "majorRaces * institutionalized",
        "majorRaces * majorGQs",
        "majorRaces * over64yearsTotal",
        "majorRaces * relgq",
        "majorRaces * sex",
        "minorGQs * agecat43",
        "minorGQs * sex",
        "noninstTotal * agecatPCO7",
        "noninstTotal * sex",
        "over17yearsTotal * gqTotal",
        "over17yearsTotal * institutionalized",
        "over17yearsTotal * majorGQs",
        "over64yearsTotal * hhgq",
        "over64yearsTotal * rel34",
        "sex * agecat",
        "sex * agecatPCT12",
        "under18years * hhgq",
        "under18years * rel32",
        "under18yearsTotal * hhgq",
        "under18yearsTotal * rel32",
        "under20years * sex",
        "under20yearsTotal * sex",
        "whiteAlone * hispanic",
        "gqCollegeTotal * sex * agecatPCO8",
        "gqCorrectionalTotal * sex * agecatPCO1",
        "gqJuvenileTotal * sex * agecatPCO1",
        "gqMilitaryTotal * sex * agecatPCO8",
        "gqNursingTotal * sex * agecatPCO5",
        "gqOtherInstTotal * sex * agecatPCO1",
        "gqOtherNoninstTotal * sex * agecatPCO7",
        "gqTotal * sex * agecat43",
        "gqTotal * sex * agecatPCO1",
        "hispanic * gqTotal * agecat43",
        "hispanic * gqTotal * sex",
        "hispanic * householder * sex",
        "hispanic * institutionalized * agecat43",
        "hispanic * institutionalized * sex",
        "hispanic * majorGQs * agecat43",
        "hispanic * majorGQs * sex",
        "hispanic * over64yearsTotal * hhgq",
        "hispanic * over64yearsTotal * rel34",
        "hispanic * sex * agecat",
        "instTotal * sex * agecatPCO1",
        "institutionalized * sex * agecat43",
        "majorGQs * sex * agecat43",
        "majorRaces * gqTotal * agecat43",
        "majorRaces * gqTotal * sex",
        "majorRaces * householder * sex",
        "majorRaces * institutionalized * agecat43",
        "majorRaces * institutionalized * sex",
        "majorRaces * majorGQs * agecat43",
        "majorRaces * majorGQs * sex",
        "majorRaces * over64yearsTotal * hhgq",
        "majorRaces * over64yearsTotal * rel34",
        "majorRaces * sex * agecat",
        "minorGQs * sex * agecat43",
        "noninstTotal * sex * agecatPCO7",
        "over17yearsTotal * gqTotal * sex",
        "over17yearsTotal * institutionalized * sex",
        "over17yearsTotal * majorGQs * sex",
        "over64yearsTotal * householder * sex",
        "whiteAlone * hispanic * agecat",
        "whiteAlone * hispanic * gqTotal",
        "whiteAlone * hispanic * hhgq",
        "whiteAlone * hispanic * institutionalized",
        "whiteAlone * hispanic * majorGQs",
        "whiteAlone * hispanic * over64yearsTotal",
        "whiteAlone * hispanic * relgq",
        "whiteAlone * hispanic * sex",
        "hispanic * gqTotal * sex * agecat43",
        "hispanic * institutionalized * sex * agecat43",
        "hispanic * majorGQs * sex * agecat43",
        "hispanic * over64yearsTotal * householder * sex",
        "majorRaces * gqTotal * sex * agecat43",
        "majorRaces * institutionalized * sex * agecat43",
        "majorRaces * majorGQs * sex * agecat43",
        "majorRaces * over64yearsTotal * householder * sex",
        "whiteAlone * hispanic * gqTotal * agecat43",
        "whiteAlone * hispanic * gqTotal * sex",
        "whiteAlone * hispanic * householder * sex",
        "whiteAlone * hispanic * institutionalized * agecat43",
        "whiteAlone * hispanic * institutionalized * sex",
        "whiteAlone * hispanic * majorGQs * agecat43",
        "whiteAlone * hispanic * majorGQs * sex",
        "whiteAlone * hispanic * over64yearsTotal * hhgq",
        "whiteAlone * hispanic * over64yearsTotal * rel34",
        "whiteAlone * hispanic * sex * agecat",
        "whiteAlone * hispanic * gqTotal * sex * agecat43",
        "whiteAlone * hispanic * institutionalized * sex * agecat43",
        "whiteAlone * hispanic * majorGQs * sex * agecat43",
        "whiteAlone * hispanic * over64yearsTotal * householder * sex"
        ]

    ###############################################################################
    # Full PL94_CVAP Workload
    ###############################################################################
    workload_PL94_CVAP = [
        'cenrace',
        'citizen',
        'gqlevels',
        'hispanic',
        'household',
        'institutionalized',
        'numraces',
        'total',
        'votingage',
        'cenrace * citizen',
        'hispanic * cenrace',
        'hispanic * citizen',
        'hispanic * numraces',
        'numraces * citizen',
        'votingage * cenrace',
        'votingage * citizen',
        'votingage * hispanic',
        'votingage * numraces',
        'hispanic * cenrace * citizen',
        'hispanic * numraces * citizen',
        'votingage * cenrace * citizen',
        'votingage * hispanic * cenrace',
        'votingage * hispanic * citizen',
        'votingage * hispanic * numraces',
        'votingage * numraces * citizen',
        'votingage * hispanic * cenrace * citizen',
        'votingage * hispanic * numraces * citizen'
    ]


    ###############################################################################
    # Full PL94 Workload
    ###############################################################################
    PL94_workload = [
        "cenrace",
        "gqlevels",
        "hispanic",
        "household",
        "institutionalized",
        "numraces",
        CC.GENERIC_RECODE_TOTAL,
        "votingage",
        "hispanic * cenrace",
        "hispanic * numraces",
        "votingage * cenrace",
        "votingage * hispanic",
        "votingage * numraces",
        "votingage * hispanic * cenrace",
        "votingage * hispanic * numraces"
    ]

    P1_workload = [
        CC.GENERIC_RECODE_TOTAL
    ]

    workload_1940 = [
        CC.GENERIC_RECODE_TOTAL,
        "race",
        "gqlevels",
        "hispanic",
        "household",
        "institutionalized",
        "votingage",
        "citizen",
        "hispanic * race",
        "citizen * race",
        "citizen * votingage",
        "citizen * hispanic",
        "votingage * race",
        "votingage * hispanic",
        "votingage * hispanic * race"
    ]


    ########################################################
    #
    #   Generic Workload that puts all privacy budget onto
    # the detailed query. A good baseline
    #
    #######################################################

    workload_generic_detailed = [CC.GENERIC_RECODE_DETAILED]

    ########################################################
    ###
    ### PL94+CVAP workload using schemamaker api
    ###
    ###
    ########################################################

    workload_pl94_cvap_over_dhcp = [
        CC.GENERIC_RECODE_TOTAL,
        CC.ATTR_CENRACE,
        CC.ATTR_CITIZEN,
        CC.HHGQ_GQLEVELS,
        CC.ATTR_HISP,
        CC.HHGQ_2LEVEL,
        CC.HHGQ_INSTLEVELS,
        CC.CENRACE_NUM,
        # total is covered by P1 in DHCP_HHGQ
        CC.AGE_VOTINGLEVELS,
        marginal(CC.ATTR_CENRACE, CC.ATTR_CITIZEN),
        marginal(CC.ATTR_HISP, CC.ATTR_CENRACE),
        marginal(CC.ATTR_HISP, CC.ATTR_CITIZEN),
        marginal(CC.ATTR_HISP, CC.CENRACE_NUM),
        marginal(CC.CENRACE_NUM, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_CENRACE),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP),
        marginal(CC.AGE_VOTINGLEVELS, CC.CENRACE_NUM),
        marginal(CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_CITIZEN),
        marginal(CC.ATTR_HISP, CC.CENRACE_NUM, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_CENRACE, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.ATTR_CENRACE),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.CENRACE_NUM),
        marginal(CC.AGE_VOTINGLEVELS, CC.CENRACE_NUM, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_CITIZEN),
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.CENRACE_NUM, CC.ATTR_CITIZEN),
    ]

    ########################################################
    #  Test DHCP workload
    ########################################################
    workload_test_dhcp = [marginal(CC.ATTR_SEX, CC.ATTR_AGE)]

    ########################################################
    ### DHCP HHGQ Workload                           #######
    ### Not supported:
    ###    P2
    ###    P17 not listed in document
    ###    P29 - join table
    ###    P34 - join table
    ###    P41 - join table
    ###    P29A - P29I join table
    ###    P34A - P34I join table
    ########################################################
    workload_dhcp_hhgq = workload_pl94_cvap_over_dhcp + [
        #P1 covered by PL94 CVAP
        #P2 unsupported
        CC.CENRACE_MAJOR, #P3
        #P4 covered by PL94 CVAP
        marginal(CC.ATTR_HISP, CC.CENRACE_MAJOR), #P5
        CC.CENRACE_COMB, #P6
        marginal(CC.ATTR_HISP, CC.CENRACE_COMB), #P7
        #P8 - P11 are deleted
        marginal(CC.ATTR_SEX, CC.ATTR_AGE), #P12 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4), #P12 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16),  # P12 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64),  # P12 (HB tree)
        #P13 covered by sex x agegroups from P12
        #P14 covered by sex x agegroups from P12
        #P15 universe unit
        marginal(CC.HHGQ_HOUSEHOLD_TOTAL, CC.AGE_VOTINGLEVELS), #P16
        #P17 not listed
        #P18 - P28  unit table
        #P29 join table
        #P30 - P33 not listed
        #P34 join table
        #P35 - P37 not listed
        #P38 unit table
        #P39 - P40 not listed
        #P41 join table
        #P42 deleted from 2020
        marginal(CC.ATTR_SEX, CC.AGE_CAT43, CC.HHGQ_GQLEVELS), # P43
        #P44 - P51 deleted from 2020
        #P52 unit table
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.CENRACE_MAJOR),  # P12 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.CENRACE_MAJOR),  # P12 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.CENRACE_MAJOR),  # P12 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.CENRACE_MAJOR),  # P12 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HISP_TOTAL),  # P12H (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HISP_TOTAL),  # P12H (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HISP_TOTAL),  # P12H (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HISP_TOTAL),  # P12H (HB tree)
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # P12I (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # P12I (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # P12I (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # P12I (HB tree)
        #P13 A-G covered by P12 A-G
        #P13 H covered by P12 H
        #P13 I covered by P12 I
        #P18A - P28I unit table
        #P29A - P29I join table
        #P34A - P34I join table
        #P38A-I Unit table
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HHGQ_2LEVEL),  # PC01 and PCT13 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HHGQ_2LEVEL),  # PC01 and PCT13(HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HHGQ_2LEVEL),  # PC01 and PCT13(HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HHGQ_2LEVEL),  # PC01 and PCT13(HB tree)
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HHGQ_GQLEVELS),  # PC03, PC04, PC05, PC06, PC08, PC09, PC10 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HHGQ_GQLEVELS),  # PC03, PC04, PC05, PC06, PC08, PC09, PC10 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HHGQ_GQLEVELS),  # PC03, PC04, PC05, PC06, PC08, PC09, PC10 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HHGQ_GQLEVELS),  # PC03, PC04, PC05, PC06, PC08, PC09, PC10 (HB tree)
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HHGQ_INSTLEVELS),  # PC02, PC07 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HHGQ_INSTLEVELS),  # PC02, PC07 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HHGQ_INSTLEVELS),  # PC02, PC07 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HHGQ_INSTLEVELS),  # PC02, PC07 (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_CAT43, CC.HHGQ_GQLEVELS, CC.CENRACE_MAJOR),  # PCO43A-G
        marginal(CC.ATTR_SEX, CC.AGE_CAT43, CC.HHGQ_GQLEVELS, CC.HISP_TOTAL),  # PCO43H
        marginal(CC.ATTR_SEX, CC.AGE_CAT43, CC.HHGQ_GQLEVELS, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # PCO43I
        #PCT12 covered by P12
        #PCT14 - PCT18 Unit table
        #PCT19 - PCT20 deleted for 2020
        marginal(CC.ATTR_SEX, CC.AGE_CAT43, CC.HHGQ_GQLEVELS), #PCT21 PARTIALLY SUPPORTED
        #PCT22 covered by P43
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HHGQ_HOUSEHOLD_TOTAL, CC.CENRACE_MAJOR),  # PCT13 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HHGQ_HOUSEHOLD_TOTAL, CC.CENRACE_MAJOR),  # PCT13 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HHGQ_HOUSEHOLD_TOTAL, CC.CENRACE_MAJOR),  # PCT13 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HHGQ_HOUSEHOLD_TOTAL, CC.CENRACE_MAJOR),  # PCT13 A-G (HB tree)
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_TOTAL),  # PCT13 H (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_TOTAL),  # PCT13 H (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_TOTAL),  # PCT13 H (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_TOTAL),  # PCT13 H (HB tree)
        marginal(CC.ATTR_SEX, CC.ATTR_AGE, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # PCT13 I (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP4, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # PCT13 I (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP16, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # PCT13 I (HB tree)
        marginal(CC.ATTR_SEX, CC.AGE_GROUP64, CC.HHGQ_HOUSEHOLD_TOTAL, CC.HISP_NOT_TOTAL, CC.CENRACE_WHITEALONE),  # PCT13 I (HB tree)
        #PCT14A - PCT14I Unit table
        # H1 - H7 Unit table
        # H8, H9 deleted from 2020
        # H10 covered by PL94 CVAP (HHGQ_2LEVEL)
        # H13 - HCT4 Units table
        ]

    workload_dhcp_hhgq_detailed = workload_dhcp_hhgq + workload_generic_detailed


    #####################################################
    ### DHCH Workload                               #####
    #####################################################

    workload_household2010 = [
        #P1 - P7 Person table
        #P8 - P11 Deleted
        #P12 - P14 Person table
        #P15
        #P16 Person table
        #P17 deleted
        #P18
        #P19
        #P20
        #P21 deleted
        #P22
        #P23
        #P24
        #P25
        #P26
        #P27 deleted
        #P28
        #P29 Person table
        #P30-P33 deleted
        #P34 Person table
        #P38
        #P41, P43 Person table
        #P52 Unit table; not supported
        #P12A-I,P13A-I Person table
        #P18A-I
        #P28A-I
        #P29A-I, P34A-I Person table
        #P38A-I
        #PCO1-PCO10 Person table
        #PCO43 Person table
        #PCT12, PCT13 Person table
        #PCT14
        #PCT15
        #PCT18
        #PCT21, PCT22 Person table
        #PCT13A-I Person table
        #PCT14A-I

        ############################
        #H1 - HCT 4 Mostly not supported; tenure
        ############################
        "total",
        CC.ATTR_HHHISP,
        marginal(CC.ATTR_HHHISP, CC.ATTR_HHRACE),
        CC.HHTYPE_FAMILY,
        CC.HHTYPE_NONFAMILY,
        CC.HHTYPE_FAMILY_MARRIED,
        CC.HHTYPE_FAMILY_OTHER,
        marginal(CC.HHTYPE_FAMILY_OTHER, CC.ATTR_HHSEX),
        CC.HHTYPE_ALONE,
        CC.HHTYPE_NOT_ALONE,
        CC.HHSIZE_ONE,
        CC.HHSIZE_TWOPLUS,
        marginal(CC.ATTR_HHSEX, CC.HHSIZE_ONE),
        CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR,
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR),
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_NONFAMILY),
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_NOT_ALONE),
        CC.HHTYPE_COHABITING,
        CC.HHTYPE_COHABITING_WITH_CHILDREN_INDICATOR,
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_NO_SPOUSE_OR_PARTNER),
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_NO_SPOUSE_OR_PARTNER_LEVELS),
        marginal(CC.ATTR_HHAGE, CC.HHTYPE_FAMILY),
        marginal(CC.ATTR_HHAGE, CC.HHTYPE_NONFAMILY),
        CC.HHELDERLY_PRESENCE_60,
        marginal(CC.HHTYPE_FAMILY, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.HHTYPE_FAMILY_MARRIED, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.HHTYPE_FAMILY_OTHER, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.HHTYPE_NONFAMILY, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.HHSIZE_ONE, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.HHSIZE_TWOPLUS, CC.HHELDERLY_PRESENCE_60),
        marginal(CC.HHTYPE_NOT_ALONE, CC.HHELDERLY_PRESENCE_60),
        CC.HHELDERLY_PRESENCE_65,
        marginal(CC.HHELDERLY_PRESENCE_65, CC.HHSIZE_ONE),
        marginal(CC.HHELDERLY_PRESENCE_65, CC.HHSIZE_TWOPLUS),
        marginal(CC.HHELDERLY_PRESENCE_65, CC.HHTYPE_NOT_ALONE),
        marginal(CC.HHELDERLY_PRESENCE_65, CC.HHTYPE_FAMILY),
        CC.HHELDERLY_PRESENCE_75,
        marginal(CC.HHELDERLY_PRESENCE_75, CC.HHSIZE_ONE),
        marginal(CC.HHELDERLY_PRESENCE_75, CC.HHSIZE_TWOPLUS),
        marginal(CC.HHELDERLY_PRESENCE_75, CC.HHTYPE_NOT_ALONE),
        marginal(CC.HHELDERLY_PRESENCE_75, CC.HHTYPE_FAMILY),
        marginal(CC.HHTYPE_FAMILY, CC.HHSIZE_NOTALONE_VECT),
        marginal(CC.HHTYPE_NONFAMILY, CC.HHSIZE_NONVACANT_VECT),
        CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS,
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS),
        CC.ATTR_HHRACE,
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_NONFAMILY),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY_MARRIED),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY_OTHER),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY_OTHER, CC.ATTR_HHSEX),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_ALONE),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_NOT_ALONE),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_NONFAMILY),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_MARRIED),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_OTHER),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_OTHER, CC.ATTR_HHSEX),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_ALONE),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_NOT_ALONE),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_NONFAMILY),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_MARRIED),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_OTHER),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_OTHER, CC.ATTR_HHSEX),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_ALONE),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_NOT_ALONE),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY, CC.HHSIZE_NOTALONE_VECT),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_NONFAMILY, CC.HHSIZE_NONVACANT_VECT),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY, CC.HHSIZE_NOTALONE_VECT),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_NONFAMILY, CC.HHSIZE_NONVACANT_VECT),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY, CC.HHSIZE_NOTALONE_VECT),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_NONFAMILY, CC.HHSIZE_NONVACANT_VECT),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR),
        marginal(CC.ATTR_HHRACE, CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS),
        marginal(CC.ATTR_HHRACE, CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR),
        marginal(CC.ATTR_HHRACE, CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR),
        marginal(CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS),
        marginal(CC.ATTR_HHHISP, CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR),
        marginal(CC.ATTR_HHHISP, CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.ATTR_HHSEX, CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS),
        CC.ATTR_HHMULTI,
        CC.HHTYPE_COUPLE_LEVELS,
        CC.HHTYPE_OPPOSITE_SEX_LEVELS,
        CC.HHTYPE_SAME_SEX_LEVELS,
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_SAME_SEX_LEVELS),
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_ALONE, CC.HHAGE_OVER65),
        marginal(CC.ATTR_HHSEX, CC.HHTYPE_NOT_ALONE, CC.HHAGE_OVER65),
        marginal(CC.ATTR_HHRACE, CC.ATTR_HHMULTI),
        marginal(CC.ATTR_HHHISP, CC.ATTR_HHMULTI),
        marginal(CC.HHRACE_WHITEALONE, CC.ATTR_HHHISP, CC.ATTR_HHMULTI)
        ]

    demo_products_workload = workload_generic_detailed + [
        CC.ATTR_HHGQ,
        marginal(CC.AGE_VOTINGLEVELS, CC.ATTR_HISP, CC.ATTR_CENRACE, CC.ATTR_CITIZEN),
        marginal(CC.ATTR_AGE, CC.ATTR_SEX),
        marginal(CC.AGE_GROUP4, CC.ATTR_SEX),
        marginal(CC.AGE_GROUP16, CC.ATTR_SEX),
        marginal(CC.AGE_GROUP64, CC.ATTR_SEX),
    ]


    old_pl94_manual_workload = workload_generic_detailed + [
        'hhgq',
        'cenrace * hispanic * votingage',
    ]

    workload_dict = {
        "SF1_Household": SF1_household,
        "SF1_Person": SF1_person,
        "PL94_CVAP": workload_PL94_CVAP,
        "PL94": PL94_workload,
        "P1"  : P1_workload,
        "WORKLOAD_1940"  : workload_1940,
        "<xl_flag>phil_manual_workload": list(WeightedWorkloads.<xl_flag>phil_manual_weighted_workload),
        CC.WORKLOAD_DETAILED: workload_generic_detailed,
        CC.WORKLOAD_DHCP_HHGQ : workload_dhcp_hhgq,
        CC.WORKLOAD_DHCP_HHGQ_DETAILED : workload_dhcp_hhgq_detailed,
        CC.WORKLOAD_DHCP_PL94_CVAP : workload_pl94_cvap_over_dhcp,
        CC.WORKLOAD_HOUSEHOLD2010 : workload_household2010,
        CC.WORKLOAD_DHCP_TEST: workload_test_dhcp,
        CC.DEMO_PRODUCTS_WORKLOAD: demo_products_workload,
        "old_pl94_manual_workload": old_pl94_manual_workload,
    }

    assert workload_keyword in workload_dict.keys(), f"workload_keyword {workload_keyword} is not in the workload dict."

    return workload_dict[workload_keyword]


def isWorkload(workload_keyword):
    """
    determines whether or not the workload_keyword is a valid workload keyword

    Inputs:
        workload_keyword: a string

    Outputs:
        a boolean value - is this keyword a valid workload keyword?
    """
    try:
        getWorkload(workload_keyword)
        valid_workload = True
    except (AssertionError, KeyError):
        valid_workload = False

    return valid_workload


def expandKeywords(querynames):
    """
    looks through the querynames provided and expands any keywords found that refer to workloads

    Inputs:
        querynames: list of strings

    Outputs:
        list of strings (querynames)

    Notes:
        This allows for the use of keywords as shorthand for commonly used
        sets of queries (e.g. PL94, P12, etc.)

        Note that it does not check to see if the queries themselves are
        valid for a particular schema; this function only expands valid
        workload keywords into querynames and appends those querynames to the
        list of querynames that will be returned.
    """
    expandednames = []
    querynames = das_utils.aslist(querynames)
    for name in querynames:
        try:
            keynames = getWorkload(name)
        except (AssertionError, KeyError):
            keynames = [name]

        expandednames += keynames

    return np.unique(expandednames).tolist()
