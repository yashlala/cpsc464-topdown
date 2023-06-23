from programs.schema.schemas.schemamaker import SchemaMaker
import programs.schema.table_building.tablebuilder as tablebuilder
from das_constants import CC

"""
This file defines the tables for the DHC-H.
Last updated 8/1/2019.
"""

def getTableDict():
    tabledict = {
        ################################################
        # Household2010 Proposed Tables for 2020
        ################################################

        # Table P15 - Hispanic or Latino Origin of Householder by Race of Householder
        # Universe: Households
        "P15": ["total",
            "hisp", "hisp * hhrace"],

        # Table P18 - Household Type
        # Universe: Households
        "P18": ["total",
            "family",
                "marriedfamily",
                "otherfamily",
                "otherfamily * hhsex",
            "nonfamily",
                "alone",
                "notalone" ],

        # Table P19 - Household Size by Household Type by Presence of Own Children
        # Universe: Households
        "P19": ["isOccupied",
            "size1",
                "size1 * hhsex",
            "size2plus",
                "size2plus * family",
                    "size2plus * marriedfamily",
                        "size2plus * married_with_children_indicator",
                     "size2plus * otherfamily",
                        "size2plus * otherfamily * hhsex",
                            "size2plus * hhsex * other_with_children_indicator",
                "size2plus * nonfamily",
                    "size2plus * nonfamily * hhsex"],

        # Table 20 - Households by Type and Presence of Own Children Under 18
        # Universe: Households
        "P20": ["total",
            "marriedfamily",
                "married_with_children_indicator",
            "cohabiting",
                "cohabiting_with_children_indicator",
            "hhsex * no_spouse_or_partner",
                "hhsex * alone" ,
                    "hhsex * alone * age_65plus",
                "hhsex * no_spouse_or_partner_levels"],

        # Table P22 - Household Type by Age of Householder
        # Universe: Households
        "P22": ["total",

            "family",
                "hhage * family",
            "nonfamily",
                "hhage * nonfamily"],

        # Table P23 - Households by Presence of People 60 Years and Over by Household Type
        # Universe: Households
        "P23": ["total",

            "presence60",
                "family * presence60",
                    "presence60 * marriedfamily",
                    "presence60 * otherfamily",
                        "presence60 * otherfamily * hhsex",
                "presence60 * nonfamily"],

        # Table P24 - Households by Presence of People 60 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P24": ["isOccupied",
            "isOccupied * presence60",
                "presence60 * size1",
                "presence60 * size2plus",
                    "presence60 * size2plus * family",
                    "presence60 * size2plus * nonfamily"],

        # Table P25 - Households by Presence of People 65 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P25": ["isOccupied",
            "isOccupied * presence65",
                "presence65 * size1",
                "presence65 * size2plus",
                    "presence65 * size2plus * family",
                    "presence65 * size2plus * nonfamily"],

        # Table P26 - Households by Presence of People 75 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P26": ["isOccupied",
            "isOccupied * presence75",
                "presence75 * size1",
                "presence75 * size2plus",
                    "presence75 * size2plus * family",
                    "presence75 * size2plus * nonfamily"],

        # Table P28 - Household Type by Household Size
        "P28": ["isOccupied",
            "isOccupied * family",
                "family * sizex01",
            "isOccupied * nonfamily",
                "nonfamily * sizex0"],

        # Table P38 - Family Type by Presence and Age of Own Children
        # Universe: Families
        "P38": ["family",

            "marriedfamily",
                "married_with_children_indicator",
                    "married_with_children_levels",
            "otherfamily",
                "hhsex * otherfamily",
                    "hhsex * other_with_children_indicator",
                        "hhsex * other_with_children_levels"],

        #Table P18A
        # Universe: Households with white alone householder
        "P18A": ["isWhiteAlone",
            "isWhiteAlone * family", "isWhiteAlone * marriedfamily", "isWhiteAlone * otherfamily", "isWhiteAlone * otherfamily * hhsex", "isWhiteAlone * nonfamily",
            "isWhiteAlone * alone", "isWhiteAlone * notalone"
        ],

        # Table P18B
        # Universe: Households with black alone householder
        "P18B": ["isBlackAlone",
                 "isBlackAlone * family", "isBlackAlone * marriedfamily", "isBlackAlone * otherfamily", "isBlackAlone * otherfamily * hhsex",
                 "isBlackAlone * nonfamily", "isBlackAlone * alone", "isBlackAlone * notalone"],

        # Table P18C
        # Universe: Households with AIAN alone householder
        "P18C": ["isAIANAlone",
                 "isAIANAlone * family", "isAIANAlone * marriedfamily", "isAIANAlone * otherfamily", "isAIANAlone * otherfamily * hhsex",
                 "isAIANAlone * nonfamily", "isAIANAlone * alone", "isAIANAlone * notalone"],

        # Table P18D
        # Universe: Households with Asian alone householder
        "P18D": ["isAsianAlone",
                 "isAsianAlone * family", "isAsianAlone * marriedfamily", "isAsianAlone * otherfamily", "isAsianAlone * otherfamily * hhsex",
                 "isAsianAlone * nonfamily", "isAsianAlone * alone", "isAsianAlone * notalone"],

        # Table P18E
        # Universe: Households with NHOPI alone householder
        "P18E": ["isNHOPIAlone",
                 "isNHOPIAlone * family", "isNHOPIAlone * marriedfamily", "isNHOPIAlone * otherfamily", "isNHOPIAlone * otherfamily * hhsex",
                 "isNHOPIAlone * nonfamily", "isNHOPIAlone * alone", "isNHOPIAlone * notalone"],

        # Table P18F
        # Universe: Households with SOR alone householder
        "P18F": ["isSORAlone",
                 "isSORAlone * family", "isSORAlone * marriedfamily", "isSORAlone * otherfamily", "isSORAlone * otherfamily * hhsex",
                 "isSORAlone * nonfamily", "isSORAlone * alone", "isSORAlone * notalone"],

        # Table P18G
        # Universe: Households with a householder of two or more races
        "P18G": ["isTMRaces",
                 "isTMRaces * family", "isTMRaces * marriedfamily", "isTMRaces * otherfamily", "isTMRaces * otherfamily * hhsex",
                 "isTMRaces * nonfamily", "isTMRaces * alone", "isTMRaces * notalone"],

        # Table P18H
        "P18H": ["isHisp",
            "isHisp * family",
                 "isHisp * marriedfamily",
                 "isHisp * otherfamily",
                    "isHisp * otherfamily * hhsex",
            "isHisp * nonfamily",
                "isHisp * alone",
                "isHisp * notalone"],

        # Table P18I
        "P18I": ["isWhiteAlone * isNotHisp",  # to get the totals for each of the white alone x hispanic categories

            "isWhiteAlone * isNotHisp * family",
                "isWhiteAlone * isNotHisp * marriedfamily",
                "isWhiteAlone * isNotHisp * otherfamily",
                    "isWhiteAlone * isNotHisp * otherfamily * hhsex",
            "isWhiteAlone * isNotHisp * nonfamily",
                "isWhiteAlone * isNotHisp * alone",
                "isWhiteAlone * isNotHisp * notalone"],

        #Table P28A
        #Universe: Households with white alone householder
        "P28A": ["isOccupied * isWhiteAlone",  # to get the totals for each race
                   "isOccupied * isWhiteAlone * family", "isWhiteAlone * family * sizex01", "isOccupied * isWhiteAlone * nonfamily", "isWhiteAlone * nonfamily * sizex0"],

        # Table P28B
        # Universe: Households with black alone householder
        "P28B": ["isOccupied * isBlackAlone",  # to get the totals for each race
                   "isOccupied * isBlackAlone * family", "isBlackAlone * family * sizex01", "isOccupied * isBlackAlone * nonfamily", "isBlackAlone * nonfamily * sizex0"],

        # Table P28C
        # Universe: Households with AIAN alone householder
        "P28C": ["isOccupied * isAIANAlone",  # to get the totals for each race
                   "isOccupied * isAIANAlone * family", "isAIANAlone * family * sizex01", "isOccupied * isAIANAlone * nonfamily", "isAIANAlone * nonfamily * sizex0"],

        # Table P28D
        # Universe: Households with Asian alone householder
        "P28D": ["isOccupied * isAsianAlone",  # to get the totals for each race
                   "isOccupied * isAsianAlone * family", "isAsianAlone * family * sizex01", "isOccupied * isAsianAlone * nonfamily", "isAsianAlone * nonfamily * sizex0"],

        # Table P28E
        # Universe: Households with NHOPI alone householder
        "P28E": ["isOccupied * isNHOPIAlone",  # to get the totals for each race
                   "isOccupied * isNHOPIAlone * family", "isNHOPIAlone * family * sizex01", "isOccupied * isNHOPIAlone * nonfamily", "isNHOPIAlone * nonfamily * sizex0"],

        # Table P28F
        # Universe: Households with SOR alone householder
        "P28F": ["isOccupied * isSORAlone",  # to get the totals for each race
                   "isOccupied * isSORAlone * family", "isSORAlone * family * sizex01", "isOccupied * isSORAlone * nonfamily", "isSORAlone * nonfamily * sizex0"],

        # Table P28G
        # Universe: Households with a householder of two or more races
        "P28G": ["isOccupied * isTMRaces",  # to get the totals for each race
                   "isOccupied * isTMRaces * family", "isTMRaces * family * sizex01", "isOccupied * isTMRaces * nonfamily", "isTMRaces * nonfamily * sizex0"],

        # Table P28H
        "P28H": ["isOccupied * isHisp",  # to get the totals for each of the hispanic categories
            "isOccupied * isHisp * family",
                 "isHisp * family * sizex01",
            "isOccupied * isHisp * nonfamily",
                 "isHisp * nonfamily * sizex0"],

        # Table P28I
        "P28I": ["isOccupied * isWhiteAlone * isNotHisp",  # to get the totals for each of the white alone x hispanic categories
            "isOccupied * isWhiteAlone * isNotHisp * family",
                 "isWhiteAlone * isNotHisp * family * sizex01",
            "isOccupied * isWhiteAlone * isNotHisp * nonfamily",
                "isWhiteAlone * isNotHisp * nonfamily * sizex0"],

        #Table P38A:
        #Universe: Families with white alone householder
        "P38A": ["isWhiteAlone * family",  # to get the totals for each race
            "isWhiteAlone * marriedfamily", "isWhiteAlone * married_with_children_indicator", "isWhiteAlone * married_with_children_levels", "isWhiteAlone * otherfamily",
            "isWhiteAlone * hhsex * otherfamily", "isWhiteAlone * hhsex * other_with_children_indicator", "isWhiteAlone * hhsex * other_with_children_levels"
        ],

        # Table P38B:
        # Universe: Families with black alone householder
        "P38B": ["isBlackAlone * family",  # to get the totals for each race
            "isBlackAlone * marriedfamily", "isBlackAlone * married_with_children_indicator", "isBlackAlone * married_with_children_levels", "isBlackAlone * otherfamily",
            "isBlackAlone * hhsex * otherfamily", "isBlackAlone * hhsex * other_with_children_indicator", "isBlackAlone * hhsex * other_with_children_levels"
        ],

        # Table P38C:
        # Universe: Families with AIAN alone householder
        "P38C": ["isAIANAlone * family",  # to get the totals for each race
            "isAIANAlone * marriedfamily", "isAIANAlone * married_with_children_indicator", "isAIANAlone * married_with_children_levels", "isAIANAlone * otherfamily",
            "isAIANAlone * hhsex * otherfamily", "isAIANAlone * hhsex * other_with_children_indicator", "isAIANAlone * hhsex * other_with_children_levels"
        ],

        # Table P38D:
        # Universe: Families with Asian alone householder
        "P38D": ["isAsianAlone * family",  # to get the totals for each race
            "isAsianAlone * marriedfamily", "isAsianAlone * married_with_children_indicator", "isAsianAlone * married_with_children_levels", "isAsianAlone * otherfamily",
            "isAsianAlone * hhsex * otherfamily", "isAsianAlone * hhsex * other_with_children_indicator", "isAsianAlone * hhsex * other_with_children_levels"
        ],

        # Table P38E:
        # Universe: Families with NHOPI alone householder
        "P38E": ["isNHOPIAlone * family",  # to get the totals for each race
            "isNHOPIAlone * marriedfamily", "isNHOPIAlone * married_with_children_indicator", "isNHOPIAlone * married_with_children_levels", "isNHOPIAlone * otherfamily",
            "isNHOPIAlone * hhsex * otherfamily", "isNHOPIAlone * hhsex * other_with_children_indicator", "isNHOPIAlone * hhsex * other_with_children_levels"
        ],

        # Table P38F:
        # Universe: Families with SOR alone householder
        "P38F": ["isSORAlone * family",  # to get the totals for each race
            "isSORAlone * marriedfamily", "isSORAlone * married_with_children_indicator", "isSORAlone * married_with_children_levels", "isSORAlone * otherfamily",
            "isSORAlone * hhsex * otherfamily", "isSORAlone * hhsex * other_with_children_indicator", "isSORAlone * hhsex * other_with_children_levels"
        ],

        # Table P38G:
        # Universe: Families with a householder of two or more races
        "P38G": ["isTMRaces * family",  # to get the totals for each race
            "isTMRaces * marriedfamily", "isTMRaces * married_with_children_indicator", "isTMRaces * married_with_children_levels", "isTMRaces * otherfamily",
            "isTMRaces * hhsex * otherfamily", "isTMRaces * hhsex * other_with_children_indicator", "isTMRaces * hhsex * other_with_children_levels"
        ],

        # Table P38H
        "P38H": [
            "isHisp * family",  # to get the totals
            "isHisp * marriedfamily",
                "isHisp * married_with_children_indicator",
                    "isHisp * married_with_children_levels",
            "isHisp * otherfamily",
                "isHisp * hhsex * otherfamily",
                    "isHisp * hhsex * other_with_children_indicator",
                        "isHisp * hhsex * other_with_children_levels"],

        # Table P38I
        "P38I": [
            "isWhiteAlone * isNotHisp * family",  # to get the totals for white alone x hispanic categories

            "isWhiteAlone * isNotHisp * marriedfamily",
                "isWhiteAlone * isNotHisp * married_with_children_indicator",
                    "isWhiteAlone * isNotHisp * married_with_children_levels",
            "isWhiteAlone * isNotHisp * otherfamily",
                "isWhiteAlone * isNotHisp * hhsex * otherfamily",
                    "isWhiteAlone * isNotHisp * hhsex * other_with_children_indicator",
                        "isWhiteAlone * isNotHisp * hhsex * other_with_children_levels"],

        # Table PCT14
        "PCT14": ["total",
            "multi"],

        # Table PCT15
        "PCT15": ["total",
            "couplelevels",
                "oppositelevels",
                "samelevels",
                    "hhsex * samelevels"],

        # Table PCT18
        "PCT18": ["nonfamily",
            "hhsex * nonfamily",
                "hhsex * alone",
                    "hhsex * alone * hhover65",
                "hhsex * notalone",
                    "hhsex * notalone * hhover65"],

        #Table PCT14A
        #Universe: Households with a householder who is white alone
        "PCT14A": ["isWhiteAlone", "isWhiteAlone * multi"],

        # Table PCT14B
        # Universe: Households with a householder who is black alone
        "PCT14B": ["isBlackAlone", "isBlackAlone * multi"],

        # Table PCT14C
        # Universe: Households with a householder who is AIAN alone
        "PCT14C": ["isAIANAlone", "isAIANAlone * multi"],

        # Table PCT14D
        # Universe: Households with a householder who is Asian alone
        "PCT14D": ["isAsianAlone", "isAsianAlone * multi"],

        # Table PCT14E
        # Universe: Households with a householder who is NHOPI alone
        "PCT14E": ["isNHOPIAlone", "isNHOPIAlone * multi"],

        # Table PCT14F
        # Universe: Households with a householder who is SOR alone
        "PCT14F": ["isSORAlone", "isSORAlone * multi"],

        # Table PCT14G
        # Universe: Households with a householder who is of two or races
        "PCT14G": ["isTMRaces", "isTMRaces * multi"],

        # Table PCT14H
        # Universe: Households with a householder who is hispanic or latino
        "PCT14H": ["isHisp", "isHisp * multi"],

        # Table PCT14I
        # Universe: Households with a householder who is white alone, non-hispanic
        "PCT14I": ["isWhiteAlone * isNotHisp", "isWhiteAlone * isNotHisp * multi"],

        # Table H1

        # Table H3

        # Table H6
        "H6": ["isOccupied",
            "isOccupied * hhrace"],

        # Table H7
        "H7": ["isOccupied",
            "isOccupied * hisp",
            "isOccupied * hisp * hhrace"],

        # Table H13
        "H13": ["isOccupied",
            "sizex0"],

    }

    return tabledict


def getTableBuilder():
    schema = SchemaMaker.fromName(CC.SCHEMA_HOUSEHOLD2010)
    tabledict = getTableDict()
    builder = tablebuilder.TableBuilder(schema, tabledict)
    return builder

#################################################################
#### Consolidated tables
#################################################################
'''
    # Tables P18A-G
    "P18A-G": ["hhrace",  # to get the totals for each race
               "hhrace * family", "hhrace * marriedfamily", "hhrace * otherfamily", "hhrace * otherfamily * hhsex", "hhrace * nonfamily",
               "hhrace * alone", "hhrace * notalone"],


    # Tables P28A-G
    "P28A-G": ["hhrace",  # to get the totals for each race
        "hhrace * family",
            "hhrace * family * sizex01",
       "hhrace * nonfamily",
            "hhrace * nonfamily * sizex0"],

    # Tables P38A-G
    "P38A-G": [
        "hhrace * family",  # to get the totals for each race
        "hhrace * marriedfamily",
            "hhrace * married_with_children_indicator",
                "hhrace * married_with_children_levels",
        "hhrace * otherfamily",
            "hhrace * hhsex * otherfamily",
                "hhrace * hhsex * other_with_children_indicator",
                    "hhrace * hhsex * other_with_children_levels"],

    # Tables PCT14A-I
    "PCT14A-I": ["hhrace", "hhrace * multi",
        "isHisp", "isHisp * multi", "isWhiteAlone * isNotHisp", "isWhiteAlone * isNotHisp * multi"
    ],

'''