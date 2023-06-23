"""
Validate the Constraints on the MDF Files
Using the following files as reference for the various structrual zeros
https://{GIT_HOST_NAME}/DAS/das_decennial/blob/refactor/
        spine_opt_gets_budget_obj/programs/constraints/Constraints_DHCH.py
https://{GIT_HOST_NAME}/DAS/das_decennial/blob/refactor/
        spine_opt_gets_budget_obj/programs/constraints/Constraints_DHCP.py
Load the finished MDF and check that all structural zero constraints specified
in the accompanying config file are met.
Usually run at the end of programs/validator/end2endvalidator.py
"""

import os, sys
from argparse import ArgumentParser
from configparser import ConfigParser
from os.path import dirname, abspath

sys.path.append(os.path.join(dirname(abspath(__file__)), "../.." ))
try:
    from ctools.hierarchical_configparser import HierarchicalConfigParser
except ModuleNotFoundError:
    from das_framework.ctools.hierarchical_configparser \
            import HierarchicalConfigParser
from das_constants import CC
from das_framework.driver import AbstractDASValidator

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib',
                             'py4j-src.zip'))

from pyspark.sql import SparkSession

class MDFConstrPL94ValPer:
    def __init__(self):
        # allows string referencing of function
        self.func_dict = {}
        self.func_dict['nurse_nva_0'] = self.validate_nurse_nva_0


    def read_line(self, line):
        fields = line.split('|')
        if len(fields) !=13:
            raise ValueError(f'expected 13 fields, found {len(fields)}')
        self.line = line
        self.SCHEMA_TYPE_CODE = fields[0]  # Schema Type Code
        self.SCHEMA_BUILD_ID = fields[1]  # Schema Build ID
        self.TABBLKST        = int(fields[2])  # 2020 Tabulation State (FIPS)
        self.TABBLKCOU       = int(fields[3])  # 2020 Tabulation County (FIPS)
        self.TABTRACTCE      = int(fields[4])  # 2020 Tabulation Census Tract
        self.TABBLKGRPCE     = int(fields[5])  # 2020 Census Block Group
        self.TABBLK          = int(fields[6])  # 2020 Block Number
        self.EPNUM           = int(fields[7])  # Privacy Edited Person Number
        self.RTYPE           = int(fields[8])  # Record Type
        self.GQTYPE_PL       = int(fields[9])  # Group Quarters Type
        self.VOTING_AGE       = int(fields[10]) # If of Voting Age
        self.CENHISP         = int(fields[11]) # Hispanic Indicator
        self.CENRACE         = int(fields[12]) # Race Attribute


    def validate_nurse_nva_0(self):
        """
        Structural zero: no minors in nursing facilities( GQ code 301)
        """
        if self.VOTING_AGE == 1 and self.GQTYPE_PL == 3:
            return False
        return True


    def validate(self, constraints):
        """
        Run through and check each constraint, ensure all pass
        if failure specify which constraints failed
        :param constraints: the constraint functions that are to be tested
        """
        reason = [f'Line Failed, line: {self.line}\nConstraints Failed']
        passing = True
        for constraint in constraints:
            if not self.func_dict[constraint]():
                reason.append(constraint)
                passing = False
        assert passing, ', '.join(reason)


    @staticmethod
    def validate_line(line, constraint_functions):
        """
        To be run line by line via an rdd, creates an instance of the validator
        parses the line and then validates the relevant constraints
        :param constraint_functions: the constraints to be tested
        """
        inst: MDFConstrPL94ValPer = MDFConstrPL94ValPer()
        inst.read_line(line)
        inst.validate(constraint_functions)
        return line



class MDFConstrValPer:
    def __init__(self):
        # allows string referencing of functions
        self.func_dict = {}
        self.func_dict['relgq0_lt15'] = self.validate_relgq0_lt15
        self.func_dict['relgq1_lt15'] = self.validate_relgq1_lt15
        self.func_dict['relgq2_lt15'] = self.validate_relgq2_lt15
        self.func_dict['relgq3_lt15'] = self.validate_relgq3_lt15
        self.func_dict['relgq4_lt15'] = self.validate_relgq4_lt15
        self.func_dict['relgq5_lt15'] = self.validate_relgq5_lt15
        self.func_dict['relgq6_gt89'] = self.validate_relgq6_gt89
        self.func_dict['relgq7_gt89'] = self.validate_relgq7_gt89
        self.func_dict['relgq8_gt89'] = self.validate_relgq8_gt89
        self.func_dict['relgq10_lt30'] = self.validate_relgq10_lt30
        self.func_dict['relgq11_gt74'] = self.validate_relgq11_gt74
        self.func_dict['relgq12_lt30'] = self.validate_relgq12_lt30
        self.func_dict['relgq13_lt15_gt89'] = self.validate_relgq13_lt15_gt89
        self.func_dict['relgq16_gt20'] = self.validate_relgq16_gt20
        self.func_dict['relgq18_lt15'] = self.validate_relgq18_lt15
        self.func_dict['relgq19_lt15'] = self.validate_relgq19_lt15
        self.func_dict['relgq20_lt15'] = self.validate_relgq20_lt15
        self.func_dict['relgq21_lt15'] = self.validate_relgq21_lt15
        self.func_dict['relgq22_lt15'] = self.validate_relgq22_lt15
        self.func_dict['relgq23_lt17_gt65'] = self.validate_relgq23_lt17_gt65
        self.func_dict['relgq24_gt25'] = self.validate_relgq24_gt25
        self.func_dict['relgq25_gt25'] = self.validate_relgq25_gt25
        self.func_dict['relgq26_gt25'] = self.validate_relgq26_gt25
        self.func_dict['relgq27_lt20'] = self.validate_relgq27_lt20
        self.func_dict['relgq31_lt17_gt65'] = self.validate_relgq31_lt17_gt65
        self.func_dict['relgq32_lt3_gt30'] = self.validate_relgq32_lt3_gt30
        self.func_dict['relgq33_lt16_gt65'] = self.validate_relgq33_lt16_gt65
        self.func_dict['relgq34_lt17_gt65'] = self.validate_relgq34_lt17_gt65
        self.func_dict['relgq35_lt17_gt65'] = self.validate_relgq35_lt17_gt65
        self.func_dict['relgq37_lt16'] = self.validate_relgq37_lt16
        self.func_dict['relgq38_lt16'] = self.validate_relgq38_lt16
        self.func_dict['relgq39_lt16_gt75'] = self.validate_relgq39_lt16_gt75
        self.func_dict['relgq40_lt16_gt75'] = self.validate_relgq40_lt16_gt75


    def read_line(self, line):
        fields = line.split('|')
        if len(fields) != 16:
            raise ValueError(f'expected 16 fields, found {len(fields)}')
        self.line = line
        self.SCHEMA_TYPE_CODE = fields[0]  # Schema Type Code
        self.SCHEMA_BUILD_ID = fields[1]  # Schema Build ID
        self.TABBLKST        = int(fields[2])  # 2020 Tabulation State (FIPS)
        self.TABBLKCOU       = int(fields[3])  # 2020 Tabulation County (FIPS)
        self.TABTRACTCE      = int(fields[4])  # 2020 Tabulation Census Tract
        self.TABBLKGRPCE     = int(fields[5])  # 2020 Census Block Group
        self.TABBLK          = int(fields[6])  # 2020 Block Number
        self.EPNUM           = int(fields[7])  # Privacy Edited Person Number
        self.RTYPE           = int(fields[8])  # Record Type
        self.GQTYPE          = int(fields[9])  # Group Quarters Type
        self.RELSHIP         = int(fields[10])  # Edited Relationship
        self.QSEX            = int(fields[11])  # Edited Sex
        self.QAGE            = int(fields[12])  # Edited Age
        self.CENHISP         = int(fields[13])  # Hispanic Origin
        self.CENRACE         = int(fields[14])  # Census Race
        self.LIVE_ALONE      = int(fields[15])  # Person Living Alone


    def validate_relgq0_lt15(self):
        """
        Structural zero: No <15 ages as Householder living alone
        """
        if self.QAGE < 15 and self.RELSHIP == 20 and self.LIVE_ALONE == 1:
            return False
        return True


    def validate_relgq1_lt15(self):
        """
        Structural zero: No <15 ages as Householder not living alone
        """
        if self.QAGE < 15 and self.RELSHIP == 20 and self.LIVE_ALONE == 2:
            return False
        return True


    def validate_relgq2_lt15(self):
        """
        Structural zero: No <15 ages as Opposite-sex husband/wife/spouse
        """
        if self.QAGE < 15 and self.RELSHIP == 21:
            return False
        return True


    def validate_relgq3_lt15(self):
        """
        Structural zero: No <15 ages as Opposite-sex unmarried partner
        """
        if self.QAGE < 15 and self.RELSHIP == 22:
            return False
        return True


    def validate_relgq4_lt15(self):
        """
        Structural zero: No <15 ages as Same-sex unmarried partner
        """
        if self.QAGE < 15 and self.RELSHIP == 23:
            return False
        return True


    def validate_relgq5_lt15(self):
        """
        Structural zero: No <15 ages as Opposite-sex unmarried partner
        """
        if self.QAGE < 15 and self.RELSHIP == 24:
            return False
        return True


    def validate_relgq6_gt89(self):
        """
        Structural zero: No >89 ages as Biological son or daughter
        """
        if self.QAGE > 89 and self.RELSHIP == 25:
            return False
        return True


    def validate_relgq7_gt89(self):
        """
        Structural zero: No >89 ages as Adopted son or daughter
        """
        if self.QAGE > 89 and self.RELSHIP == 26:
            return False
        return True


    def validate_relgq8_gt89(self):
        """
        Structural zero: No >89 ages as Stepson / Stepdaughter
        """
        if self.QAGE > 89 and self.RELSHIP == 27:
            return False
        return True


    ### Code 9 - No Brother or Sister spans full range, no constraints needed


    def validate_relgq10_lt30(self):
        """
        Structural zero: No <30 ages as parents (father or mother)
        """
        if self.QAGE < 30 and self.RELSHIP == 29:
            return False
        return True


    def validate_relgq11_gt74(self):
        """
        Structural zero: No >74 ages as grandchildren
        """
        if self.QAGE > 74 and self.RELSHIP == 30:
            return False
        return True


    def validate_relgq12_lt30(self):
        """
        Structural zero: No <30 ages as parent-in-law (father or mother)
        """
        if self.QAGE < 30 and self.RELSHIP == 31:
            return False
        return True


    def validate_relgq13_lt15_gt89(self):
        """
        Structural zero: No <15 or >89 ages as Son-in-law / Daughter-in-law
        """
        if (self.QAGE < 15 or self.QAGE > 89) and self.RELSHIP == 32:
            return False
        return True


    ### Code 14 - Other Relative - spans the full age range - no constraint
    ### Code 15 - Housemate/Roommate - spans the full age range - no constraint


    def validate_relgq16_gt20(self):
        """
        Structural zero: No >20 ages as Foster Child
        """
        if self.QAGE > 20 and self.RELSHIP == 35:
            return False
        return True


    ## Code 17 - Other Nonrelative - probably spans full age range


    def validate_relgq18_lt15(self):
        """
        Structural zero: No <15 ages in GQ 101 Federal Detention Centers
        """
        if self.QAGE < 15 and self.GQTYPE == 101:
            return False
        return True


    def validate_relgq19_lt15(self):
        """
        Structural zero: No <15 ages in GQ 102 Federal Prisons
        """
        if self.QAGE < 15 and self.GQTYPE == 102:
            return False
        return True


    def validate_relgq20_lt15(self):
        """
        Structural zero: No <15 ages in GQ 103 State Prisons
        """
        if self.QAGE < 15 and self.GQTYPE == 103:
            return False
        return True


    def validate_relgq21_lt15(self):
        """
        Structural zero: No <15 ages in GQ 104 Local Jails and other
        Municipal Confinements
        """
        if self.QAGE < 15 and self.GQTYPE == 104:
            return False
        return True


    def validate_relgq22_lt15(self):
        """
        Structural zero: No <15 ages in GQ 105 Correctional
        Residential Facilities
        """
        if self.QAGE < 15 and self.GQTYPE == 105:
            return False
        return True


    def validate_relgq23_lt17_gt65(self):
        """
        Structural zero: No <17 or >65 ages in GQ 106 Military
        Disciplinary Barracks
        """
        if (self.QAGE < 17 or self.QAGE > 65) and self.GQTYPE == 106:
            return False
        return True


    def validate_relgq24_gt25(self):
        """
        Structural zero: No >25 ages in GQ 201 Group Homes for Juveniles
        """
        if self.QAGE > 25 and self.GQTYPE == 201:
            return False
        return True


    def validate_relgq25_gt25(self):
        """
        Structural zero: No >25 ages in GQ 202 Residential
        Treatment Centers for Juveniles
        """
        if self.QAGE > 25 and self.GQTYPE == 202:
            return False
        return True


    def validate_relgq26_gt25(self):
        """
        Structural zero: No >25 ages in GQ 203 Correctional
        Facilities intended for Juveniles
        """
        if self.QAGE > 25 and self.GQTYPE == 203:
            return False
        return True


    def validate_relgq27_lt20(self):
        """
        Structural zero: No <20 ages in GQ 301 Nursing Facilities
        """
        if self.QAGE < 20 and self.GQTYPE == 301:
            return False
        return True


    ### Code 28 - GQ 401 Mental Hospitals - Spans full age range
    ### Code 29 - GQ 402 Hospitals with homeless patients, full age range
    ### Code 30 - GQ 403 In-patient Hospice Facilities - full age range


    def validate_relgq31_lt17_gt65(self):
        """
        Structural zero: No <17 or >65 ages in GQ 404 Military
        Treatment Facilities
        """
        if (self.QAGE < 17 or self.QAGE > 65) and self.GQTYPE == 404:
            return False
        return True


    def validate_relgq32_lt3_gt30(self):
        """
        Structural zero: No <3 or >30 ages in GQ 405
        Residential schools for people with disabilities
        """
        if (self.QAGE < 3 or self.QAGE > 30) and self.GQTYPE == 405:
            return False
        return True


    def validate_relgq33_lt16_gt65(self):
        """
        Structural zero: No <16 or >65 ages in GQ 501
        College/University Student Housing
        """
        if (self.QAGE < 16 or self.QAGE > 65) and self.GQTYPE == 501:
            return False
        return True


    def validate_relgq34_lt17_gt65(self):
        """
        Structural zero: No <17 or >65 ages in GQ 601 Military Quarters
        """
        if (self.QAGE < 17 or self.QAGE > 65) and self.GQTYPE == 601:
            return False
        return True


    def validate_relgq35_lt17_gt65(self):
        """
        Structural zero: No <17 or >65 ages in GQ 602 Military Ships
        """
        if (self.QAGE < 17 or self.QAGE > 65) and self.GQTYPE == 602:
            return False
        return True


    ### Code 36 - GQ 701 Emergency and transitional shelters - full age range


    def validate_relgq37_lt16(self):
        """
        Structural zero: No <16 ages in GQ 801 Group Homes intended for adults
        """
        if self.QAGE < 16 and self.GQTYPE == 801:
            return False
        return True


    def validate_relgq38_lt16(self):
        """
        Structural zero: No <16 ages in GQ 802 Residential Treatment
        Centers for Adults
        """
        if self.QAGE < 16 and self.GQTYPE == 802:
            return False
        return True


    def validate_relgq39_lt16_gt75(self):
        """
        Structural zero: No <16 or >75 ages in GQ 900 Maritime/Merchant Vessels
        """
        if (self.QAGE < 16 or self.QAGE > 75) and self.GQTYPE == 900:
            return False
        return True


    def validate_relgq40_lt16_gt75(self):
        """
        Structural zero: No <16 or >75 ages in GQ 901 Workers' group
        living quarters and job corps centers
        """
        if (self.QAGE < 16 or self.QAGE > 75) and self.GQTYPE == 901:
            return False
        return True


    ### Code 41 - GQ 997 Other Noninstitutional (702, 704, 706, 903, 904)
    ### Spans full age range


    def validate(self, constraints):
        """
        Run through and check each constraint, ensure all pass
        if failure specify which constraints failed
        :param constraints: the constraint functions that are to be tested
        """
        reason = [f'Line Failed, line: {self.line}\nConstraints Failed']
        passing = True
        for constraint in constraints:
            if not self.func_dict[constraint]():
                reason.append(constraint)
                passing = False
        assert passing, ', '.join(reason)


    @staticmethod
    def validate_line(line, constraint_functions):
        """
        To be run line by line via an rdd, creates an instance of the validator
        parses the line and then validates the relevant constraints
        :param constraint_functions: the constraints to be tested
        """
        inst: MDFConstrValPer = MDFConstrValPer()
        inst.read_line(line)
        inst.validate(constraint_functions)
        return line


class MDFConstrValUnit: # MDF Constraints Validator Unit
    def __init__(self):
        # allows string referencing of functions
        self.func_dict = {}
        self.func_dict['living_alone'] = self.validate_living_alone
        self.func_dict['size2'] = self.validate_size2
        self.func_dict['size3'] = self.validate_size3
        self.func_dict['size4'] = self.validate_size4
        self.func_dict['age_child'] = self.validate_age_child
        self.func_dict['hh_elderly'] = self.validate_hh_elderly


    def read_line(self, line):
        fields = line.split('|')
        if len(fields) != 27:
            raise ValueError(f'expected 27 fields, found {len(fields)}')
        self.line = line
        self.SCHEMA_TYPE_CODE = fields[0]  # Schema Type Code
        self.SCHEMA_BUILD_ID = fields[1]  # Schema Build ID
        self.TABBLKST        = int(fields[2])  # 2020 Tabulation State (FIPS)
        self.TABBLKCOU       = int(fields[3])  # 2020 Tabulation County (FIPS)
        self.TABTRACTCE      = int(fields[4])  # 2020 Tabulation Census Tract
        self.TABBLKGRPCE     = int(fields[5])  # 2020 Census Block Group
        self.TABBLK          = int(fields[6])  # 2020 Block Number
        self.RTYPE           = int(fields[7])  # Record Type
        self.GQTYPE          = int(fields[8])  # Group Quarters Type
        self.TEN             = int(fields[9])  # Tenure
        self.VACS            = int(fields[10])  # Vacancy Status
        self.HHSIZE          = int(fields[11])  # Population Count
        self.HHT             = int(fields[12])  # Household/Family Type
        # Household/FamilyType (Includes Cohabiting)
        self.HHT2            = int(fields[13])
        self.CPLT            = int(fields[14])  # Couple Type
        # Presence and Type of Unmarried Partner Household
        self.UPART           = int(fields[15])
        self.MULTG           = int(fields[16])  # Multigenerational Household
        self.THHLDRAGE       = int(fields[17])  # Age of Householder
        self.THHSPAN         = int(fields[18])  # Hispanic Householder
        self.THHRACE         = int(fields[19])  # Race of Householder
        # Presence and Age of Own Children Under 18
        self.PAOC            = int(fields[20])
        # Presence of People Under 18 Years in Household
        self.TP18            = int(fields[21])
        # Presence of People 60 Years and Over In Household
        self.TP60            = int(fields[22])
        # Presence of People 65 Years and Over in Household
        self.TP65            = int(fields[23])
        # Presence of People 75 Years and Over in Household
        self.TP75            = int(fields[24])
        # Presence and Age of Children Under 18
        self.PAC             = int(fields[25])
        self.HHSEX           = int(fields[26])  # Sex of Householder


    def validate_living_alone(self):
        """
        if size = 1, then elderly presence is defined by the
        householder age only.
        HH under 60 => can't be anyone over 60
        HH between 60 and 64 => cannot be elderly over 64 present
        HH between 65 and 74 => cannot be elderly below 65 or above 74 present
        HH 75+ => cannot be elderly below 75 present
        Also if there are children, that means the householder is a child and
        so there's a limit on householder age
        """
        if self.HHSIZE == 1:
            if 0 < self.THHLDRAGE < 6:
                if self.TP60 == 1 or self.TP65 == 1 or self.TP75 == 1:
                    return False
            if self.THHLDRAGE == 6:
                if self.TP60 == 0 or self.TP65 == 1 or self.TP75 == 1:
                    return False
            if self.THHLDRAGE == 7:
                if self.TP60 == 0 or self.TP65 == 0 or self.TP75 == 1:
                    return False
            if self.THHLDRAGE >= 8:
                if self.TP60 == 0 or self.TP65 == 0 or self.TP75 == 0:
                    return False
            if self.TP18 == 1:
                if self.THHLDRAGE > 1:
                    return False
        return True


    def validate_size2(self):
        """
        if size=2, and the second person is a child,
        elderly completely deterimined by hh age,
        the same as for living_alone structural zeros above
        Also, exclude 50+ years difference between partners
        """
        if self.HHSIZE == 2 and self.THHLDRAGE == 1 and self.TP75 == 1:
            if self.CPLT in (1, 2, 3, 4):
                return False
        # Decrement size by one and TP18 by one, then run live alone
        if self.HHSIZE == 2 and self.PAC in (1, 2, 3):
            self.HHSIZE = 1
            self.TP18 = 0
            result = self.validate_living_alone()
            self.HHSIZE = 2
            self.TP18 = 1
            return result
        return True


    def validate_size3(self):
        """
        if size=3,
        the same as size=2, but two children (third person is a child)
        """
        if self.HHSIZE == 3:
            if self.PAOC in (1, 2):
                if self.THHLDRAGE == 1 and self.TP75 == 1:
                    if self.CPLT in (1, 2, 3, 4):
                        return False
            if self.PAOC == 3: # at least two children in household
                self.HHSIZE = 1
                self.TP18 = 0
                if not self.validate_living_alone():
                    return False
                self.HHSIZE = 3
                self.TP18 = 1
        return True


    def validate_size4(self):
        """
        If the household is size 4, has a couple and there are two children,
        one less than 6 the other 7-18,
        and the householder is 15-24, and there is someone age > 75,
        this indicates the householder would be in a relationship with the
        > 75 person and they have children. Which is listed as not possible,
        so it should never happen. This checks that the above scenario does
        not occur
        > 50 year age gaps are specifically prohibited via edit constraints
        """
        # If household size is four and there are children in both age ranges
        # And householder is in relationship
        if self.HHSIZE == 4 and self.PAOC == 3 and self.CPLT in (1, 2, 3, 4):
            # Householder is 15-24 and Other inhabitant is > 75
            if self.THHLDRAGE == 1 and self.TP75 == 1:
                return False
        return True


    def validate_age_child(self):
        """
        Male householder can't have children more than 69 yrs younger.
        Female householder cant' have children more than 50 yrs younger.
        if sex=male and age>=75, cannot have own children under 6 yrs
        if sex=female and age>=75, cannot have own children under 18 yrs.
        if sex=female and age>=60, cannot have own children under 6yrs.
        """
        if self.HHSEX == 1 and self.THHLDRAGE >= 8 and self.PAOC in (1, 3):
            return False
        if self.HHSEX == 2 and self.THHLDRAGE >= 8 and self.PAOC in (1, 2, 3):
            return False
        if self.HHSEX == 2 and self.THHLDRAGE >= 6 and self.PAOC in (1, 3):
            return False
        return True


    def validate_hh_elderly(self):
        """
        In general, householder age partially defines elderly.
        If householder is elderly, then the ELDERLY variable cannot be
        for age lower than HHAGE
        """
        if self.THHLDRAGE >= 8:
            if self.TP60 == 0 or self.TP65 == 0 or self.TP75 == 0:
                return False
        elif self.THHLDRAGE >= 7:
            if self.TP60 == 0 or self.TP65 == 0:
                return False
        elif self.THHLDRAGE >= 6:
            if self.TP60 == 0:
                return False
        return True


    def validate(self, constraints):
        """
        Run through and check each constraint, ensure all pass
        if failure specify which constraints failed
        :param constraints: the constraint functions that are to be tested
        """
        reason = [f'Line Failed, line: {self.line}\nConstraints Failed']
        passing = True
        for constraint in constraints:
            if not self.func_dict[constraint]():
                reason.append(constraint)
                passing = False
        assert passing, ', '.join(reason)


    @staticmethod
    def validate_line(line, constraint_functions: list):
        """
        To be run line by line via an rdd, creates an instance of the validator
        parses the line and then validates the relevant constraints
        :param constraint_functions: the constraints to be tested
        """
        inst: MDFConstrValUnit = MDFConstrValUnit()
        inst.read_line(line)
        inst.validate(constraint_functions)
        return line



class ConstraintsValidator(AbstractDASValidator):
    def __init__(self, config: str, schema: str) -> None:
        # super().__init__ is not needed
        self.config = config
        self.schema = schema


    def get_relgq_constraints(self, pl94:bool=False):
        """
        Use the config to get the relevant structural zero constraints for
        the person MDF
        """
        constraints = self.getconfig("theconstraints.Block",
                                     section=CC.CONSTRAINTS)
        constraints = constraints.split(', ')
        per_constraints = []
        if pl94:
            test_validator = MDFConstrPL94ValPer()
        else:
            test_validator = MDFConstrValPer()
        for constraint in constraints:
            if "relgq" in constraint or "nurse" in constraint:
                if constraint in test_validator.func_dict:
                    per_constraints.append(constraint)
                else:
                    print(f'Could not find relgq constraint: {constraint}')
        return per_constraints


    def get_unit_constraints(self):
        """
        Use the config to get the relevant structural zero constraints for
        the unit MDF
        """
        constraints = self.getconfig("theconstraints.Block",
                                     section=CC.CONSTRAINTS)
        constraints = constraints.split(', ')
        unit_constraints = []
        test_validator =  MDFConstrValUnit()
        testable_constraints = ['living_alone', 'size2', 'size3', 'size4',
                                'age_child', 'hh_elderly']
        for constraint in constraints:
            if constraint in testable_constraints:
                unit_constraints.append(constraint)
        return unit_constraints


    def validate_mdf_cons(self, spark: SparkSession, path: str, mdf_type: str,
                          pl94:bool=False):
        """
        Driver function for validating the MDF files once setup is done
        determines whether to launch person or unit validation, passing through
        which constraints to validate
        :param spark: Instance of spark
        :param path: the MDF file path
        :param mdf_type: 'person' or 'unit'
        """
        df = spark.read.format('csv').options(header='true', delimiter='|',
                                              comment='#').load(path)
        rdd = df.rdd.map(lambda row: '|'.join([str(c) for c in row])).filter(
                         lambda line: line)
        if mdf_type == 'person':
            constraints = self.get_relgq_constraints(pl94)
            print('The constraints being validated')
            print(constraints)
            if pl94:
                rdd = rdd.map(lambda line: MDFConstrPL94ValPer.validate_line(
                              line, constraints))
            else:
                rdd = rdd.map(lambda line:  MDFConstrValPer.validate_line(line,
                                  constraints))
            message = 'MDF Per Structural Zero Validation Completed'
        else:
            constraints = self.get_unit_constraints()
            print(f'The constraints being validated')
            print(constraints)
            rdd = rdd.map(lambda line:  MDFConstrValUnit.validate_line(line,
                          constraints))
            message = 'MDF Unit constraint validation completed'
        rdd.persist()
        x = rdd.count() # needed to force the rdd to perform the operation
        print(message)



def get_config(run_configuration: str, run_type: str, nohierconfig: bool):
    """
    Determines the full path of the config file to be used and whether to
    use the default configparser or the hierarchical_configparser
    :param run_configuration: The config file path
    :param run_type: either 'person' or 'unit'
    :param nohierconfig: True or False, whether or not to use default parser
    :return config: a configparser or hierarch parser
    """
    config_path = run_configuration
    if "configs" not in config_path:
        config_path = "configs/" + config_path
    if '.ini' not in config_path:
        if run_type == "person":
            config_path = config_path + "/person_US.ini"
        elif run_type == "unit":
            config_path = config_path + "/unit_US.ini"
    config = ConfigParser() if nohierconfig else HierarchicalConfigParser()
    config.read(config_path)
    print(f' the config path for constraint validation: {config_path}')
    return config


def main(args, spark=None):
    print(f'THE SCHEMA: {args.schema}')
    name = args.schema.lower()
    if 'dhc' in name:
        pl94 = False
    elif 'pl' in name or 'h1' in name:
        if args.type in 'person':
            pl94 = True
        else:
            print(f'No Struct Zeros for H1 Unit Schema, returning')
            return
    else:
        raise RuntimeError(f"Unknown schema: {args.schema}, Cannot Validate")
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    config = get_config(args.run_configuration, args.type, args.nohierconfig)
    cv = ConstraintsValidator(config=config, schema=args.schema)
    if args.type == 'person':
        mdf_file = args.mdf_per_file
    else:
        mdf_file = args.mdf_unit_file
    print(f'the mdf_{args.type}_file being validated: {mdf_file}')
    cv.validate_mdf_cons(spark, mdf_file, args.type, pl94)


if __name__ == "__main__":
    parser = ArgumentParser(description="Validate constraints in MDF Files")
    parser.add_argument("--mdf_per_file", type=str, default=None,
                        help="Path to mdf_per_file")
    parser.add_argument("--mdf_unit_file", type=str, default=None,
                        help="Path to the mdf_unit_file")
    parser.add_argument("--schema", type=str, default='DHCP_SCHEMA',
                        help="Indiciate whether to Use PL94 or DHC Schema")
    parser.add_argument("--run_configuration", type=str,
                        default="dhc/pdp1/experiment2",
                        help="The run_config for picking the configs")
    parser.add_argument("--type", default="person",
                        help="person or unit, to determine which to validate")
    parser.add_argument("--nohierconfig", action="store_true",
                        help="use normal configparser instead of hierarch one")
    args = parser.parse_args()
    args.type = args.type.lower()
    if args.type not in ['person', 'unit']:
        raise RuntimeError("--type argument must be either 'person' or 'unit'")
    main(args)
