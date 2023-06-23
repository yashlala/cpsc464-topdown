import pytest
from programs.writer.hh2010_to_mdfunit2020 import Household2010ToMDFUnit2020Recoder
from das_constants import CC

HHTYPE = CC.ATTR_HHTYPE  # maybe CC.ATTR_HHTYPE_DHCH
MULTI = CC.ATTR_HHMULTI
SIZE = CC.ATTR_HHSIZE
HHSEX = CC.ATTR_HHSEX


class TestHousehold2010ToMDFUnit2020Recoder:
    recoder = Household2010ToMDFUnit2020Recoder()

    @pytest.mark.parametrize("rowdict, ans", [
        #                 'Married opposite-sex with own children under 18, under 6 yrs only'     : [0],
        #                 'Married opposite-sex with own children under 18, between 6 and 17 only': [1],
        #                 'Married opposite-sex with own children under 18, both ranges'          : [2],
        #                 'Married opposite-sex no own children under 18'                         : [3],
        #                 'Married same-sex with own children only under 6 yrs'                   : [4],
        #                 'Married same-sex with own children between 6 and 17'                   : [5],
        #                 'Married same-sex with own children in both ranges'                     : [6],
        #                 'Married same-sex no own children under 18'                             : [7],
        #                 'Cohabiting opposite-sex with own children only under 6 yrs'            : [8],
        #                 'Cohabiting opposite-sex with own children between 6 and 17'            : [9],
        #                 'Cohabiting opposite-sex with own children in both ranges'              : [10],
        #                 'Cohabiting opposite-sex with relatives, no own children under 18'      : [11],
        #                 'Cohabiting opposite-sex without relatives, no own children under 18'   : [12],
        #                 'Cohabiting same-sex with own children only under 6 yrs'                : [13],
        #                 'Cohabiting same-sex with own children between 6 and 17'                : [14],
        #                 'Cohabiting same-sex with own children in both ranges'                  : [15],
        #                 'Cohabiting same-sex with relatives, no own children under 18'          : [16],
        #                 'Cohabiting same-sex without relatives, no own children under 18'       : [17],
        #                 'No spouse or partner, alone'                                           : [18],
        #                 'No spouse or partner with own children under 6'                        : [19],
        #                 'No spouse or partner with own children between 6 and 17.'              : [20],
        #                 'No spouse or partner with own children in both ranges'                 : [21],
        #                 'No spouse or partner living with relatives but no own children'        : [22],
        #                 'No spouse or partner and no relatives, not alone.'                     : [23]
        #############
        #############                 TO
        #############
        # PAOC  Presence and Age of Own Children Under 18       CHAR(1)
        #           0 = NIU     (RTYPE = 2 and HHSIZE = 0) or RTYPE = 4
        #                       1 = With own children under 6 years only        RTYPE = 2 and HHSIZE > 1 and (RELSHIP = 25-27 and QAGE < 6 for at least one person in housing unit) and (RELSHIP = 25-27 and QAGE = 6-17 for no person in housing unit)
        #                       2 = With own children 6-17 years only   RTYPE = 2 and HHSIZE > 1 and (RELSHIP = 25-27 and QAGE = 6-17 for at least one person in housing unit) and (RELSHIP = 25-27 and QAGE < 6 for no person in housing unit)
        #                       3 = With own children under 6 years and 6-17 years      RTYPE = 2 and HHSIZE > 1 and (RELSHIP = 25-27 and QAGE < 6 for at least one person in housing unit) and (RELSHIP = 25-27 and QAGE = 6-17 for at least one person in housing unit)
        #                       4 = No own children     RTYPE = 2 and HHSIZE = 1 or (HHSIZE > 1 and (RELSHIP = 25-27 and QAGE < 18 for no persons in housing unit)

        ({HHTYPE: 0}, "1"),   # 'Married opposite-sex with own children under 18, under 6 yrs only'      to 'With own children under 6 years only'
        ({HHTYPE: 1}, "2"),   # 'Married opposite-sex with own children under 18, between 6 and 17 only' to 'With own children 6-17 years only'
        ({HHTYPE: 2}, "3"),   # 'Married opposite-sex with own children under 18, both ranges'           to 'With own children under 6 years and 6-17 years'
        ({HHTYPE: 3}, "4"),   # 'Married opposite-sex no own children under 18'                          to 'No own children'
        ({HHTYPE: 4}, "1"),   # 'Married same-sex with own children only under 6 yrs'                    to 'With own children under 6 years only'
        ({HHTYPE: 5}, "2"),   # 'Married same-sex with own children between 6 and 17'                    to 'With own children 6-17 years only'
        ({HHTYPE: 6}, "3"),   # 'Married same-sex with own children in both ranges'                      to 'With own children under 6 years and 6-17 years'
        ({HHTYPE: 7}, "4"),   # 'Married same-sex no own children under 18'                              to 'No own children'
        ({HHTYPE: 8}, "1"),   # 'Cohabiting opposite-sex with own children only under 6 yrs'             to 'With own children under 6 years only'
        ({HHTYPE: 9}, "2"),   # 'Cohabiting opposite-sex with own children between 6 and 17'             to 'With own children 6-17 years only'
        ({HHTYPE: 10}, "3"),  # 'Cohabiting opposite-sex with own children in both ranges'               to 'With own children under 6 years and 6-17 years'
        ({HHTYPE: 11}, "4"),  # 'Cohabiting opposite-sex with relatives, no own children under 18'       to 'No own children'
        ({HHTYPE: 12}, "4"),  # 'Cohabiting opposite-sex without relatives, no own children under 18'    to 'No own children'
        ({HHTYPE: 13}, "1"),  # 'Cohabiting same-sex with own children only under 6 yrs'                 to 'With own children under 6 years only'
        ({HHTYPE: 14}, "2"),  # 'Cohabiting same-sex with own children between 6 and 17'                 to 'With own children 6-17 years only'
        ({HHTYPE: 15}, "3"),  # 'Cohabiting same-sex with own children in both ranges'                   to 'With own children under 6 years and 6-17 years'
        ({HHTYPE: 16}, "4"),  # 'Cohabiting same-sex with relatives, no own children under 18'           to 'No own children'
        ({HHTYPE: 17}, "4"),  # 'Cohabiting same-sex without relatives, no own children under 18'        to 'No own children'
        ({HHTYPE: 18}, "4"),  # 'No spouse or partner, alone'                                            to 'No own children'
        ({HHTYPE: 19}, "1"),  # 'No spouse or partner with own children under 6'                         to 'With own children under 6 years only'
        ({HHTYPE: 20}, "2"),  # 'No spouse or partner with own children between 6 and 17.'               to 'With own children 6-17 years only'
        ({HHTYPE: 21}, "3"),  # 'No spouse or partner with own children in both ranges'                  to 'With own children under 6 years and 6-17 years'
        ({HHTYPE: 22}, "4"),  # 'No spouse or partner living with relatives but no own children'         to 'No own children'
        ({HHTYPE: 23}, "4"),  # 'No spouse or partner and no relatives, not alone.'                      to 'No own children'
    ])
    def test_paoc_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        # recode_func = lambda row: self.recoder.paoc_recoder(row, nullfiller=None)  # This is to use if calling the recode function multiple times, so as not to repeat nullfiller=None
        assert self.recoder.paoc_recoder(rowdict_mangled, nullfiller=None)['PAOC'] == ans


    @pytest.mark.parametrize("rowdict, ans", [
        #                 'Married opposite-sex with own children under 18, under 6 yrs only'     : [0],
        #                 'Married opposite-sex with own children under 18, between 6 and 17 only': [1],
        #                 'Married opposite-sex with own children under 18, both ranges'          : [2],
        #                 'Married opposite-sex no own children under 18'                         : [3],
        #                 'Married same-sex with own children only under 6 yrs'                   : [4],
        #                 'Married same-sex with own children between 6 and 17'                   : [5],
        #                 'Married same-sex with own children in both ranges'                     : [6],
        #                 'Married same-sex no own children under 18'                             : [7],
        #                 'Cohabiting opposite-sex with own children only under 6 yrs'            : [8],
        #                 'Cohabiting opposite-sex with own children between 6 and 17'            : [9],
        #                 'Cohabiting opposite-sex with own children in both ranges'              : [10],
        #                 'Cohabiting opposite-sex with relatives, no own children under 18'      : [11],
        #                 'Cohabiting opposite-sex without relatives, no own children under 18'   : [12],
        #                 'Cohabiting same-sex with own children only under 6 yrs'                : [13],
        #                 'Cohabiting same-sex with own children between 6 and 17'                : [14],
        #                 'Cohabiting same-sex with own children in both ranges'                  : [15],
        #                 'Cohabiting same-sex with relatives, no own children under 18'          : [16],
        #                 'Cohabiting same-sex without relatives, no own children under 18'       : [17],
        #                 'No spouse or partner, alone'                                           : [18],
        #                 'No spouse or partner with own children under 6'                        : [19],
        #                 'No spouse or partner with own children between 6 and 17.'              : [20],
        #                 'No spouse or partner with own children in both ranges'                 : [21],
        #                 'No spouse or partner living with relatives but no own children'        : [22],
        #                 'No spouse or partner and no relatives, not alone.'                     : [23]
        #############
        #############                 TO
        #############
        # HHT   Household/Family Type   CHAR(1)
        #           0 = NIU     (RTYPE = 2 and HHSIZE = 0) or RTYPE = 4
        #                       1 = Married couple household    RTYPE = 2 and HHSIZE > 1 and (RELSHIP = 21, 23 for one person in housing unit)
        #                       2 = Other family household: Male householder    RTYPE = 2 and HHSIZE > 1 and (RELSHIP ≠ 21, 23 for any person in housing unit) and (RELSHIP = 25-33 for one or more people in housing unit) and (QSEX = 1 for householder)
        #                       3 = Other family household: Female householder  RTYPE = 2 and HHSIZE > 1 and (RELSHIP ≠ 21, 23 for any person in housing unit) and (RELSHIP = 25-33 for one or more people in housing unit) and (QSEX = 2 for householder)
        #                       4 = Nonfamily household: Male householder, living alone RTYPE = 2 and HHSIZE = 1 and (QSEX = 1 for householder)
        #                       5 = Nonfamily household: Male householder, not living alone     RTYPE = 2 and HHSIZE ≥ 2 and (RELSHIP = 22, 24, 34-36 for all people in housing unit besides householder) and (QSEX = 1 for householder)
        #                       6 = Nonfamily household: Female householder, living alone       RTYPE = 2 and HHSIZE = 1 and (QSEX = 2 for householder)
        #                       7 = Nonfamily household: Female householder, not living alone   RTYPE = 2 and HHSIZE ≥ 2 and (RELSHIP = 22, 24, 34-36 for all persons in housing unit besides householder) and (QSEX = 2 for householder)


        ({HHTYPE: 18, HHSEX: 0, SIZE: 1}, "4"),  # 'No spouse or partner, alone'                                            to  Nonfamily household: Male householder, living alone
        ({HHTYPE: 18, HHSEX: 1, SIZE: 1}, "6"),  # 'No spouse or partner, alone'                                            to  Nonfamily household: Female householder, living alone5


        ({HHTYPE:  0, HHSEX: 0, SIZE: 5}, "1"),  # 'Married opposite-sex with own children under 18, under 6 yrs only'      to  1 = Married couple household
        ({HHTYPE:  1, HHSEX: 0, SIZE: 5}, "1"),  # 'Married opposite-sex with own children under 18, between 6 and 17 only' to  1 = Married couple household
        ({HHTYPE:  2, HHSEX: 0, SIZE: 5}, "1"),  # 'Married opposite-sex with own children under 18, both ranges'           to  1 = Married couple household
        ({HHTYPE:  3, HHSEX: 0, SIZE: 5}, "1"),  # 'Married opposite-sex no own children under 18'                          to  1 = Married couple household
        ({HHTYPE:  4, HHSEX: 0, SIZE: 5}, "1"),  # 'Married same-sex with own children only under 6 yrs'                    to  1 = Married couple household
        ({HHTYPE:  5, HHSEX: 0, SIZE: 5}, "1"),  # 'Married same-sex with own children between 6 and 17'                    to  1 = Married couple household
        ({HHTYPE:  6, HHSEX: 0, SIZE: 5}, "1"),  # 'Married same-sex with own children in both ranges'                      to  1 = Married couple household
        ({HHTYPE:  7, HHSEX: 0, SIZE: 5}, "1"),  # 'Married same-sex no own children under 18'                              to  1 = Married couple household
        ({HHTYPE:  8, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting opposite-sex with own children only under 6 yrs'             to  2 = Other family household: Male householder
        ({HHTYPE:  9, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting opposite-sex with own children between 6 and 17'             to  2 = Other family household: Male householder
        ({HHTYPE: 10, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting opposite-sex with own children in both ranges'               to  2 = Other family household: Male householder
        ({HHTYPE: 11, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting opposite-sex with relatives, no own children under 18'       to  2 = Other family household: Male householder
        ({HHTYPE: 12, HHSEX: 0, SIZE: 5}, "5"),  # 'Cohabiting opposite-sex without relatives, no own children under 18'    to  5 = Nonfamily household: Male householder, not living alone
        ({HHTYPE: 13, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting same-sex with own children only under 6 yrs'                 to  2 = Other family household: Male householder
        ({HHTYPE: 14, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting same-sex with own children between 6 and 17'                 to  2 = Other family household: Male householder
        ({HHTYPE: 15, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting same-sex with own children in both ranges'                   to  2 = Other family household: Male householder
        ({HHTYPE: 16, HHSEX: 0, SIZE: 5}, "2"),  # 'Cohabiting same-sex with relatives, no own children under 18'           to  2 = Other family household: Male householder
        ({HHTYPE: 17, HHSEX: 0, SIZE: 5}, "5"),  # 'Cohabiting same-sex without relatives, no own children under 18'        to  5 = Nonfamily household: Male householder, not living alone
        ({HHTYPE: 19, HHSEX: 0, SIZE: 5}, "2"),  # 'No spouse or partner with own children under 6'                         to  2 = Other family household: Male householder
        ({HHTYPE: 20, HHSEX: 0, SIZE: 5}, "2"),  # 'No spouse or partner with own children between 6 and 17.'               to  2 = Other family household: Male householder
        ({HHTYPE: 21, HHSEX: 0, SIZE: 5}, "2"),  # 'No spouse or partner with own children in both ranges'                  to  2 = Other family household: Male householder
        ({HHTYPE: 22, HHSEX: 0, SIZE: 5}, "2"),  # 'No spouse or partner living with relatives but no own children'         to  2 = Other family household: Male householder
        ({HHTYPE: 23, HHSEX: 0, SIZE: 5}, "5"),  # 'No spouse or partner and no relatives, not alone.'                      to  5 = Nonfamily household: Male householder, not living alone

        ({HHTYPE:  0, HHSEX: 1, SIZE: 5}, "1"),  # 'Married opposite-sex with own children under 18, under 6 yrs only'      to  1 = Married couple household
        ({HHTYPE:  1, HHSEX: 1, SIZE: 5}, "1"),  # 'Married opposite-sex with own children under 18, between 6 and 17 only' to  1 = Married couple household
        ({HHTYPE:  2, HHSEX: 1, SIZE: 5}, "1"),  # 'Married opposite-sex with own children under 18, both ranges'           to  1 = Married couple household
        ({HHTYPE:  3, HHSEX: 1, SIZE: 5}, "1"),  # 'Married opposite-sex no own children under 18'                          to  1 = Married couple household
        ({HHTYPE:  4, HHSEX: 1, SIZE: 5}, "1"),  # 'Married same-sex with own children only under 6 yrs'                    to  1 = Married couple household
        ({HHTYPE:  5, HHSEX: 1, SIZE: 5}, "1"),  # 'Married same-sex with own children between 6 and 17'                    to  1 = Married couple household
        ({HHTYPE:  6, HHSEX: 1, SIZE: 5}, "1"),  # 'Married same-sex with own children in both ranges'                      to  1 = Married couple household
        ({HHTYPE:  7, HHSEX: 1, SIZE: 5}, "1"),  # 'Married same-sex no own children under 18'                              to  1 = Married couple household
        ({HHTYPE:  8, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting opposite-sex with own children only under 6 yrs'             to  3 = Other family household: Female householder
        ({HHTYPE:  9, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting opposite-sex with own children between 6 and 17'             to  3 = Other family household: Female householder
        ({HHTYPE: 10, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting opposite-sex with own children in both ranges'               to  3 = Other family household: Female householder
        ({HHTYPE: 11, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting opposite-sex with relatives, no own children under 18'       to  3 = Other family household: Female householder
        ({HHTYPE: 12, HHSEX: 1, SIZE: 5}, "7"),  # 'Cohabiting opposite-sex without relatives, no own children under 18'    to  7 = Nonfamily household: Female householder, not living alone
        ({HHTYPE: 13, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting same-sex with own children only under 6 yrs'                 to  3 = Other family household: Female householder
        ({HHTYPE: 14, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting same-sex with own children between 6 and 17'                 to  3 = Other family household: Female householder
        ({HHTYPE: 15, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting same-sex with own children in both ranges'                   to  3 = Other family household: Female householder
        ({HHTYPE: 16, HHSEX: 1, SIZE: 5}, "3"),  # 'Cohabiting same-sex with relatives, no own children under 18'           to  3 = Other family household: Female householder
        ({HHTYPE: 17, HHSEX: 1, SIZE: 5}, "7"),  # 'Cohabiting same-sex without relatives, no own children under 18'        to  7 = Nonfamily household: Female householder, not living alone
        ({HHTYPE: 19, HHSEX: 1, SIZE: 5}, "3"),  # 'No spouse or partner with own children under 6'                         to  3 = Other family household: Female householder
        ({HHTYPE: 20, HHSEX: 1, SIZE: 5}, "3"),  # 'No spouse or partner with own children between 6 and 17.'               to  3 = Other family household: Female householder
        ({HHTYPE: 21, HHSEX: 1, SIZE: 5}, "3"),  # 'No spouse or partner with own children in both ranges'                  to  3 = Other family household: Female householder
        ({HHTYPE: 22, HHSEX: 1, SIZE: 5}, "3"),  # 'No spouse or partner living with relatives but no own children'         to  3 = Other family household: Female householder
        ({HHTYPE: 23, HHSEX: 1, SIZE: 5}, "7"),  # 'No spouse or partner and no relatives, not alone.'                      to  7 = Nonfamily household: Female householder, not living alone
    ])
    def test_hht_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        # recode_func = lambda row: self.recoder.paoc_recoder(row, nullfiller=None)  # This is to use if calling the recode function multiple times, so as not to repeat nullfiller=None
        assert self.recoder.hht_recoder(rowdict_mangled, nullfiller=None)['HHT'] == ans

    @pytest.mark.parametrize("rowdict, ans", [
        # MULTG Multigenerational Household CHAR(1)
        #           0 = NIU (RTYPE = 2 and HHSIZE <= 2) or RTYPE = 4
        #           1 = Not a multigenerational household Not MULTG = 0, 2
        #           2 = Yes, a multigenerational household RTYPE = 2 and HHSIZE ≥ 3 and (RELSHIP = 25-27 for at least one person in housing unit) and [(RELSHIP = 30 for at least one person in housing unit) or (RELSHIP = 29, 31 for at least one person in housing unit)]
        ({MULTI: 0, SIZE: 2}, "0"),  # Size too small
        ({MULTI: 1, SIZE: 2}, "0"),  # Size too small
        ({MULTI: 1, SIZE: 1}, "0"),  # Size too small
        ({MULTI: 1, SIZE: 1}, "0"),  # Size too small
        ({MULTI: 1, SIZE: 3}, "2"),  # Size enough, multi 1
        ({MULTI: 0, SIZE: 7}, "1"),  # Size enough, multi 0
    ])
    def test_multg_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        # recode_func = lambda row: self.recoder.paoc_recoder(row, nullfiller=None)  # This is to use if calling the recode function multiple times, so as not to repeat nullfiller=None
        assert self.recoder.multg_recoder(rowdict_mangled, nullfiller=None)['MULTG'] == ans
