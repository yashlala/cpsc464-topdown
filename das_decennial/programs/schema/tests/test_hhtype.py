import pytest
from typing import Dict, List

from programs.schema.attributes.dhch.hhtype_dhch import HHTypeDHCHAttr
from das_constants import CC


class TestHHTypeDHCHAttribute:
    hh = HHTypeDHCHAttr()

    def _verify_attributes(self, index: int, in_attr: Dict):
        ret_attr = self.hh.get_attributes(index=index)
        for k, v in ret_attr.items():
            cur_v = in_attr.get(k, 0)
            assert v == cur_v

    @pytest.mark.parametrize(
        "index, attr",
        [
            (10, {"NO_SPOUSE_OR_PARTNER": 0, "NOT_ALONE": 1, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0,
                  "MARRIED_SAME_SEX": 0, "MARRIED_OPPOSITE_SEX": 1, "NO_RELATIVES": 0, "WITH_RELATIVES": 1,
                  "NO_OWN_CHILD_UNDER_18": 0, "OWN_CHILD_UNDER_18": 1, "OWN_CHILD_IN_BOTH_RANGES": 0,
                  "OWN_CHILD_BETWEEN_6_AND_17": 0, "OWN_CHILD_UNDER_6_ONLY": 1, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0,
                  "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 1, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 0,
                  "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 0, "SIZE": 5, "COHABITING": 0, "MARRIED": 1}),
            (104, {"NO_SPOUSE_OR_PARTNER": 0, "NOT_ALONE": 1, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0,
                   "MARRIED_SAME_SEX": 1, "MARRIED_OPPOSITE_SEX": 0, "NO_RELATIVES": 0, "WITH_RELATIVES": 1,
                   "NO_OWN_CHILD_UNDER_18": 0, "OWN_CHILD_UNDER_18": 1, "OWN_CHILD_IN_BOTH_RANGES": 0,
                   "OWN_CHILD_BETWEEN_6_AND_17": 1, "OWN_CHILD_UNDER_6_ONLY": 0, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0,
                   "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 0, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 1,
                   "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 0, "SIZE": 4, "COHABITING": 0, "MARRIED": 1}),
            (141, {"NO_SPOUSE_OR_PARTNER": 0, "NOT_ALONE": 1, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0, "MARRIED_SAME_SEX": 1,
                   "MARRIED_OPPOSITE_SEX": 0, "NO_RELATIVES": 0, "WITH_RELATIVES": 1, "NO_OWN_CHILD_UNDER_18": 1, "OWN_CHILD_UNDER_18": 0,
                   "OWN_CHILD_IN_BOTH_RANGES": 0, "OWN_CHILD_BETWEEN_6_AND_17": 0, "OWN_CHILD_UNDER_6_ONLY": 0, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0,
                   "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 0, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 1,
                   "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 1, "SIZE": 4, "MARRIED": 1, "COHABITING": 0}),
            (393, {"NO_SPOUSE_OR_PARTNER": 1, "NOT_ALONE": 0, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0,
                   "MARRIED_SAME_SEX": 0, "MARRIED_OPPOSITE_SEX": 0, "NO_RELATIVES": 1, "WITH_RELATIVES": 0,
                   "NO_OWN_CHILD_UNDER_18": 1, "OWN_CHILD_UNDER_18": 0, "OWN_CHILD_IN_BOTH_RANGES": 0,
                   "OWN_CHILD_BETWEEN_6_AND_17": 0, "OWN_CHILD_UNDER_6_ONLY": 0, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1,
                   "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 0, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 0,
                   "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 0, "SIZE": 1, "COHABITING": 0, "MARRIED": 0}),
            (482, {'SIZE': 3, 'CHILD_UNDER_18': 0, 'OWN_CHILD_UNDER_18': 0, 'OWN_CHILD_UNDER_6_ONLY': 0, 'OWN_CHILD_BETWEEN_6_AND_17': 0, 'OWN_CHILD_IN_BOTH_RANGES': 0,
                   'CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER': 0, 'CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER': 0, 'CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER': 0,
                   'CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER': 0, 'MULTIG': 1, 'MARRIED': 0, 'MARRIED_SAME_SEX': 0, 'MARRIED_OPPOSITE_SEX': 0, 'COHABITING': 0,
                   'COHABITING_SAME_SEX': 0, 'COHABITING_OPPOSITE_SEX': 0, 'NO_SPOUSE_OR_PARTNER': 1, 'WITH_RELATIVES': 1, 'NOT_ALONE': 1, 'NO_OWN_CHILD_UNDER_18': 1,
                   'NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER': 1, 'NO_RELATIVES': 0}),
            (172, {'SIZE': 3, 'CHILD_UNDER_18': 1, 'OWN_CHILD_UNDER_18': 1, 'OWN_CHILD_UNDER_6_ONLY': 1, 'OWN_CHILD_BETWEEN_6_AND_17': 0, 'OWN_CHILD_IN_BOTH_RANGES': 0,
                   'CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER': 1, 'CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER': 1, 'CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER': 0,
                   'CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER': 0, 'MULTIG': 0, 'MARRIED': 0, 'MARRIED_SAME_SEX': 0, 'MARRIED_OPPOSITE_SEX': 0, 'COHABITING': 1, 'COHABITING_SAME_SEX': 0,
                   'COHABITING_OPPOSITE_SEX': 1, 'NO_SPOUSE_OR_PARTNER': 0, 'WITH_RELATIVES': 1, 'NOT_ALONE': 1, 'NO_OWN_CHILD_UNDER_18': 0, 'NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER': 0, 'NO_RELATIVES': 0})
        ]
    )
    def test_get_index(self, index, attr):
        cur_index = self.hh.get_index(in_attr=attr)
        assert index == cur_index
        self._verify_attributes(index=cur_index, in_attr=attr)

    @pytest.mark.parametrize(
        "index, attr",
        [
            (10, {"NO_SPOUSE_OR_PARTNER": 1, "NOT_ALONE": 1, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0,
                  "MARRIED_SAME_SEX": 0, "MARRIED_OPPOSITE_SEX": 1, "NO_RELATIVES": 0, "WITH_RELATIVES": 1,
                  "NO_OWN_CHILD_UNDER_18": 0, "OWN_CHILD_UNDER_18": 1, "OWN_CHILD_IN_BOTH_RANGES": 0,
                  "OWN_CHILD_BETWEEN_6_AND_17": 0, "OWN_CHILD_UNDER_6_ONLY": 1, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0,
                  "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 1, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 0,
                  "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 0, "SIZE": 5, "COHABITING": 0, "MARRIED": 1}),
            (104, {"NO_SPOUSE_OR_PARTNER": 0, "NOT_ALONE": 1, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0,
                   "MARRIED_SAME_SEX": 1, "MARRIED_OPPOSITE_SEX": 0, "NO_RELATIVES": 0, "WITH_RELATIVES": 1,
                   "NO_OWN_CHILD_UNDER_18": 1, "OWN_CHILD_UNDER_18": 1, "OWN_CHILD_IN_BOTH_RANGES": 0,
                   "OWN_CHILD_BETWEEN_6_AND_17": 1, "OWN_CHILD_UNDER_6_ONLY": 0, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0,
                   "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 0, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 1,
                   "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 0, "SIZE": 4, "COHABITING": 0, "MARRIED": 1}),
            (141, {"NOT_ALONE": 1, "MARRIED_SAME_SEX": 1, "WITH_RELATIVES": 1, "NO_OWN_CHILD_UNDER_18": 1,
                   "OWN_CHILD_BETWEEN_6_AND_17": 0, "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 1,
                   "CHILD_UNDER_18": 1, "MULTIG": 1, "SIZE": 4, "COHABITING": 0, "MARRIED": 1}),
            (393, {"NO_SPOUSE_OR_PARTNER": 1, "NOT_ALONE": 1, "COHABITING_SAME_SEX": 0, "COHABITING_OPPOSITE_SEX": 0,
                   "MARRIED_SAME_SEX": 0, "MARRIED_OPPOSITE_SEX": 0, "NO_RELATIVES": 1, "WITH_RELATIVES": 0,
                   "NO_OWN_CHILD_UNDER_18": 1, "OWN_CHILD_UNDER_18": 0, "OWN_CHILD_IN_BOTH_RANGES": 0,
                   "OWN_CHILD_BETWEEN_6_AND_17": 0, "OWN_CHILD_UNDER_6_ONLY": 0, "NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 1,
                   "CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER": 0, "CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER": 0,
                   "CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER": 0, "CHILD_UNDER_18": 1, "MULTIG": 0, "SIZE": 1, "COHABITING": 0, "MARRIED": 0}),
        ]
    )
    def test_get_index_fail(self, index, attr):
            with pytest.raises(Exception):
                self.hh.get_index(in_attr=attr)


    def test_dhch_recode_size_one_person(self):
        name, group = self.hh.recodeSizeOnePerson()
        assert name == CC.HHSIZE_ONE
        assert len(group) == 1
        for k, v in group.items():
            assert k == "1-person household"
            indexes = [392, 393]
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_get_levels(self):
        computed_levels = self.hh.getLevels()
        manual_levels = TestHHTypeDHCHAttribute.getManualLevels()

        assert len(computed_levels) == len(manual_levels) == 522
        assert computed_levels == manual_levels

        for i in range(len(computed_levels)):
            attrs: Dict = self.hh.get_attributes(i)
            description: str = self.hh.get_description(attrs)
            cur_index: List[int] = computed_levels[description]
            assert len(cur_index) == 1
            assert i == cur_index[0]

    def test_dhch_recode_family(self):
        name, group = self.hh.recodeFamily()
        assert name == CC.HHTYPE_FAMILY
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Family"
            indexes = list(range(0, 256)) + list(range(282, 366)) + list(range(394, 493))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_nonfamily(self):
        name, group = self.hh.recodeNonfamily()
        assert name == CC.HHTYPE_NONFAMILY
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Non-Family"
            indexes = list(range(256, 282)) + list(range(366, 394)) + list(range(493, 522))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_family_married(self):
        name, group = self.hh.recodeFamilyMarried()
        assert name == CC.HHTYPE_FAMILY_MARRIED
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Married Family"
            indexes = list(range(0, 172))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_family_married_with_children_indicator(self):
        name, group = self.hh.recodeFamilyMarriedWithChildrenIndicator()
        assert name == CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR
        assert len(group) == 2
        for k, v in group.items():
            if k == "Married with own children under 18":
                indexes = list(range(0, 41)) + list(range(86, 127))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            elif k == "Married without own children under 18":
                indexes = list(range(41, 86)) + list(range(127, 172))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            else:
                assert False

    def test_dhch_recode_family_married_with_children_levels(self):
        name, group = self.hh.recodeFamilyMarriedWithChildrenLevels()
        assert name == CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS
        assert len(group) == 3
        for k, v in group.items():
            if k == "Married with children under 6 only":
                indexes = list(range(0, 17)) + list(range(86, 103))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            elif k == "Married with children 6 to 17 years only":
                indexes = list(range(17, 34)) + list(range(103, 120))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            elif k == "Married with children under 6 years and 6 to 17 years":
                indexes = list(range(34, 41)) + list(range(120, 127))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            else:
                assert False

    def test_dhch_recode_family_other(self):
        name, group = self.hh.recodeFamilyOther()
        assert name == CC.HHTYPE_FAMILY_OTHER
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Other Family"
            indexes = list(range(172, 256)) + list(range(282, 366)) + list(range(394, 493))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_family_other_with_children_indicator(self):
        name, group = self.hh.recodeFamilyOtherWithChildrenIndicator()
        assert name == CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR
        assert len(group) == 2
        for k, v in group.items():
            if k == "Other with own children under 18":
                indexes = list(range(172, 213)) + list(range(282, 323)) + list(range(394, 445))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            elif k == "Other without own children under 18":
                indexes = list(range(213, 282)) + list(range(323, 394)) + list(range(445, 522))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            else:
                assert False

    def test_dhch_recode_family_other_with_children_levels(self):
        name, group = self.hh.recodeFamilyOtherWithChildrenLevels()
        assert name == CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS
        assert len(group) == 3
        for k, v in group.items():
            if k == "Other with children under 6 only":
                indexes = list(range(172, 189)) + list(range(282, 299)) + list(range(394, 415))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            elif k == "Other with children 6 to 17 years only":
                indexes = list(range(189, 206)) + list(range(299, 316)) + list(range(415, 436))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            elif k == "Other with children under 6 years and 6 to 17 years":
                indexes = list(range(206, 213)) + list(range(316, 323)) + list(range(436, 445))
                assert len(v) == len(indexes)
                assert all(elem in indexes for elem in v)
            else:
                assert False

    def test_dhch_recode_alone(self):
        name, group = self.hh.recodeAlone()
        assert name == CC.HHTYPE_ALONE
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Alone"
            indexes = [392, 393]
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_not_alone(self):
        name, group = self.hh.recodeNotAlone()
        assert name == CC.HHTYPE_NOT_ALONE
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Not alone"
            indexes = list(range(0, 392)) + list(range(394, 522))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_nonfamily_not_alone(self):
        name, group = self.hh.recodeNonfamilyNotAlone()
        assert name == CC.HHTYPE_NONFAMILY_NOT_ALONE
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Non-Family not alone"
            indexes = list(range(256, 282)) + list(range(366, 392)) + list(range(493, 522))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_not_size_two(self):
        name, group = self.hh.recodeNotSizeTwo()
        assert name == CC.HHTYPE_NOT_SIZE_TWO
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Not size two"
            indexes = list(range(0, 66)) + list(range(67, 76)) + list(range(77, 152)) + list(range(153, 162)) + \
                      list(range(163, 270)) + list(range(271, 276)) + list(range(277, 380)) + list(range(381, 386)) + \
                      list(range(387, 394)) + list(range(395, 415)) + list(range(416, 445)) + list(range(446, 456)) + \
                      list(range(457, 476)) + list(range(477, 487)) + list(range(488, 493)) + list(range(494, 499)) + \
                      list(range(500, 510)) + list(range(511, 516)) + list(range(517, 522))
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    def test_dhch_recode_size_two_with_child(self):
        name, group = self.hh.recodeSizeTwoWithChild()
        assert name == CC.HHTYPE_SIZE_TWO_WITH_CHILD
        assert len(group) == 1
        for k, v in group.items():
            assert k == "Size two with child"
            indexes = [76, 162, 276, 386, 394, 415, 445, 456, 487, 493, 499, 516]
            assert len(v) == len(indexes)
            assert all(elem in indexes for elem in v)

    @staticmethod
    def getManualLevels():
        return {
            "Size 3, not multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [0],
            "Size 4, not multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [1],
            "Size 5, not multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [2],
            "Size 6, not multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [3],
            "Size 7+, not multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [4],
            "Size 4, multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [5],
            "Size 5, multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [6],
            "Size 6, multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [7],
            "Size 7+, multig, married opposite-sex with oc u6 only, no cxhhsup 6to17": [8],
            "Size 4, not multig, married opposite-sex with oc u6 only, cxhhsup both": [9],
            "Size 5, not multig, married opposite-sex with oc u6 only, cxhhsup both": [10],
            "Size 6, not multig, married opposite-sex with oc u6 only, cxhhsup both": [11],
            "Size 7+, not multig, married opposite-sex with oc u6 only, cxhhsup both": [12],
            "Size 4, multig, married opposite-sex with oc u6 only, cxhhsup both": [13],
            "Size 5, multig, married opposite-sex with oc u6 only, cxhhsup both": [14],
            "Size 6, multig, married opposite-sex with oc u6 only, cxhhsup both": [15],
            "Size 7+, multig, married opposite-sex with oc u6 only, cxhhsup both": [16],
            "Size 3, not multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [17],
            "Size 4, not multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [18],
            "Size 5, not multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [19],
            "Size 6, not multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [20],
            "Size 7+, not multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [21],
            "Size 4, multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [22],
            "Size 5, multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [23],
            "Size 6, multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [24],
            "Size 7+, multig, married opposite-sex with oc 6to17 only, no cxhhsup u6": [25],
            "Size 4, not multig, married opposite-sex with oc 6to17 only, cxhhsup both": [26],
            "Size 5, not multig, married opposite-sex with oc 6to17 only, cxhhsup both": [27],
            "Size 6, not multig, married opposite-sex with oc 6to17 only, cxhhsup both": [28],
            "Size 7+, not multig, married opposite-sex with oc 6to17 only, cxhhsup both": [29],
            "Size 4, multig, married opposite-sex with oc 6to17 only, cxhhsup both": [30],
            "Size 5, multig, married opposite-sex with oc 6to17 only, cxhhsup both": [31],
            "Size 6, multig, married opposite-sex with oc 6to17 only, cxhhsup both": [32],
            "Size 7+, multig, married opposite-sex with oc 6to17 only, cxhhsup both": [33],
            "Size 4, not multig, married opposite-sex with oc both": [34],
            "Size 5, not multig, married opposite-sex with oc both": [35],
            "Size 6, not multig, married opposite-sex with oc both": [36],
            "Size 7+, not multig, married opposite-sex with oc both": [37],
            "Size 5, multig, married opposite-sex with oc both": [38],
            "Size 6, multig, married opposite-sex with oc both": [39],
            "Size 7+, multig, married opposite-sex with oc both": [40],
            "Size 3, not multig, married opposite-sex with no oc, but has cxhhsup u6 only": [41],
            "Size 4, not multig, married opposite-sex with no oc, but has cxhhsup u6 only": [42],
            "Size 5, not multig, married opposite-sex with no oc, but has cxhhsup u6 only": [43],
            "Size 6, not multig, married opposite-sex with no oc, but has cxhhsup u6 only": [44],
            "Size 7+, not multig, married opposite-sex with no oc, but has cxhhsup u6 only": [45],
            "Size 4, multig, married opposite-sex with no oc, but has cxhhsup u6 only": [46],
            "Size 5, multig, married opposite-sex with no oc, but has cxhhsup u6 only": [47],
            "Size 6, multig, married opposite-sex with no oc, but has cxhhsup u6 only": [48],
            "Size 7+, multig, married opposite-sex with no oc, but has cxhhsup u6 only": [49],
            "Size 3, not multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [50],
            "Size 4, not multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [51],
            "Size 5, not multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [52],
            "Size 6, not multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [53],
            "Size 7+, not multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [54],
            "Size 4, multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [55],
            "Size 5, multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [56],
            "Size 6, multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [57],
            "Size 7+, multig, married opposite-sex with no oc, but has cxhhsup 6to17 only": [58],
            "Size 4, not multig, married opposite-sex with no oc, but has cxhhsup both": [59],
            "Size 5, not multig, married opposite-sex with no oc, but has cxhhsup both": [60],
            "Size 6, not multig, married opposite-sex with no oc, but has cxhhsup both": [61],
            "Size 7+, not multig, married opposite-sex with no oc, but has cxhhsup both": [62],
            "Size 5, multig, married opposite-sex with no oc, but has cxhhsup both": [63],
            "Size 6, multig, married opposite-sex with no oc, but has cxhhsup both": [64],
            "Size 7+, multig, married opposite-sex with no oc, but has cxhhsup both": [65],
            "Size 2, not multig, married opposite-sex with no oc, no cxhhsup, no c": [66],
            "Size 3, not multig, married opposite-sex with no oc, no cxhhsup, no c": [67],
            "Size 4, not multig, married opposite-sex with no oc, no cxhhsup, no c": [68],
            "Size 5, not multig, married opposite-sex with no oc, no cxhhsup, no c": [69],
            "Size 6, not multig, married opposite-sex with no oc, no cxhhsup, no c": [70],
            "Size 7+, not multig, married opposite-sex with no oc, no cxhhsup, no c": [71],
            "Size 4, multig, married opposite-sex with no oc, no cxhhsup, no c": [72],
            "Size 5, multig, married opposite-sex with no oc, no cxhhsup, no c": [73],
            "Size 6, multig, married opposite-sex with no oc, no cxhhsup, no c": [74],
            "Size 7+, multig, married opposite-sex with no oc, no cxhhsup, no c": [75],
            "Size 2, not multig, married opposite-sex with no oc, no cxhhsup, but has c": [76],
            "Size 3, not multig, married opposite-sex with no oc, no cxhhsup, but has c": [77],
            "Size 4, not multig, married opposite-sex with no oc, no cxhhsup, but has c": [78],
            "Size 5, not multig, married opposite-sex with no oc, no cxhhsup, but has c": [79],
            "Size 6, not multig, married opposite-sex with no oc, no cxhhsup, but has c": [80],
            "Size 7+, not multig, married opposite-sex with no oc, no cxhhsup, but has c": [81],
            "Size 4, multig, married opposite-sex with no oc, no cxhhsup, but has c": [82],
            "Size 5, multig, married opposite-sex with no oc, no cxhhsup, but has c": [83],
            "Size 6, multig, married opposite-sex with no oc, no cxhhsup, but has c": [84],
            "Size 7+, multig, married opposite-sex with no oc, no cxhhsup, but has c": [85],
            "Size 3, not multig, married same-sex with oc u6 only, no cxhhsup 6to17": [86],
            "Size 4, not multig, married same-sex with oc u6 only, no cxhhsup 6to17": [87],
            "Size 5, not multig, married same-sex with oc u6 only, no cxhhsup 6to17": [88],
            "Size 6, not multig, married same-sex with oc u6 only, no cxhhsup 6to17": [89],
            "Size 7+, not multig, married same-sex with oc u6 only, no cxhhsup 6to17": [90],
            "Size 4, multig, married same-sex with oc u6 only, no cxhhsup 6to17": [91],
            "Size 5, multig, married same-sex with oc u6 only, no cxhhsup 6to17": [92],
            "Size 6, multig, married same-sex with oc u6 only, no cxhhsup 6to17": [93],
            "Size 7+, multig, married same-sex with oc u6 only, no cxhhsup 6to17": [94],
            "Size 4, not multig, married same-sex with oc u6 only, cxhhsup both": [95],
            "Size 5, not multig, married same-sex with oc u6 only, cxhhsup both": [96],
            "Size 6, not multig, married same-sex with oc u6 only, cxhhsup both": [97],
            "Size 7+, not multig, married same-sex with oc u6 only, cxhhsup both": [98],
            "Size 4, multig, married same-sex with oc u6 only, cxhhsup both": [99],
            "Size 5, multig, married same-sex with oc u6 only, cxhhsup both": [100],
            "Size 6, multig, married same-sex with oc u6 only, cxhhsup both": [101],
            "Size 7+, multig, married same-sex with oc u6 only, cxhhsup both": [102],
            "Size 3, not multig, married same-sex with oc 6to17 only, no cxhhsup u6": [103],
            "Size 4, not multig, married same-sex with oc 6to17 only, no cxhhsup u6": [104],
            "Size 5, not multig, married same-sex with oc 6to17 only, no cxhhsup u6": [105],
            "Size 6, not multig, married same-sex with oc 6to17 only, no cxhhsup u6": [106],
            "Size 7+, not multig, married same-sex with oc 6to17 only, no cxhhsup u6": [107],
            "Size 4, multig, married same-sex with oc 6to17 only, no cxhhsup u6": [108],
            "Size 5, multig, married same-sex with oc 6to17 only, no cxhhsup u6": [109],
            "Size 6, multig, married same-sex with oc 6to17 only, no cxhhsup u6": [110],
            "Size 7+, multig, married same-sex with oc 6to17 only, no cxhhsup u6": [111],
            "Size 4, not multig, married same-sex with oc 6to17 only, cxhhsup both": [112],
            "Size 5, not multig, married same-sex with oc 6to17 only, cxhhsup both": [113],
            "Size 6, not multig, married same-sex with oc 6to17 only, cxhhsup both": [114],
            "Size 7+, not multig, married same-sex with oc 6to17 only, cxhhsup both": [115],
            "Size 4, multig, married same-sex with oc 6to17 only, cxhhsup both": [116],
            "Size 5, multig, married same-sex with oc 6to17 only, cxhhsup both": [117],
            "Size 6, multig, married same-sex with oc 6to17 only, cxhhsup both": [118],
            "Size 7+, multig, married same-sex with oc 6to17 only, cxhhsup both": [119],
            "Size 4, not multig, married same-sex with oc both": [120],
            "Size 5, not multig, married same-sex with oc both": [121],
            "Size 6, not multig, married same-sex with oc both": [122],
            "Size 7+, not multig, married same-sex with oc both": [123],
            "Size 5, multig, married same-sex with oc both": [124],
            "Size 6, multig, married same-sex with oc both": [125],
            "Size 7+, multig, married same-sex with oc both": [126],
            "Size 3, not multig, married same-sex with no oc, but has cxhhsup u6 only": [127],
            "Size 4, not multig, married same-sex with no oc, but has cxhhsup u6 only": [128],
            "Size 5, not multig, married same-sex with no oc, but has cxhhsup u6 only": [129],
            "Size 6, not multig, married same-sex with no oc, but has cxhhsup u6 only": [130],
            "Size 7+, not multig, married same-sex with no oc, but has cxhhsup u6 only": [131],
            "Size 4, multig, married same-sex with no oc, but has cxhhsup u6 only": [132],
            "Size 5, multig, married same-sex with no oc, but has cxhhsup u6 only": [133],
            "Size 6, multig, married same-sex with no oc, but has cxhhsup u6 only": [134],
            "Size 7+, multig, married same-sex with no oc, but has cxhhsup u6 only": [135],
            "Size 3, not multig, married same-sex with no oc, but has cxhhsup 6to17 only": [136],
            "Size 4, not multig, married same-sex with no oc, but has cxhhsup 6to17 only": [137],
            "Size 5, not multig, married same-sex with no oc, but has cxhhsup 6to17 only": [138],
            "Size 6, not multig, married same-sex with no oc, but has cxhhsup 6to17 only": [139],
            "Size 7+, not multig, married same-sex with no oc, but has cxhhsup 6to17 only": [140],
            "Size 4, multig, married same-sex with no oc, but has cxhhsup 6to17 only": [141],
            "Size 5, multig, married same-sex with no oc, but has cxhhsup 6to17 only": [142],
            "Size 6, multig, married same-sex with no oc, but has cxhhsup 6to17 only": [143],
            "Size 7+, multig, married same-sex with no oc, but has cxhhsup 6to17 only": [144],
            "Size 4, not multig, married same-sex with no oc, but has cxhhsup both": [145],
            "Size 5, not multig, married same-sex with no oc, but has cxhhsup both": [146],
            "Size 6, not multig, married same-sex with no oc, but has cxhhsup both": [147],
            "Size 7+, not multig, married same-sex with no oc, but has cxhhsup both": [148],
            "Size 5, multig, married same-sex with no oc, but has cxhhsup both": [149],
            "Size 6, multig, married same-sex with no oc, but has cxhhsup both": [150],
            "Size 7+, multig, married same-sex with no oc, but has cxhhsup both": [151],
            "Size 2, not multig, married same-sex with no oc, no cxhhsup, no c": [152],
            "Size 3, not multig, married same-sex with no oc, no cxhhsup, no c": [153],
            "Size 4, not multig, married same-sex with no oc, no cxhhsup, no c": [154],
            "Size 5, not multig, married same-sex with no oc, no cxhhsup, no c": [155],
            "Size 6, not multig, married same-sex with no oc, no cxhhsup, no c": [156],
            "Size 7+, not multig, married same-sex with no oc, no cxhhsup, no c": [157],
            "Size 4, multig, married same-sex with no oc, no cxhhsup, no c": [158],
            "Size 5, multig, married same-sex with no oc, no cxhhsup, no c": [159],
            "Size 6, multig, married same-sex with no oc, no cxhhsup, no c": [160],
            "Size 7+, multig, married same-sex with no oc, no cxhhsup, no c": [161],
            "Size 2, not multig, married same-sex with no oc, no cxhhsup, but has c": [162],
            "Size 3, not multig, married same-sex with no oc, no cxhhsup, but has c": [163],
            "Size 4, not multig, married same-sex with no oc, no cxhhsup, but has c": [164],
            "Size 5, not multig, married same-sex with no oc, no cxhhsup, but has c": [165],
            "Size 6, not multig, married same-sex with no oc, no cxhhsup, but has c": [166],
            "Size 7+, not multig, married same-sex with no oc, no cxhhsup, but has c": [167],
            "Size 4, multig, married same-sex with no oc, no cxhhsup, but has c": [168],
            "Size 5, multig, married same-sex with no oc, no cxhhsup, but has c": [169],
            "Size 6, multig, married same-sex with no oc, no cxhhsup, but has c": [170],
            "Size 7+, multig, married same-sex with no oc, no cxhhsup, but has c": [171],
            "Size 3, not multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [172],
            "Size 4, not multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [173],
            "Size 5, not multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [174],
            "Size 6, not multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [175],
            "Size 7+, not multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [176],
            "Size 4, multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [177],
            "Size 5, multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [178],
            "Size 6, multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [179],
            "Size 7+, multig, cohabiting opposite-sex with oc u6 only, no cxhhsup 6to17": [180],
            "Size 4, not multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [181],
            "Size 5, not multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [182],
            "Size 6, not multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [183],
            "Size 7+, not multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [184],
            "Size 4, multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [185],
            "Size 5, multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [186],
            "Size 6, multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [187],
            "Size 7+, multig, cohabiting opposite-sex with oc u6 only, cxhhsup both": [188],
            "Size 3, not multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [189],
            "Size 4, not multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [190],
            "Size 5, not multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [191],
            "Size 6, not multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [192],
            "Size 7+, not multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [193],
            "Size 4, multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [194],
            "Size 5, multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [195],
            "Size 6, multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [196],
            "Size 7+, multig, cohabiting opposite-sex with oc 6to17 only, no cxhhsup u6": [197],
            "Size 4, not multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [198],
            "Size 5, not multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [199],
            "Size 6, not multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [200],
            "Size 7+, not multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [201],
            "Size 4, multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [202],
            "Size 5, multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [203],
            "Size 6, multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [204],
            "Size 7+, multig, cohabiting opposite-sex with oc 6to17 only, cxhhsup both": [205],
            "Size 4, not multig, cohabiting opposite-sex with oc both": [206],
            "Size 5, not multig, cohabiting opposite-sex with oc both": [207],
            "Size 6, not multig, cohabiting opposite-sex with oc both": [208],
            "Size 7+, not multig, cohabiting opposite-sex with oc both": [209],
            "Size 5, multig, cohabiting opposite-sex with oc both": [210],
            "Size 6, multig, cohabiting opposite-sex with oc both": [211],
            "Size 7+, multig, cohabiting opposite-sex with oc both": [212],
            "Size 3, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [213],
            "Size 4, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [214],
            "Size 5, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [215],
            "Size 6, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [216],
            "Size 7+, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [217],
            "Size 4, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [218],
            "Size 5, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [219],
            "Size 6, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [220],
            "Size 7+, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup u6 only": [221],
            "Size 3, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [222],
            "Size 4, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [223],
            "Size 5, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [224],
            "Size 6, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [225],
            "Size 7+, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [226],
            "Size 4, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [227],
            "Size 5, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [228],
            "Size 6, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [229],
            "Size 7+, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup 6to17": [230],
            "Size 4, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [231],
            "Size 5, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [232],
            "Size 6, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [233],
            "Size 7+, not multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [234],
            "Size 5, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [235],
            "Size 6, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [236],
            "Size 7+, multig, cohabiting opposite-sex with relatives, no oc, but has cxhhsup both": [237],
            "Size 3, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [238],
            "Size 4, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [239],
            "Size 5, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [240],
            "Size 6, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [241],
            "Size 7+, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [242],
            "Size 4, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [243],
            "Size 5, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [244],
            "Size 6, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [245],
            "Size 7+, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, no c": [246],
            "Size 3, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [247],
            "Size 4, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [248],
            "Size 5, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [249],
            "Size 6, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [250],
            "Size 7+, not multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [251],
            "Size 4, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [252],
            "Size 5, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [253],
            "Size 6, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [254],
            "Size 7+, multig, cohabiting opposite-sex with relatives, no oc, no cxhhsup, but has c": [255],
            "Size 3, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup u6 only": [256],
            "Size 4, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup u6 only": [257],
            "Size 5, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup u6 only": [258],
            "Size 6, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup u6 only": [259],
            "Size 7+, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup u6 only": [260],
            "Size 3, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup 6to17": [261],
            "Size 4, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup 6to17": [262],
            "Size 5, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup 6to17": [263],
            "Size 6, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup 6to17": [264],
            "Size 7+, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup 6to17": [265],
            "Size 4, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup both": [266],
            "Size 5, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup both": [267],
            "Size 6, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup both": [268],
            "Size 7+, not multig, cohabiting opposite-sex with no relatives, no oc, but has cxhhsup both": [269],
            "Size 2, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, no c": [270],
            "Size 3, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, no c": [271],
            "Size 4, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, no c": [272],
            "Size 5, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, no c": [273],
            "Size 6, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, no c": [274],
            "Size 7+, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, no c": [275],
            "Size 2, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, but has c": [276],
            "Size 3, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, but has c": [277],
            "Size 4, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, but has c": [278],
            "Size 5, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, but has c": [279],
            "Size 6, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, but has c": [280],
            "Size 7+, not multig, cohabiting opposite-sex with no relatives, no oc, no cxhhsup, but has c": [281],
            "Size 3, not multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [282],
            "Size 4, not multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [283],
            "Size 5, not multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [284],
            "Size 6, not multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [285],
            "Size 7+, not multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [286],
            "Size 4, multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [287],
            "Size 5, multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [288],
            "Size 6, multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [289],
            "Size 7+, multig, cohabiting same-sex with oc u6 only, no cxhhsup 6to17": [290],
            "Size 4, not multig, cohabiting same-sex with oc u6 only, cxhhsup both": [291],
            "Size 5, not multig, cohabiting same-sex with oc u6 only, cxhhsup both": [292],
            "Size 6, not multig, cohabiting same-sex with oc u6 only, cxhhsup both": [293],
            "Size 7+, not multig, cohabiting same-sex with oc u6 only, cxhhsup both": [294],
            "Size 4, multig, cohabiting same-sex with oc u6 only, cxhhsup both": [295],
            "Size 5, multig, cohabiting same-sex with oc u6 only, cxhhsup both": [296],
            "Size 6, multig, cohabiting same-sex with oc u6 only, cxhhsup both": [297],
            "Size 7+, multig, cohabiting same-sex with oc u6 only, cxhhsup both": [298],
            "Size 3, not multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [299],
            "Size 4, not multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [300],
            "Size 5, not multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [301],
            "Size 6, not multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [302],
            "Size 7+, not multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [303],
            "Size 4, multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [304],
            "Size 5, multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [305],
            "Size 6, multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [306],
            "Size 7+, multig, cohabiting same-sex with oc 6to17 only, no cxhhsup u6": [307],
            "Size 4, not multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [308],
            "Size 5, not multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [309],
            "Size 6, not multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [310],
            "Size 7+, not multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [311],
            "Size 4, multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [312],
            "Size 5, multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [313],
            "Size 6, multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [314],
            "Size 7+, multig, cohabiting same-sex with oc 6to17 only, cxhhsup both": [315],
            "Size 4, not multig, cohabiting same-sex with oc both": [316],
            "Size 5, not multig, cohabiting same-sex with oc both": [317],
            "Size 6, not multig, cohabiting same-sex with oc both": [318],
            "Size 7+, not multig, cohabiting same-sex with oc both": [319],
            "Size 5, multig, cohabiting same-sex with oc both": [320],
            "Size 6, multig, cohabiting same-sex with oc both": [321],
            "Size 7+, multig, cohabiting same-sex with oc both": [322],
            "Size 3, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [323],
            "Size 4, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [324],
            "Size 5, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [325],
            "Size 6, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [326],
            "Size 7+, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [327],
            "Size 4, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [328],
            "Size 5, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [329],
            "Size 6, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [330],
            "Size 7+, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup u6 only": [331],
            "Size 3, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [332],
            "Size 4, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [333],
            "Size 5, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [334],
            "Size 6, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [335],
            "Size 7+, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [336],
            "Size 4, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [337],
            "Size 5, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [338],
            "Size 6, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [339],
            "Size 7+, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup 6to17": [340],
            "Size 4, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [341],
            "Size 5, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [342],
            "Size 6, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [343],
            "Size 7+, not multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [344],
            "Size 5, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [345],
            "Size 6, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [346],
            "Size 7+, multig, cohabiting same-sex with relatives, no oc, but has cxhhsup both": [347],
            "Size 3, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [348],
            "Size 4, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [349],
            "Size 5, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [350],
            "Size 6, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [351],
            "Size 7+, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [352],
            "Size 4, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [353],
            "Size 5, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [354],
            "Size 6, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [355],
            "Size 7+, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, no c": [356],
            "Size 3, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [357],
            "Size 4, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [358],
            "Size 5, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [359],
            "Size 6, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [360],
            "Size 7+, not multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [361],
            "Size 4, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [362],
            "Size 5, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [363],
            "Size 6, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [364],
            "Size 7+, multig, cohabiting same-sex with relatives, no oc, no cxhhsup, but has c": [365],
            "Size 3, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup u6 only": [366],
            "Size 4, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup u6 only": [367],
            "Size 5, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup u6 only": [368],
            "Size 6, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup u6 only": [369],
            "Size 7+, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup u6 only": [370],
            "Size 3, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup 6to17": [371],
            "Size 4, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup 6to17": [372],
            "Size 5, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup 6to17": [373],
            "Size 6, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup 6to17": [374],
            "Size 7+, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup 6to17": [375],
            "Size 4, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup both": [376],
            "Size 5, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup both": [377],
            "Size 6, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup both": [378],
            "Size 7+, not multig, cohabiting same-sex with no relatives, no oc, but has cxhhsup both": [379],
            "Size 2, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, no c": [380],
            "Size 3, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, no c": [381],
            "Size 4, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, no c": [382],
            "Size 5, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, no c": [383],
            "Size 6, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, no c": [384],
            "Size 7+, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, no c": [385],
            "Size 2, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, but has c": [386],
            "Size 3, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, but has c": [387],
            "Size 4, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, but has c": [388],
            "Size 5, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, but has c": [389],
            "Size 6, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, but has c": [390],
            "Size 7+, not multig, cohabiting same-sex with no relatives, no oc, no cxhhsup, but has c": [391],
            "Size 1, not multig, no spouse or partner, living alone, no c": [392],
            "Size 1, not multig, no spouse or partner, living alone, c": [393],
            "Size 2, not multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [394],
            "Size 3, not multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [395],
            "Size 4, not multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [396],
            "Size 5, not multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [397],
            "Size 6, not multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [398],
            "Size 7+, not multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [399],
            "Size 3, multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [400],
            "Size 4, multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [401],
            "Size 5, multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [402],
            "Size 6, multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [403],
            "Size 7+, multig, no spouse or partner, with oc u6 only, no cxhhsup 6to17": [404],
            "Size 3, not multig, no spouse or partner, with oc u6 only, cxhhsup both": [405],
            "Size 4, not multig, no spouse or partner, with oc u6 only, cxhhsup both": [406],
            "Size 5, not multig, no spouse or partner, with oc u6 only, cxhhsup both": [407],
            "Size 6, not multig, no spouse or partner, with oc u6 only, cxhhsup both": [408],
            "Size 7+, not multig, no spouse or partner, with oc u6 only, cxhhsup both": [409],
            "Size 3, multig, no spouse or partner, with oc u6 only, cxhhsup both": [410],
            "Size 4, multig, no spouse or partner, with oc u6 only, cxhhsup both": [411],
            "Size 5, multig, no spouse or partner, with oc u6 only, cxhhsup both": [412],
            "Size 6, multig, no spouse or partner, with oc u6 only, cxhhsup both": [413],
            "Size 7+, multig, no spouse or partner, with oc u6 only, cxhhsup both": [414],
            "Size 2, not multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [415],
            "Size 3, not multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [416],
            "Size 4, not multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [417],
            "Size 5, not multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [418],
            "Size 6, not multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [419],
            "Size 7+, not multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [420],
            "Size 3, multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [421],
            "Size 4, multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [422],
            "Size 5, multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [423],
            "Size 6, multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [424],
            "Size 7+, multig, no spouse or partner, with oc 6to17 only, no cxhhsup u6": [425],
            "Size 3, not multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [426],
            "Size 4, not multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [427],
            "Size 5, not multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [428],
            "Size 6, not multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [429],
            "Size 7+, not multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [430],
            "Size 3, multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [431],
            "Size 4, multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [432],
            "Size 5, multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [433],
            "Size 6, multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [434],
            "Size 7+, multig, no spouse or partner, with oc 6to17 only, cxhhsup both": [435],
            "Size 3, not multig, no spouse or partner, with oc both": [436],
            "Size 4, not multig, no spouse or partner, with oc both": [437],
            "Size 5, not multig, no spouse or partner, with oc both": [438],
            "Size 6, not multig, no spouse or partner, with oc both": [439],
            "Size 7+, not multig, no spouse or partner, with oc both": [440],
            "Size 4, multig, no spouse or partner, with oc both": [441],
            "Size 5, multig, no spouse or partner, with oc both": [442],
            "Size 6, multig, no spouse or partner, with oc both": [443],
            "Size 7+, multig, no spouse or partner, with oc both": [444],
            "Size 2, not multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [445],
            "Size 3, not multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [446],
            "Size 4, not multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [447],
            "Size 5, not multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [448],
            "Size 6, not multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [449],
            "Size 7+, not multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [450],
            "Size 3, multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [451],
            "Size 4, multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [452],
            "Size 5, multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [453],
            "Size 6, multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [454],
            "Size 7+, multig, no spouse or partner with relatives, no oc, but has cxhhsup u6 only": [455],
            "Size 2, not multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [456],
            "Size 3, not multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [457],
            "Size 4, not multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [458],
            "Size 5, not multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [459],
            "Size 6, not multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [460],
            "Size 7+, not multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [461],
            "Size 3, multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [462],
            "Size 4, multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [463],
            "Size 5, multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [464],
            "Size 6, multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [465],
            "Size 7+, multig, no spouse or partner with relatives, no oc, but has cxhhsup 6to17": [466],
            "Size 3, not multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [467],
            "Size 4, not multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [468],
            "Size 5, not multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [469],
            "Size 6, not multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [470],
            "Size 7+, not multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [471],
            "Size 4, multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [472],
            "Size 5, multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [473],
            "Size 6, multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [474],
            "Size 7+, multig, no spouse or partner with relatives, no oc, but has cxhhsup both": [475],
            "Size 2, not multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [476],
            "Size 3, not multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [477],
            "Size 4, not multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [478],
            "Size 5, not multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [479],
            "Size 6, not multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [480],
            "Size 7+, not multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [481],
            "Size 3, multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [482],
            "Size 4, multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [483],
            "Size 5, multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [484],
            "Size 6, multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [485],
            "Size 7+, multig, no spouse or partner with relatives, no oc, no cxhhsup, no c": [486],
            "Size 2, not multig, no spouse or partner with relatives, no oc, no cxhhsup, but has c": [487],
            "Size 3, not multig, no spouse or partner with relatives, no oc, no cxhhsup, but has c": [488],
            "Size 4, not multig, no spouse or partner with relatives, no oc, no cxhhsup, but has c": [489],
            "Size 5, not multig, no spouse or partner with relatives, no oc, no cxhhsup, but has c": [490],
            "Size 6, not multig, no spouse or partner with relatives, no oc, no cxhhsup, but has c": [491],
            "Size 7+, not multig, no spouse or partner with relatives, no oc, no cxhhsup, but has c": [492],
            "Size 2, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup u6 only": [493],
            "Size 3, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup u6 only": [494],
            "Size 4, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup u6 only": [495],
            "Size 5, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup u6 only": [496],
            "Size 6, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup u6 only": [497],
            "Size 7+, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup u6 only": [498],
            "Size 2, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup 6to17": [499],
            "Size 3, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup 6to17": [500],
            "Size 4, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup 6to17": [501],
            "Size 5, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup 6to17": [502],
            "Size 6, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup 6to17": [503],
            "Size 7+, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup 6to17": [504],
            "Size 3, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup both": [505],
            "Size 4, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup both": [506],
            "Size 5, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup both": [507],
            "Size 6, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup both": [508],
            "Size 7+, not multig, no spouse or partner with no relatives, no oc, but has cxhhsup both": [509],
            "Size 2, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, no c": [510],
            "Size 3, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, no c": [511],
            "Size 4, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, no c": [512],
            "Size 5, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, no c": [513],
            "Size 6, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, no c": [514],
            "Size 7+, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, no c": [515],
            "Size 2, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, but has c": [516],
            "Size 3, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, but has c": [517],
            "Size 4, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, but has c": [518],
            "Size 5, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, but has c": [519],
            "Size 6, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, but has c": [520],
            "Size 7+, not multig, no spouse or partner with no relatives, no oc, no cxhhsup, but has c": [521]
        }
