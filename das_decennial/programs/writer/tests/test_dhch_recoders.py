import pytest
import programs.writer.cef_2020.dhch_to_mdf2020_recoders as DHCH
from programs.writer.cef_2020.dhch_to_mdf2020_recoders import DHCHToMDF2020HouseholdRecoder, DHCHToMDF2020UnitRecoder
from pyspark.sql import Row
from das_constants import CC

RELGQ = CC.ATTR_RELGQ
SEX = CC.ATTR_SEX
AGE = CC.ATTR_AGE
HISP = CC.ATTR_HISP
CENRACE = CC.ATTR_CENRACE


class TestDHCHToMDF2020HouseholdRecoder:
    def test_row(
        self, hhsex=1, hhage=66, hhhisp=1, hhrace=1, hhtype=10, hhelderly=3, hhtenshort=0,
            geocode="123456890abcdef"
    ):
        schemaname=CC.SCHEMA_DHCH

        # The only values in the rows coming in after DAS will be those from the histogram,
        # and will be "mangled" (including the schema name to differentiate
        row_dict: dict = {
            f'{CC.ATTR_SEX}_{schemaname}':hhsex,
            f'{CC.ATTR_HHAGE}_{schemaname}':hhage,
            f'{CC.ATTR_HHHISP}_{schemaname}':hhhisp,
            f'{CC.ATTR_RACE}_{schemaname}':hhrace,
            f'{CC.ATTR_HHTYPE_DHCH  }_{schemaname}':hhtype,
            f'{CC.ATTR_HHELDERLY}_{schemaname}':hhelderly,
            f'{CC.ATTR_HHTENSHORT}_{schemaname}':hhtenshort,
            'geocode':geocode
        }

        return Row(**row_dict)

    recoder = DHCHToMDF2020HouseholdRecoder()

    @pytest.mark.parametrize(
        "hhtype, married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize, ans",
        [
            (152, 1, 0, 0, 0, 2, 2, "2"),
            ( 66, 0, 1, 0, 0, 2, 2, "1"),
            (380, 0, 0, 1, 0, 2, 2, "4"),
            (270, 0, 0, 0, 1, 2, 2, "3"),

        ],
    )
    def test_cef_cplt_recoder(self, hhtype, married_same_sex, married_opposite_sex, cohabiting_same_sex,
                              cohabiting_opposite_sex, rtype, hhsize, ans):
        # Ensure the hhsex recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.cplt_recode(married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize) == ans

            # Ensure recoder correctly sets cplt, using hhtype
            row: Row = self.test_row(hhtype=hhtype)
            assert self.recoder.recode(row)[CC.ATTR_MDF_CPLT] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.cplt_recode(married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize)

    @pytest.mark.parametrize(
        "hhtype, married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, hhsex, rtype, hhsize, ans",
        [
            # All households of size < 2 have a UPART of 0=NIU
            (392, 0, 0, 0, 0, 1, 2, 1, "0"),
            # All married households with size >= 2 have a UPART of 5 = All other households
            (152, 1, 0, 0, 0, 0, 2, 2, "5"),
            (152, 1, 0, 0, 0, 1, 2, 2, "5"),
            ( 66, 0, 1, 0, 0, 0, 2, 2, "5"),
            ( 66, 0, 1, 0, 0, 1, 2, 2, "5"),
            # All unmarried households with size >= 2 have a detailed UPART
            (380, 0, 0, 1, 0, 0, 2, 2, "1"), # 1 = Male householder and male partner
            (380, 0, 0, 1, 0, 1, 2, 2, "3"), # 3 = Female householder and female partner
            (270, 0, 0, 0, 1, 0, 2, 2, "2"), # 2 = Male householder and female partner
            (270, 0, 0, 0, 1, 1, 2, 2, "4"), # 4 = Female householder and male partner
        ],
    )
    def test_cef_upart_recoder(self, hhtype, hhsex, married_same_sex, married_opposite_sex, cohabiting_same_sex,
                              cohabiting_opposite_sex, rtype, hhsize, ans):
        # Ensure the hhsex recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.upart_recode(hhsex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize) == ans

            row: Row = self.test_row(hhtype=hhtype, hhsex=hhsex)  # Ensure recoder correctly sets upart
            assert self.recoder.recode(row)[CC.ATTR_MDF_UPART] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.upart_recode(hhsex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize)

    @pytest.mark.parametrize(
        "multig, rtype, hhsize, ans",
        [
            (0, 2, 3, "1"),
            (1, 2, 3, "2"),
            (0, 2, 2, "0"),
            (1, 2, 2, "0"),
            (1, 4, 17, "0")
        ]
    )
    def test_cef_multg_recoder(self, multig, rtype, hhsize, ans):
        if ans is not None:
            assert DHCH.multg_recode(multig, rtype, hhsize) == ans
            row: Row = self.test_row()
            assert self.recoder.recode(row)[CC.ATTR_MDF_MULTG]
        else:
            with pytest.raises(Exception):
                assert DHCH.multg_recode(multig, rtype, hhsize)

    @pytest.mark.parametrize(
        "hhage, rtype, hhsize, ans",
        [
            (1, 0, 0, "2"),
            (7, 0, 0, "8"),
            (9, 0, 0, "0")
        ]
    )
    def test_cef_hhldrage_recoder(self, hhage, rtype, hhsize, ans):
        if ans is not None:
            assert DHCH.hhldrage_recode(hhage, rtype, hhsize) == ans
            row: Row = self.test_row(hhage=hhage)
            assert self.recoder.recode(row)[CC.ATTR_MDF_HHLDRAGE] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhldrage_recode(hhage, rtype, hhsize)

    @pytest.mark.parametrize(
        "hhhisp, rtype, hhsize, ans",
        [
            (0, 0, 0, "1"),
            (1, 0, 0, "2")
        ]
    )
    def test_cef_hhspan_recoder(self, hhhisp, rtype, hhsize, ans):
        if ans is not None:
            assert DHCH.hhspan_recode(hhhisp, rtype, hhsize) == ans
            row: Row = self.test_row(hhhisp=hhhisp)
            assert self.recoder.recode(row)[CC.ATTR_MDF_HHSPAN] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhspan_recode(hhhisp, rtype, hhsize)

    @pytest.mark.parametrize(
        "hhrace, rtype, hhsize, ans",
        [
            (0, 0, 0, "01"),
            (1, 0, 0, "02")
        ]
    )
    def test_cef_hhrace_recoder(self, hhrace, rtype, hhsize, ans):
        if ans is not None:
            assert DHCH.hhrace_recode(hhrace, rtype, hhsize) == ans
            row: Row = self.test_row(hhrace=hhrace)
            assert self.recoder.recode(row)[CC.ATTR_MDF_HHRACE] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhrace_recode(hhrace, rtype, hhsize)

    @pytest.mark.parametrize(
        "hhtype, own_child_under_6_only, own_child_between_6_and_17, own_child_in_both_ranges, rtype, hhsize, ans",
        [
            (  0, 1, 0, 0, 2, 3, "1"),
            ( 17, 0, 1, 0, 2, 3, "2"),
            ( 34, 0, 0, 1, 2, 3, "3"),
            ( 66, 0, 0, 0, 2, 2, "4")
        ]
    )
    def test_cef_paoc_recoder(self, hhtype, own_child_under_6_only, own_child_between_6_and_17, own_child_in_both_ranges, rtype, hhsize, ans):
        if ans is not None:
            assert DHCH.paoc_recode(own_child_under_6_only, own_child_between_6_and_17, own_child_in_both_ranges, rtype, hhsize) == ans

            row: Row = self.test_row(hhtype=hhtype)
            assert self.recoder.recode(row)[CC.ATTR_MDF_PAOC] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.paoc_recode(own_child_under_6_only, own_child_between_6_and_17, own_child_in_both_ranges, rtype, hhsize)

    @pytest.mark.parametrize(
        "hhelderly, ans",
        [
            (-1, None),
            (0, "0"),
            (1, "1"),
            (2, "1"),
            (3, "1"),
            (4, None),
            (99, None),
        ]
    )
    def test_cef_p60_recoder(self, hhelderly, ans):
        if ans is not None:
            assert DHCH.p60_recode(hhelderly) == ans
            row: Row = self.test_row(hhelderly=hhelderly)

            assert self.recoder.recode(row)[CC.ATTR_MDF_P60] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.p60_recode(hhelderly)

    @pytest.mark.parametrize(
        "hhelderly, ans",
        [
            (-1, None),
            (0, "0"),
            (1, "0"),
            (2, "1"),
            (3, "1"),
            (4, None),
            (99, None),
        ]
    )
    def test_cef_p65_recoder(self, hhelderly, ans):
        if ans is not None:
            assert DHCH.p65_recode(hhelderly) == ans
            row: Row = self.test_row(hhelderly=hhelderly)

            assert self.recoder.recode(row)[CC.ATTR_MDF_P65] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.p65_recode(hhelderly)

    @pytest.mark.parametrize(
        "hhelderly, ans",
        [
            (-1, None),
            (0, "0"),
            (1, "0"),
            (2, "0"),
            (3, "1"),
            (4, None),
            (99, None),
        ]
    )
    def test_cef_p75_recoder(self, hhelderly, ans):
        if ans is not None:
            assert DHCH.p75_recode(hhelderly) == ans
            row: Row = self.test_row(hhelderly=hhelderly)

            assert self.recoder.recode(row)[CC.ATTR_MDF_P75] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.p75_recode(hhelderly)

    @pytest.mark.parametrize(
        "hhtype, child_under_6_only_excluding_householder_spouse_partner, child_between_6_and_17_excluding_householder_spouse_partner, child_in_both_ranges_excluding_householder_spouse_partner, rtype, hhsize, ans",
        [
            (  0, 1, 0, 0, 2, 3, "1"),
            ( 17, 0, 1, 0, 2, 3, "2"),
            ( 34, 0, 0, 1, 2, 3, "3"),
            ( 66, 0, 0, 0, 2, 2, "4"),
        ]
    )
    def test_cef_pac_recoder(self, hhtype, child_under_6_only_excluding_householder_spouse_partner, child_between_6_and_17_excluding_householder_spouse_partner, child_in_both_ranges_excluding_householder_spouse_partner, rtype, hhsize, ans):
        if ans is not None:
            assert DHCH.pac_recode(child_under_6_only_excluding_householder_spouse_partner, child_between_6_and_17_excluding_householder_spouse_partner, child_in_both_ranges_excluding_householder_spouse_partner, rtype, hhsize) == ans

            row: Row = self.test_row(hhtype=hhtype)
            assert self.recoder.recode(row)[CC.ATTR_MDF_PAC] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.pac_recode(child_under_6_only_excluding_householder_spouse_partner, child_between_6_and_17_excluding_householder_spouse_partner, child_in_both_ranges_excluding_householder_spouse_partner, rtype, hhsize)


class TestDHCHToMDF2020UnitRecoder:
    def test_row(self, tenvacgq = 0):
        schemaname=CC.SCHEMA_DHCH

        # The only values in the rows coming in after DAS will be those from the histogram,
        # and will be "mangled" (including the schema name to differentiate
        row_dict: dict = {
            f'{CC.ATTR_MDF_GQTYPE}_{schemaname}':tenvacgq,
        }

        return Row(**row_dict)

    recoder = DHCHToMDF2020UnitRecoder()

    @pytest.mark.parametrize(
            "tenvacgq, ans",
        [
            (0, "000"),
            (11, "101"),
            (12, "102"),
            (13, "103"),
            (14, "104"),
            (15, "105"),
            (16, "106"),
            (17, "201"),
            (18, "202"),
            (19, "203"),
            (20, "301"),
            (21, "401"),
            (22, "402"),
            (23, "403"),
            (24, "404"),
            (25, "405"),
            (26, "501"),
            (27, "601"),
            (28, "602"),
            (29, "701"),
            (30, "801"),
            (31, "802"),
            (32, "900"),
            (33, "901"),
            (34, "997")

        ],
    )
    def test_cef_gqtype_recoder(self, tenvacgq, ans):
        # Ensure the tenvacgq recode method directly returns the correct answer
        assert DHCH.gqtype_recode(tenvacgq) == ans

    @pytest.mark.parametrize(
        "tenvacgq, ans",
        [
            (-1,  None),
            (0,  "0"),
            (4,  "1"),
            (5,  "2"),
            (6,  "3"),
            (7,  "4"),
            (8,  "5"),
            (9,  "6"),
            (10, "7"),
            (11, "0"),
            (34, "0"),
            (35, None),
        ],
    )
    def test_cef_vacs_recoder(self, tenvacgq, ans):
        if ans is not None:
            # Ensure the tenvacgq recode method directly returns the correct answer
            assert DHCH.vacs_recode(tenvacgq) == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.vacs_recode(tenvacgq)

    @pytest.mark.parametrize(
        "married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize, ans",
        [
            (0, 0, 0, 0, 2, 0, "0"),
            (0, 0, 0, 0, 4, 0, "0"),
            (0, 0, 0, 0, 2, 1, "0"),
            (0, 1, 0, 0, 2, 2, "1"),
            (1, 0, 0, 0, 2, 2, "2"),
            (0, 0, 0, 1, 2, 2, "3"),
            (0, 0, 1, 0, 2, 2, "4")
        ],
    )
    def test_cef_cplt_recoder(self, married_same_sex, married_opposite_sex, cohabiting_same_sex,
                              cohabiting_opposite_sex, rtype, hhsize, ans):
        # Ensure the cplt recode method directly returns the correct answer
        assert DHCH.cplt_recode(married_same_sex, married_opposite_sex, cohabiting_same_sex, cohabiting_opposite_sex, rtype, hhsize) == ans
