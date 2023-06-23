import pytest
from pyspark.sql import Row
from typing import Tuple
import programs.reader.hh_recoder
import numpy as np
from programs.reader.from_mdf_recoder import (
    DHCRecoder,
    DHCPRecoder,
    DHCPUnitRecoder,
    DHCHHouseholdRecoderHHSEX,
)
from programs.writer.hh2010_to_mdfunit2020 import Household2010ToMDFUnit2020Recoder
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import DHCPHHGQToMDFPersons2020Recoder
import programs.reader.cef_2020.cef_2020_dhch_reader as DHCH
import programs.reader.cef_2020.cef_2020_dhcp_reader as DHCP
from das_constants import CC


class Tvar:
    def __init__(self, name):
        self.name = name


def test_table10recoder_simple():
    recoder = programs.reader.hh_recoder.Table10RecoderSimple(("hhgq",))
    assert (
        recoder.recode(Row(MAFID="123456789", geocode="123456789ABCDEFG", hhgq=0))[
            "hhgqinv"
        ]
        == 0
    )
    assert (
        recoder.recode(Row(MAFID="123456789", geocode="123456789ABCDEFG", hhgq=1))[
            "hhgqinv"
        ]
        == 0
    )
    assert (
        recoder.recode(Row(MAFID="123456789", geocode="123456789ABCDEFG", hhgq=10))[
            "hhgqinv"
        ]
        == 9
    )


def test_table10recoder():
    recoder = programs.reader.hh_recoder.Table10Recoder(("gqtype", "vacs"))
    assert (
        recoder.recode(
            Row(MAFID="123456789", geocode="123456789ABCDEFG", gqtype="000", vacs=0)
        )["hhgq"]
        == 0
    )
    assert (
        recoder.recode(
            Row(MAFID="123456789", geocode="123456789ABCDEFG", gqtype="000", vacs=1)
        )["hhgq"]
        == 1
    )
    assert (
        recoder.recode(
            Row(MAFID="123456789", geocode="123456789ABCDEFG", gqtype="801", vacs=0)
        )["hhgq"]
        == 24
    )
    assert (
        recoder.recode(
            Row(MAFID="123456789", geocode="123456789ABCDEFG", gqtype="405", vacs=1)
        )["hhgq"]
        == 16
    )


class TestDHCRecoder:

    recoder = DHCRecoder(
        ("TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK",),
        tuple(map(Tvar, ("geocode",))),
    )
    reverse_recoder = None

    geo_rows = [
        (
            Row(
                TABBLKST="12",
                TABBLKCOU="123",
                TABTRACTCE="123456",
                TABBLKGRPCE="1",
                TABBLK="1234",
            ),
            "1212312345611234",
        ),
    ]

    def check_recoded_row(self, rowlist):
        d = {}
        ans = {}
        for i, subrow in enumerate(rowlist):
            d.update(subrow[0].asDict())
            if i < len(self.recoder.recode_vars):
                ans.update({self.recoder.recode_vars[i].name: subrow[1]})

        recoded_row = self.recoder.recode(Row(**d))

        for var in self.recoder.recode_vars:
            if ans[var.name] is not None:
                assert recoded_row[var.name] == ans[var.name], f"Varname: {var.name}"

        if self.reverse_recoder is not None:
            rec_back = self.reverse_recoder.recode(recoded_row.asDict())
            for k in set(rec_back.keys()).intersection(d.keys()):
                assert d[k] == rec_back[k], f"Key: {k}"

    def test_geocode_recoder(self):
        for geo_row, ans in self.geo_rows:
            assert self.recoder.geocode_recode(geo_row) == ans

    def test_recoder(self, *args):
        for geo_row, ans in self.geo_rows:
            assert self.recoder.recode(geo_row)["geocode"] == ans


class TestDHCPRecoder(TestDHCRecoder):
    recoder = DHCPRecoder(
        ("TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK",),
        ("GQTYPE",),
        ("QSEX",),
        ("CENHISP",),
        ("CENRACE",),
        ("CITIZEN",),
        tuple(
            map(
                Tvar,
                ("geocode",)
                + tuple(
                    f"{vname}_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"
                    for vname in ("hhgq", "sex", "hispanic", "cenrace", "citizen")
                ),
            )
        ),
    )

    reverse_recoder = DHCPHHGQToMDFPersons2020Recoder()

    sex_rows = [(Row(QSEX="1"), 0), (Row(QSEX="2"), 1)]

    cenrace_rows = [(Row(CENRACE="01"), 0), (Row(CENRACE="02"), 1)]

    cenhisp_rows = [(Row(CENHISP="1"), 0), (Row(CENHISP="2"), 1)]

    citizen_rows = [(Row(CITIZEN="1"), 1), (Row(CITIZEN="2"), 0)]

    hhgq_rows = [
        (Row(GQTYPE="000"), 0),
        (Row(GQTYPE="000"), 0),
        (Row(GQTYPE="101"), 1),
        (Row(GQTYPE="501"), 5),
        (Row(GQTYPE="701"), 7),
        # (Row(GQTYPE='901'), 7),  # Our MDF writer doesn't produce over 700
    ]

    @pytest.mark.parametrize(
        "rows, varname",
        [
            (sex_rows, f"sex_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"),
            (citizen_rows, f"citizen_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"),
            (cenrace_rows, f"cenrace_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"),
            (cenhisp_rows, f"hispanic_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"),
            (hhgq_rows, f"hhgq_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"),
        ],
    )
    def test_single_recoder(self, rows, varname):
        for row, ans in rows:
            assert self.recoder.recode_funcs[varname](row) == ans

    def test_hhgq_recoder(self):
        self.test_single_recoder(
            self.hhgq_rows, f"hhgq_{DHCPHHGQToMDFPersons2020Recoder.schema_name}"
        )

    @pytest.mark.parametrize("hhgq_row", hhgq_rows)
    @pytest.mark.parametrize("sex_row", sex_rows)
    @pytest.mark.parametrize("cenrace_row", cenrace_rows)
    @pytest.mark.parametrize("citizen_row", citizen_rows)
    @pytest.mark.parametrize("cenhisp_row", cenhisp_rows)
    def test_recoder(self, hhgq_row, sex_row, cenrace_row, citizen_row, cenhisp_row):
        for geo_row in self.geo_rows:
            self.check_recoded_row(
                [
                    geo_row,
                    hhgq_row,
                    sex_row,
                    cenhisp_row,
                    cenrace_row,
                    citizen_row,
                    (Row(age_DHCP_HHGQ=50), 50),
                ]
            )


class TestDHCPUnitRecoder(TestDHCPRecoder):
    recoder = DHCPUnitRecoder(
        ("TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK",),
        ("GQTYPE",),
        tuple(map(Tvar, ("geocode", "hhgq",))),
    )

    reverse_recoder = None

    def test_single_recoder(self, *args):
        """ Pytest decorator parametrization and inheritance don't work well together. so this stub is needed"""
        pass

    def test_recoder(self, *args):
        for geo_row in self.geo_rows:
            for hhgq_row in self.hhgq_rows:
                self.check_recoded_row([geo_row, hhgq_row])


class TestDHCHHouseholdRecoderHHSEX(TestDHCRecoder):
    recoder = DHCHHouseholdRecoderHHSEX(
        ("TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK",),
        ("HHSEX",),
        ("HHLDRAGE",),
        ("HHSPAN",),
        ("HHRACE",),
        ("HHT", "HHT2", "CPLT", "UPART", "PAOC",),
        ("P60", "P65", "P75"),
        ("MULTG",),
        tuple(
            map(
                Tvar,
                (
                    "geocode",
                    f"hhsex_{Household2010ToMDFUnit2020Recoder.schema_name}",
                    f"hhage_{Household2010ToMDFUnit2020Recoder.schema_name}",
                    f"hisp_{Household2010ToMDFUnit2020Recoder.schema_name}",
                    f"hhrace_{Household2010ToMDFUnit2020Recoder.schema_name}",
                    f"hhtype_{Household2010ToMDFUnit2020Recoder.schema_name}",
                    f"elderly_{Household2010ToMDFUnit2020Recoder.schema_name}",
                    f"multi_{Household2010ToMDFUnit2020Recoder.schema_name}",
                ),
            )
        ),
    )

    reverse_recoder = Household2010ToMDFUnit2020Recoder()

    nzhhtype = np.array(
        [
            [1, 1, 1, 5, 1, 0],
            [1, 1, 1, 5, 2, 1],
            [1, 1, 1, 5, 3, 2],
            [1, 1, 2, 5, 1, 4],
            [1, 1, 2, 5, 2, 5],
            [1, 1, 2, 5, 3, 6],
            [1, 2, 1, 5, 4, 3],
            [1, 2, 2, 5, 4, 7],
            [2, 3, 3, 2, 1, 8],
            [2, 3, 3, 2, 2, 9],
            [2, 3, 3, 2, 3, 10],
            [2, 3, 3, 4, 1, 8],
            [2, 3, 3, 4, 2, 9],
            [2, 3, 3, 4, 3, 10],
            [2, 3, 4, 1, 1, 13],
            [2, 3, 4, 1, 2, 14],
            [2, 3, 4, 1, 3, 15],
            [2, 3, 4, 3, 1, 13],
            [2, 3, 4, 3, 2, 14],
            [2, 3, 4, 3, 3, 15],
            [2, 4, 3, 2, 4, 11],
            [2, 4, 3, 4, 4, 11],
            [2, 4, 4, 1, 4, 16],
            [2, 4, 4, 3, 4, 16],
            [2, 6, 0, 5, 1, 19],
            [2, 6, 0, 5, 2, 20],
            [2, 6, 0, 5, 3, 21],
            [2, 7, 0, 5, 4, 22],
            [2, 10, 0, 5, 1, 19],
            [2, 10, 0, 5, 2, 20],
            [2, 10, 0, 5, 3, 21],
            [2, 11, 0, 5, 4, 22],
            [3, 3, 3, 2, 1, 8],
            [3, 3, 3, 2, 2, 9],
            [3, 3, 3, 2, 3, 10],
            [3, 3, 3, 4, 1, 8],
            [3, 3, 3, 4, 2, 9],
            [3, 3, 3, 4, 3, 10],
            [3, 3, 4, 1, 1, 13],
            [3, 3, 4, 1, 2, 14],
            [3, 3, 4, 1, 3, 15],
            [3, 3, 4, 3, 1, 13],
            [3, 3, 4, 3, 2, 14],
            [3, 3, 4, 3, 3, 15],
            [3, 4, 3, 2, 4, 11],
            [3, 4, 3, 4, 4, 11],
            [3, 4, 4, 1, 4, 16],
            [3, 4, 4, 3, 4, 16],
            [3, 6, 0, 5, 1, 19],
            [3, 6, 0, 5, 2, 20],
            [3, 6, 0, 5, 3, 21],
            [3, 7, 0, 5, 4, 22],
            [3, 10, 0, 5, 1, 19],
            [3, 10, 0, 5, 2, 20],
            [3, 10, 0, 5, 3, 21],
            [3, 11, 0, 5, 4, 22],
            [4, 5, 0, 5, 4, 18],
            [4, 9, 0, 5, 4, 18],
            [5, 4, 3, 2, 4, 12],
            [5, 4, 3, 4, 4, 12],
            [5, 4, 4, 1, 4, 17],
            [5, 4, 4, 3, 4, 17],
            [5, 8, 0, 5, 4, 23],
            [5, 12, 0, 5, 4, 23],
            [6, 5, 0, 5, 4, 18],
            [6, 9, 0, 5, 4, 18],
            [7, 4, 3, 2, 4, 12],
            [7, 4, 3, 4, 4, 12],
            [7, 4, 4, 1, 4, 17],
            [7, 4, 4, 3, 4, 17],
            [7, 8, 0, 5, 4, 23],
            [7, 12, 0, 5, 4, 23],
        ]
    )

    rows = {
        f"hhsex_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (Row(HHSEX="2"), 1),
            (Row(HHSEX="1"), 0),
        ],
        f"hhage_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (Row(HHLDRAGE=1), 0),
            (Row(HHLDRAGE=5), 4),
        ],
        f"hisp_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (Row(HHSPAN="1"), 0),
            (Row(HHSPAN="2"), 1),
        ],
        f"hhrace_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (Row(HHRACE="01"), 0),
            (Row(HHRACE="03"), 2),
        ],
        f"elderly_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (Row(P60=1, P65=1, P75=1), 3),
            (Row(P60=1, P65=1, P75=0), 2),
            (Row(P60=1, P65=0, P75=0), 1),
            (Row(P60=0, P65=0, P75=0), 0),
        ],
        f"multi_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (Row(MULTG="1"), 0),
            (Row(MULTG="2"), 1),
        ],
        # 'hhtype_{Household2010ToMDFUnit2020Recoder.schema_name}': [(Row(HHT=f"{hht}", HHT2=f"{hht2:02}", CPLT=f"{cplt}", UPART=f"{upart}", PAOC=f"{paoc}"), None) for hht in range(1,8) for hht2 in range(1,13) for cplt in range(5) for upart in range(1,6) for paoc in range(1,5)]
        # The answers to this one are obtained from the function it tests, not independently!
        f"hhtype_{Household2010ToMDFUnit2020Recoder.schema_name}": [
            (
                Row(
                    HHT=f"{hht}",
                    HHT2=f"{hht2:02}",
                    CPLT=f"{cplt}",
                    UPART=f"{upart}",
                    PAOC=f"{paoc}",
                ),
                ans,
            )
            for hht, hht2, cplt, upart, paoc, ans in nzhhtype
        ],
    }

    @pytest.mark.parametrize(
        "varname",
        [
            f"hhage_{Household2010ToMDFUnit2020Recoder.schema_name}",
            f"hisp_{Household2010ToMDFUnit2020Recoder.schema_name}",
            f"hhrace_{Household2010ToMDFUnit2020Recoder.schema_name}",
            f"elderly_{Household2010ToMDFUnit2020Recoder.schema_name}",
            f"multi_{Household2010ToMDFUnit2020Recoder.schema_name}",
            f"hhtype_{Household2010ToMDFUnit2020Recoder.schema_name}",
        ],
    )
    def test_single_recoder(self, varname):
        for row, ans in self.rows[varname]:
            assert self.recoder.recode_funcs[varname](row) == ans

    @pytest.mark.parametrize(
        "hhsex_row", rows[f"hhsex_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    @pytest.mark.parametrize(
        "age_row", rows[f"hhage_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    @pytest.mark.parametrize(
        "hisp_row", rows[f"hisp_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    @pytest.mark.parametrize(
        "race_row", rows[f"hhrace_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    @pytest.mark.parametrize(
        "elderly_row", rows[f"elderly_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    @pytest.mark.parametrize(
        "multi_row", rows[f"multi_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    @pytest.mark.parametrize(
        "hhtype_row", rows[f"hhtype_{Household2010ToMDFUnit2020Recoder.schema_name}"]
    )
    def test_recoder(
        self,
        hhsex_row: Tuple[Row, int],
        age_row,
        hisp_row,
        race_row,
        elderly_row,
        multi_row,
        hhtype_row: Tuple[Row, int],
    ):
        hhsex: Row = hhsex_row[0]
        hhtype: Row = hhtype_row[0]
        if hhsex["HHSEX"] == "2":
            if (
                hhtype.HHT in ["2", "4", "5"]
                or hhtype.UPART in ["1", "2"]
                or hhtype.HHT2 in ["09", "10", "11", "12"]
            ):
                return
        if hhsex["HHSEX"] == "1":
            hhtype: Row = hhtype_row[0]
            if (
                hhtype.HHT in ["3", "6", "7"]
                or hhtype.UPART in ["3", "4"]
                or hhtype.HHT2 in ["05", "06", "07", "08"]
            ):
                return
        if hhtype.HHT in ["4", "6"]:
            hhsize_row = (
                Row(**{f"size_{Household2010ToMDFUnit2020Recoder.schema_name}": 1}),
                1,
            )
            multi_row = (Row(MULTG="0"), 0)
        else:
            hhsize_row = (
                Row(**{f"size_{Household2010ToMDFUnit2020Recoder.schema_name}": 5}),
                5,
            )
        for geo_row in self.geo_rows:
            self.check_recoded_row(
                [
                    geo_row,
                    hhsex_row,
                    age_row,
                    hisp_row,
                    race_row,
                    hhtype_row,
                    elderly_row,
                    multi_row,
                    hhsize_row,
                ]
            )


class TestCEFDHCPRecoder:
    def test_row(
        self, relship=20, qgqtyp="101", final_pop=1, qsex="1", qage=0, cenhisp="1", cenrace="1"
    ):
        return Row(
            relship=relship,
            qgqtyp=qgqtyp,
            final_pop=final_pop,
            qage=qage,
            qsex=qsex,
            cenhisp=cenhisp,
            cenrace=cenrace
        )

    recoder = DHCP.DHCP_recoder(
        ("relship", "qgqtyp"), ("qsex",), ("qage",), ("cenhisp",), ("cenrace",)
    )

    @pytest.mark.parametrize(
        "relship, qgqtyp, final_pop, ans",
        [
            (20, "   ",     1, 0),  # relship of 20 (householder) and final_pop of 1  == relgq of 0 (householder living alone)
            (20, "   ",     2, 1),  # relship of 20 (householder) and final_pop of >1 == relgq of 1 (householder not living alone)
            (20, "   ", 99999, 1),
            (21, "   ",     2, 2),
            (22, "   ",     2, 3),
        ],
    )
    def test_cef_relgq_recoder(self, relship, qgqtyp, final_pop, ans):
        # Ensure the relgq recode method directly returns the correct answer
        assert DHCP.relgq_recode(relship, qgqtyp, final_pop) == ans

        # Ensure the recoder correctly sets the value of the relgq variable
        row: Row = self.test_row(relship=relship, qgqtyp=qgqtyp, final_pop=final_pop)
        assert self.recoder.recode(row)[CC.ATTR_RELGQ] == ans

    @pytest.mark.parametrize("qsex, ans", [("1", 0), ("2", 1)])
    def test_cef_sex_recoder(self, qsex, ans):
        # Ensure the relgq recode method directly returns the correct answer
        assert DHCP.sex_recode(qsex) == ans

        # Ensure the recoder correctly sets the value of the sex variable
        row: Row = self.test_row(qsex=qsex)
        assert self.recoder.recode(row)[CC.ATTR_SEX] == ans

    @pytest.mark.parametrize("qage, ans", [(0, 0), (1, 1), (115, 115)])
    def test_cef_hispanic_recoder(self, qage, ans):
        # Ensure the age recode method directly returns the correct answer
        assert DHCP.age_recode(qage) == ans

        # Ensure the recoder correctly sets the value of the age variable
        row: Row = self.test_row(qage=qage)
        assert self.recoder.recode(row)[CC.ATTR_AGE] == ans

    @pytest.mark.parametrize("cenhisp, ans", [("1", 0), ("2", 1)])
    def test_cef_hispanic_recoder(self, cenhisp, ans):
        # Ensure the relgq recode method directly returns the correct answer
        assert DHCP.hispanic_recode(cenhisp) == ans

        # Ensure the recoder correctly sets the value of the cenhisp variable
        row: Row = self.test_row(cenhisp=cenhisp)
        assert self.recoder.recode(row)[CC.ATTR_HISP] == ans

    @pytest.mark.parametrize("cenrace, ans", [("1", 0), ("2", 1), ("42", 41)])
    def test_cef_cenrace_recoder(self, cenrace, ans):
        assert DHCP.cenrace_recode(cenrace) == ans

        row: Row = self.test_row(cenrace=cenrace)
        assert self.recoder.recode(row)[CC.ATTR_CENRACE_DAS] == ans


class TestCEFDHCHRecoder:
    @staticmethod
    def test_row(
        hhsex = "2", hhldrage = 67, hhspan = "1", hhrace = "47", p60 = 3, p65 = 2, p75 = 0, final_pop = "6", ten = "1",
        p18 = 1, paoc = "1", pac = "1", multg = "1", cplt = "1", hht = "1"
    ):
        return Row(
            #qgqtyp = qgqtype,
            sex = hhsex,
            hhldrage = hhldrage,
            hhspan = hhspan,
            race = hhrace,
            p60 = p60,
            p65 = p65,
            p75 = p75,
            final_pop = final_pop,
            TEN = ten,
            p18 = p18,
            paoc = paoc,
            pac = pac,
            multg = multg,
            cplt = cplt,
            HHT = hht
        )

    recoder = DHCH.DHCH_Household_recoder(
        (CC.ATTR_SEX, ), #hhsex
        ("hhldrage", ), #hhldrage
        ("hhspan", ), #hhspan
        (CC.ATTR_RACE, ), #hhrace
        ("p60", "p65", "p75",), #p60, p65, p75
        (CC.ATTR_MDF_TEN, ), #ten
        (CC.ATTR_MDF_HHT, ), #hht
    )

    @pytest.mark.parametrize(
        "input_row",
        [
        (["2", 67, "1", "47", 3, 2, 0, "2", "1", "0", "4", "4", "1", "1", "1"]),
        (["1", 50, "1", "50", 2, 1, 1, "2", "1", "1", "4", "4", "1", "1", "3"])
        ])
    def test_cef_full_recode(self, input_row):
        row: Row = Row(
                  sex = input_row[0],
                  hhldrage = input_row[1],
                  hhspan = input_row[2],
                  race = input_row[3],
                  p60 = input_row[4],
                  p65 = input_row[5],
                  p75 = input_row[6],
                  final_pop = input_row[7],
                  TEN = input_row[8],
                  p18 = input_row[9],
                  paoc = input_row[10],
                  pac = input_row[11],
                  multg = input_row[12],
                  cplt = input_row[13],
                  HHT = input_row[14]
                  )
        output_row = self.recoder.recode(row) # simply check that this doesn't fail


    @pytest.mark.parametrize(
        "hhsex, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 1),
            ("3", None),
        ],
    )
    def test_cef_hhsex_recoder(self, hhsex, ans):
        # Ensure the hhsex recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.hhsex_recode(hhsex) == ans

            row: Row = self.test_row(hhsex=hhsex)  # Ensure recoder correctly sets hhsex
            assert self.recoder.recode(row)[CC.ATTR_SEX] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhsex_recode(hhsex)

    @pytest.mark.parametrize(
        "hhldrage, ans",
        [
            (0, None),
            (14, None),
            (15, 0),
            (34, 1),
            (44, 2),
            (54, 3),
            (59, 4),
            (64, 5),
            (74, 6),
            (84, 7),
            (90, 8),
            (115, 8),
            (116, None),
            (130, None),
        ]
    )
    def test_cef_hhage_recoder(self, hhldrage, ans):
        # Ensure the hhage recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.hhage_recode(hhldrage) == ans

            row: Row = self.test_row(hhldrage=hhldrage)  # Ensure recoder correctly sets hhage
            assert self.recoder.recode(row)[CC.ATTR_HHAGE] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhage_recode(hhldrage)


    @pytest.mark.parametrize(
        "hhspan, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 1),
            ("3", None)
        ],
    )
    def test_cef_hhhisp_recoder(self, hhspan, ans):
        # Ensure the hhhisp recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.hhhisp_recode(hhspan) == ans

            row: Row = self.test_row(hhspan=hhspan)  # Ensure recoder correctly sets hhhisp
            assert self.recoder.recode(row)[CC.ATTR_HHHISP] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhhisp_recode(hhspan)

    @pytest.mark.parametrize(
        "hhrace, ans",
        [
            ("1", 0),
            ("2", 1),
            ("3", 2),
            ("4", 3),
            ("5", 4),
            ("6", 5),
            ("20", 6),
            ("70", None)
        ]
    )
    def test_cef_hhrace_recoder(self, hhrace, ans):
        # Ensure the hhrace recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.hhrace_recode(hhrace) == ans

            row: Row = self.test_row(hhrace=hhrace)  # Ensure recoder correctly sets hhage
            assert self.recoder.recode(row)[CC.ATTR_RACE] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.hhrace_recode(hhrace)

    @pytest.mark.parametrize(
        "p60, p65, p75, ans",
        [
            (1, 0, 0, 1),
            (0, 1, 0, 2),
            (1, 1, 0, 2),
            (0, 0, 1, 3),
            (1, 1, 1, 3),
            (0, 0, 0, 0),
            (100, 0, 0, None),
            (0, 100, 0, None),
            (0, 0, 100, None)
        ]
    )
    def test_cef_elderly_recoder(self, p60, p65, p75, ans):
        # Ensure the elderly recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.elderly_recode(p60, p65, p75) == ans

            row: Row = self.test_row(p60=p60, p65=p65, p75=p75)  # Ensure recoder correctly sets hhsex
            assert self.recoder.recode(row)[CC.ATTR_HHELDERLY] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.elderly_recode(p60, p65, p75)

    @pytest.mark.parametrize(
        "ten, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 1),
            ("4", 1),
            ("5", None)
        ],
    )
    def test_cef_tenshort_recoder(self, ten, ans):
        # Ensure the tenshort recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.DHCH_Household_recoder.tenure_recode(ten) == ans

            row: Row = self.test_row(ten=ten)  # Ensure recoder correctly sets tenshort
            assert self.recoder.recode(row)[CC.ATTR_HHTENSHORT] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.DHCH_Household_recoder.tenure_recode(ten)

    @pytest.mark.parametrize(
        "final_pop, ans",
        [
            ("-1", None),
            ("0", None),
            ("5", 5),
            ("8", 7),
            ("50", 7),
            ("97", 7),
            ("99999", 7),
            ("100000", None)
        ]
    )
    def test_cef_size_recoder(self, final_pop, ans):
        # Ensure the size recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.size_recode(final_pop) == ans

            row: Row = self.test_row(final_pop=final_pop)
            assert self.recoder.recode(row)[CC.ATTR_SIZE] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.size_recode(final_pop)

    @pytest.mark.parametrize(
        "p18, ans",
        [
            (-1, None),
            (0, 0),
            (2, 1),
            (100, None)
        ]
    )
    def test_cef_child_under_18_recoder(self, p18, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.child_under_18_recode(p18) == ans

            #row: Row = self.test_row(p18=p18)
            #assert self.recoder.recode(row)[CC.ATTR_CHILD_UNDER_18] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.child_under_18_recode(p18)

    @pytest.mark.parametrize(
        "paoc, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 1),
            ("3", 1),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_own_child_under_18_recoder(self, paoc, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.own_child_under_18_recode(paoc) == ans

            #row: Row = self.test_row(paoc=paoc)
            #assert self.recoder.recode(row)[CC.ATTR_OWN_CHILD_UNDER_18] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.own_child_under_18_recode(paoc)

    @pytest.mark.parametrize(
        "paoc, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 0),
            ("3", 0),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_own_child_under_6_only_recoder(self, paoc, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.own_child_under_6_only_recode(paoc) == ans

            #row: Row = self.test_row(paoc=paoc)
            #assert self.recoder.recode(row)[CC.ATTR_OWN_CHILD_UNDER_6_ONLY] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.own_child_under_6_only_recode(paoc)

    @pytest.mark.parametrize(
        "paoc, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 1),
            ("3", 0),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_own_child_between_6_and_17_recoder(self, paoc, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.own_child_between_6_and_17_recode(paoc) == ans

            #row: Row = self.test_row(paoc=paoc)
            #assert self.recoder.recode(row)[CC.ATTR_OWN_CHILD_BETWEEN_6_AND_17] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.own_child_between_6_and_17_recode(paoc)

    @pytest.mark.parametrize(
        "paoc, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 1),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_own_child_in_both_ranges_recode(self, paoc, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.own_child_in_both_ranges_recode(paoc) == ans

            #row: Row = self.test_row(paoc=paoc)
            #assert self.recoder.recode(row)[CC.ATTR_OWN_CHILD_IN_BOTH_RANGES] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.own_child_in_both_ranges_recode(paoc)

    @pytest.mark.parametrize(
        "pac, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 1),
            ("3", 1),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_child_under_18_excluding_householder_spouse_partner_recode(self, pac, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.child_under_18_excluding_householder_spouse_partner_recode(pac) == ans

            #row: Row = self.test_row(pac=pac)
            #assert self.recoder.recode(row)[CC.ATTR_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.child_under_18_excluding_householder_spouse_partner_recode(pac)

    @pytest.mark.parametrize(
        "pac, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 0),
            ("3", 0),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_child_under_6_only_excluding_householder_spouse_partner_recode(self, pac, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.child_under_6_only_excluding_householder_spouse_partner_recode(pac) == ans

            #row: Row = self.test_row(pac=pac)
            #assert self.recoder.recode(row)[CC.ATTR_CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.child_under_6_only_excluding_householder_spouse_partner_recode(pac)

    @pytest.mark.parametrize(
        "pac, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 1),
            ("3", 0),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_child_between_6_and_17_excluding_householder_spouse_partner_recode(self, pac, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.child_between_6_and_17_excluding_householder_spouse_partner_recode(pac) == ans

            #row: Row = self.test_row(pac=pac)
            #assert self.recoder.recode(row)[CC.ATTR_CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.child_between_6_and_17_excluding_householder_spouse_partner_recode(pac)

    @pytest.mark.parametrize(
        "pac, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 1),
            ("4", 0),
            ("5", None)
        ]
    )
    def test_cef_child_in_both_ranges_excluding_householder_spouse_partner_recode(self, pac, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.child_in_both_ranges_excluding_householder_spouse_partner_recode(pac) == ans

            #row: Row = self.test_row(pac=pac)
            #assert self.recoder.recode(row)[CC.ATTR_CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.child_in_both_ranges_excluding_householder_spouse_partner_recode(pac)

    @pytest.mark.parametrize(
        "multg, final_pop, ans",
        [
            ("0", "1", 0),
            ("1", "4", 0),
            ("2", "4", 1),
            ("3", "4", None),
            ("0", "0", None)
        ]
    )
    def test_cef_multig_recode(self, multg, final_pop, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.multig_recode(multg, final_pop) == ans

            row: Row = self.test_row(multg=multg)
            assert self.recoder.recode(row)[CC.ATTR_MULTIG] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.multig_recode(multg, final_pop)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 1),
            ("3", 0),
            ("4", 0),
            ("5", 0),
            ("6", None)
        ]
    )
    def test_cef_married_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.married_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_MARRIED] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.married_recode(cplt)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 1),
            ("3", 0),
            ("4", 0),
            ("5", 0),
            ("6", None)
        ]
    )
    def test_cef_married_same_sex_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.married_same_sex_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_MARRIED_SAME_SEX] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.married_same_sex_recode(cplt)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 0),
            ("3", 0),
            ("4", 0),
            ("5", 0),
            ("6", None)
        ]
    )
    def test_cef_married_opposite_sex_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.married_opposite_sex_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_MARRIED_OPPOSITE_SEX] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.married_opposite_sex_recode(cplt)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 1),
            ("4", 1),
            ("5", 0),
            ("6", None)
        ]
    )
    def test_cef_cohabiting_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.cohabiting_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_COHABITING] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.cohabiting_recode(cplt)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 0),
            ("4", 1),
            ("5", 0),
            ("6", None)
        ]
    )
    def test_cef_cohabiting_same_sex_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.cohabiting_same_sex_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_COHABITING_SAME_SEX] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.cohabiting_same_sex_recode(cplt)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 1),
            ("4", 0),
            ("5", 0),
            ("6", None)
        ]
    )
    def test_cef_cohabiting_opposite_sex_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.cohabiting_opposite_sex_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_COHABITING_OPPOSITE_SEX] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.cohabiting_opposite_sex_recode(cplt)

    @pytest.mark.parametrize(
        "cplt, ans",
        [
            ("1", 0),
            ("2", 0),
            ("3", 0),
            ("4", 0),
            ("5", 1),
            ("6", None)
        ]
    )
    def test_cef_no_spouse_or_partner_recode(self, cplt, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.no_spouse_or_partner_recode(cplt) == ans

            row: Row = self.test_row(cplt=cplt)
            assert self.recoder.recode(row)[CC.ATTR_NO_SPOUSE_OR_PARTNER] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.no_spouse_or_partner_recode(cplt)

    @pytest.mark.parametrize(
        "hht, ans",
        [
            ("0", None),
            ("1", 1),
            ("2", 1),
            ("3", 1),
            ("4", 0),
            ("5", 0),
            ("6", 0),
            ("7", 0),
            ("8", None)
        ]
    )
    def test_cef_with_relatives_recode(self, hht, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.with_relatives_recode(hht) == ans

            #row: Row = self.test_row(hht=hht)
            #assert self.recoder.recode(row)[CC.ATTR_WITH_RELATIVES] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.with_relatives_recode(hht)

    @pytest.mark.parametrize(
        "final_pop, ans",
        [
            ("1", 0),
            ("2", 1),
            ("10", 1)
        ]
    )
    def test_cef_not_alone_recode(self, final_pop, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.not_alone_recode(final_pop) == ans

            #row: Row = self.test_row(final_pop=final_pop)
            #assert self.recoder.recode(row)[CC.ATTR_NOT_ALONE] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.not_alone_recode(final_pop)

    @pytest.mark.parametrize(
        "paoc, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 0),
            ("4", 1),
            ("5", None)
        ]
    )
    def test_cef_no_own_children_under_18(self, paoc, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.no_own_children_under_18(paoc) == ans

            #row: Row = self.test_row(paoc=paoc)
            #assert self.recoder.recode(row)[CC.ATTR_NO_OWN_CHILDREN_UNDER_18] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.no_own_children_under_18(paoc)

    @pytest.mark.parametrize(
        "pac, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 0),
            ("4", 1),
            ("5", None)
        ]
    )
    def test_cef_no_child_under_18_excluding_householder_spouse_partner_recode(self, pac, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.no_child_under_18_excluding_householder_spouse_partner(pac) == ans

            #row: Row = self.test_row(pac=pac)
            #assert self.recoder.recode(row)[CC.ATTR_NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.no_child_under_18_excluding_householder_spouse_partner(pac)

    @pytest.mark.parametrize(
        "hht, ans",
        [
            ("0", None),
            ("1", 0),
            ("2", 0),
            ("3", 0),
            ("4", 1),
            ("5", 1),
            ("6", 1),
            ("7", 1),
            ("8", None)
        ]
    )
    def test_cef_with_relatives_recode(self, hht, ans):
        # Ensure the recode method directly returns the correct answer
        if ans is not None:
            assert DHCH.no_relatives_recode(hht) == ans

            #row: Row = self.test_row(hht=hht)
            #assert self.recoder.recode(row)[CC.ATTR_NO_RELATIVES] == ans
        else:
            with pytest.raises(Exception):
                assert DHCH.no_relatives_recode(hht)
