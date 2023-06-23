import os
from programs.reader.fwf_reader import FixedWidthFormatTable
from programs.reader.cef_2020.cef_2020_grfc import load_grfc
from programs.schema.attributes.tenvacgq import TenureVacancyGQAttr
from programs.schema.attributes.vacgq import VacancyGQAttr
from programs.schema.attributes.dhch.hhtype_dhch import HHTypeDHCHAttr
from programs.reader.cef_2020.cef_2020_dhcp_reader import hhgq_recode
from typing import List, Tuple
from das_constants import CC

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.types import StringType, IntegerType
    from pyspark.sql.functions import concat, col
except ImportError as e:
    pass


class CEF2020DHCHUnitTable(FixedWidthFormatTable):
    def load(self, filter=None):
        self.annotate("In Unit load")
        # Load the GRF-C to obtain a DataFrame with oidtb and geocode
        grfc_path = os.path.expandvars(self.getconfig("grfc_path", section=CC.READER))
        geo_data = load_grfc(grfc_path)

        # Load the Unit table
        unit_data = super().load(filter)
        # Join with geo_data on oidtb, which will add the geocode to the Unit DataFrame
        unit_with_geocode = unit_data.join(geo_data, on=["oidtb"], how="left")

        # Make the unit_with_geocode DataFrame available
        units_without_geocode = unit_with_geocode.where(col('geocode').isNull())

        assert  units_without_geocode.count() == 0, f'Some units could not match with geocode! {units_without_geocode.show()}'
        self.annotate("Finished Unit load")
        return unit_with_geocode


class CEF2020DHCHHouseholdTable(CEF2020DHCHUnitTable):
    def load(self, filter=None):
        self.annotate("In Household load")
        household_rdd = super().load(filter=lambda line: line[0] == '2' and not (line[17:22] == '00000' or line[17:22] == '    0'))
        self.annotate("Finished Household load")
        return household_rdd


class CEF2020H1HouseholdTable(CEF2020DHCHUnitTable):
    def load(self, filter=None):
        self.annotate("In Household load")
        household_rdd = super().load(filter=lambda line: line[0] == '2')
        self.annotate("Finished Household load")
        return household_rdd


class DHCH_Household_recoder:
    """
    This is the recoder for table1 of the Decennial Census.
    It creates the hhgq_unit_dhcp, hhsex, hhage, hhhisp, tenure, and hhtype variables.
    """
    tenure_attr_name = CC.ATTR_HHTENSHORT

    def __init__(
            self,
            hhsex_varname_list: List,
            hhage_varname_list: List,
            hhhisp_varname_list: List,
            hhrace_varname_list: List,
            elderly_varname_list: List,
            tenure_varname_list: List,
            hht_varname_list: List
    ):
        self.hhsex_varname_list: Tuple[str] = hhsex_varname_list
        self.hhage_varname_list: Tuple[str] = hhage_varname_list
        self.hhhisp_varname_list: Tuple[str] = hhhisp_varname_list
        self.hhrace_varname_list: Tuple[str] = hhrace_varname_list
        self.elderly_varname_list: Tuple[str] = elderly_varname_list
        self.tenure_varname_list: Tuple[str] = tenure_varname_list
        self.hht_varname_list: Tuple[str] = hht_varname_list

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row.
                The row is expected to contain the values from the CEF 2020 Schema

            Output:
                dataframe Row with recode variables added
        """
        row_dict: dict = row.asDict()
        try:
            hhsex: str = str(row[self.hhsex_varname_list[0]])
            hhage: int = int(row[self.hhage_varname_list[0]])
            hhhisp: str = str(row[self.hhhisp_varname_list[0]])
            hhrace: str = str(row[self.hhrace_varname_list[0]])
            p60: int = int(row[self.elderly_varname_list[0]])
            p65: int = int(row[self.elderly_varname_list[1]])
            p75: int = int(row[self.elderly_varname_list[2]])
            ten: str = str(row[self.tenure_varname_list[0]])
            final_pop: str = str(row['final_pop'])
            p18: int = int(row['p18'])
            paoc: str = str(row['paoc'])
            pac: str = str(row['pac'])
            multg: str = str(row['multg'])
            cplt: str = str(row['cplt'])
            hht: str = str(row[self.hht_varname_list[0]])

            row_dict[CC.ATTR_SEX] = hhsex_recode(hhsex)
            row_dict[CC.ATTR_HHAGE] = hhage_recode(hhage)
            row_dict[CC.ATTR_HHHISP] = hhhisp_recode(hhhisp)
            row_dict[CC.ATTR_RACE] = hhrace_recode(hhrace)
            row_dict[CC.ATTR_HHELDERLY] = elderly_recode(p60, p65, p75)
            row_dict[self.tenure_attr_name] = self.tenure_recode(ten)
            # Recodes needed for HHTYPE generation
            row_dict[CC.ATTR_SIZE] = size_recode(final_pop)
            row_dict[CC.ATTR_CHILD_UNDER_18] = child_under_18_recode(p18)
            row_dict[CC.ATTR_OWN_CHILD_UNDER_18] = own_child_under_18_recode(paoc)
            row_dict[CC.ATTR_OWN_CHILD_UNDER_6_ONLY] = own_child_under_6_only_recode(paoc)
            row_dict[CC.ATTR_OWN_CHILD_BETWEEN_6_AND_17] = own_child_between_6_and_17_recode(paoc)
            row_dict[CC.ATTR_OWN_CHILD_IN_BOTH_RANGES] = own_child_in_both_ranges_recode(paoc)
            row_dict[CC.ATTR_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER] = child_under_18_excluding_householder_spouse_partner_recode(pac)
            row_dict[CC.ATTR_CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER] = child_under_6_only_excluding_householder_spouse_partner_recode(pac)
            row_dict[CC.ATTR_CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER] = child_between_6_and_17_excluding_householder_spouse_partner_recode(pac)
            row_dict[CC.ATTR_CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER] = child_in_both_ranges_excluding_householder_spouse_partner_recode(pac)
            row_dict[CC.ATTR_MULTIG] = multig_recode(multg, final_pop)
            row_dict[CC.ATTR_MARRIED] = married_recode(cplt)
            row_dict[CC.ATTR_MARRIED_SAME_SEX] = married_same_sex_recode(cplt)
            row_dict[CC.ATTR_MARRIED_OPPOSITE_SEX] = married_opposite_sex_recode(cplt)
            row_dict[CC.ATTR_COHABITING] = cohabiting_recode(cplt)
            row_dict[CC.ATTR_COHABITING_SAME_SEX] = cohabiting_same_sex_recode(cplt)
            row_dict[CC.ATTR_COHABITING_OPPOSITE_SEX] = cohabiting_opposite_sex_recode(cplt)
            row_dict[CC.ATTR_NO_SPOUSE_OR_PARTNER] = no_spouse_or_partner_recode(cplt)
            row_dict[CC.ATTR_WITH_RELATIVES] = with_relatives_recode(hht)
            row_dict[CC.ATTR_NOT_ALONE] = not_alone_recode(final_pop)
            row_dict[CC.ATTR_NO_OWN_CHILDREN_UNDER_18] = no_own_children_under_18(paoc)
            row_dict[CC.ATTR_NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER] = no_child_under_18_excluding_householder_spouse_partner(pac)
            row_dict[CC.ATTR_NO_RELATIVES] = no_relatives_recode(hht)
            row_dict[CC.ATTR_HHTYPE_DHCH] = hhtype_recoder(row_dict)
            return Row(**row_dict)
        except Exception as e:
            raise Exception(f"Unable to recode row: {str(row_dict)}, exception {e}")

    @staticmethod
    def tenure_recode(ten: str):
        assert ten != "0", f"The entered TENSHORT: {ten}, is out of household universe, as specified in the CEF specification's CEF20_UNIT tab"
        assert ten in ["1", "2", "3", "4"]
        if ten in ["1", "2"]:
            return 0
        return 1


class DHCH_Household_recoder_Ten_3Lev(DHCH_Household_recoder):
    """
    This is the recoder for table1 of the Decennial Census.
    It creates the hhgq_unit_dhcp, hhsex, hhage, hhhisp, tenure, and hhtype variables.
    """
    tenure_attr_name = CC.ATTR_HHTENSHORT_3LEV

    def __init__(self, *args):
        super().__init__(*args)

    @staticmethod
    def tenure_recode(ten: str):
        assert ten in ["1", "2", "3", "4"]
        if ten == "1":
            return 0
        elif ten == "2":
            return 1
        return 2


def hhsex_recode(hhsex: str):
    assert hhsex != "0", f"The entered HHSEX: {hhsex}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert hhsex in ["1", "2"], f'HHSEX is illegal value {hhsex}, should be 1 or 2, refer CEF spec CEF20_Unit Row 7'
    if hhsex == "1":
        return 0
    else:
        return 1

def hhage_recode(hhldrage: int):
    assert 15 <= hhldrage <= 115, f'HHLDRAGE is illegal value {hhldrage}, should be 15-115, refer CEF spec CEF20_UNIT'
    if 15 <= hhldrage <= 24:
        return 0
    elif hhldrage <= 34:
        return 1
    elif hhldrage <= 44:
        return 2
    elif hhldrage <= 54:
        return 3
    elif hhldrage <= 59:
        return 4
    elif hhldrage <= 64:
        return 5
    elif hhldrage <= 74:
        return 6
    elif hhldrage <= 84:
        return 7
    return 8

def hhhisp_recode(hhspan: str):
    assert hhspan != "0", f"The entered HHSPAN: {hhspan}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert hhspan in ["1", "2"], f'HHSPAN is illegal value {hhspan}, should be 0 or 1, refer CEF spec CEF20_UNIT'
    if hhspan == "1":
        return 0
    return 1

def hhrace_recode(hhrace: str):
    assert hhrace != "0", f"The entered HHRACE: {hhrace}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    hhrace_int = int(hhrace)
    assert 0 < hhrace_int <= 63, f'HHRACE is illegal value {hhrace}, should be between 0 and 63, refer CEF spec CEF20_UNIT'
    if hhrace_int == 1:
        return 0
    elif hhrace_int == 2:
        return 1
    elif hhrace_int == 3:
        return 2
    elif hhrace_int == 4:
        return 3
    elif hhrace_int == 5:
        return 4
    elif hhrace_int == 6:
        return 5
    else:
        return 6

def elderly_recode(p60: int, p65: int, p75: int):
    assert 0 <= p60 <= 99, f'P60 is illegal value {p60}, should be 0 - 99, refer CEF spec CEF20_Unit'
    assert 0 <= p65 <= 99, f'P65 is illegal value {p65}, should be 0 - 99, refer CEF spec CEF20_Unit'
    assert 0 <= p75 <= 99, f'P75 is illegal value {p75}, should be 0 - 99, refer CEF spec CEF20_Unit'
    if int(p75) > 0:
        return 3
    if int(p65) > 0:
        return 2
    if int(p60) > 0:
        return 1
    return 0


def size_recode(final_pop: str):
    assert final_pop != "0", f"The entered FINAL_POP: {final_pop}, is out of household universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert 0 < int(final_pop) <= 99999, f'FINAL_POP is illegal value {final_pop}, should be between 0 and 99999, refer CEF spec CEF20_UNIT'
    return min(int(final_pop), 7)


def child_under_18_recode(p18: int):
    assert 0 <= p18 <= 99, f'P18 is illegal value {p18}, should be between 0 and 99, refer CEF spec CEF20_Unit'
    return min(p18, 1)


def verify_paoc(paoc: str):
    assert paoc != "0", f"The entered PAOC: {paoc}, is out of household universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert paoc in ["1", "2", "3", "4"], f'PAOC is illegal value {paoc}, should be 1, 2, 3, or 4, refer CEF spec CEF20_UNIT tab'


def verify_pac(pac: str):
    assert pac != "0", f"The entered PAC: {pac}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert pac in ["1", "2", "3", "4"], f'PAC is illegal value {pac}, should be 1, 2, 3, or 4, refer CEF spec CEF20_UNIT tab'

def verify_cplt(cplt: str):
    assert cplt in ["0", "1", "2", "3", "4", "5"], f'CPLT is illegal value {cplt}, should be 1, 2, 3, 4, or 5, refer CEF spec CEF20_UNIT tab'

def verify_hht(hht: str):
    assert hht != "0", f"The entered HHT: {hht}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert hht in ["1", "2", "3", "4", "5", "6", "7"], f'HHT is illegal value {hht}, should be 1, 2, 3, 4, or 5, refer CEF spec CEF20_UNIT tab'


def own_child_under_18_recode(paoc: str):
    verify_paoc(paoc)
    if paoc in ["1", "2", "3"]:
        return 1
    return 0

def own_child_under_6_only_recode(paoc: str):
    verify_paoc(paoc)
    if paoc == "1":
        return 1
    return 0

def own_child_between_6_and_17_recode(paoc: str):
    verify_paoc(paoc)
    if paoc == "2":
        return 1
    return 0

def own_child_in_both_ranges_recode(paoc: str):
    verify_paoc(paoc)
    if paoc == "3":
        return 1
    return 0

def child_under_18_excluding_householder_spouse_partner_recode(pac: str):
    verify_pac(pac)
    if pac in ["1", "2", "3"]:
        return 1
    return 0

def child_under_6_only_excluding_householder_spouse_partner_recode(pac: str):
    verify_pac(pac)
    if pac == "1":
        return 1
    return 0

def child_between_6_and_17_excluding_householder_spouse_partner_recode(pac: str):
    verify_pac(pac)
    if pac == "2":
        return 1
    return 0

def child_in_both_ranges_excluding_householder_spouse_partner_recode(pac: str):
    verify_pac(pac)
    if pac == "3":
        return 1
    return 0

def multig_recode(multg: str, final_pop: str):
    assert final_pop != "0", f"final_pop: {final_pop}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert multg in ["0", "1", "2"], f"MULTG is illegal value {multg}, should be 0, 1, or 2, refer to CEF spec CEF20_UNIT tab"
    if multg == "2":
        return 1
    return 0

def married_recode(cplt: str):
    verify_cplt(cplt)
    if cplt in ["1", "2"]:
        return 1
    return 0

def married_same_sex_recode(cplt: str):
    verify_cplt(cplt)
    if cplt == "2":
        return 1
    return 0

def married_opposite_sex_recode(cplt: str):
    verify_cplt(cplt)
    if cplt == "1":
        return 1
    return 0

def cohabiting_recode(cplt: str):
    verify_cplt(cplt)
    if cplt in ["3", "4"]:
        return 1
    return 0

def cohabiting_same_sex_recode(cplt: str):
    verify_cplt(cplt)
    if cplt == "4":
        return 1
    return 0

def cohabiting_opposite_sex_recode(cplt: str):
    verify_cplt(cplt)
    if cplt == "3":
        return 1
    return 0

def no_spouse_or_partner_recode(cplt: str):
    verify_cplt(cplt)
    if cplt not in ["1", "2", "3", "4"]:
        return 1
    return 0

def with_relatives_recode(hht: str):
    verify_hht(hht)
    if hht in ["1", "2", "3"]:
        return 1
    return 0

def not_alone_recode(final_pop: str):
    assert final_pop != "0", f"The entered FINAL_POP: {final_pop}, is out of universe, as specified in the CEF specification's CEF20_UNIT tab"
    assert 0 < int(final_pop) <= 99999, f'FINAL_POP is illegal value {final_pop}, should be between 0 and 99999, refer CEF spec CEF20_UNIT'
    if int(final_pop) > 1:
        return 1
    return 0

def no_own_children_under_18(paoc: str):
    verify_paoc(paoc)
    if paoc == "4":
        return 1
    return 0

def no_child_under_18_excluding_householder_spouse_partner(pac: str):
    verify_pac(pac)
    if pac == "4":
        return 1
    return 0

def no_relatives_recode(hht: str):
    verify_hht(hht)
    if hht not in ["1", "2", "3"]:
        return 1
    return 0


def hhtype_recoder(row_dict: dict) -> int:
    attr = HHTypeDHCHAttr()

    return attr.get_index(row_dict)


class DHCH_FullTenure_Household_recoder(DHCH_Household_recoder):
    """
        This is the recoder for table1 of the Decennial Census.
        It creates the hhgq_unit_dhcp, hhsex, hhage, hhhisp, tenure, and hhtype variables.
        """
    tenure_attr_name = CC.ATTR_TENURE

    @staticmethod
    def tenure_recode(ten: str):
        assert ten != "0", f"The entered TEN: {ten}, is out of household universe, as specified in the CEF specification's CEF20_UNIT tab"
        assert ten in ["1", "2", "3", "4"]
        return int(ten) - 1



class DHCH_Unit_recoder:
    """
        Unit Table recoder
        Creates and adds the tenvacgq attribute to the row
    """
    def __init__(self, tenvacgq_list: List):
        self.ten = tenvacgq_list[0]
        self.vacs = tenvacgq_list[1]
        self.gqtype = tenvacgq_list[2]

    def recode(self, row: Row):
        tenvacgq = TenureVacancyGQAttr.cef2das(row[self.ten], row[self.vacs], row[self.gqtype])

        return Row(**row.asDict(), tenvacgq=tenvacgq)


class DHCH_FullTenure_Unit_recoder(DHCH_Unit_recoder):
    def recode(self, row):
        vacgq = VacancyGQAttr.cef2das(row[self.ten], row[self.vacs], row[self.gqtype])

        return Row(**row.asDict(), vacgq=vacgq)



class H1_2020_recoder:
    """
    H1 schema is "unit table run as person table" with only occupied/vacant 2-cell hist
    """
    def __init__(self, gqvac_list: List):
        self.gqtype = gqvac_list[0]
        self.vacs = gqvac_list[1]

    def recode(self, row: Row):
        row_dict: dict = row.asDict()
        try:
            qgqtyp: str = str(row[self.gqtype])
            vacs_str: str = str(row[self.vacs])

            hhgq: int  = hhgq_recode(qgqtyp)
            vacs = int(int(vacs_str) > 0)

            if hhgq > 0 and vacs > 0:
                raise ValueError("Group quarters unit set as vacant")

            row_dict[CC.ATTR_HHGQ] = hhgq
            row_dict[CC.ATTR_H1] = 1 - vacs  # the h1 attribute has 0 for vacant and 1 for occupied

            return Row(**row_dict)

        except Exception:
            raise Exception(f"Unable to recode row: {str(row)}")
