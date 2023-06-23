"""
Implements the recodes needed to read DAS output made with MDF2020PersonPL94HistogramWriter as input for CVAP product DAS run
"""
from pyspark.sql import Row
from das_utils import int_wlog

class PL94ToCVAPRaceRecoder:
    """
    Subclasses implement the recodes needed to read DAS output made with MDF2020(Person/Household)Writer as input DHC product DAS run
    """

    def __init__(self, geocode_list, cvap_race_list, recode_variables):
        self.recode_vars = recode_variables
        self.geocode_list = geocode_list
        self.cenhisp, self.cenrace = cvap_race_list
        self.tabblkst, self.tabblkcou, self.tabtractce, self.tabblkgrpce, self.tabblk = self.geocode_list


        self.recode_funcs = {
            recode_variables[0].name: self.geocode_recode,
            recode_variables[1].name: self.cvap_race_recode,
        }

    def recode(self, row: Row) -> Row:
        row_dict = row.asDict()
        for rname, rfunc in self.recode_funcs.items():
            row_dict[rname] = rfunc(row_dict)
        # for key in row_dict:
        #     if key not in self.
        #         row_dict.pop(k)
        return Row(**row_dict)

    def geocode_recode(self, row: dict):
        return row[self.tabblkst] + row[self.tabblkcou] + row[self.tabtractce] + row[self.tabblkgrpce] + row[self.tabblk]

    def cvap_race_recode(self, row: dict):
        _NOT_HISPANIC = "1"
        _HISPANIC = "2"
        allowed = [_HISPANIC, _NOT_HISPANIC]
        cenhisp = row[self.cenhisp]
        if cenhisp not in allowed:
            raise ValueError(f"CENHISP not in {allowed}")
        if cenhisp == _HISPANIC:
            return 0

        cenrace = int_wlog(row[self.cenrace], "CENRACE")
        if not 1 <= cenrace <= 63:
            raise ValueError(f"CENRACE not in 1-63")

        # 1-6 in MDF/CEF are the categories for single race; 7-9 are the three combinations with white in the same order as in CVAPRace attribute
        if cenrace in range(1, 10):
            #  We normally subtract 1 since histograms start with 0, but here we'd add it back in, to account for Hispanic level 0 in CVAPRace attribute
            return cenrace
        # Skip 10-11, which are white+nhopi and white+SOR, not in CVAPRace separately
        # 12 is Black and AIAN
        if cenrace == 12:
            return 10

        # The rest is remainder of 2+ races
        return 11
