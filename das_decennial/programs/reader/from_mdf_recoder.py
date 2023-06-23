"""
Implements the recodes needed to read DAS output made with MDF2020(Person/Household)Writer as input for DHC product DAS run
"""
from programs.schema.attributes.hhgq_unit_demoproduct import HHGQUnitDemoProductAttr
from pyspark.sql import Row

from das_utils import int_wlog
from exceptions import DASValueError

class DHCRecoder:
    """
    Subclasses implement the recodes needed to read DAS output made with MDF2020(Person/Household)Writer as input DHC product DAS run
    """

    def __init__(self, geocode_list, recode_variables):
        self.recode_vars = recode_variables
        self.geocode_list = geocode_list
        self.tabblkst, self.tabblkcou, self.tabtractce, self.tabblkgrpce, self.tabblk = self.geocode_list

        self.recode_funcs = {recode_variables[0].name: self.geocode_recode}

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

    @staticmethod
    def subtract_one(row, name):
        return int_wlog(row[name], name) - 1


class DHCPRecoder(DHCRecoder):
    """
    Implements the recodes needed to read DAS output made with MDF2020(Person/Household)Writer as input DHC product DAS run
    """
    def __init__(self, geocode_list, hhgq_list, sex_list, hispanic_list, cenrace_das_list, citizen_das_list, recode_variables):

        super().__init__(geocode_list, recode_variables)

        self.gqtype = hhgq_list[0]
        self.qsex = sex_list[0]
        self.cenhisp = hispanic_list[0]
        self.cenrace = cenrace_das_list[0]
        self.citizen = citizen_das_list[0]

        self.recode_funcs = {
            rvar.name: rlist for rvar, rlist in
                        zip(
                            recode_variables,
                            (
                                self.geocode_recode,
                                self.hhgq_recode,
                                lambda row: self.subtract_one(row, self.qsex),
                                lambda row: self.subtract_one(row, self.cenhisp),
                                lambda row: self.subtract_one(row, self.cenrace),
                                self.citizen_das_recode,
                            )
                        )
        }

    def citizen_das_recode(self, row: dict):
        return 2 - int_wlog(row[self.citizen], self.citizen)

    def hhgq_recode(self, row: dict):
        gq = int_wlog(row[self.gqtype], self.gqtype)
        return gq // 100
        # Our own MDFWriter does not produce more than 700
        # if gq < 700:
        #     return gq // 100
        # if gq >= 700:
        #     return 7

    def recode_sql(self):
        geocode = f"CONCAT({','.join(self.geocode_list)}) AS {self.recode_vars[0]}"
        sex = f"CAST({self.qsex} AS INT) - 1 AS {self.recode_vars[2]}"
        hispanic = f"CAST({self.cenhisp} AS INT) - 1 AS {self.recode_vars[3]}"
        cenrace_das = f"CAST({self.cenrace} AS INT) - 1 AS {self.recode_vars[4]}"
        citizen_das = f"2 - CAST({self.citizen} AS INT) AS {self.recode_vars[5]}"
        # Our own MDFWriter does not produce more than 700
        # hhgq = f"IF(CAST(SUBSTRING({self.gqtype},0,2) AS INT)>7,7,CAST(SUBSTRING({self.gqtype},0,2) AS INT)) AS {self.recode_vars[1]}"
        hhgq = f"CAST(SUBSTRING({self.gqtype},0,2) AS INT) AS {self.recode_vars[1]}"
        query = f"SELECT *,{geocode},{hhgq},{sex},{hispanic},{cenrace_das},{citizen_das}"
        return query


class DHCPUnitRecoder(DHCPRecoder):

    def __init__(self, geocode_list, hhgq_list, recode_variables):

        DHCRecoder.__init__(self, geocode_list, recode_variables)

        self.gqtype = hhgq_list[0]

        self.recode_funcs = {rvar.name: rlist for rvar, rlist in
                             zip(recode_variables, (self.geocode_recode, self.hhgq_recode))
                             }


class DHCHHouseholdRecoder(DHCRecoder):

    def __init__(self, geocode_list, sex_list, hhage_list, hisp_list, race_list, hhtype_list, elderly_list, multi_list, recode_variables):
        super().__init__(geocode_list, recode_variables)

        self.recode_funcs = {}
        for rvar, rlist in zip(recode_variables,
                               (
                                       self.geocode_recode,
                                       self.hhsex_recode,
                                       lambda row: self.subtract_one(row, self.hhldrage),
                                       lambda row: self.subtract_one(row, self.cenhisp),
                                       lambda row: self.subtract_one(row, self.race),
                                       self.hhtype_recode,
                                       self.elderly_recode,
                                       self.multi_recode,
                               )
                              ):
            self.recode_funcs[rvar.name] = rlist

        self.hhldrage = hhage_list[0]
        self.cenhisp = hisp_list[0]
        self.race = race_list[0]
        self.multi = multi_list[0]

        self.p60, self.p65, self.p75 = elderly_list

        self.hht, self.hht2, self.cplt, self.upart, self.paoc = hhtype_list
        self.hht1, self.hht21, self.upart1 = sex_list

        assertmsg = "HHSEX and HHTYPE variables needed for recodes list inconsistent, check order of hht,hht2,cplt,upart"
        assert self.hht==self.hht1, assertmsg
        assert self.hht2== self.hht21, assertmsg
        assert self.upart == self.upart1, assertmsg


    def hhsex_recode(self, row: dict):
        from numpy import random
        _MALE = 0
        _FEMALE = 1
        if row[self.upart] in ["3", "4"]:
            return _FEMALE
        if row[self.upart] in ["1", "2"]:
            return _MALE
        if row[self.hht2] in ["05", "06", "07", "08"]:
            return _FEMALE
        if row[self.hht2] in ["09", "10", "11", "12"]:
            return _MALE
        if row[self.hht] in ["3", "6", "7"]:
            return _FEMALE
        if row[self.hht] in ["2", "4", "5"]:
            return _MALE
        raise RuntimeError("You are using wrong recoder. Use DHCHHouseholdRecoderHHSEX, and HHSEX has to be saved in MDF beforehand")
        # return int(random.rand() > .5)  # TODO: THIS IS TEMPORARY FOR TESTING



    def hhtype_recode(self, row: dict):
        hht_to_hhtype = {"1": {0, 1, 2, 3, 4, 5, 6, 7},
                         "2": {8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22},
                         "3": {8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22},
                         "4": {18},
                         "5": {12, 17, 23},
                         "6": {18},
                         "7": {12, 17, 23}}
        hht2_to_hhtype = {"01": {0, 1, 2, 4, 5, 6},
                          "02": {3, 7},
                          "03": {8, 9, 10, 13, 14, 15},
                          "04": {11, 12, 16, 17},
                          "05": {18},
                          "06": {19, 20, 21},
                          "07": {22},
                          "08": {23},
                          "09": {18},
                          "10": {19, 20, 21},
                          "11": {22},
                          "12": {23},
                          }
        cplt_to_hhtype = {"1": {0, 1, 2, 3},
                          "2": {4, 5, 6, 7},
                          "3": {8, 9, 10, 11, 12},
                          "4": {13, 14, 15, 16, 17},
                          "0": {18, 19, 20, 21, 22, 23},
                          }
        upart_to_hhtype = {"1": {13, 14, 15, 16, 17},
                           "2": {8, 9, 10, 11, 12},
                           "3": {13, 14, 15, 16, 17},
                           "4": {8, 9, 10, 11, 12},
                           "5": {0, 1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22, 23}
                           }
        paoc_to_hhtype = {"1": {0, 4, 8, 13, 19},
                          "2": {1, 5, 9, 14, 20},
                          "3": {2, 6, 10, 15, 21},
                          "4": {3, 7, 11, 12, 16, 17, 18, 22, 23},
                        }

        # try:
        #     hht_to_hhtypeset = hht_to_hhtype[row[self.hht]]
        # except KeyError:
        #     raise DASValueError("HHT value is not transferable to HHTYPE", row[self.hht])
        #
        # try:
        #     hht2_to_hhtypeset = hht2_to_hhtype[row[self.hht2]]
        # except KeyError:
        #     raise DASValueError("HHT2 value is not transferable to HHTYPE", row[self.hht2])
        #
        # try:
        #     cplt_to_hhtypeset = cplt_to_hhtype[row[self.cplt]]
        # except KeyError:
        #     raise DASValueError("CPLT value is not transferable to HHTYPE", row[self.cplt])
        #
        # try:
        #     upart_to_hhtypeset = upart_to_hhtype[row[self.upart]]
        # except KeyError:
        #     raise DASValueError("UPART value is not transferable to HHTYPE", row[self.upart])
        #
        # try:
        #     paoc_to_hhtypeset = paoc_to_hhtype[row[self.paoc]]
        # except KeyError:
        #     raise DASValueError("PAOC value is not transferable to HHTYPE", row[self.paoc])
        try:
            hht_to_hhtypeset = hht_to_hhtype[row[self.hht]]

            hht2_to_hhtypeset = hht2_to_hhtype[row[self.hht2]]

            cplt_to_hhtypeset = cplt_to_hhtype[row[self.cplt]]

            upart_to_hhtypeset = upart_to_hhtype[row[self.upart]]

            paoc_to_hhtypeset = paoc_to_hhtype[row[self.paoc]]
        except KeyError:
            return None

        hhtypeset = hht_to_hhtypeset\
            .intersection(hht2_to_hhtypeset)\
            .intersection(cplt_to_hhtypeset)\
            .intersection(upart_to_hhtypeset)\
            .intersection(paoc_to_hhtypeset)

        #assert len(hht_to_hhtype)==0, "HHTYPE recode set > 1!"
        if len(list(hht_to_hhtype))==0:
            raise ValueError
        return list(hhtypeset)[0]

    def elderly_recode(self, row: dict):
        if int_wlog(row[self.p75], self.p75) == 1:
            return 3
        elif int_wlog(row[self.p65], self.p65) == 1:
            return 2
        elif int_wlog(row[self.p60], self.p60) == 1:
            return 1
        else:
            return 0
        # if  row[self.p75] == "1":
        #     return 3
        # elif row[self.p65] == "1":
        #     return 2
        # elif row[self.p60] == "1":
        #     return 1
        # else:
        #     return 0

    def multi_recode(self, row: dict):
        multi = int_wlog(row[self.multi], self.multi)
        if multi in [0, 1]:
            return 0
        elif multi == 2:
            return 1
        else:
            raise DASValueError(f"MULTG variable values {multi} is out of specification", multi)

class DHCHHouseholdRecoderHHSEX(DHCHHouseholdRecoder):

    def __init__(self, geocode_list, sex_list, hhage_list, hisp_list, race_list, hhtype_list, elderly_list, multi_list, recode_variables):
        DHCRecoder.__init__(self, geocode_list, recode_variables)

        self.recode_funcs = {}
        for rvar, rlist in zip(recode_variables,
                               (
                                       self.geocode_recode,
                                       lambda row: self.subtract_one(row, self.sex),
                                       lambda row: self.subtract_one(row, self.hhldrage),
                                       lambda row: self.subtract_one(row, self.cenhisp),
                                       lambda row: self.subtract_one(row, self.race),
                                       self.hhtype_recode,
                                       self.elderly_recode,
                                       self.multi_recode,
                               )
                              ):
            self.recode_funcs[rvar.name] = rlist

        self.hhldrage = hhage_list[0]
        self.cenhisp = hisp_list[0]
        self.race = race_list[0]
        self.multi = multi_list[0]

        self.p60, self.p65, self.p75 = elderly_list

        self.hht, self.hht2, self.cplt, self.upart, self.paoc = hhtype_list
        self.sex = sex_list[0]


class DHCHUnitRecoder(DHCRecoder):

    def __init__(self, geocode_list, hhgq_list, recode_variables):
        super().__init__(geocode_list, recode_variables)

        self.recode_funcs = {rvar.name: rlist for rvar, rlist in
                        zip(recode_variables, (self.geocode_recode, self.hhgq_recode))
                        }

        self.gqtype = hhgq_list[0]
        self.vacs = hhgq_list[1]

    def hhgq_recode(self, row: dict):
        gqtype = row[self.gqtype]
        vacs = row[self.vacs]
        if gqtype == "000":
            if int(vacs) == 0:
                return 0
            return 1
        return HHGQUnitDemoProductAttr.cef2das(gqtype)


class PL94Recoder(DHCRecoder):
    """ To read PL94 MDF. Mainly to use for constraints for DHCP runs"""
    def __init__(self, geocode_list, voting_age_list, hispanic_list, cenrace_das_list, recode_variables):

        super().__init__(geocode_list, recode_variables)

        self.votingage = voting_age_list[0]
        self.cenhisp = hispanic_list[0]
        self.cenrace = cenrace_das_list[0]
        self.recode_funcs = {
            rvar.name: rlist for rvar, rlist in
                        zip(
                            recode_variables,
                            (
                                self.geocode_recode,
                                lambda row: self.subtract_one(row, self.votingage),
                                lambda row: self.subtract_one(row, self.cenhisp),
                                lambda row: self.subtract_one(row, self.cenrace),
                            )
                        )
        }