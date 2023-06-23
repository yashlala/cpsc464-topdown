# e2e_recoder.py
# Some Human
# Last Modified: 2/16/18
# Documentation strengthened by Some Human on 5/22/18.

"""
    This module is a variable recoder library for the different test data sets used
    in the 2018 DAS development process. Each class is specific to a table.

    The recoder class must contain a method called "recode".
    The recode method operates on a single SparkDataFrame row
    and returns the row with the recoded variable(s) added.

    A recoder class is specified in the main config file.
    The config file reader section must contain the following options:
        table_name.recoder: recoder class to be used with table_name
        table_name.recode_variables: space-delimited list of new variable names
                               e.g.: votingage cenrace
        for each new variable the config file contains:
        var_name: space-delimited list of original table variable names needed to create var_name
            e.g.: votingage: age
            e.g.: cenrace: white black sor
    Note: recode variable names can't clash with original variable names.

    The recoder class __init__ method must have one arg for each new variable.
    An arg is a list of original table variable names needed to create one recode variable.
    The args should be ordered according to the ordering of table_name.recode_variables.

    Sample class using the above recode variable examples.
    class sample_recoder:
        def __init__(self, arg1_list, arg2_list):
            # arg1_list = ["age"]
            # arg2_list = ["white", "black", "sor"]
            self.arg1 = arg1
            self.arg2 = arg2
        def recode(self, row):
            do_some_stuff()
            return new_row

"""

from itertools import product
from pyspark.sql import Row
from typing import List


class DHCP2020_recoder:
    """
        This is the recoder for table13
        It shifts the cenrace values down by 1.
    """
    def __init__(self, race_list: List):
        self.race = race_list[0] # list contains one variable "race"

    def recode(self, row: Row):
        return Row(**row.asDict(), cenrace=int(row[self.race])-1)


class DHCP_HHGQ_recoder:
    """
        This is the recoder for table1 of the Decennial Census.
        It creates cenrace, voting age, and gqtype.
    """
    def __init__(self, race_list: List, rel_varname_list: List):
        self.races = race_list
        self.rel = rel_varname_list[0] # list contains one variable "relation"

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = cenrace_recode(row, self.races)
        row = hhgq_detail_recode(row, self.rel)
        return row

class DHCP_HHGQ_PCT12_recoder:
    """
        This is the recoder for table1 of the Decennial Census.
        It creates cenrace, age wiht 85+ in 5 yr bins, and gqtype.
    """
    def __init__(self, race_list: List, rel_varname_list: List, age_list: List):
        self.races = race_list
        self.rel = rel_varname_list[0] # list contains one variable "relation"
        self.age = age_list[0]

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = cenrace_recode(row, self.races)
        row = hhgq_detail_recode(row, self.rel)
        row = age_pct12_recode(row, self.age)
        return row

class person_recoder:
    """
        This is the recoder for table1 of the Decennial Census.
        It creates cenrace, voting age, and gqtype.
    """
    def __init__(self, race_list: List, age_varname_list: List, rel_varname_list: List):
        self.races = race_list
        self.age = age_varname_list[0] # list contains one variable "age"
        self.rel = rel_varname_list[0] # list contains one variable "relation"

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = cenrace_recode(row, self.races)
        row = votingage_recode(row, self.age)
        row = relation_recode(row, self.rel)
        return row

class table8_recoder:
    """
        This is the recoder for table8 of the Decennial Census.
        It creates cenrace.
    """
    def __init__(self, race_list: List):
        self.race = race_list

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        return cenrace_recode(row, self.race)


class table9a_recoder:
    """
        This is the recoder for table8 of the Decennial Census.
        It creates cenrace.
    """
    def __init__(self, race_list: List, age: List):
        self.race = race_list
        self.age = age[0]

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = cenrace_recode(row, self.race)
        row = age_cat_recode(row, self.age)
        return row

class table9a_CVAP_recoder:
    """
    This is the recoder for table9a of the Decennial Census.
    It creates cenrace and votingage.
    """
    def __init__(self, race_list: List, age: List):
        self.race = race_list
        self.age = age[0]

    def recode(self, row: Row):
        """
        Input:
            row: original dataframe Row

        Output:
            dataframe Row with recode variables added
        """
        row = cenrace_recode(row, self.race)
        row = votingage_recode2(row, self.age)
        return row

class votingage_recoder:
    """
        This is the recoder for the data from the 1920 Decennial Census.
        It performs the voting age recode and shifts race to start at 0.
    """
    def __init__(self, age_varnamelist: List, race_varnamelist: List, gqtype_varnamelist: List):
        self.age = age_varnamelist[0] # list only contains one variable "AGE"
        self.race = race_varnamelist[0] # list only contains one variable. Not a cenrace recode.
        self.gqtype= gqtype_varnamelist[0]

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = Row(**row.asDict(), RACE0=int(row[self.race])-1)
        row = gqtype1940_recode(row, self.gqtype)
        return votingage_recode(row, self.age)

class gqunit_recoder:
    """
        This is the recoder for the data from the 1920 Decennial Census.
        It performs the voting age recode and shifts race to start at 0.
    """
    def __init__(self, gqtype_varnamelist: List):
        self.gqtype= gqtype_varnamelist[0]

    def recode(self, row: Row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        return gqtype1940_recode(row, self.gqtype)


def cenrace_recode(row: Row, races):
    """
        This function recodes races to cenrace.

        Inputs:
            row: a SQL Row
            races: a list of names of race variables

        Output:
            a new row with cenrace added
    """
    new_row = row.asDict()
    race_map = ["".join(x) for x in product("10", repeat=len(races))]
    race_map.sort(key=lambda s: s.count("1"))
    cenrace = "".join([str(row[race]) for race in races])
    new_row["cenrace"] = race_map.index(cenrace)-1
    return Row(**new_row)


def age_cat_recode(row: Row, age):
    """
    table p12 recode
    under 5 years, 5-9,10-14,15-17,18-19,20-21,22-24,25-29,30-34,35-39,40-44,45-49,50-54,55-59,60-61
    62-64,65,66,67-69,70-74,75-79,80-84,85+ (23 cats)
    116 values 0-115
    """
    cutoffs = [0,5,10,15,18,20,22,25,30,35,40,45,50,55,60,62,65,66,67,70,75,80,85,116]
    dict = {}
    for i in range(len(cutoffs)-1):
        for j in range(cutoffs[i],cutoffs[i+1],1):
            dict[j] = i

    return Row(**row.asDict(), age_cat=int(dict[int(row[age])]))


def age_pct12_recode(row: Row, age):
    """
    table pct12 recodes (new) 0,1,2,...,84, 85-89, 90-94, 95-99, 100-104, 105-109, 110-114, 115+ (92 cats)
    116 values 0-115
    """
    if int(row[age]) >= 85:
        age = 68 + int(int(row[age])/5)
    else:
        age = int(row[age])

    return Row(**row.asDict(), pct12_age=age)


def votingage_recode2(row: Row, age_var):
    return Row(**row.asDict(), votingage=int(int(row[age_var]) >= 18))


def votingage_recode(row: Row, age_var):
    """
        This function recodes age to voting age indicator.

        Inputs:
            row: a SQL Row
            age_var: the name of age variable

        Output:
            a new row with voting age added
    """
    return Row(**row.asDict(), VA=int(int(row[age_var]) >= 18))


def gqtype1940_recode(row: Row, gqtype_var):
    """
        This function recodes age to voting age indicator.

        Inputs:
            row: a SQL Row
            age_var: the name of age variable

        Output:
            a new row with voting age added
    """
    dict= {0:0,2:1,3:2,4:3,6:4,7:5,8:6,9:7}

    return Row(**row.asDict(), GQTYPE2=int(dict[int(row[gqtype_var])]))


def relation_recode(row: Row, rel_var):
    """
        This function recodes the relation to 3 levels for development/testing.

        Inputs:
            row: a SQL Row
            rel_var: the name of the relationship variable

        Output:
            a new row with gqtype added
    """
    gq_type = 2 if int(row[rel_var]) == 16 else 1 if int(row[rel_var]) == 15 else 0
    return Row(**row.asDict(), gqtype=gq_type)


def hhgq_detail_recode(row: Row, rel_var):
    """
    """
    if 0 <= int(row[rel_var]) <= 14: hhgq = 0
    if 15 <= int(row[rel_var]) <= 20: hhgq = 1
    if 21 <= int(row[rel_var]) <= 23: hhgq = 2
    if int(row[rel_var]) == 24: hhgq = 3
    if 25 <= int(row[rel_var]) <= 29: hhgq = 4
    if int(row[rel_var]) == 30: hhgq = 5
    if 31 <= int(row[rel_var]) <= 32: hhgq = 6
    if 33 <= int(row[rel_var]): hhgq = 7
    return Row(**row.asDict(), hhgq=hhgq)

class unit_recoder:
    """
        This is the recoder for the table1 household table for the Decennial Census.
        It creates gqtype.
    """
    def __init__(self, rel_varname: List):
        self.rel = rel_varname[0]

    def recode(self, row: Row):
        """
            Input:
                row: an original dataframe Row

            Output:
                a dataframe Row with recode variables added
        """
        row = relation_recode(row, self.rel)
        return row
