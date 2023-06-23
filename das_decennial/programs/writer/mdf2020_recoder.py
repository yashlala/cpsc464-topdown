import logging
from typing import Dict, Union
from programs.schema.schemas.schemamaker import SchemaMaker
from das_constants import CC

from pyspark.sql import  Row


class MDF2020Recoder:

    schema_name = None

    def __init__(self, geocode_dict: Dict = CC.DEFAULT_GEOCODE_DICT):
        self.schema = SchemaMaker.fromName(self.schema_name)

        self.mangled_dimnames = list(map(lambda name: name + f"_{self.schema.name}", self.schema.dimnames))

        self.geocode = CC.GEOCODE

        self.geocode_dict = geocode_dict

    def niuFiller(self,  length: int, type: str = 'str'):
        if type == 'int':
            return 0
        elif type == 'str':
            return '0' * length
        else:
            raise ValueError("Only filling int or str")

    def notSupprortedFiller(self,  length: int, type: str = 'str'):
        if type == 'int':
            return int(10 ** length - 1)
        elif type == 'str':
            return '9' * length
        else:
            raise ValueError("Only filling int or str")

    def getMangledName(self, name: str):
        return self.mangled_dimnames[self.schema.dimnames.index(name)]

    def format_geocode(self, geocode, index_1: Union[int, str], index_2: Union[int, str]):
        try:
            if type(index_1) is str:
                index_1 = self.geocode_dict[index_1]
            if type(index_2) is str:
                index_2 = self.geocode_dict[index_2]
        except KeyError as e:
            msg = f'Attempted to find index_1:{index_1} or index_2:{index_2} in self.geocode_dict: {self.geocode_dict}'
            logging.error(msg)
            raise KeyError(msg)

        truncated_geocode = geocode[index_1:index_2]

        width = index_2 - index_1

        return str(truncated_geocode) + ('X' * (width - len(str(truncated_geocode))))

    @staticmethod
    def schema_build_id_recoder(row: Row):
        """
        adds the SCHEMA_BUILD_ID column to the row
        CHAR(5)
        row: dict
        """
        row['SCHEMA_BUILD_ID'] = "1.1.0"
        return row

    def das_spine_geocode_format_recoder(self, row: Row):
        # Only perform this recode when topdown engine was stopped before the lowest geolevel:
        if len(row[self.geocode]) < max(self.geocode_dict.values()):
            geocode_dict_reversed = {v:k for k,v in self.geocode_dict.items()}
            lens = sorted(list(self.geocode_dict.values()))
            if 0 not in lens:
                geocode_dict_reversed[0] = "US"
                lens = [0] + lens
            for geoid_length, geoid_length_lower in zip(lens[:-1], lens[1:]):
                row[geocode_dict_reversed[geoid_length_lower]] = self.format_geocode(row[self.geocode], geoid_length, geoid_length_lower)
        return row

    def tabblkst_recoder(self, row: Row):
        """
        adds the TABBLKST column to the row
        refers to the 2020 Tabulation State (FIPS)
        CHAR(2)
        row: dict
        """
        if len(row[self.geocode]) == max(self.geocode_dict.values()):
            row['TABBLKST'] = self.format_geocode(row[self.geocode], 0, CC.GEOLEVEL_STATE)
        return row

    def tabblkcou_recoder(self, row: Row):
        """
        adds the TABBLKCOU column to the row
        refers to the 2020 Tabulation County (FIPS)
        CHAR(3)
        row: dict
        """
        if len(row[self.geocode]) == max(self.geocode_dict.values()):
            row['TABBLKCOU'] = self.format_geocode(row[self.geocode], CC.GEOLEVEL_STATE, CC.GEOLEVEL_COUNTY)
        return row

    def tabtractce_recoder(self, row: Row):
        """
        adds the TABTRACTCE column to the row
        refers to the 2020 Tabulation Census Tract
        CHAR(6)
        row: dict
        """
        if len(row[self.geocode]) == max(self.geocode_dict.values()):
            row['TABTRACTCE'] = self.format_geocode(row[self.geocode], CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_TRACT)
        return row

    def tabblkgrpce_recoder(self, row: Row):
        """
        adds the TABBLKGRPCE column to the row
        refers to the 2020 Census Block Group
        CHAR(1)
        row: dict
        """
        if len(row[self.geocode]) == max(self.geocode_dict.values()):
            row['TABBLKGRPCE'] = self.format_geocode(row[self.geocode], CC.GEOLEVEL_TRACT, CC.GEOLEVEL_BLOCK_GROUP)
        return row

    def tabblk_recoder(self, row: Row):
        """
        adds the TABBLK column to the row
        refers to the 2020 Block Number
        CHAR(4)
        row: dict
        """
        if len(row[self.geocode]) == max(self.geocode_dict.values()):
            row['TABBLK'] = self.format_geocode(row[self.geocode], 'Block_Group', CC.GEOLEVEL_BLOCK)
        return row
