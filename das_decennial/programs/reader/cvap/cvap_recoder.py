""" Recode CVAP_RACE variable from CVAP file. Amounts to subtracting 1"""
from das_utils import int_wlog
from pyspark.sql import Row

class CVAPRace:

    def __init__(self, cvap_race_list):
        self.cvap_race = cvap_race_list[0]

    def recode(self, row):
        """
        Coding in the CVAP file is as follows

            1 = non-Hispanic AIAN
            2 = non-Hispanic Asian
            3 = non-Hispanic Black
            4 = non-Hispanic NHOPI
            5 = non-Hispanic White
            6 = non-Hispanic Some Other Race
            7 = non-Hispanic AIAN & White
            8 = non-Hispanic Asian & White
            9 = non-Hispanic Black & White
            10 = non-Hispanic AIAN & Black
            11 = non-Hispanic Remainder of Two or More Races
            12 = Hispanic or Latino

        Convert them to the ones in the DAS schema attribute programs.schema.attributes.race_cvap.CVAPRace:

             "Hispanic or Latino": [0],
             "White alone": [1],
             "Black or African American alone": [2],
             "American Indian and Alaska Native alone": [3],
             "Asian alone": [4],
             "Native Hawaiian and Other Pacific Islander alone": [5],
             "Some Other Race alone": [6],
             "Black or African American and White": [7],
             "American Indian and Alaska Native and White": [8],
             "Asian and White": [9],
             "American Indian and Alaska Native and Black or African American": [10],
             "Remainder of Two or More Race Responses": [11],

        """
        cvap_race = int_wlog(row[self.cvap_race], "cvap_race")
        if not 1 <= cvap_race <= 12:
            raise ValueError("CVAP_RACE is not in 1-12")

        cvap_file_2_das = {
            1: 3,    # AIAN
            2: 4,    # Asian
            3: 2,    # Black
            4: 5,    # NHOPI
            5: 1,    # White
            6: 6,    # SOR
            7: 8,    # AIAN + White
            8: 9,    # Asian + White
            9: 7,    # Black + White
            10: 10,  # AIAN + Black
            11: 11,  # Remainder of Two or More Races
            12: 0,   # Hispanic
        }

        race_cvap_das = cvap_file_2_das[cvap_race]
        return Row(**row.asDict(), race_cvap_das=race_cvap_das)