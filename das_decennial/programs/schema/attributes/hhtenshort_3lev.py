from programs.schema.attributes.abstractattribute import AbstractAttribute
from das_constants import CC

class HHTenShort3LevAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHTENSHORT_3LEV

    @staticmethod
    def getLevels():
        return {
            "Owned with mortgage"   : [0],
            "Owned without mortgage"  : [1],
            "Rented"  : [2]
        }

    @staticmethod
    def recodeTenure3Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.TEN_3LEV
        groups = {
            "Owned with mortgage"   : [0],
            "Owned without mortgage"  : [1],
            "Not owned"  : [2]
        }
        return name, groups

    @staticmethod
    def recodeTenure2Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = "hhtenshort_2lev"
        groups = {
            "Owned": [0, 1],
            "Not owned": [2]
        }
        return name, groups
