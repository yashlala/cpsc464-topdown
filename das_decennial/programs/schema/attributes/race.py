from programs.schema.attributes.hhrace import HHRaceAttr
from das_constants import CC


class RaceAttr(HHRaceAttr):

    @staticmethod
    def getName():
        return CC.ATTR_RACE
