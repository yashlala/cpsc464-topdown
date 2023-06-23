from programs.schema.attributes.sex import SexAttr
from das_constants import CC


class Sex1940Attr(SexAttr):
    @staticmethod
    def getName():
        return CC.ATTR_SEX_1940
