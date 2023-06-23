import programs.writer.hh2010_to_mdfunit2020 as household_recoder
import programs.writer.dhcp_hhgq_to_mdfpersons2020 as person_recoder

def getElem(schema_name):
    if schema_name == "DHCP_HHGQ":
        elem = "persons"
    else:
        elem = "data"
    return elem


def getMDFRecoders(schema_name):
    if schema_name == "DHCP_HHGQ":
        recoders = person_recoder.DHCPHHGQToMDFPersons2020Recoder().recode
    elif schema_name == "Household2010":
        recoders = household_recoder.Household2010ToMDFUnit2020Recoder().recode
    else:
        recoders = None
    return recoders
