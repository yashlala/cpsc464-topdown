import pytest

from programs.validator.constraints_validator import *


@pytest.mark.parametrize(
        "voting_age, gqtype_pl, output",
        [
            (1, 0, True),
            (1, 3, False),
            (2, 0, True),
            (2, 3, True)
        ])
def test_pl94_validator(voting_age, gqtype_pl, output):
    validator = MDFConstrPL94ValPer()
    validator.VOTING_AGE = voting_age
    validator.GQTYPE_PL = gqtype_pl
    assert validator.func_dict['nurse_nva_0']() == output


@pytest.mark.parametrize(
        "rel_test, qage, relship, live_alone, gqtype, output",
        [
            # True means passing/constraint is met
            ("relgq0_lt15", 1, 20, 1, None, False),
            ("relgq0_lt15", 1, 20, 2, None, True),
            ("relgq0_lt15", 15, 20, 1, None, True),
            ("relgq0_lt15", 15, 20, 2, None, True),
            ("relgq0_lt15", 50, 20, 1, None, True),

            ("relgq1_lt15", 1, 20, 1, None, True),
            ("relgq1_lt15", 1, 20, 2, None, False),
            ("relgq1_lt15", 15, 20, 1, None, True),
            ("relgq1_lt15", 15, 20, 2, None, True),
            ("relgq1_lt15", 50, 20, 1, None, True),

            ("relgq2_lt15", 1, 21, None, None, False),
            ("relgq2_lt15", 15, 21, None, None, True),
            ("relgq2_lt15", 50, 21, None, None, True),

            ("relgq3_lt15", 1, 22, None, None, False),
            ("relgq3_lt15", 15, 22, None, None, True),
            ("relgq3_lt15", 50, 22, None, None, True),


            ("relgq4_lt15", 1, 23, None, None, False),
            ("relgq4_lt15", 15, 23, None, None, True),
            ("relgq4_lt15", 50, 23, None, None, True),

            ("relgq5_lt15", 1, 24, None, None, False),
            ("relgq5_lt15", 15, 24, None, None, True),
            ("relgq5_lt15", 50, 24, None, None, True),

            ("relgq6_gt89", 1, 25, None, None, True),
            ("relgq6_gt89", 89, 25, None, None, True),
            ("relgq6_gt89", 90, 25, None, None, False),

            ("relgq7_gt89", 1, 26, None, None, True),
            ("relgq7_gt89", 89, 26, None, None, True),
            ("relgq7_gt89", 90, 26, None, None, False),


            ("relgq8_gt89", 1, 27, None, None, True),
            ("relgq8_gt89", 89, 27, None, None, True),
            ("relgq8_gt89", 90, 27, None, None, False),


            ("relgq10_lt30", 29, 29, None, None, False),
            ("relgq10_lt30", 30, 29, None, None, True),
            ("relgq10_lt30", 31, 29, None, None, True),

            ("relgq11_gt74", 25, 30, None, None, True),
            ("relgq11_gt74", 74, 30, None, None, True),
            ("relgq11_gt74", 75, 30, None, None, False),

            ("relgq12_lt30", 29, 31, None, None, False),
            ("relgq12_lt30", 30, 31, None, None, True),
            ("relgq12_lt30", 31, 31, None, None, True),

            ("relgq13_lt15_gt89", 14, 32, None, None, False),
            ("relgq13_lt15_gt89", 15, 32, None, None, True),
            ("relgq13_lt15_gt89", 16, 32, None, None, True),
            ("relgq13_lt15_gt89", 88, 32, None, None, True),
            ("relgq13_lt15_gt89", 89, 32, None, None, True),
            ("relgq13_lt15_gt89", 90, 32, None, None, False),

            # No checks for 14, 15

            ("relgq16_gt20", 19, 35, None, None, True),
            ("relgq16_gt20", 20, 35, None, None, True),
            ("relgq16_gt20", 21, 35, None, None, False),

            # No checks for 17

            ("relgq18_lt15", 14, None, None, 101, False),
            ("relgq18_lt15", 15, None, None, 101, True),
            ("relgq18_lt15", 16, None, None, 101, True),

            ("relgq19_lt15", 14, None, None, 102, False),
            ("relgq19_lt15", 15, None, None, 102, True),
            ("relgq19_lt15", 16, None, None, 102, True),

            ("relgq20_lt15", 14, None, None, 103, False),
            ("relgq20_lt15", 15, None, None, 103, True),
            ("relgq20_lt15", 16, None, None, 103, True),

            ("relgq21_lt15", 14, None, None, 104, False),
            ("relgq21_lt15", 15, None, None, 104, True),
            ("relgq21_lt15", 16, None, None, 104, True),

            ("relgq22_lt15", 14, None, None, 105, False),
            ("relgq22_lt15", 15, None, None, 105, True),
            ("relgq22_lt15", 16, None, None, 105, True),

            ("relgq23_lt17_gt65", 16, None, None, 106, False),
            ("relgq23_lt17_gt65", 17, None, None, 106, True),
            ("relgq23_lt17_gt65", 18, None, None, 106, True),
            ("relgq23_lt17_gt65", 64, None, None, 106, True),
            ("relgq23_lt17_gt65", 65, None, None, 106, True),
            ("relgq23_lt17_gt65", 66, None, None, 106, False),

            ("relgq24_gt25", 24, None, None, 201, True),
            ("relgq24_gt25", 25, None, None, 201, True),
            ("relgq24_gt25", 26, None, None, 201, False),

            ("relgq25_gt25", 24, None, None, 202, True),
            ("relgq25_gt25", 25, None, None, 202, True),
            ("relgq25_gt25", 26, None, None, 202, False),

            ("relgq26_gt25", 24, None, None, 203, True),
            ("relgq26_gt25", 25, None, None, 203, True),
            ("relgq26_gt25", 26, None, None, 203, False),

            ("relgq27_lt20", 19, None, None, 301, False),
            ("relgq27_lt20", 20, None, None, 301, True),
            ("relgq27_lt20", 21, None, None, 301, True),

            # No checks for 28, 29, 30

            ("relgq31_lt17_gt65", 16, None, None, 404, False),
            ("relgq31_lt17_gt65", 17, None, None, 404, True),
            ("relgq31_lt17_gt65", 18, None, None, 404, True),
            ("relgq31_lt17_gt65", 64, None, None, 404, True),
            ("relgq31_lt17_gt65", 65, None, None, 404, True),
            ("relgq31_lt17_gt65", 66, None, None, 404, False),

            ("relgq32_lt3_gt30", 2, None, None, 405, False),
            ("relgq32_lt3_gt30", 3, None, None, 405, True),
            ("relgq32_lt3_gt30", 4, None, None, 405, True),
            ("relgq32_lt3_gt30", 29, None, None, 405, True),
            ("relgq32_lt3_gt30", 30, None, None, 405, True),
            ("relgq32_lt3_gt30", 31, None, None, 405, False),

            ("relgq33_lt16_gt65", 15, None, None, 501, False),
            ("relgq33_lt16_gt65", 16, None, None, 501, True),
            ("relgq33_lt16_gt65", 17, None, None, 501, True),
            ("relgq33_lt16_gt65", 64, None, None, 501, True),
            ("relgq33_lt16_gt65", 65, None, None, 501, True),
            ("relgq33_lt16_gt65", 66, None, None, 501, False),

            ("relgq34_lt17_gt65", 16, None, None, 601, False),
            ("relgq34_lt17_gt65", 17, None, None, 601, True),
            ("relgq34_lt17_gt65", 18, None, None, 601, True),
            ("relgq34_lt17_gt65", 64, None, None, 601, True),
            ("relgq34_lt17_gt65", 65, None, None, 601, True),
            ("relgq34_lt17_gt65", 66, None, None, 601, False),

            ("relgq35_lt17_gt65", 16, None, None, 602, False),
            ("relgq35_lt17_gt65", 17, None, None, 602, True),
            ("relgq35_lt17_gt65", 18, None, None, 602, True),
            ("relgq35_lt17_gt65", 64, None, None, 602, True),
            ("relgq35_lt17_gt65", 65, None, None, 602, True),
            ("relgq35_lt17_gt65", 66, None, None, 602, False),

            # No checks for 36

            ("relgq37_lt16", 15, None, None, 801, False),
            ("relgq37_lt16", 16, None, None, 801, True),
            ("relgq37_lt16", 17, None, None, 801, True),

            ("relgq38_lt16", 15, None, None, 802, False),
            ("relgq38_lt16", 16, None, None, 802, True),
            ("relgq38_lt16", 17, None, None, 802, True),

            ("relgq39_lt16_gt75", 15, None, None, 900, False),
            ("relgq39_lt16_gt75", 16, None, None, 900, True),
            ("relgq39_lt16_gt75", 17, None, None, 900, True),
            ("relgq39_lt16_gt75", 74, None, None, 900, True),
            ("relgq39_lt16_gt75", 75, None, None, 900, True),
            ("relgq39_lt16_gt75", 76, None, None, 900, False),

            ("relgq40_lt16_gt75", 15, None, None, 901, False),
            ("relgq40_lt16_gt75", 16, None, None, 901, True),
            ("relgq40_lt16_gt75", 17, None, None, 901, True),
            ("relgq40_lt16_gt75", 74, None, None, 901, True),
            ("relgq40_lt16_gt75", 75, None, None, 901, True),
            ("relgq40_lt16_gt75", 76, None, None, 901, False),

            # No checks for 41
        ])

def test_per_validator(rel_test, qage, relship, live_alone, gqtype, output):
    validator = MDFConstrValPer()
    validator.QAGE = qage
    validator.RELSHIP = relship
    validator.LIVE_ALONE = live_alone
    validator.GQTYPE = gqtype
    assert validator.func_dict[rel_test]() == output


parameters = "con_test, hhsize, cplt, thhldrage, paoc," + \
             " tp18, tp60, tp65, tp75, pac, output"
@pytest.mark.parametrize(
    parameters,
    [
        ("living_alone", 1, None, 1, None, 0, 0, 0, 0, None, True),
        ("living_alone", 1, None, 1, None, 1, 0, 0, 0, None, True),
        ("living_alone", 1, None, 2, None, 1, 0, 0, 0, None, False),
        ("living_alone", 1, None, 5, None, 1, 0, 0, 0, None, False),
        ("living_alone", 1, None, 5, None, 0, 1, 0, 0, None, False),
        ("living_alone", 1, None, 5, None, 0, 0, 1, 0, None, False),
        ("living_alone", 1, None, 5, None, 0, 0, 0, 1, None, False),
        ("living_alone", 1, None, 5, None, 0, 1, 1, 0, None, False),
        ("living_alone", 1, None, 5, None, 0, 1, 1, 1, None, False),
        ("living_alone", 1, None, 6, None, 0, 0, 0, 0, None, False),
        ("living_alone", 1, None, 6, None, 0, 1, 0, 0, None, True),
        ("living_alone", 1, None, 6, None, 0, 1, 1, 0, None, False),
        ("living_alone", 1, None, 6, None, 0, 1, 1, 1, None, False),
        ("living_alone", 1, None, 7, None, 0, 0, 0, 0, None, False),
        ("living_alone", 1, None, 7, None, 0, 1, 0, 0, None, False),
        ("living_alone", 1, None, 7, None, 0, 1, 1, 0, None, True),
        ("living_alone", 1, None, 7, None, 0, 1, 1, 1, None, False),
        ("living_alone", 1, None, 8, None, 0, 1, 0, 0, None, False),
        ("living_alone", 1, None, 8, None, 0, 1, 1, 0, None, False),
        ("living_alone", 1, None, 9, None, 0, 1, 1, 1, None, True),
        ("living_alone", 1, None, 9, None, 0, 1, 1, 1, None, True),
        ("living_alone", 2, None, 2, None, 1, 0, 0, 0, None, True),

        ("size2", 2, None, 1, None, 0, 0, 0, 0, 1, True),
        ("size2", 2, None, 1, None, 1, 0, 0, 0, 2, True),
        ("size2", 2, None, 2, None, 1, 0, 0, 0, 3, True),
        ("size2", 2, None, 6, None, 1, 0, 0, 0, 1, False),
        ("size2", 2, None, 6, None, 1, 1, 0, 0, 1, True),
        ("size2", 2, None, 6, None, 1, 0, 0, 0, 4, True),
        ("size2", 2, 1, 1, None, 1, 0, 0, 0, None, True),
        ("size2", 2, 1, 1, None, 1, 0, 1, 0, None, True),
        ("size2", 2, 1, 1, None, 1, 0, 0, 1, None, False),
        ("size2", 2, 1, 2, None, 1, 0, 0, 0, None, True),
        ("size2", 2, 2, 1, None, 1, 0, 0, 1, None, False),
        ("size2", 2, 4, 1, None, 1, 0, 0, 1, None, False),
        ("size2", 2, 5, 1, None, 1, 0, 0, 1, None, True),
        ("size2", 3, None, 6, None, 1, 0, 0, 0, 1, True),

        ("size3", 3, None, 2, 1, 0, 0, 0, 0, None, True),
        ("size3", 3, None, 2, 3, 0, 0, 0, 0, None, True),
        ("size3", 3, None, 2, 3, 0, 1, 0, 0, None, False),
        ("size3", 3, None, 2, 3, 0, 1, 1, 0, None, False),
        ("size3", 3, None, 2, 3, 0, 1, 1, 1, None, False),
        ("size3", 3, None, 6, 3, 0, 1, 0, 0, None, True),
        ("size3", 3, None, 6, 3, 0, 1, 1, 0, None, False),
        ("size3", 3, 1, 1, 2, 0, 1, 1, 0, None, True),
        ("size3", 3, 1, 1, 1, 0, 1, 1, 1, None, False),
        ("size3", 3, 2, 1, 2, 0, 1, 1, 1, None, False),
        ("size3", 3, 1, 1, 4, 0, 1, 1, 0, None, True),
        ("size3", 3, 1, 1, 4, 0, 1, 1, 1, None, True),
        ("size3", 3, 5, 1, 1, 0, 1, 1, 1, None, True),
        ("size3", 4, None, 2, 3, 0, 1, 0, 0, None, True),

        ("size4", 4, 1, 1, 3, None, 0, 0, 0, None, True),
        ("size4", 4, 1, 1, 3, None, 0, 0, 1, None, False),
        ("size4", 4, 4, 1, 3, None, 0, 0, 0, None, True),
        ("size4", 4, 4, 1, 3, None, 0, 0, 1, None, False),
        ("size4", 4, 5, 1, 3, None, 0, 0, 1, None, True),
        ("size4", 4, 1, 1, 1, None, 0, 0, 1, None, True),
        ("size4", 4, 1, 1, 4, None, 0, 0, 1, None, True),
        ("size4", 5, 1, 1, 3, None, 0, 0, 1, None, True),
    ])

def test_unit_size_validator(con_test, hhsize, cplt, thhldrage, paoc, tp18,
                             tp60, tp65, tp75, pac, output):
    validator = MDFConstrValUnit()
    validator.HHSIZE = hhsize
    validator.CPLT = cplt
    validator.THHLDRAGE = thhldrage
    validator.PAOC = paoc
    validator.TP18 = tp18
    validator.TP60 = tp60
    validator.TP65 = tp65
    validator.TP75 = tp75
    validator.PAC  = pac
    assert validator.func_dict[con_test]() == output


@pytest.mark.parametrize(
        "con_test, thhldrage, paoc, tp60, tp65, tp75, hhsex, output",
        [
            ("age_child", 7, 1, 1, 1, 1, 1, True),
            ("age_child", 8, 1, 1, 1, 1, 1, False),
            ("age_child", 8, 2, 1, 1, 1, 1, True),
            ("age_child", 8, 3, 1, 1, 1, 1, False),
            ("age_child", 7, 2, 1, 1, 1, 2, True),
            ("age_child", 8, 1, 1, 1, 1, 2, False),
            ("age_child", 8, 2, 1, 1, 1, 2, False),
            ("age_child", 8, 3, 1, 1, 1, 2, False),
            ("age_child", 8, 4, 1, 1, 1, 2, True),
            ("age_child", 9, 2, 1, 1, 1, 2, False),
            ("age_child", 5, 1, 1, 1, 1, 2, True),
            ("age_child", 6, 1, 1, 1, 1, 2, False),
            ("age_child", 6, 2, 1, 1, 1, 2, True),
            ("age_child", 6, 3, 1, 1, 1, 2, False),
            ("age_child", 6, 4, 1, 1, 1, 2, True),


            ("hh_elderly", 5, None, 0, 0, 0, None, True),
            ("hh_elderly", 6, None, 0, 0, 0, None, False),
            ("hh_elderly", 6, None, 1, 0, 0, None, True),
            ("hh_elderly", 7, None, 1, 0, 0, None, False),
            ("hh_elderly", 7, None, 1, 1, 0, None, True),
            ("hh_elderly", 8, None, 1, 1, 0, None, False),
            ("hh_elderly", 8, None, 1, 1, 1, None, True),
            ("hh_elderly", 9, None, 1, 1, 0, None, False),
            ("hh_elderly", 9, None, 1, 1, 1, None, True),

        ])

def test_unit_other_validator(con_test, thhldrage, paoc, tp60, tp65, tp75,
                              hhsex, output):
    validator = MDFConstrValUnit()
    validator.THHLDRAGE = thhldrage
    validator.PAOC = paoc
    validator.TP60 = tp60
    validator.TP65 = tp65
    validator.TP75 = tp75
    validator.HHSEX = hhsex
    assert validator.func_dict[con_test]() == output


@pytest.mark.parametrize(
        "voting_age, gqtype_pl, output",
        [
            (1, 0, True),
            (1, 3, False),
            (2, 3, True),
        ])

def test_PL94_validator(voting_age, gqtype_pl, output):
    validator = MDFConstrPL94ValPer()
    validator.VOTING_AGE = voting_age
    validator.GQTYPE_PL = gqtype_pl
    assert validator.func_dict['nurse_nva_0']() == output


def test_DHCP_readline():
    val = MDFConstrValPer()
    test_string = '0'
    for i in range(1, 16):
       test_string = test_string + f'|{i}'
    val.read_line(test_string)
    assert val.SCHEMA_TYPE_CODE == '0'
    assert val.RTYPE == 8
    assert val.LIVE_ALONE == 15
    test_string = test_string + '|16'
    with pytest.raises(Exception):
        val = val.read_line(test_string)


def test_DHCH_readline():
    val = MDFConstrValUnit()
    test_string = '0'
    for i in range(1, 27):
       test_string = test_string + f'|{i}'
    val.read_line(test_string)
    assert val.SCHEMA_TYPE_CODE == '0'
    assert val.GQTYPE == 8
    assert val.UPART == 15
    assert val.HHSEX == 26
    test_string = test_string + '|27'
    with pytest.raises(Exception):
        val = val.read_line(test_string)


def test_PL94_validator():
    val = MDFConstrPL94ValPer()
    test_string = '0'
    for i in range(1, 13):
       test_string = test_string + f'|{i}'
    val.read_line(test_string)
    assert val.SCHEMA_TYPE_CODE == '0'
    assert val.RTYPE == 8
    assert val.CENRACE == 12
    test_string = test_string + '|13'
    with pytest.raises(Exception):
        val = val.read_line(test_string)
