import pytest
from programs.writer.dhcp_to_mdf2020 import DHCPToMDF2020Recoder
from das_constants import CC

RELGQ = CC.ATTR_RELGQ
SEX = CC.ATTR_SEX
AGE = CC.ATTR_AGE
HISP = CC.ATTR_HISP
CENRACE = CC.ATTR_CENRACE

class TestDHCPToMDF2020Recoder:
    recoder = DHCPToMDF2020Recoder()

    # rtype recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'relgq': 0}, '3'), # '3' is for Housing Unit
        ({'relgq': 1}, '3'),
        ({'relgq': 2}, '3'),
        ({'relgq': 3}, '3'),
        ({'relgq': 4}, '3'),
        ({'relgq': 5}, '3'),
        ({'relgq': 6}, '3'),
        ({'relgq': 7}, '3'),
        ({'relgq': 8}, '3'),
        ({'relgq': 9}, '3'),
        ({'relgq': 10}, '3'),
        ({'relgq': 11}, '3'),
        ({'relgq': 12}, '3'),
        ({'relgq': 13}, '3'),
        ({'relgq': 14}, '3'),
        ({'relgq': 15}, '3'),
        ({'relgq': 16}, '3'),
        ({'relgq': 17}, '3'),
        ({'relgq': 18}, '5'), # '5' is for Group Quarters
        ({'relgq': 19}, '5'),
        ({'relgq': 20}, '5'),
        ({'relgq': 21}, '5'),
        ({'relgq': 22}, '5'),
        ({'relgq': 23}, '5'),
        ({'relgq': 24}, '5'),
        ({'relgq': 25}, '5'),
        ({'relgq': 26}, '5'),
        ({'relgq': 27}, '5'),
        ({'relgq': 28}, '5'),
        ({'relgq': 29}, '5'),
        ({'relgq': 30}, '5'),
        ({'relgq': 31}, '5'),
        ({'relgq': 32}, '5'),
        ({'relgq': 33}, '5'),
        ({'relgq': 34}, '5'),
        ({'relgq': 35}, '5'),
        ({'relgq': 36}, '5'),
        ({'relgq': 37}, '5'),
        ({'relgq': 38}, '5'),
        ({'relgq': 39}, '5'),
        ({'relgq': 40}, '5'),
        ({'relgq': 41}, '5')
     ])

    def test_rtype_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.rtype_recoder(rowdict_mangled)['RTYPE'] == ans

    # gqtype recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'relgq': 0}, '000'), # '000' NIU / household
        ({'relgq': 1}, '000'),
        ({'relgq': 2}, '000'),
        ({'relgq': 3}, '000'),
        ({'relgq': 4}, '000'),
        ({'relgq': 5}, '000'),
        ({'relgq': 6}, '000'),
        ({'relgq': 7}, '000'),
        ({'relgq': 8}, '000'),
        ({'relgq': 9}, '000'),
        ({'relgq': 10}, '000'),
        ({'relgq': 11}, '000'),
        ({'relgq': 12}, '000'),
        ({'relgq': 13}, '000'),
        ({'relgq': 14}, '000'),
        ({'relgq': 15}, '000'),
        ({'relgq': 16}, '000'),
        ({'relgq': 17}, '000'),
        ({'relgq': 18}, '101'),
        ({'relgq': 19}, '102'),
        ({'relgq': 20}, '103'),
        ({'relgq': 21}, '104'),
        ({'relgq': 22}, '105'),
        ({'relgq': 23}, '106'),
        ({'relgq': 24}, '201'),
        ({'relgq': 25}, '202'),
        ({'relgq': 26}, '203'),
        ({'relgq': 27}, '301'),
        ({'relgq': 28}, '401'),
        ({'relgq': 29}, '402'),
        ({'relgq': 30}, '403'),
        ({'relgq': 31}, '404'),
        ({'relgq': 32}, '405'),
        ({'relgq': 33}, '501'),
        ({'relgq': 34}, '601'),
        ({'relgq': 35}, '602'),
        ({'relgq': 36}, '701'),
        ({'relgq': 37}, '801'),
        ({'relgq': 38}, '802'),
        ({'relgq': 39}, '900'),
        ({'relgq': 40}, '901'),
        ({'relgq': 41}, '997')
     ])

    def test_gqtype_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.gqtype_recoder(rowdict_mangled)['GQTYPE'] == ans

    # relship recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'relgq': 0}, '20'),
        ({'relgq': 1}, '20'),
        ({'relgq': 2}, '21'),
        ({'relgq': 3}, '22'),
        ({'relgq': 4}, '23'),
        ({'relgq': 5}, '24'),
        ({'relgq': 6}, '25'),
        ({'relgq': 7}, '26'),
        ({'relgq': 8}, '27'),
        ({'relgq': 9}, '28'),
        ({'relgq': 10}, '29'),
        ({'relgq': 11}, '30'),
        ({'relgq': 12}, '31'),
        ({'relgq': 13}, '32'),
        ({'relgq': 14}, '33'),
        ({'relgq': 15}, '34'),
        ({'relgq': 16}, '35'),
        ({'relgq': 17}, '36'),
        ({'relgq': 18}, '37'), # '37' Institutional GQ person
        ({'relgq': 19}, '37'),
        ({'relgq': 20}, '37'),
        ({'relgq': 21}, '37'),
        ({'relgq': 22}, '37'),
        ({'relgq': 23}, '37'),
        ({'relgq': 24}, '37'),
        ({'relgq': 25}, '37'),
        ({'relgq': 26}, '37'),
        ({'relgq': 27}, '37'),
        ({'relgq': 28}, '37'),
        ({'relgq': 29}, '37'),
        ({'relgq': 30}, '37'),
        ({'relgq': 31}, '37'),
        ({'relgq': 32}, '37'),
        ({'relgq': 33}, '38'), # '38' Non-institutional GQ person
        ({'relgq': 34}, '38'),
        ({'relgq': 35}, '38'),
        ({'relgq': 36}, '38'),
        ({'relgq': 37}, '38'),
        ({'relgq': 38}, '38'),
        ({'relgq': 39}, '38'),
        ({'relgq': 40}, '38'),
        ({'relgq': 41}, '38')
     ])

    def test_relship_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.relship_recoder(rowdict_mangled)['RELSHIP'] == ans

    # qsex recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'sex': 0}, '1'),
        ({'sex': 1}, '2')
    ])

    def test_qsex_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.qsex_recoder(rowdict_mangled)['QSEX'] == ans

    # qage recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'age': 0}, 0),
        ({'age': 1}, 1),
        ({'age': 2}, 2),
        ({'age': 3}, 3),
        ({'age': 4}, 4),
        ({'age': 5}, 5),
        ({'age': 6}, 6),
        ({'age': 7}, 7),
        ({'age': 8}, 8),
        ({'age': 9}, 9),
        ({'age': 10}, 10),
        ({'age': 11}, 11),
        ({'age': 12}, 12),
        ({'age': 13}, 13),
        ({'age': 14}, 14),
        ({'age': 15}, 15),
        ({'age': 16}, 16),
        ({'age': 17}, 17),
        ({'age': 18}, 18),
        ({'age': 19}, 19),
        ({'age': 20}, 20),
        ({'age': 21}, 21),
        ({'age': 22}, 22),
        ({'age': 23}, 23),
        ({'age': 24}, 24),
        ({'age': 25}, 25),
        ({'age': 26}, 26),
        ({'age': 27}, 27),
        ({'age': 28}, 28),
        ({'age': 29}, 29),
        ({'age': 30}, 30),
        ({'age': 31}, 31),
        ({'age': 32}, 32),
        ({'age': 33}, 33),
        ({'age': 34}, 34),
        ({'age': 35}, 35),
        ({'age': 36}, 36),
        ({'age': 37}, 37),
        ({'age': 38}, 38),
        ({'age': 39}, 39),
        ({'age': 40}, 40),
        ({'age': 41}, 41),
        ({'age': 42}, 42),
        ({'age': 43}, 43),
        ({'age': 44}, 44),
        ({'age': 45}, 45),
        ({'age': 46}, 46),
        ({'age': 47}, 47),
        ({'age': 48}, 48),
        ({'age': 49}, 49),
        ({'age': 50}, 50),
        ({'age': 51}, 51),
        ({'age': 52}, 52),
        ({'age': 53}, 53),
        ({'age': 54}, 54),
        ({'age': 55}, 55),
        ({'age': 56}, 56),
        ({'age': 57}, 57),
        ({'age': 58}, 58),
        ({'age': 59}, 59),
        ({'age': 60}, 60),
        ({'age': 61}, 61),
        ({'age': 62}, 62),
        ({'age': 63}, 63),
        ({'age': 64}, 64),
        ({'age': 65}, 65),
        ({'age': 66}, 66),
        ({'age': 67}, 67),
        ({'age': 68}, 68),
        ({'age': 69}, 69),
        ({'age': 70}, 70),
        ({'age': 71}, 71),
        ({'age': 72}, 72),
        ({'age': 73}, 73),
        ({'age': 74}, 74),
        ({'age': 75}, 75),
        ({'age': 76}, 76),
        ({'age': 77}, 77),
        ({'age': 78}, 78),
        ({'age': 79}, 79),
        ({'age': 80}, 80),
        ({'age': 81}, 81),
        ({'age': 82}, 82),
        ({'age': 83}, 83),
        ({'age': 84}, 84),
        ({'age': 85}, 85),
        ({'age': 86}, 86),
        ({'age': 87}, 87),
        ({'age': 88}, 88),
        ({'age': 89}, 89),
        ({'age': 90}, 90),
        ({'age': 91}, 91),
        ({'age': 92}, 92),
        ({'age': 93}, 93),
        ({'age': 94}, 94),
        ({'age': 95}, 95),
        ({'age': 96}, 96),
        ({'age': 97}, 97),
        ({'age': 98}, 98),
        ({'age': 99}, 99),
        ({'age': 100}, 100),
        ({'age': 101}, 101),
        ({'age': 102}, 102),
        ({'age': 103}, 103),
        ({'age': 104}, 104),
        ({'age': 105}, 105),
        ({'age': 106}, 106),
        ({'age': 107}, 107),
        ({'age': 108}, 108),
        ({'age': 109}, 109),
        ({'age': 110}, 110),
        ({'age': 111}, 111),
        ({'age': 112}, 112),
        ({'age': 113}, 113),
        ({'age': 114}, 114),
        ({'age': 115}, 115)
    ])

    def test_qage_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.qage_recoder(rowdict_mangled)['QAGE'] == ans


    # cenhisp recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'hispanic': 0}, '1'),
        ({'hispanic': 1}, '2')
    ])

    def test_cenhisp_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.cenhisp_recoder(rowdict_mangled)['CENHISP'] == ans


    # cenrace recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'cenrace': 0}, '01'),
        ({'cenrace': 1}, '02'),
        ({'cenrace': 2}, '03'),
        ({'cenrace': 3}, '04'),
        ({'cenrace': 4}, '05'),
        ({'cenrace': 5}, '06'),
        ({'cenrace': 6}, '07'),
        ({'cenrace': 7}, '08'),
        ({'cenrace': 8}, '09'),
        ({'cenrace': 9}, '10'),
        ({'cenrace': 10}, '11'),
        ({'cenrace': 11}, '12'),
        ({'cenrace': 12}, '13'),
        ({'cenrace': 13}, '14'),
        ({'cenrace': 14}, '15'),
        ({'cenrace': 15}, '16'),
        ({'cenrace': 16}, '17'),
        ({'cenrace': 17}, '18'),
        ({'cenrace': 18}, '19'),
        ({'cenrace': 19}, '20'),
        ({'cenrace': 20}, '21'),
        ({'cenrace': 21}, '22'),
        ({'cenrace': 22}, '23'),
        ({'cenrace': 23}, '24'),
        ({'cenrace': 24}, '25'),
        ({'cenrace': 25}, '26'),
        ({'cenrace': 26}, '27'),
        ({'cenrace': 27}, '28'),
        ({'cenrace': 28}, '29'),
        ({'cenrace': 29}, '30'),
        ({'cenrace': 30}, '31'),
        ({'cenrace': 31}, '32'),
        ({'cenrace': 32}, '33'),
        ({'cenrace': 33}, '34'),
        ({'cenrace': 34}, '35'),
        ({'cenrace': 35}, '36'),
        ({'cenrace': 36}, '37'),
        ({'cenrace': 37}, '38'),
        ({'cenrace': 38}, '39'),
        ({'cenrace': 39}, '40'),
        ({'cenrace': 40}, '41'),
        ({'cenrace': 41}, '42'),
        ({'cenrace': 42}, '43'),
        ({'cenrace': 43}, '44'),
        ({'cenrace': 44}, '45'),
        ({'cenrace': 45}, '46'),
        ({'cenrace': 46}, '47'),
        ({'cenrace': 47}, '48'),
        ({'cenrace': 48}, '49'),
        ({'cenrace': 49}, '50'),
        ({'cenrace': 50}, '51'),
        ({'cenrace': 51}, '52'),
        ({'cenrace': 52}, '53'),
        ({'cenrace': 53}, '54'),
        ({'cenrace': 54}, '55'),
        ({'cenrace': 55}, '56'),
        ({'cenrace': 56}, '57'),
        ({'cenrace': 57}, '58'),
        ({'cenrace': 58}, '59'),
        ({'cenrace': 59}, '60'),
        ({'cenrace': 60}, '61'),
        ({'cenrace': 61}, '62'),
        ({'cenrace': 62}, '63')
    ])

    def test_cenrace_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.cenrace_recoder(rowdict_mangled)['CENRACE'] == ans

    # live alone recoder
    @pytest.mark.parametrize("rowdict, ans", [
        ({'relgq': 0}, '1'), # Householder living alone
        ({'relgq': 1}, '2'),
        ({'relgq': 2}, '2'), # Relationships to Household include a person of relationship type + the householder
        ({'relgq': 3}, '2'), # so any relationship that isn't the householder themself is not living alone
        ({'relgq': 4}, '2'),
        ({'relgq': 5}, '2'),
        ({'relgq': 6}, '2'),
        ({'relgq': 7}, '2'),
        ({'relgq': 8}, '2'),
        ({'relgq': 9}, '2'),
        ({'relgq': 10}, '2'),
        ({'relgq': 11}, '2'),
        ({'relgq': 12}, '2'),
        ({'relgq': 13}, '2'),
        ({'relgq': 14}, '2'),
        ({'relgq': 15}, '2'),
        ({'relgq': 16}, '2'),
        ({'relgq': 17}, '2'),
        ({'relgq': 18}, '0'), # GQs are NIU (i.e. RTYPE = '5' => 'NIU')
        ({'relgq': 19}, '0'),
        ({'relgq': 20}, '0'),
        ({'relgq': 21}, '0'),
        ({'relgq': 22}, '0'),
        ({'relgq': 23}, '0'),
        ({'relgq': 24}, '0'),
        ({'relgq': 25}, '0'),
        ({'relgq': 26}, '0'),
        ({'relgq': 27}, '0'),
        ({'relgq': 28}, '0'),
        ({'relgq': 29}, '0'),
        ({'relgq': 30}, '0'),
        ({'relgq': 31}, '0'),
        ({'relgq': 32}, '0'),
        ({'relgq': 33}, '0'),
        ({'relgq': 34}, '0'),
        ({'relgq': 35}, '0'),
        ({'relgq': 36}, '0'),
        ({'relgq': 37}, '0'),
        ({'relgq': 38}, '0'),
        ({'relgq': 39}, '0'),
        ({'relgq': 40}, '0'),
        ({'relgq': 41}, '0')
     ])

    def test_live_alone_recoder(self, rowdict, ans):
        rowdict_mangled = {self.recoder.getMangledName(key): value for key, value in rowdict.items()}
        assert self.recoder.live_alone_recoder(rowdict_mangled)['LIVE_ALONE'] == ans
