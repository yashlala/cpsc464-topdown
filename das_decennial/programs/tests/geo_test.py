from programs.utilities.geo import GeoHierarchy

import pytest


@pytest.fixture
def geo_hierarchy() -> GeoHierarchy:
    names = ("Block", "Block_SubGroup", "Tract", "County", "State", "USA")
    leng = (16, 14, 11, 5, 2, 0)
    geolen = 16
    gh = GeoHierarchy(geolen, names, leng)
    return gh


def test_getLevel(geo_hierarchy: GeoHierarchy) -> None:
    assert geo_hierarchy.getLevel("1111111111111111") == 5
    assert geo_hierarchy.getLevel("11111111111111") == 4
    assert geo_hierarchy.getLevel("11111111111") == 3
    assert geo_hierarchy.getLevel("11111") == 2
    assert geo_hierarchy.getLevel("11") == 1
    assert geo_hierarchy.getLevel("") == 0


def test_getLevelNames(geo_hierarchy: GeoHierarchy) -> None:
    assert geo_hierarchy.levelNames == ("USA", "State", "County", "Tract", "Block_SubGroup", "Block")


def test_numLevels(geo_hierarchy: GeoHierarchy) -> None:
    assert geo_hierarchy.numLevels == 6


def test_getParentGeocode(geo_hierarchy: GeoHierarchy) -> None:
    assert geo_hierarchy.getParentGeocode("1111111111111111") == "11111111111111"
    assert geo_hierarchy.getParentGeocode("11111111111111") == "11111111111"
    assert geo_hierarchy.getParentGeocode("11111111111") == "11111"
    assert geo_hierarchy.getParentGeocode("11111") == "11"
    assert geo_hierarchy.getParentGeocode("11") == ""
    assert geo_hierarchy.getParentGeocode("") is None
