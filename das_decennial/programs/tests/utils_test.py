import io
import pytest
from configparser import ConfigParser

from das_utils import class_from_config

s_config = """
[A]
aa = aaa.bbb.ccc

[B]
bb = das_framework.driver.ddd
adm = das_framework.driver.AbstractDASModule
"""

config = ConfigParser()
config.read_file( io.StringIO( s_config))


#def test_class_from_config(caplog):
def test_class_from_config() -> None:
    with pytest.raises(KeyError) as err:
        class_from_config(config, "aaa", "A")
    assert 'Key "aaa" in config section [A] not found' in str(err.value)

    with pytest.raises(ImportError) as err:
        class_from_config(config, "aa", "A")
    assert 'Module aaa.bbb import failed.\nCurrent directory' in str(err.value)

    with pytest.raises(AttributeError) as err:
        class_from_config(config, "bb", "B")
    assert "[B]/bb option" in str(err.value)

    from das_framework.driver import AbstractDASModule as adm
    assert type(class_from_config(config, "adm", "B")) == type(adm)
