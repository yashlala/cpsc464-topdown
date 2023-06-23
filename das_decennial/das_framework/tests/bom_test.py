import pytest
import os
import os.path
import warnings

from certificate.bom import get_bom

def test_get_bom():
    for (name, path, ver, bytecount) in get_bom(content=False):
        (base,ext) = os.path.splitext(name)
        assert ext!='.md'
