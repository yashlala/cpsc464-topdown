import pytest
import os
from os.path import dirname,abspath
import pathlib
import logging

"""
Apparently you don't need to much with the path afterall!
use `pytest -k git` to run this test alone.

We had to muck with the path because I wanted to do:
pytest tests/test_git_commit.py

There is probably some way to muck with the pytest config file to accomplish this as well.
"""



import das2020_driver

def test_get_git_hash():
    h = " ".join(das2020_driver.get_git_hash( pathlib.Path(dirname(abspath(__file__))).parent))
    print("h=%s" % h)
    words = h.split()
    assert words[0]=='das_decennial'
    assert len(words[2])==40
