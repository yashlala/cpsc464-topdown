import pytest
import os
import tempfile
import das_utils

def test_loadConfigFile():
    f = tempfile.NamedTemporaryFile('w', delete=False)
    name = f.name
    f.write("[a]\na=1\nb=2\n\n[b]\nc=3")
    f.close()
    config = das_utils.loadConfigFile(name)
    assert config.getint('a','a') == 1
    assert config.get('a', 'b') == '2'
    assert config.get('b', 'c') == '3'
    os.remove(name)


def test_splitBySize():
    import numpy as np
    a = sorted((np.random.random(10)*1000).astype(int))
    counts = {k:c for k,c in zip('abcdefghij', a)}
    a_split = das_utils.splitBySize(zip('abcdefghij', a))
    pdict = {}
    for geocode, count in counts.items():
        part = a_split[geocode]
        if part in pdict:
            pdict[part] += count
        else:
            pdict[part] = count
    for count in pdict.values():
        assert 0.6 < count < 1.1 * max(a)
