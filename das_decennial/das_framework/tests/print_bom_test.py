import subprocess
import os
import sys
import tempfile
import pytest
from os.path import abspath,dirname,basename

CONFIG_FILE   = os.path.join( dirname(__file__), "graph_test.ini")
DRIVER         = os.path.join( dirname(dirname( abspath(__file__))), "driver.py")

def cmd(line, outfile=sys.stdout, cwd=None):
    print(line)
    p = subprocess.Popen(line.split(" "),stdout=outfile, cwd=cwd)
    if p.wait()!=0:
        raise RuntimeError(f"{line} returned {p.wait()}")


@pytest.mark.skipif(os.getenv('PYTEST_COV'),reason='pytest-cov cannot trace through subprocesses')
def test_make_bom():
    # Make sure that if we --dump_config, we can read the results
    cmd = [sys.executable, DRIVER, '--print_bom', CONFIG_FILE]
    print(" ".join(cmd))
    out = subprocess.check_output(cmd, encoding='utf-8')
    lines = out.strip().split("\n")

    # Make sure there are no blank lines
    if "" in lines:
        for (ct,line) in enumerate(lines):
            print(f"{ct}: '{line}'",file=sys.stderr)
        raise RuntimeError("blank line in print_bom")

    # Make sure that the same line doesn't appear twice
    seen = set()
    for line in lines:
        assert line not in seen
        seen.add(line)

if __name__=="__main__":
    test_make_bom()
