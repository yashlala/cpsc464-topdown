import subprocess
import os
import sys
import tempfile
import pytest
import zipfile

DDECDIR =os.path.dirname(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(DDECDIR, "configs/DHCH2020/MUD.ini")
CONFIG_FILE1 = os.path.join(DDECDIR, "configs/PL94/topdown_RI.ini")
RELEASE_ZIP = 'config.zip'
NEWCONFIG1 = 'newconfig1.ini'
NEWCONFIG2 = 'newconfig2.ini'
NEWCONFIG3 = 'newconfig3.ini'
PYTHON = 'python3'

def cmd(line, outfile=sys.stdout, cwd=None):
    print(line)
    p = subprocess.Popen(line.split(" "),stdout=outfile, cwd=cwd)
    if p.wait()!=0:
        raise RuntimeError(f"{line} returned {p.wait()}")


def test_config_dump():
    # Make sure that if we --dump_config, we can read the results
    with tempfile.TemporaryDirectory(dir='/mnt/tmp/') as tempdir:
        CONFIG_FILE_PATH=os.path.join( tempdir, CONFIG_FILE)
        NEWCONFIG1_PATH=os.path.join( tempdir, NEWCONFIG1)
        NEWCONFIG2_PATH=os.path.join( tempdir, NEWCONFIG2)

        cmd(f"{PYTHON} {DDECDIR}/das2020_driver.py --dump_config {CONFIG_FILE}",     outfile=open( NEWCONFIG1_PATH,"w"))
        cmd(f"{PYTHON} {DDECDIR}/das2020_driver.py --dump_config {NEWCONFIG1_PATH}", outfile=open( NEWCONFIG2_PATH,"w"))
        assert open( NEWCONFIG1_PATH, "r").read() == open( NEWCONFIG2_PATH,"r").read()

        # Make sure that every non-comment line of CONFIG_FILE is somewhere in NEWCONFIG1
        newconfig1_lines = [line.strip().replace(" = ",": ").lower() for line in open( NEWCONFIG1_PATH, "r").read().split("\n")]
        missing = 0
        for line in open(CONFIG_FILE_PATH):
            if line[0]=='#':
                continue
            line = line.strip().lower()
            if "include" in line:
                continue
            if line not in newconfig1_lines:
                print("NOT FOUND: ",line,file=sys.stderr)
                missing += 1
        if missing>0:
            print("NEWCONFIG1:",file=sys.stderr)
            for line in newconfig1_lines:
                print(line,file=sys.stderr)
        assert missing == 0


def test_config_release():
    with tempfile.TemporaryDirectory() as tempdir:
        RELEASE_ZIP_PATH=os.path.join( tempdir, RELEASE_ZIP)
        cmd(f"{PYTHON} {DDECDIR}/das2020_driver.py --make_release {RELEASE_ZIP_PATH} {CONFIG_FILE1}")

        # Validate the zip file with python package. Don't just unzip it.
        assert zipfile.is_zipfile(RELEASE_ZIP_PATH)
        zf = zipfile.ZipFile(RELEASE_ZIP_PATH)
        assert len(zf.namelist()) > 0
