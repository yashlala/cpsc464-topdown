#!/usr/bin/env python3
"""
dvs_register.py:
register a DAS dataset with DVS retroactively.

${DAS_S3ROOT}/users/user007/topdown_RI/noisy_measurements/application_1607440845980_0017-TractOptimized.pickle/part-00937
${DAS_S3ROOT}/users/user007/topdown_RI/data/part-00985

Also check out:
s3://uscb-decennial-ite-das-logs/j-2WMD341MUF0L/DAS/das2020logs/DAS-2020-04-06_0761_EXPIRED_SOCK.dfxml

"""

import logging
import os
import re
import warnings
import json
import sys
import boto3
import zipfile
import io
from urllib.parse import urlparse
from os.path import abspath,dirname
from collections import defaultdict


PARENT_DIR = dirname(dirname(abspath(__file__)))
FRAMEWORK_DIR = os.path.join(PARENT_DIR, "das_framework")

for d in [PARENT_DIR,FRAMEWORK_DIR]:
    if d not in sys.path:
        sys.path.append(d)

import ctools
import ctools.s3
import programs.python_dvs.dvs as dvs

S3RE = re.compile(r'(s3://[^\s"]+)') # regular expression for an S3 file
S3CAT_SUFFIX = re.compile(r"s3cat.*suffix=([^()]+)")
LOGFILE_BEFORE_RE = re.compile("person.path|unit.path|read",re.I)
LOGFILE_AFTER_RE = re.compile("write|writing|save|saving|s3cat",re.I)
LOGFILE_IGNORE_RE = re.compile("clear|upload|das_s3root|lambda|combining|output_path",re.I)



def make_s3prefix(s3path):
    if not s3path.endswith("/"):
        s3path += "/"
    return s3path

def make_s3path(s3path):
    if s3path.endswith("/"):
        s3path = s3path[:-1]
    return s3path

def s3path_exists(s3path):
    """Return true if an s3 s3path exists"""
    # https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    p = urlparse( s3path)
    s3 = boto3.resource('s3')
    try:
        for _ in s3.Bucket(p.netloc).objects.filter(Prefix=p.path[1:]):
            return _.key==p.path[1:]
    except s3.meta.client.exceptions.NoSuchBucket as e:
        print(str(e),file=sys.stderr)
        print("Unknown bucket in: ",s3path,file=sys.stderr)
    return False

def s3prefix_exists(s3prefix):
    """Return true if there is at least one object underneath the s3prefix when it is interpreted as aprefix"""
    assert s3prefix.endswith("/")
    p = urlparse( s3prefix )
    for obj in boto3.resource('s3').Bucket(p.netloc).objects.filter(Prefix=p.path[1:]).page_size(10):
        return True
    return False                # nothing there

def find_mission(mission_name):
    """Given a mission name, find it in S3"""
    s3root = os.getenv('DAS_S3ROOT')
    for prefix in ['log/das','rpc/upload']:
        p = urlparse(s3root)
        for obj in boto3.resource('s3').Bucket(p.netloc).objects.filter(Prefix=prefix):
            if mission_name in obj.key:
                return os.path.join(s3root,obj.key)
    print("Cannot find mission",mission_name,file=sys.stderr)
    exit(1)


def determine_s3_path(fname):
    for ch in "'\"[],()":
        fname = fname.replace(ch,"")

    fname_prefix = make_s3prefix(fname)

    if s3path_exists(fname):
        return fname
    if s3path_exists(fname+".txt"):
        return fname+".txt"
    if s3prefix_exists(fname_prefix):
        return fname_prefix

    logging.warning("%s no longer exists",fname)
    return None

def characterize_line(line):
    m = S3CAT_SUFFIX.search(line)
    if m:
        suffix = m.group(1)
    else:
        suffix = ""

    m = S3RE.search(line)
    if m:
        fname = m.group(1)+suffix
        p = urlparse( fname )
        if len(p.path)<1:   # no possible path
            return (None,None)
        s3path = determine_s3_path(fname)
        if s3path:

            # Figure out the context in which it was seen
            if LOGFILE_IGNORE_RE.search(line):
                return (None, None)
            elif LOGFILE_BEFORE_RE.search(line):
                return (dvs.COMMIT_BEFORE, s3path)
            elif LOGFILE_AFTER_RE.search(line):
                return (dvs.COMMIT_AFTER, s3path)
            else:
                warnings.warn(f"Unknown line in log file: {line}")
    return (None, None)

def extract_paths_from_log(f):
    """Given a logfile, return a dict of sets of paths"""

    paths = defaultdict(set)
    for line in f:
        if "Git commit info" in line:
            for part in line.split("|"):
                words = part.split(" ")
                if len(words)==3 and words[1]=="commit":
                    paths[dvs.COMMIT_METHOD].add(part)
            continue


        # Check for lines that mention S3 and, if we find them, process
        (which,what) = characterize_line(line)
        if which and what:
            paths[which].add(what)

    return paths

def extract_paths_from_dfxml(f):
    raise RuntimeError("Ingest from DFXML not implemented yet.")

def extract_paths(logfile):
    # Import DVS here so that it is not brought in when running TDA on workers
    if logfile.startswith("s3://") and logfile.endswith(".zip"):
        zf = zipfile.ZipFile(ctools.s3.S3File(logfile))
        names = [name for name in zf.namelist() if name.endswith(".log")]
        if len(names)!=1:
            print(f"There is not a single file ending in .log in {logfile}",file=sys.stderr)
            print(f"Zipfile components:",file=sys.stderr)
            for name in zf.namelist():
                print(f"   ",name,file=sys.stderr)
            exit(1)
        f = io.TextIOWrapper(zf.open(names[0]))
        logfile = names[0]
    elif logfile.startswith("s3://"):
        f = ctools.s3.sopen(logfile,"r")
    else:
        f = open(logfile,"r")

    if logfile.endswith(".log"):
        return extract_paths_from_log(f)
    elif logfile.endswith(".dfxml"):
        return extract_paths_from_dfxml(f)
    else:
        raise RuntimeError("Unknown file format: "+logfile)


def dvs_dc_populate(dc, logfile, ephemeral=False):
    """Create a DVS context, populate with things for the logfile, and return"""
    import programs.python_dvs.dvs as dvs
    if ephemeral:
        dc.set_attribute(dc.ATTRIBUTE_EPHEMERAL)
    if "JBID" in os.environ:
        dc.add_kv(key="x-jbid", value=os.getenv("JBID"))
    for (key, vals) in extract_paths(logfile).items():
        dc.add_s3_paths_or_prefixes(key, [val for val in vals if val.startswith("s3://") ])
        for git_info in [val for val in vals if not val.startswith("s3://") ]:
            words = git_info.split(" ")
            assert words[1]=='commit'
            url=os.environ['DAS_GITROOT']+words[0]
            dc.add_git_commit( key, url=url, commit=words[2] )
    return dc

def dvs_register(logfile, ephemeral=False, search=False):
    import programs.python_dvs.dvs as dvs
    dc = dvs.DVS()
    if search:
        dc.set_option( dvs.SEARCH )
        dc.set_option( dvs.OPTION_SEARCH_FOR_AFTERS )

    dvs_dc_populate(dc, logfile, ephemeral=ephemeral)
    return dc.commit()


if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,
                            description="Register TDA inputs, code base, and outputs")
    parser.add_argument("--message", help="A message (commit comment) for the DVS")
    parser.add_argument("--ephemeral", help="Make commit ephemeral", action='store_true')
    parser.add_argument("--debug", help="Enable debugging", action='store_true')
    parser.add_argument("--search", help="Use the search endpoint for cache checking",action='store_true')
    parser.add_argument("logfile", help="A TDA logfile that is analyzed")
    parser.add_argument("--mission", help="Treat logfile as a mission name to find in S3",action='store_true')
    parser.add_argument("--dump", help="Instead of making the mission, just dump what was learned from the logfile", action='store_true')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    if args.mission:
        args.logfile = find_mission(args.logfile.upper())
        print("Using",args.logfile)

    if args.dump:
        paths = extract_paths(args.logfile)
        print(json.dumps( paths, indent=4, default=str))
        for which in [dvs.COMMIT_BEFORE,dvs.COMMIT_AFTER]:
            print(which)
            for pre in paths[which]:
                p = urlparse( pre )
                bucket = boto3.resource('s3').Bucket( p.netloc)
                print(f"{pre}    {len(list(bucket.objects.page_size(100).filter(Prefix=p.path[1:])))}")

        exit(0)

    res = dvs_register(args.logfile, ephemeral=args.debug or args.ephemeral, search=args.search)
    print(json.dumps(res,indent=4))
