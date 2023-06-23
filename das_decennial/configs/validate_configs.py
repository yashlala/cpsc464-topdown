#!/usr/bin/env python3
#
# read a hiearchical config file and generate an error if it doesn't validate

import sys
import os
import time
from os.path import abspath
from os.path import dirname

sys.path.append(dirname(dirname(abspath(__file__))))


from das_framework.ctools.hierarchical_configparser import HierarchicalConfigParser
from das_framework.driver import config_validate


if __name__=="__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("configs",nargs='*')
    args = parser.parse_args()
    os.environ['MISSION_NAME'] = 'FAKE MISSION'
    for fname in args.configs:
        print("{}...".format(fname),end='',flush=True)
        t0 = time.time()
        config = HierarchicalConfigParser(debug=args.debug)
        config.read(fname)
        config_validate(config)
        del config
        t1 = time.time()
        print("validates in {} seconds".format(round(t1-t0,2)))
