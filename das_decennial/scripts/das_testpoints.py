#!/usr/bin/env python3
#
"""
testpoints.py module

This program is used by the Bootstrap and Step scripts to send the number and text for a testpoint
To the syslog local1 facility, which ends up sending it to Splunk. Normal testpoints are sent
with LOCAL1.INFO, while failure testpoints are sent with LOCAL1.ERR.

The find_testpoints() entry is also used by das_decennial.

This could use the python syslog class, but we use a subprocess call to logger.

2020-06-20 - slg - added comments.

"""

import sys
import os

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from das2020_driver import log_tp, get_testpoint_filename


if __name__=="__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("testpoint",
                        help=f"Log a testpoint code from testpoint file {get_testpoint_filename()} with logger.")
    args = parser.parse_args()
    log_tp(args.testpoint)
