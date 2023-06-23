#!/usr/bin/env python3
"""
Program to print information about spark logs
"""

import os
import sys
import time
import json
import datetime

try:
    from das_constants import CC
except ImportError as e:
    sys.path.append( os.path.join( os.path.dirname(__file__),".."))
    sys.path.append( os.path.join( os.path.dirname(__file__),"../das_framework"))
    from das_constants import CC

JSON_LOG_TEMPLATE='/mnt/tmp/logs/{applicationId}'

import pytz
TZ=pytz.timezone('America/New_York')


def showtime(t):
    if t>time.time() * 100:
        t = t/1000              # did it come in msec?
    return datetime.datetime.fromtimestamp(t, TZ).isoformat()

def timestamp(obj):
    try:
        return showtime(obj['Timestamp'])
    except KeyError:
        pass
    try:
        t = obj['Task Info']['Finish Time']
        if t==0:
            t = obj['Task Info']['Start Time']
        return showtime(t)
    except KeyError:
        pass

    for key in obj.keys():
        if "timestamp" in key.lower():
            return key+" "+datetime.datetime.fromtimestamp(obj[key]/1000, TZ).isoformat()
    return ""

def decode(obj):
    event = obj['Event']
    if event=='SparkListenerLogStart':
        print('Start Spark Version',obj['Spark Version'])
    elif event=='SparkListenerBlockManagerAdded':
        print(timestamp(obj),event,'Maximum Memory:',obj['Maximum Memory'])
    elif event=='SparkListenerExecutorRemoved':
        print(timestamp(obj), event, obj['Executor ID'],obj['Removed Reason'])
    elif event=='SparkListenerTaskEnd':
        msec = obj['Task Info']['Finish Time'] - obj['Task Info']['Launch Time']
        sec  = msec/1000
        print(f"{timestamp(obj)}  {event} Stage {obj['Stage ID']} Executor {obj['Task Info']['Executor ID']} {obj['Task Type']} {obj['Task End Reason']['Reason']} time={sec:.1f}s")
    else:
        print("   ",event,timestamp(obj))


def show_logs(fname):
    print("================",fname,"================")
    with open(fname,"r") as f:
        for line in f:
            decode(json.loads(line))


def find_logs(applicationId):
    fname = JSON_LOG_TEMPLATE.format(applicationId=applicationId)
    if os.path.exists(fname):
        show_logs(fname)
    if os.path.exists(fname+".inprogress"):
        show_logs(fname+".inprogress")



if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Test the dashboard." )
    parser.add_argument("applicationId",help="specify an applicationId")
    args = parser.parse_args()
    find_logs(args.applicationId)
