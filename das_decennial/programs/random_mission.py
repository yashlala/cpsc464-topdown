"""
random_mission.py:
Generate a random mission name.
Do a quick check of the server to see if the mission already exists; if it does, make a new one.
In either event, time out after 1 second.
"""
import os
import os.path
import sys
import random
import json
import urllib
import logging
import socket
import time

import ctools.aws as aws
import ctools.env as env

from das_constants import CC

from os.path import dirname,basename,abspath

from ctools.paths import mkpath

# We copy get_config here until we get the import structure straightened out

# # https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
# class Singleton(type):
#     _instances = {}
#     def __call__(cls, *args, **kwargs):
#         if cls not in cls._instances:
#             cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
#         return cls._instances[cls]

# ################################################################
# # Code to pass configuration information to workers
# # JCR is the JSONConfigReader that reads the configuration information from das_config.json
# # Note that we can't instantiate this on the workers on import because the DAS_ENVIRONMENT variable
# # isn't set up at that point. That's why it is a singleton
# class JCR(metaclass=Singleton):
#     def __init__(self):
#         self.jcr = env.JSONConfigReader(path=CC.DAS_CONFIG_JSON, envar='DAS_ENVIRONMENT')
#     def get_config(self, var):
#         return self.jcr.get_config(var)

# local_jcr=JCR()

# def get_config(name):
#     if "." not in name:
#         val = os.environ[name]
#         if val:
#             return val
#     return local_jcr.get_config(name)

IGNORE=True
ETC_DIR     = os.path.join( dirname(dirname(abspath(__file__))), "etc")
NOUNS       = os.path.join(ETC_DIR,'nouns.txt')
ADJECTIVES  = os.path.join(ETC_DIR,'adjectives.txt')
MISSION_TIMEOUT = 1.0

def mission_exists(mission_name):
    """return True if the mission exists"""
    dashboard_url=os.environ['DAS_DASHBOARD_URL']
    if dashboard_url == "none":
        return False
    try:
        url = mkpath(dashboard_url,
                     "api/mission/{mission_name}".format(mission_name=mission_name))
        ret = json.loads(aws.get_url(url, timeout=MISSION_TIMEOUT, ignore_cert=IGNORE))
    except (urllib.error.URLError,socket.timeout) as e:
        logging.warning(url)
        logging.warning(e)
        return []            # don't know if it exists or not, return False
    return ret

def random_line(filename):
    lines = open(filename,"r").read().split("\n")[1:]
    return random.choice(lines)

def random_mission():
    while True:
        if os.environ.get('DAS_TIER','')=='ITE':
            mission_name = random_line(ADJECTIVES) + " " + random_line(NOUNS)
        else:
            mission_name = time.strftime("MISSION_%Y-%m-%dT%H_%M_%S")
        mission_name = mission_name.upper().replace(' ','_')
        if not mission_exists(mission_name):
            return mission_name

if __name__=="__main__":
    print(random_mission())
