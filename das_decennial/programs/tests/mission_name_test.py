import os
import sys

HOME_DIR=os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__))))
if HOME_DIR not in sys.path:
    sys.path.append( HOME_DIR)

import programs.random_mission as random_mission

def test_mission_name():
    a = random_mission.random_mission()
    assert "_" in a
    assert a[0:1].isupper()
    tier = os.environ['DAS_TIER']
    os.environ['DAS_TIER'] = 'XXX'

    b = random_mission.random_mission()
    assert b[0:8]=='MISSION_'

    os.environ['DAS_TIER'] = tier
