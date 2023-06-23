"""
Get the current log from the mission from any command line and print it.
Use the -f option to watch in real time (similar to tail -f on a file)

"""

import os
import sys
import time
import requests

try:
    from das_constants import CC
except ImportError as e:
    sys.path.append( os.path.join( os.path.dirname(__file__),".."))
    sys.path.append( os.path.join( os.path.dirname(__file__),"../das_framework"))
    from das_constants import CC
import dashboard
import colors

def print_mission_log(identifier, follow=False, debug=False):
    url = dashboard.api_url(debug_url=debug) + dashboard.DAS_MISSION_API.format(identifier=identifier)
    r = requests.get(url)
    try:
        row = r.json()[0]
    except IndexError:
        print("No such mission ",identifier)
        exit(1)
    for (k,v) in row.items():
        print(f"{k:20} {str(v)[0:60]}")
    mission_name = row['mission_name']

    last = 0
    while follow:
        r = requests.get(dashboard.api_url() + dashboard.DAS_MISSION_LOG.format(identifier=identifier,last=last))
        rows = r.json()
        for row in rows:
            if last<row['id']:
                emph = ''
                if row['code']==dashboard.CODE_ALERT:
                    emph = colors.CRED+colors.CWHITEBG2 # red on white
                print(f"{emph}{mission_name}: {row['t']} {row['message']}{colors.CEND}")
                last = row['id']
        if follow:
            time.sleep(10)




if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Test the dashboard." )
    parser.add_argument("-f","--follow", action='store_true', help='Poll the server')
    parser.add_argument("--debug", action='store_true', help='use debug endpoint')
    parser.add_argument("mission",help="das_run_id, das_run_uuid or mission_name")

    args = parser.parse_args()

    print_mission_log(args.mission, follow=args.follow, debug=args.debug)
