#!/usr/bin/env python3
import requests
import urllib3
import json
import time
import codecs
from urllib.parse import urlparse
import http.client
import urllib
import base64
import subprocess
import ssl

SENDER_POLL_DELAY = 1

urllib3.disable_warnings()

def run(rpserver, sparkui, poll=SENDER_POLL_DELAY, debug=False):
    o = urlparse(rpserver)
    rp_host = o.hostname
    rp_path = o.path

    while True:
        conn = http.client.HTTPSConnection(rp_host,
                                           context = ssl._create_unverified_context() )
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        # Every query must have a q, which is what we send to the rp server
        # In this version, 'q' is a urllencoded json object
        q = {}
        conn.request("POST", rp_path, urllib.parse.urlencode({'q':json.dumps(q)}), headers)
        r = conn.getresponse()
        if r.status!=200:
            print("RP Server {} responded with error: {}".format(rpserver, r.status))
            print("Headers:")
            print(r.headers)
            print("Response:")
            print(codecs.decode(r.read()))
            exit(1)
        try:
            data = r.read()
            reply = json.loads(data)
        except json.decoder.JSONDecodeError as e:
            print(e)
            open("error.html","wb").write(data)
            subprocess.call(['/usr/bin/open','error.html'])
            exit(1)
        print("reply:",reply)

        if 'url' in reply:
            # Get the requested URL.
            # Note that we only handle get requests
            query = sparkui + reply['url'][1:]
            print("query {}".format(query))
            r2 = requests.get(query)
            print("   {} Received {} bytes".format(r2.status_code,len(r2.text)))
            q['id']      = reply['id']
            q['url']     = reply['url']
            q['status']  = r2.status_code
            q['contentbase64'] = codecs.decode(base64.b64encode(r2.content),'utf-8')
            # Copy all the headers, the status and the content
            for (key,val) in r2.headers.items():
                print("    {}: {}".format(key,val))
                q[key] = val

            print("q:",q)
            # Now send the requested URL back
            conn = http.client.HTTPSConnection(rp_host)
            headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
            conn.request("POST", o.path, urllib.parse.urlencode({'q':json.dumps(q)}), headers)
            r3 = conn.getresponse()
            if r3.status==200:
                try:
                    data = r3.read()
                    reply3 = json.loads(data)
                    print("    response sent okay!",reply3)
                except json.decoder.JSONDecodeError:
                    print("    *** COULD NOT DECODE: ",data)
                continue        # do another loop without the delay
            else:
                print("could not send r3")
                print(r3.status)
                print(r3.read())
                exit(2)
        time.sleep(poll)

if __name__=="__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("rpserver",help="URL of RPSERVER",type=str)
    parser.add_argument("sparkui",help="URL of the sparkui",type=str)
    parser.add_argument("--poll",type=float,default=SENDER_POLL_DELAY,help="Polling time, in seconds")
    parser.add_argument("--debug",action='store_true',help='debug')
    args = parser.parse_args()
    run(rpserver=args.rpserver, sparkui=args.sparkui,poll=args.poll,debug=args.debug)
