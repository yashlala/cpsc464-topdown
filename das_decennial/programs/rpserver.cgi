#!/home/user007/anaconda3/bin/python3
import cgi, cgitb 
import os
import json
import sqlite3
import sys
import codecs
import base64
"""
Design:
RP Server.

Implements a simple rendezvous protocol server (RPSERVER).

This is the server script that runs on the webserver to proxy URI
requests from the WEB BROWSER to the SPARK SENDER.  The SPARK SENDER
periodically polls the RPSERVER to see if there are any URI requests. If
there are, it executs them locally and then sends the response back to
the RPSERVER, where it is stored in a DATABASE.  This script notices the
update in the database and sends the response back to the WEB BROWSER.

References:
https://stackoverflow.com/questions/881319/having-a-cgi-script-catch-all-requests-to-a-domain-with-apache

Response from spark sender is mainatined as a JSON object in resp.

Developed with SQLite3. Turns out we need a concurrent database.
$ pip install mysqlclient
"""

import MySQLdb
import dbconfig
import time

BROWSER_POLL_DELAY   = 0.5
BROWSER_WAIT_SECONDS = 10
CACHE_EXP_SECONDS = 30

MYNAME = "/" + os.path.basename(__file__)
CONTENTBASE64='contentbase64'
REQUEST_URI='REQUEST_URI'

SCHEMA="""
DROP TABLE IF EXISTS requests;
CREATE TABLE requests (
  id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
  t INTEGER,
  url VARCHAR(255) NOT NULL UNIQUE,
  resp LONGTEXT,
  INDEX id_i (id),
  INDEX url_i (url)
  );
"""

log = open("log.txt","a",buffering=1)

conn = MySQLdb.connect(dbconfig.dbserver, dbconfig.dbuser,
                       dbconfig.dbpassword, dbconfig.dbname)
c = conn.cursor()

def create_db():
    c.execute("DROP TABLE IF EXISTS requests")
    for stmt in SCHEMA.split(";"):
        if len(stmt.strip())>0:
            c.execute(stmt)

def get_bincontent(resp):
    """Returns content in binary form"""
    return base64.b64decode(resp[CONTENTBASE64])

def dump():
    c.execute("SELECT id,t,url,resp from requests order by id")
    for (id,t,url,r) in c:
        if r is None:
            resp = "None"
            bincontent = "n/a"
            ct = "n/a"
        else:
            resp = json.loads(r)
            bincontent = get_bincontent(resp)
            ct = resp.get('Content-Type','N/A')
        print("{:5} {} {:20} c-len:{:6} c-type: {} Content-Type: {}".format(
            id,t,url,len(bincontent),type(bincontent),ct))

def get_pending_work():
    """Return a dictionary with pending work"""
    c.execute('SELECT id,url FROM requests where resp is null order by id desc limit 1')
    row = c.fetchone()
    if row is not None:
        return {'id':row[0],
                'url':row[1]}
    return {}

def process_server_command(debug=False):
    """All messages to the server are encoded as a HTTP POST command with the field 'q'. It contains a JSON-encoded dictionary.
    Commands to the server are sent from the spark sender.

    Parameters:
    q['id']             = ID of the reply message; content fields below:
    q[CONTENTBASE64]   = Content of the response
    q['status']         = status code
    q['Content-type'] = Content-type of the response
    q['Content-length'] = Content-length of the response
    q['Content-encoding'] = Content-encoding

    Basically, we're going to put q in the database and send any attribute beginning with a capital letter to the browser in the header

    Returns:
    reply - dictionary of what's sent back
    """
    form  = cgi.FieldStorage()
    if 'q' not in form:
        log.write("Error: expecting form with a 'q' field.\n")
        exit(0)
    try:
        data = form.getvalue('q')
        q  = json.loads(data)
    except Exception as e:
        log.write(f"Error: Cannot decode JSON command:\n{data}\n{e}\n")
        exit(0)
    reply = {}

    if 'id' in q:
        resp = json.dumps(q)
        log.write(f">> received id={q['id']} url={q['url']}\n")
        c.execute('UPDATE requests set resp=%s,t=%s where id=%s', (resp,int(time.time()),q['id']))
        log.write(f">> resp={resp}\n")
        conn.commit()
        reply['updated'] = c.rowcount
        
    # Construct a server reply with the most recent query to satisfy
    reply = {**reply, **get_pending_work()}
    log.write(f">> sending reply {reply}\n")
    # And send it back to the spark sender
    sys.stdout.write("Content-type: application/text\r\n\r\n")
    sys.stdout.write(json.dumps(reply))
    sys.stdout.write("\r\n")
    exit(0)

def request_url(url):
    "Request the url and wait for it. It if is already in the cache, return it as a JSON object if it is not too old. Otherwise delete it and get another."
    pid = os.getpid()
    t0 = time.time()
    while time.time() < t0 + BROWSER_WAIT_SECONDS:
        log.write(f"{pid} {int(time.time())} checking for url={url}\n")
        c.execute('SELECT id,t,resp FROM requests where url=%s and resp is not null',
                  (url,))
        r = c.fetchone()
        if r is not None:
            (id,t,resp) = r
            if (time.time() - t) < CACHE_EXP_SECONDS:
                log.write(f"{pid} found in cache id={id} t={t} url={url} \n")
                try:
                    return json.loads(resp)
                except Exception as e:
                    log.write(f"invalid resp in database resp={resp} ")
                    raise e
            # It's out of date. Erase the response so it is gotten again
            log.write(f"{pid}  del in cache id={id} t={t} url={url} \n")
            c.execute('UPDATE requests set t=NULL,resp=NULL WHERE id=%s',(id,))
        else:
            log.write(f"{pid}  add to cache url={url} \n")
            c.execute('INSERT IGNORE INTO requests (url) VALUES (%s)', (os.environ[REQUEST_URI],))
        conn.commit()
        time.sleep(BROWSER_POLL_DELAY)
    log.write(f"{pid} {int(time.time())} timeout\n")
    exit(0)

def do_cgi( ):
    # Make sure output of text is encoded as UTF8, because that's what the scripts and the browser expect.
    # This should properly be taken from web headers, but is isn't.
    pid = os.getpid()
    cgitb.enable(logdir='/home/user007/logdir')
    if sys.stdout.encoding != 'UTF-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

    # If server.cgi was run, then process a server command
    uri = os.environ[REQUEST_URI]
    if uri.startswith( MYNAME ):
        log.write(f"{pid} --- process_server_command({uri}) ---\n")
        process_server_command()
        exit(0)

    # If we got here, we were being contacted by the web browser client.
    # Record the request in the database and then wait for a response.
    
    log.write(f"{pid} ================ BEGIN PROXY {uri} ================ {time.asctime()}\n")
    resp = request_url(uri)
    log.write(f"{pid} got resp\n")

    # We will write to the client in binary and manually convert the headers
    client = open(sys.stdout.fileno(), mode='wb', buffering=0) 

    # Output the headers we care about
    for header in ['Content-Type',
                   'X-Frame-Options',
                   'X-XSS-Protection',
                   'X-Content-Type-Options']:
        if header in resp:
            client.write("{}: {}\r\n".format(header,resp[header]).encode('utf-8'))
            log.write("{}: {}\r\n".format(header,resp[header]))

    client.write(b'\r\n')
    client.write( get_bincontent(resp) )
    client.close()

    # log
    log.write("\r\n{} ... {} ({} bytes)\n".format( bincontent[0:100],bincontent[:100],len(bincontent)))
    log.write(f"{os.getpid()} =======================================\n")
    exit(0)

        
if __name__=="__main__":
    import argparse
    a = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    a.add_argument("--dump","--list",help="Dump the database",action='store_true')
    a.add_argument("--clear",help="Clear the database",action='store_true')
    a.add_argument("--create", help="Create the database",action='store_true')
    a.add_argument('--cgi', help='Run the CGI handler and execute a poll', action='store_true')
    a.add_argument('--pending', help='Show pending work', action='store_true')
    args = a.parse_args()

    # If REQUEST_URI is set, then we are running as a cgi_script
    if (REQUEST_URI in os.environ):
        try:
            do_cgi( )
        except Exception as e:
            log.write(f"\nEXCEPTION\n{e}\n")

    if args.cgi:
        process_server_command()

    if args.pending:
        print(get_pending_work())
    if args.dump:
        dump()
    if args.clear:
        print("Clearing database")
        c.execute("DELETE FROM requests")
        c.execute("COMMIT")
    if args.create:
        print("Clearing and creating new database")
        create_db()
