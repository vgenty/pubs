#! /usr/bin/env python

import sys, os
os.environ['PUB_LOGGER_LEVEL'] = 'kLOGGER_ERROR'

from dstream.ds_api import ds_reader
from pub_dbi import pubdb_conn_info

# DB connection.

dbi = ds_reader(pubdb_conn_info.reader_info())
try:
    dbi.connect()
    print "Connection successful."
except:
    print "Connection failed."
    sys.exit(1)


# Get project from command line (if any).

prjname = None
if len(sys.argv) > 1:
    prjname = sys.argv[1]

# Loop over projects.

for probj in dbi.list_projects():
    project = probj._project
    if prjname != None and prjname != project:
        continue
    print '\nProject %s:' % project
    
    # Query failed subruns for this project.

    query = 'select run, subrun from %s where status=3' % project
    query += ' order by run desc, subrun'
    ok = dbi.execute(query)
    if ok and dbi.nrows()>0:
        for row in dbi:
            run = int(row[0])
            subrun = int(row[1])
            print run, subrun

sys.exit(0)
