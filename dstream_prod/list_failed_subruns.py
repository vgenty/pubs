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


bad_runs = [5220, 5244, 5246, 5248, 5250, 5255, 5257, 5259, 5260]

# Loop over projects.

for probj in dbi.list_projects():
    project = probj._project
    print '\nProject %s:' % project
    
    # Query failed subruns for this project.

    query = 'select run, subrun from %s where status>1000' % project
    for run in bad_runs:
        query += ' and run != %d' % run
    query += ' order by run desc, subrun'
    ok = dbi.execute(query)
    if ok and dbi.nrows()>0:
        for row in dbi:
            run = int(row[0])
            subrun = int(row[1])
            print run, subrun

sys.exit(0)
