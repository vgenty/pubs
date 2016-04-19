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


bad_runs = [5113, 5220, 5244, 5246, 5248, 5250, 5255, 5257, 5259, 5260,
            5267, 5268, 5269,
            5348, 5349, 5350, 5351, 5352,
            5677, 5678,
            5785, 5786, 5787, 5788, 5789, 5790, 5791, 5792, 5793, 5794,
            5795, 5796, 5797, 5798, 5799, 5800, 5801, 5802, 5803, 5804,
            5805, 5806, 5807, 5808, 5809, 5810, 5811, 5812, 5813, 5814,
            5815, 5816, 5817, 5818]

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
