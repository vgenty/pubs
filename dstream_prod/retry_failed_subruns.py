#! /usr/bin/env python

import sys, os
os.environ['PUB_LOGGER_LEVEL'] = 'kLOGGER_ERROR'

from dstream.ds_api import ds_writer
from pub_dbi import pubdb_conn_info

# DB connection.

dbi = ds_writer(pubdb_conn_info.writer_info())
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
            5815, 5816, 5817, 5818, 6512, 6515, 6516, 6741, 6742, 6743, 6745, 6746]

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

    query = 'select run, subrun, status from %s where status>1000' % project
    for run in bad_runs:
        query += ' and run != %d' % run
    query += ' order by run desc, subrun'
    ok = dbi.execute(query)
    if ok and dbi.nrows()>0:
        rslist = []
        for row in dbi:
            run = int(row[0])
            subrun = int(row[1])
            status = int(row[2])
            rslist.append((run, subrun, status))

        for rs in rslist:
            run = rs[0]
            subrun = rs[1]
            status = rs[2]
            print run, subrun, status
            new_status = status - 1000 - status%10 + 1
            print 'Resetting status from %d to %d' % (status, new_status)
            update_query = 'update %s set status=%d where run=%d and subrun=%d and status=%d' % (
                project, new_status, run, subrun, status)
            ok = dbi.commit(update_query)
            if ok:
                print 'Update OK.'

sys.exit(0)
