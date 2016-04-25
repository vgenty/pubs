#! /usr/bin/env python

import sys, os
import project_utilities
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

# Hard-wired parameters.

complete_status = 10
prjname='prod_anatree_ext_bnb_v5'
dim = 'file_type data and file_format root and data_tier root-tuple and ub_project.name anatree_outextbnb and ub_project.stage anatree and ub_project.version prod_v05_08_00 and availability: anylocation'

# Get samweb

samweb = project_utilities.samweb()


# Get project from command line (if any).

#prjname = None
#if len(sys.argv) > 1:
#    prjname = sys.argv[1]

# Loop over projects.

for probj in dbi.list_projects():
    project = probj._project
    if prjname != None and prjname != project:
        continue
    print '\nProject %s:' % project
    
    # Query completed subruns for this project.

    query = 'select run, subrun from %s where status=%d' % (project, complete_status)
    query += ' order by run desc, subrun'
    ok = dbi.execute(query)
    if ok and dbi.nrows()>0:
        rs = []
        for row in dbi:
            rs.append((int(row[0]), int(row[1])))
        for run, subrun in rs:
            print run, subrun
            rs_dim = dim + ' and run_number %d.%d' % (run, subrun)
            has_location = True
            filelist = samweb.listFiles(dimensions=rs_dim)
            for filename in filelist:
                locs = samweb.locateFile(filename)
                if len(locs) == 0:
                    has_location = False
            if not has_location:
                print filename
                update_query = 'update %s set status=%d where status=%d and run=%d and subrun=%d' % (
                    prjname, complete_status-2, complete_status, run, subrun)
                ok = dbi.commit(update_query)
                if ok:
                    print 'Update OK.'
            

sys.exit(0)
