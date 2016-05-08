#! /usr/bin/env python

import sys, os
import project_utilities

def printrange(range):
    range.sort()
    inrange = False
    minrange = 0
    maxrange = 0
    sep=''
    for ele in range:
        if inrange:
            if ele == maxrange+1:
                maxrange = ele
            else:
                if minrange == maxrange:
                    print '%s %d' % (sep, minrange),
                else:
                    print '%s %d-%d' % (sep, minrange, maxrange),
                sep = ','
                minrange = ele
                maxrange = ele
        else:
            minrange = ele
            maxrange = ele
            inrange = True
    if inrange:
        if minrange == maxrange:
            print '%s %d' % (sep, minrange)
        else:
            print '%s %d-%d' % (sep, minrange, maxrange)
    else:
        print
        


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

complete_status = 20
prjname='prod_reco_bnb_v5'
dim = 'file_type data and data_tier reconstructed and ub_project.name reco_outbnb and ub_project.stage reco2 and ub_project.version prod_v05_08_00'
min_run=4952
max_run=1000000

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

    rundict = {}
    
    # Query completed subruns for this project.

    query = 'select run, subrun from %s where status=%d' % (project, complete_status)
    query += ' order by run desc, subrun'
    ok = dbi.execute(query)
    if ok and dbi.nrows()>0:
        rs = []
        for row in dbi:
            rs.append((int(row[0]), int(row[1])))
        for run, subrun in rs:
            if run < min_run or run > max_run:
                continue
            #print run, subrun
            rs_dim = dim + ' and run_number %d.%d' % (run, subrun)
            filelist = samweb.listFiles(dimensions=rs_dim)
            for file in filelist:
                #print file
                md = samweb.getMetadata(file)
                for r in md['runs']:
                    run = r[0]
                    subrun = r[1]
                    #print run, subrun
                    if run not in rundict.keys():
                        rundict[run] = []
                    if subrun not in rundict[run]:
                        rundict[run].append(subrun)
    for run in rundict.keys():
        print 'Run %d, Subruns:' % run,
        printrange(rundict[run])
            

sys.exit(0)
