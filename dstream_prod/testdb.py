#! /usr/bin/env python

import sys
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


project='prod_reco_bnb_v4'
parent='prod_swizzle_merge_bnb_v3'
parent_status=10
query = 'select %s.status from %s,%s where %s.run=%s.run and %s.subrun=%s.subrun and %s.status=%d' % (project, project, parent, project, parent, project, parent, parent, parent_status)
print query
ok = dbi.execute(query)
ptable={}
if ok and dbi.nrows()>0:
    for row in dbi:
        status=row[0]
        if status not in ptable:
            ptable[status] = 1
        else:
            ptable[status] += 1
for status in ptable.keys():
    print status, ptable[status]

# Project statuses.

#stat = dbi.list_status()
#for prj in stat.keys():
#    print prj
#    print stat[prj]

sys.exit(0)
