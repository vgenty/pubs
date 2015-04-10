#!/usr/bin/env python
# python import
import sys
# dstream import
from dstream.ds_api import ds_reader
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

sname = ''
if len(sys.argv) > 1:
    sname = sys.argv[1]

logger = pub_logger.get_logger('list_log')

# DB interface for altering ProcessTable
k=ds_reader(pubdb_conn_info.reader_info(), logger)

# Connect to DB
k.connect()

# Define a project
projects = k.list_daemon_log(sname)

if not projects: 
    print 'No project found... aborting!'
    print
    sys.exit(1)

for x in projects:

    print x

print
sys.exit(0)

