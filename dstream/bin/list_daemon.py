#!/usr/bin/env python
# python import
import sys
# dstream import
from dstream.ds_api import ds_reader
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

logger = pub_logger.get_logger('list_daemon')

# DB interface for altering ProcessTable
k=ds_reader(pubdb_conn_info.reader_info(), logger)

# Connect to DB
k.connect()

# Define a project
daemons = k.list_daemon()

if not daemons:
    print 'No daemon found... aborting!'
    print
    sys.exit(1)

for x in daemons:

    print
    print x

print
sys.exit(0)

