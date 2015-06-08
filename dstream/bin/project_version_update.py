#!/usr/bin/env python
import sys
from dstream.ds_api import ds_master
from pub_dbi        import pubdb_conn_info
from pub_util       import pub_logger
import time

if not len(sys.argv) == 2:
    print
    print 'Usage: %s PROJECT_NAME' % sys.argv[0]
    print
    sys.exit(1)
    
logger = pub_logger.get_logger('ds_master')
k=ds_master( pubdb_conn_info.admin_info(),
             logger )

if not k.connect():
    sys.exit(1)

k.project_version_update(sys.argv[1])

sys.exit(0)
