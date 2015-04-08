#!/usr/bin/env python
# python import
import argparse, sys
# dstream import
from dstream.ds_api import ds_master
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

logger = pub_logger.get_logger('update_project')

check = True
if not len(sys.argv) == 3 and not (len(sys.argv)==4 and sys.argv[3] in ['0','1']):
    logger.error('Invalid argument. Usage: %s PROJECT_NAME VALUE [0|1]' % sys.argv[0])
    sys.exit(1)

project = sys.argv[1]
value   = sys.argv[2]
if len(sys.argv) == 4:
    check = bool(int(sys.argv[3]))

# DB interface for altering ProcessTable
k=ds_master(pubdb_conn_info.writer_info(), logger)
            
# Connect to DB
k.connect()

if not k.project_exist(project):
    logger.error('Project %s does not exist!' % project)
    sys.exit(1)

orig_info = k.project_info(project)

try:
    if not value.lower() in ['true','false','0','1']:
        raise ValueError
    orig_info._enable = ( value.lower() in ['true','1'] )
    if not orig_info.is_valid():
        raise ValueError
except ValueError:
    logger.error('Invalid argument: %s' % value)
    sys.exit(1)

if k.update_project(orig_info,check):
    sys.exit(0)
else:
    sys.exit(1)

