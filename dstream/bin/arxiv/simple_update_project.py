# python import
import argparse, sys
# dstream import
from dstream.ds_api import ds_master
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

myparser = argparse.ArgumentParser(description='Routine to alter an existing project')

myparser.add_argument('--name', dest='name', action='store',
                      default='', type=str,
                      help='name of a new project')

myparser.add_argument('--command',dest='cmd',action='store',
                      default='', type=str,
                      help='command to be executed')

myparser.add_argument('--period',dest='period',action='store',
                      default=0,type=int,
                      help='duration between executions in second')

myparser.add_argument('--contact',dest='email',action='store',
                      default='',type=str,
                      help='contact email address')

myparser.add_argument('--enable',dest='enable',action='store',
                      default='DEFAULT',type=str,
                      help='enable this project')

args = myparser.parse_args()

logger = pub_logger.get_logger('update_project')

# DB interface for altering ProcessTable
k=ds_master(pubdb_conn_info.writer_info(), logger)
            
# Connect to DB
k.connect()

if not k.project_exist(args.name):
    logger.critical('Project %s does not exist!' % args.name)
    sys.exit(1)

orig_info = k.project_info(args.name)

if args.cmd: 
    orig_info._command = args.cmd

if args.period:
    orig_info._period = int(args.period)

if args.email:
    orig_info._email = args.email

if not args.enable == 'DEFAULT':
    if not args.enable in ['True','False','0','1']:
        logger.error("--enable argument must be 'True' or 'False' (you provided: '%s')" % args.enable)
        sys.exit(1)
    else:
        exec("orig_info._enable = bool(%s)" % args.enable)

# Define a project
k.update_project( orig_info )

