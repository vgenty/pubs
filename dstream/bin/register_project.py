# python import
import argparse
# dstream import
from dstream.ds_api  import ds_master
from dstream.ds_data import ds_project
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

myparser = argparse.ArgumentParser(description='Routine to register a new project')

myparser.add_argument('--name', dest='name', action='store',
                      default='', type=str,
                      help='name of a new project')

myparser.add_argument('--command',dest='cmd',action='store',
                      default='', type=str,
                      help='command to be executed')

myparser.add_argument('--period',dest='period',action='store',
                      default='',type=int,
                      help='duration between executions in second')

myparser.add_argument('--contact',dest='email',action='store',
                      default='',type=str,
                      help='contact email address')

myparser.add_argument('--run',dest='run',action='store',
                      default=0,type=int,
                      help='starting run number')

myparser.add_argument('--subrun',dest='subrun',action='store',
                      default=0,type=int,
                      help='starting sub-run number')

args = myparser.parse_args()

# DB interface for altering ProcessTable
k=ds_master(pubdb_conn_info.writer_info(),
            pub_logger.get_logger('register_project'))

# Connect to DB
k.connect()

# Define a project
k.define_project( ds_project( project = args.name,
                              command = args.cmd,
                              period  = args.period,
                              email   = args.email,
                              run     = args.run,
                              subrun  = args.subrun,
                              enable  = True ) )

