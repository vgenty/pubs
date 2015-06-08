#!/usr/bin/env python
import argparse, sys
from dstream.ds_api import death_star
from pub_dbi        import pubdb_conn_info
from pub_util       import pub_logger
import time

myparser = argparse.ArgumentParser(description='Filling a run table w/ new run/subrun')

myparser.add_argument('--name', dest='name', action='store',
                      default='TestRun', type=str,
                      help='Name of a run table to create/alter')

myparser.add_argument('--run', dest='run', action='store',
                      default=0, type=int,
                      help='Run number to be added')

myparser.add_argument('--nsubruns',dest='nsubruns',action='store',
                      default=0, type=int,
                      help='Number of sub-runs to be added')

args = myparser.parse_args()

logger = pub_logger.get_logger('death_star')
k=death_star( pubdb_conn_info.admin_info(),
              logger )

if not k.connect():
    sys.exit(1)

# fake time stamp
ts = time.strftime( '%Y-%m-%d %H:%M:%S', time.localtime( time.time() ) )

for subrun in xrange(args.nsubruns):
    k.insert_into_death_star(args.name,args.run,subrun,ts,ts)

