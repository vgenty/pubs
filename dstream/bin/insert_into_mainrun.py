#!/usr/bin/env python
import argparse, sys
from dstream.ds_api import death_star
from pub_dbi        import pubdb_conn_info
from pub_util       import pub_logger

from datetime import tzinfo, timedelta, datetime

myparser = argparse.ArgumentParser(description='Insert individual run/subrun into MainRun DB table.')

ts = (datetime.now()+timedelta(seconds= 0)).strftime('%Y-%m-%d %H:%M:%S')
te = (datetime.now()+timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')

myparser.add_argument('--run', dest='run', action='store',
                      default=0, type=int,
                      help='run to fill a new MainRun')

myparser.add_argument('--subrun',dest='subrun',action='store',
                      default=0, type=int,
                      help='subrun to fill into a new MainRun')

myparser.add_argument('--timestart',dest='ts',action='store',
                      default=(datetime.now()+timedelta(seconds= 0)).strftime('%Y-%m-%d %H:%M:%S'), type=str,
                      help='run/subrun start to fill into a new MainRun')

myparser.add_argument('--timeend',dest='te',action='store',
                      default=(datetime.now()+timedelta(seconds=60)).strftime('%Y-%m-%d %H:%M:%S'), type=str,
                      help='run/subrun end to fill into a new MainRun')

args = myparser.parse_args()

logger = pub_logger.get_logger('death_star')
k=death_star( pubdb_conn_info.admin_info(),
              logger )

if not k.connect():
    sys.exit(1)

k.insert_into_death_star(args.run,args.subrun,ts,te)

