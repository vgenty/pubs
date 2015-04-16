#!/usr/bin/env python
import argparse, sys
from dstream.ds_api import death_star
from pub_dbi        import pubdb_conn_info
from pub_util       import pub_logger

myparser = argparse.ArgumentParser(description='Create a run table.')

if not len(sys.argv) == 2:
    print 'Usage: %s TABLE_NAME' % sys.argv[0]
    sys.exit(1)

tname = sys.argv[1]
logger = pub_logger.get_logger('death_star')
k=death_star( pubdb_conn_info.admin_info(),
              logger )

if not k.connect():
    sys.exit(1)

k.create_death_star(tname)

