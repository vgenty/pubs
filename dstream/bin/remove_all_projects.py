#!/usr/bin/env python
import sys
from dstream.ds_api import death_star
from pub_dbi        import pubdb_conn_info
from pub_util       import pub_logger

logger = pub_logger.get_logger('death_star')
k=death_star( pubdb_conn_info.admin_info(),
              logger )

if not k.connect():
    sys.exit(1)

k.superbeam()

