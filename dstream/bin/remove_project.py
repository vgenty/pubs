# python import
import sys
# dstream import
from dstream.ds_api import ds_master
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

# DB interface for altering ProcessTable
k=ds_master(pubdb_conn_info.writer_info(),
            pub_logger.get_logger('define_project'))

# Connect to DB
k.connect()

# Define a project
k.remove_project(sys.argv[1])

