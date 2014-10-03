# python import
import sys
# dstream import
from dstream.ds_api import ds_master
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

logger = pub_logger.get_logger('define_project')

if not len(sys.argv) == 2:
    logger.error('Usage: %s $PROJECT_NAME' % sys.argv[0])
    sys.exit(1)

# DB interface for altering ProcessTable
k=ds_master(pubdb_conn_info.writer_info(), logger)

# Connect to DB
k.connect()

# Define a project
k.remove_project(sys.argv[1])
sys.exit(0)
