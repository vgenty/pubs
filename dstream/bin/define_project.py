import sys

import dstream
from dstream.ds_api import ds_master
from pub_util import pub_logger
from pub_dbi  import pubdb_conn_info
k=ds_master(pubdb_conn_info.writer_info(),
            pub_logger.get_logger('define_project'))
k.connect()
k.define_project(sys.argv[1],sys.argv[2],int(sys.argv[3]),sys.argv[4])

