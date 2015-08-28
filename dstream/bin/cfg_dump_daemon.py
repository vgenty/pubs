#!/usr/bin/env python
# python import
import sys,os
# dstream import
from dstream.ds_api import ds_reader
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

logger = pub_logger.get_logger('cfg_dump_daemon')

if not len(sys.argv) in [2,3]:
    
    logger.error('Invalid argument. Usage: %s OUTPUT_FILENAME [SERVER]' % sys.argv[0])
    sys.exit(1)

out_file = sys.argv[1]
if os.path.isfile(out_file) or os.path.isdir(out_file):
    logger.error('File/Dir already exists: %s' % out_file)
    sys.exit(1)

# DB interface for altering ProcessTable
k=ds_reader(pubdb_conn_info.reader_info(), logger)
# Connect to DB
k.connect()

daemon_info_v=[]
if len(sys.argv)==3:
    info = k.daemon_info(sys.argv[2])
    if not info:
        logger.error('No daemon for the server: %s' % sys.argv[2])
        sys.exit(1)
    daemon_info_v.append(info)
else:
    daemon_info_v = k.list_daemon()

if not len(daemon_info_v):
    logger.info('No daemon found.')
    sys.exit(0)

fout=open(out_file,'w')
for info in daemon_info_v:
    fout.write('\n')
    fout.write('DAEMON_BEGIN\n')
    fout.write('SERVER        %s\n' % info._server)
    fout.write('MAX_CTR       %s\n' % info._max_proj_ctr)
    fout.write('LIFETIME      %s\n' % info._lifetime)
    fout.write('CLEANUP_TIME  %s\n' % info._cleanup_time)
    fout.write('LOG_LIFETIME  %s\n' % info._log_lifetime)
    fout.write('SYNC_TIME     %s\n' % info._runsync_time)
    fout.write('UPDATE_TIME   %s\n' % info._update_time)
    fout.write('CONTACT       %s\n' % info._email)
    fout.write('ENABLE        %s\n' % info._enable)
    fout.write('DAEMON_END\n')

fout.close()
sys.exit(0)
