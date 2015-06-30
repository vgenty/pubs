#!/usr/bin/env python
# python import
import sys,os
# dstream import
from dstream.ds_api import ds_reader
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info

logger = pub_logger.get_logger('cfg_dump_project')

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

project_info_v=[]

for p in k.list_all_projects():
    if len(sys.argv) < 3 or p._server == sys.argv[2]:
        project_info_v.append(p)

if not project_info_v:
    logger.error('No project found...')
    sys.exit(1)

fout=open(out_file,'w')
for info in project_info_v:
    fout.write('\n')
    fout.write('PROJECT_BEGIN\n')
    fout.write('NAME      %s\n' % info._project)
    fout.write('COMMAND   %s\n' % info._command)
    fout.write('CONTACT   %s\n' % info._email)
    fout.write('SLEEP     %s\n' % info._sleep)
    fout.write('PERIOD    %s\n' % info._period)
    fout.write('SERVER    %s\n' % info._server)
    fout.write('RUNTABLE  %s\n' % info._runtable)
    fout.write('RUN       %s\n' % info._run)
    fout.write('SUBRUN    %s\n' % info._subrun)
    fout.write('ENABLE    %s\n' % info._enable)
    for key in info._resource:
        fout.write('RESOURCE %s => %s\n' % (key,info._resource[key]))
    fout.write('PROJECT_END\n')
fout.close()
sys.exit(0)
