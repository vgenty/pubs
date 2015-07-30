#! /bin/env python
## @namespace dstream_online.correct_file_status
#  @ingroup dstream_online
#  @brief Defines a project transfer
#  @author Kirby

# python include
import time, os, sys, subprocess
# pub_dbi package include
from pub_dbi import DBException
from pub_util import pub_logger
from pub_dbi        import pubdb_conn_info
# dstream class include
from dstream import DSException
from dstream import ds_project
from dstream import ds_status
from dstream.ds_api import ds_reader, ds_writer

if len(sys.argv)<5:
    print "You haven't given me enough to do!!!!!"
    print ""
    print "Usage: correct_file_status.py <table> <old_status> <new_status> <run> {subrun}"
    print ""
    print "The subrun is optional and if not specified indicates all subruns in the run."
    print "EXITING WITH STATUS = 1"
    sys.exit(1)

table = sys.argv[1]
old_status = int(sys.argv[2])
new_status = int(sys.argv[3])
run = int(sys.argv[4])
subrun = -1
if len(sys.argv)==6:
    subrun = int(sys.argv[5])

logger = pub_logger.get_logger('table')
reader = ds_reader(pubdb_conn_info.reader_info(), logger)
writer = ds_writer(pubdb_conn_info.writer_info(), logger)

if not reader.project_exist(table) :
    print 'The table you gave me does not exist: %s' % table

for x in reader.get_runs( table, old_status ):

    if run==x[0]:

        if subrun==-1:
            logger.info('In table %s, changing status of run %d, subrun %d from old_status=%d to new_status=%d' % (table, int(x[0]),int(x[1]), old_status, new_status) )
            updated_status = ds_status( project = table,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = new_status )
            writer.log_status(updated_status)

        else:
            if subrun==x[1]:
                logger.info('In table %s, changing status of run %d, subrun %d from old_status=%d to new_status=%d' % (table, int(x[0]),int(x[1]), old_status, new_status) )
                updated_status = ds_status( project = table,
                                    run     = int(x[0]),
                                    subrun  = int(x[1]),
                                    seq     = 0,
                                    status  = new_status )
                writer.log_status(updated_status)

print 'Finished updating table %s' % table
