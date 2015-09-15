import sys
from dstream.ds_api import death_star
from pub_dbi        import pubdb_conn_info
from pub_util       import pub_logger

from datetime import tzinfo, timedelta, datetime

runtable = "MainRun"
fname = "RunSubrun.txt"
f = open( fname, 'r' )

for l in f:
    run = int(l.split()[0])
    nsubrun = int(l.split()[1])

    for subrun in xrange(nsubrun):
        # print "Run %d, Subrun %d" %( run, subrun )
        ts = (datetime.now()+timedelta(seconds= 0)).strftime('%Y-%m-%d %H:%M:%S')
        te = (datetime.now()+timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')

        logger = pub_logger.get_logger('death_star')
        k=death_star( pubdb_conn_info.admin_info(),
                      logger )

        if not k.connect():
            sys.exit(1)

        k.insert_into_death_star(runtable, run, subrun, ts, te)

