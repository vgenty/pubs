import os,sys,copy
import numpy as np

import datetime

from dstream.ds_api import ds_reader
from pub_dbi.pubdb_conn import pubdb_conn
from pub_dbi import DBException, pubdb_conn_info


# define start/end time for search
start = datetime.datetime.strptime('2015-10-16T00:00:00','%Y-%m-%dT%H:%M:%S')
end   = datetime.datetime.strptime('2015-10-17T00:00:00','%Y-%m-%dT%H:%M:%S')
# output file
outfile = 'runs.csv'



utc_timezone = None

def getRunTimes(start=start,end=end,fout=outfile):

    fout = open(outfile,'w+')

    print 'Scan between times : [%s, %s]'%(start,end)

    k=ds_reader()
    k.connect()
    
    # get last run information
    last_run = k.get_last_run('MainRun')
    print 'last run on DB is : ',last_run

    # go backwards in run-time until a run
    # falls before the start time we are
    # interested in
    currentRun = last_run

    while 1:

        nsubrun   = k.get_last_subrun('MainRun',currentRun)

        # get beginning time for the run
        ss,se = k.run_timestamp('MainRun',currentRun,0)

        # get end time for the run
        es,ee = k.run_timestamp('MainRun',currentRun,nsubrun)

        # only if time-values are valid
        if ( (ss != None) and (ee != None) ):

            ss_utc = copy.copy(ss.tzinfo)
            ss     = ss_utc.fromutc(ss).replace(tzinfo=None)
            
            ee_utc = copy.copy(ee.tzinfo)
            ee     = ee_utc.fromutc(ee).replace(tzinfo=None)

            # if the start time comes before the end of the
            # time period that we are interested in looking
            # at -> ignore the run
            if (ss > end):
                currentRun -= 1
                continue
        
            print 'run start : %s'%ss
            print 'run end   : %s'%ee

            fout.write('%i,%s,%s,%i\n'%(currentRun,ss,ee,nsubrun))

            # if this run's end time is before the start time we care
            # to save information for -> break out of the loop
            if (ee < start):
                break

        # go to run before
        currentRun -= 1

    fout.close()


# unit test function
if __name__ == '__main__' :
    
    getRunTimes()
