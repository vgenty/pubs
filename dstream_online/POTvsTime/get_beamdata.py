import os,sys,commands
import numpy
import datetime 

# verbosity flag
VERBOSE=False

# define start/end time for search
start = datetime.datetime.strptime('2015-10-16T00:00:00','%Y-%m-%dT%H:%M:%S')
end   = datetime.datetime.strptime('2015-10-17T00:00:00','%Y-%m-%dT%H:%M:%S')
# output file
outfile = 'beam.csv'

def getBeamData(start=start,end=end,outfile=outfile):

    tstart = start.strftime('%Y-%m-%dT%H:%M:%S')
    tend   = end.strftime('%Y-%m-%dT%H:%M:%S')

    webpage = '\'http://ifb-data.fnal.gov:8089/ifbeam/data/data?v=E:TOR875&e=e,1d&t0=%s&t1=%s&f=csv\''%(tstart,tend)


    cmd = 'wget -O %s %s'%(outfile,webpage)

    if VERBOSE:
        print
        print 'saving beam info between times: [%s, %s]'%(start,end)
        print 'saving to output file %s'%outfile
        print cmd
    
    stdout = commands.getoutput(cmd)
    if VERBOSE:
        print stdout
        print 'done!'
        print

if __name__ == '__main__' :
    
    getBeamData()
    
