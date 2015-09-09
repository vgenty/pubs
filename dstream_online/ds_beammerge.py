## @namespace dummy_dstream.ds_beammerge
#  @ingroup dummy_dstream
#  @brief Defines a project ds_beammerge
#  @author zarko

# python include
import time,os,datetime
import json
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
import subprocess

## @class ds_beammerge
#  @brief Merge beam and detector data
#  @details
#  Script merges beam and det binary files

class ds_beammerge(ds_project_base):

    # Define project name as class attribute
    _project = 'ds_beammerge'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(ds_beammerge,self).__init__()

        self._nruns    = None
        self._logdir   = ''
        self._logfile  = ''
        self._infodir  = ''
        self._infofile = ''
        self._outdir  = ''
        self._outfile = ''
        self._beamdir  = ''
        self._beamfile = ''
        self._detdir  = ''
        self._detfile = ''
        self._parent_project = ''

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns    = int(resource['NRUNS'])
            self._logdir   = resource['LOGDIR']
            self._logfile  = resource['LOGFILE']
            self._infodir  = resource['INFODIR']
            self._infofile = resource['INFOFILE']
            self._beamdir  = resource['BEAMDIR']
            self._beamfile = resource['BEAMFILE']
            self._detdir   = resource['DETDIR']
            self._detfile  = resource['DETFILE']
            self._outdir   = resource['OUTDIR']
            self._outfile  = resource['OUTFILE']
            self._parent_project = resource['PARENT_PROJECT']

        ctr = self._nruns
        self.info('***************** %s'%(self._infofile%(1,1)))
        for x in self.get_xtable_runs([self._project, self._parent_project], 
                                      [            1,                    0]):
            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            # Report starting
            self.info('Getting info beam data: run=%d, subrun=%d' % (run,subrun))
            
            info_file=open('%s/%s'%(self._infodir,self._infofile%(run,subrun)))
            nfiles=0
            cmd=''
            for line in info_file:
                if "events" in line:
                    wds=line.split()
                    if (int(wds[2])>0):
                        self.info('Merging %i %s events'%(int(wds[2]),wds[0]))
                        cmd+=' -b %s/%s'%(self._beamdir,self._beamfile%(wds[0],run,subrun))
                        nfiles+=1

            if (nfiles==0):
                self.info('No beam events to merge!')
                return

            cmd+=' -d %s/%s'%(self._detdir,self._detfile%(run,subrun))
            cmd+=' -o %s/%s'%(self._outdir,self._outfile%(run,subrun))
            cmd+=' > %s/%s'%(self._logdir,self._logfile%(run,subrun))

            cmd='bdaq_merge '+cmd

            self.info('Run cmd: %s'%cmd)
            subprocess.call( cmd, shell = True )
            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = 2 )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break


    ## @brief access DB and validate finished runs
    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns    = int(resource['NRUNS'])
            self._beamdir  = resource['BEAMDIR']
            self._beamfile = resource['BEAMFILE']
            self._infodir  = resource['INFODIR']
            self._infofile = resource['INFOFILE']

        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])
            status = 0
            
            self.info('Parse into log file and check number events')
            logfname='%s/%s'%(self._logdir,self._logfile%(run,subrun))
            if not os.path.isfile(logfname):
                # change status
                self.error('% not created'%logfname)
                continue
            if os.stat(logfname).st_size == 0:
                # change status
                self.error('%s is empty'%logfname)
                continue
            log_file = open(logfname)
            last_line = log_file.readlines()[-1].split()
            log_num_evts = int(last_line[0])
            if not log_num_evts > 0:
                self.error('No events written')
                continue

            
            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = ds_beammerge()

    test_obj.process_newruns()

    test_obj.validate()



