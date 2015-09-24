## @namespace dummy_dstream.ds_beamdaq
#  @ingroup dummy_dstream
#  @brief Defines a project ds_beamdaq
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
from dstream.ds_api import ds_reader
import subprocess

## @class ds_beamdaq
#  @brief Script that fetches beam data
#  @details
#  Scripts gets the beam date between tbegin and tend for each (run, subrun)

class ds_beamdaq(ds_project_base):

    # Define project name as class attribute
    _project = 'ds_beamdaq'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(ds_beamdaq,self).__init__()

        self._nruns    = None
        self._istest   = None
        self._fcldir   = ''
        self._fclfile  = ''
        self._fclnew    = ''
        self._infodir  = ''
        self._infofile = ''
        self._logdir   = ''
        self._logfile  = ''
        self._beampath = ''
        self._timedir  = ''
        self._timefile = ''

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
            self._istest   = int(resource['ISTEST'])
            self._fcldir   = resource['FCLDIR']
            self._fclfile  = resource['FCLFILE']
            self._infodir  = resource['INFODIR']
            self._infofile = resource['INFOFILE']
            self._logdir   = resource['LOGDIR']
            self._logfile  = resource['LOGFILE']
            self._beampath = resource['BEAMPATH']
            if self._istest == 1:
            	self._timedir  = resource['TIMEDIR']
            	self._timefile = resource['TIMEFILE']
            self._fclnew   = self._fclfile.replace(".fcl","_local.fcl")

        ctr = self._nruns
        for x in self.get_runs(self._project,1):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            if self._istest == 1:
                # Read timestamp from text file for testing
                tsfname = '%s/%s'%(self._timedir,self._timefile%(run,subrun))
                if not os.path.isfile(tsfname):
                    self.info('Waiting for time stamp file %s'%tsfname)
                    continue
                if os.stat(tsfname).st_size == 0:
                    self.info('Waiting for time stamp info')
                    continue
                ts_file = open(tsfname)
                ts_lines = ts_file.readlines()
                bgn_line = ts_lines[0].split('"')
                end_line = ts_lines[-1].split('"')
                tbegin=datetime.datetime.strptime(bgn_line[1], "%a %b %d %H:%M:%S %Z %Y")
                tend=datetime.datetime.strptime(end_line[3], "%a %b %d %H:%M:%S %Z %Y")
            else:
                # Read timestamp from DB
                timestamp = self._api.run_timestamp('MainRun',run,subrun)
                #tbegin=datetime.datetime.strptime(timestamp[0], "%a %b %d %H:%M:%S %Z %Y")
                #tend=datetime.datetime.strptime(timestamp[1], "%a %b %d %H:%M:%S %Z %Y")
                tbegin=timestamp[0]
                tend=timestamp[1]

            # Report starting
            self.info('Getting beam data: run=%d, subrun=%d' % (run,subrun))
            self.info('  t0=%s, t1=%s' % (tbegin,tend))

            # Put parameters into fhicl
            fcl_file=open('%s/%s'%(self._fcldir,self._fclfile),'r')
            fcl_new =open(self._fclnew,'w')
            fcl_tmp = fcl_file.read()
            fcl_tmp = fcl_tmp.replace('BEAM_PATH',self._beampath)
            fcl_tmp = fcl_tmp.replace('INFO_PATH',self._infodir)
            fcl_tmp = fcl_tmp.replace('LOG_FILE','%s/%s'%(self._logdir,self._logfile%(run,subrun)))
            fcl_tmp = fcl_tmp.replace('vxx_yy_zz',os.environ["LARSOFT_VERSION"])
            fcl_new.write(fcl_tmp)
            fcl_file.close()
            fcl_new.close()

            cmd='bdaq_get --run-number %i --subrun-number %i --begin-time %i %i --end-time %i %i -f %s/%s'%(run,subrun,int(tbegin.strftime("%s")),0,int(tend.strftime("%s"))+1,0,self._fcldir,self._fclfile)
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

            self._nruns = int(resource['NRUNS'])
            self._infodir  = resource['INFODIR']
            self._infofile = resource['INFOFILE']

        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])
            status = 0
            
            fname='%s/%s'%(self._infodir,self._infofile%(run,subrun))
           
            # check that info was created and look for beam events
            # if beam events, check for beam file 
            self.info('Parse info file %s and check created files'%fname)
            if not os.path.isfile(fname):
                # change status?
                self.error('%s not created'%fname)
            info_file=open(fname)
            for line in info_file:
                if "events" in line:
                    wds=line.split()
                    if int(wds[2]) > 0:
                        beamfname = '%s/beam_%s_%07i_%05i.dat'%(self._infodir,wds[0],run,subrun)
                        if not os.path.isfile(beamfname):
                            # change to appropriate status
                            self.error('%s not created'%beamfname)
                            return
                        if os.stat(beamfname).st_size == 0:
                            # change to appropriate status
                            self.error('%s is empty'%beamfname)
                            return

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

    test_obj = ds_beamdaq()

    test_obj.process_newruns()

    test_obj.validate()



