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

## @class ds_beamdaq
#  @brief Script that fetches beam data
#  @details
#  Scripts gets the beam date between tbegin and tend for each (run, subrun)
#  We also create a second json file here which is identical to the original one
#  from which we get the mentioned-tbegin and tend, but fills out the beam data.

class ds_beamdaq(ds_project_base):

    # Define project name as class attribute
    _project = 'ds_beamdaq'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(ds_beamdaq,self).__init__()

        self._nruns    = None
        self._infodir  = ''
        self._infofile = ''
        self._jsondir  = ''
        self._jsonfile = ''
        self._jsonfile_out = ''

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
            self._infodir  = resource['INFODIR']
            self._infofile = resource['INFOFILE']
            self._jsondir  = resource['JSONDIR']
            self._jsonfile = resource['JSONFILE']
            self._jsonfile_out = resource['JSONFILEOUT']

        ctr = self._nruns
        for x in self.get_runs(self._project,1):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            jsonfname='%s/%s'%(self._jsondir,self._jsonfile%(run,subrun))
            jsonfname_out='%s/%s'%(self._jsondir,self._jsonfile_out%(run,subrun))
            if (not os.path.isfile(jsonfname)):
                self.info('Waiting for json file %s'%jsonfname)
                continue
            json_file=open(jsonfname)
            json_data=json.load(json_file)

            # For now, let's over-ride the begin, end times, cuz those we're getting
            # from the DAQ test subruns may be garbage or have no corresponding beamdata.
            # EC, 13-Feb-2015.
            tbegin=datetime.datetime.strptime(json_data["stime"], "%a %b %d %H:%M:%S %Z %Y")
            tend=datetime.datetime.strptime(json_data["etime"], "%a %b %d %H:%M:%S %Z %Y")
            json_file.close()

            json_file=open(jsonfname_out)
            json_data=json.load(json_file)
            # Fill in what had formerly been placeholders with known beam data.
#            json_data["params"]: { "MicroBooNE_MetaData": {'bnb.horn_polarity':"forward", 'numi.horn1_polarity':"forward",'numi.horn2_polarity':"forward" } }
            json_data.dump()
            json.dump(json_data, json_file, sort_keys = True, indent = 4, ensure_ascii=False)
            json_file.close()

            # Report starting
            self.info('Getting beam data: run=%d, subrun=%d' % (run,subrun))
            self.info('  t0=%s, t1=%s' % (tbegin,tend))

            cmd='bdaq_get --run-number %i --subrun-number --%i --begin-time %i %i --end-time %i %i'%(run,subrun,int(tbegin.strftime("%s")),0,int(tend.strftime("%s"))+1,0)
            self.info('Run cmd: %s'%cmd)
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
            jsonfname_out='%s/%s'%(self._jsondir,self._jsonfile_out%(run,subrun)) 
            self.info('Parse info file %s and check created files'%fname)
            self.info('Parse json_out file %s'%jsonfname_out)

            if os.path.isfile('%s/%s' % (self._jsondir, self._jsonfile_out % (run,subrun))):
                self.info('beam json file created: run=%d, subrun=%d ...' % (run,subrun))
            else:
                self.error('error with json output file on run: run=%d, subrun=%d ...' % (run,subrun))
                status = 1

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



