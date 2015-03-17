## @namespace dummy_dstream.dummy_nubin_xfer
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_nubin_xfer
#  @author kazuhiro

# python include
import time, os, shutil, sys, gc
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from ROOT import *
import time, json
import samweb_cli

gSystem.Load("libudata_types.so")

## @Class dummy_nubin_xfer
#  @brief A dummy nu bin file xfer project
#  @details
#  This project opens daq bin files mv'd by mv_assembler_daq_files project, opens it and extracts some metadata,\n
#  stuffs this into and writes out a json file.
#  Next process registers the file with samweb *.ubdaq and mv's it to a dropbox directory for SAM to whisk it away...
class get_assembler_metadata(ds_project_base):


    # Define project name as class attribute
    _project = 'get_assembler_metadata'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(get_assembler_metadata,self).__init__()

        self._nruns = None
        self._out_dir = ''
        self._outfile_format = ''
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._jrun = 0
        self._jsubrun = 0
        self._jstime = 0
        self._jsnsec = 0
        self._jetime = 0
        self._jensec = 0
        self._jeevt = -12
        self._jsevt = -12

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [1,0]):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            
            # Check input file exists. Otherwise report error
            in_file = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

# Any effort to read a 2nd evt brings a system error, without all garbage collecting below and re-instantiating DaqFile(). 
# Further, any effort -- sometimes -- to construct a second Integral object and/or run integrate() a 2nd time does the same. 

#
# Looks fine now, but if there are new troubles: run this project with NRUNS=1
#
            if os.path.isfile(in_file):
                self.info('Found %s' % (in_file))
#                shutil.copyfile(in_file,out_file)

                try:

                    d = DaqFile(in_file)
                    e = d.GetEventObj(d.NumEvents()-1) 
                    integ = Integral()
                    print "Load last event in file."
                    integ.integrate(e)
                    print "Loaded."
                    self._jrun = integ.m_run
                    self._jsubrun = integ.m_subrun
                    self._jetime = time.ctime(integ.m_time_of_cur_event)
                    self._jensec = integ.m_time_of_cur_event.GetNanoSec()
                    self._jeevt = integ.m_event
                    del integ
                    del e
                    del d
                    gc.collect()
                    d = DaqFile(in_file)
                    e2 = d.GetEventObj(0)
                    integ = Integral()
                    print "Load first event in file."
                    integ.integrate(e2)
                    print "Loaded"
                    self._jstime = time.ctime(integ.m_time_of_first_event)
                    self._jsnsec = integ.m_time_of_first_event.GetNanoSec()
                    self._jsevt = integ.m_event
                    del integ
                    del e2
                    del d
                    gc.collect()

                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    # print "Give some null properties to this meta data"
                    print "Give this file a status 100"
                    status = 100
                    
                fsize = os.path.getsize(in_file)
                jsonData={'file_name': os.path.basename(in_file), 'file_type': "data", 'file_size': fsize, 'file_format': "binaryraw-uncompressed", 'runs': [ [self._jrun,  self._jsubrun, 'physics'] ], 'first_event': self._jsevt, 'file_date': str(self._jetime), 'stime': str(self._jstime), 'snsec': str(self._jsnsec), 'last_event': self._jeevt, 'etime': str(self._jetime), 'ensec': str(self._jensec), 'group': 'uboone', "crc": { "crc_value":"116146095L",  "crc_type":"adler 32 crc type" }, "application": {  "family": "online",  "name": "assembler", "version": "v6_00_00" } }
#, "params": { "MicroBooNE_MetaData": {'bnb.horn_polarity':"forward", 'numi.horn1_polarity':"forward",'numi.horn2_polarity':"forward", 'detector.pmt':"off", 'trigger.name':"open" } }
#                print jsonData

                if not status==100:
                    with open(out_file, 'w') as ofile:
                        json.dump(jsonData, ofile, sort_keys = True, indent = 4, ensure_ascii=False)
                        try:
                            samweb = samweb_cli.SAMWebClient(experiment="uboone")
                            samweb.validateFileMetadata(json_file) # this throws/raises exception
                            status = 2
                        except:
                            print "Problem with samweb metadata: ", jsonData
                            print sys.exc_info()[0]
                            "                            status=100
 

            else:
                status = 100

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

    ## @brief access DB and retrieves processed run for validation
    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('validating run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            in_file = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

            if os.path.isfile(out_file):
#                os.system('rm %s' % in_file)
                status = 0
            else:
                status = 100

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = int(x[2]),
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

    ## @brief access DB and retrieves runs for which 1st process failed. Clean up.
    def error_handle(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project,100):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('cleaning failed run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1

            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

            if os.path.isfile(out_file):
                os.system('rm %s' % out_file)

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':
    gSystem.Load("libudata_types.so")
    test_obj = get_assembler_metadata()

    test_obj.process_newruns()

    test_obj.error_handle()

    test_obj.validate()

