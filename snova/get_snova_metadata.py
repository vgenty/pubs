# psycopg2 include for
import psycopg2
# python include
import time, os, shutil, sys, gc
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_multiprocess
from ds_online_constants import *
from ds_online_util import *
import datetime, json
from snova_util import *
import subprocess

class get_metadata( ds_project_base ):


    # Define project name as class attribute
    _project = 'get_metadata'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( get_metadata, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise DSException

        self._project = arg

        self._nruns = None
        self._in_dir = str("")
        self._infile_format = str("")
        self._parent_project = str("")
        self._pubsver = str("v6_00_00")

        self._action_map = { kUBDAQ_METADATA : self.process_ubdaq_files }

        self._metadata_type = kMAXTYPE_METADATA        
        self._max_proc_time = int(50)
        self._parallelize = int(0)
        self._min_run = int(0)
        self._max_run = int(1e8)
        self._nretrial = 5

        self._seb = str("")
        self._remote_host = str("")
        self._json_location = str("")

        self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])

        self._parent_project = resource['PARENT_PROJECT']

        if not 'METADATA_TYPE' in resource:
            raise DSException('Metadata type not specified in resource...')

        if not is_valid_metadata_type(resource['METADATA_TYPE']):
            raise DSException('Invalid metadata type: %s' % resource['METADATA_TYPE'])

        exec('self._metadata_type = int(%s)' % resource['METADATA_TYPE'])

        if not self._metadata_type in self._action_map:
            raise DSException('Specified action type not supported! (%d)' % self._metadata_type)

        if 'PARALLELIZE' in resource:
            self._parallelize = int(resource['PARALLELIZE'])

        if 'MAX_PROC_TIME' in resource:
            self._max_proc_time = int(resource['MAX_PROC_TIME'])

        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])

        if 'MAX_RUN' in resource:
            self._max_run = int(resource['MAX_RUN'])

        self._ref_project = resource['PARENT_PROJECT']

        self._seb = resource["SEB"]

        # location at ws02 for metadata file
        self._json_location = resource['JSON_LOCATION']

        if "REMOTE_HOST" in resource:
            self._remote_host = resource["REMOTE_HOST"]
        
    def get_action(self):

        if not self._metadata_type in self._action_map:
            raise DSException('Specified action type not supported! (%d)' % self._metadata_type)

        return self._action_map[self._metadata_type]
    
    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        try:
            if not self.connect():
                self.error('Cannot connect to DB! Aborting...')
                return
        except psycopg2.InternalError:
            self.error('Empty table! Return.')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Here, self._nruns=%d ... ' % (self._nruns))

        action = self.get_action()

        runid_v    = []
        infile_v   = []
        outfile_v  = []
        checksum_v = []

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [kSTATUS_INIT,kSTATUS_DONE]):

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: break
            if run > self._max_run: continue

            # Counter decreases by 1
            if ctr <= 0: break

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            status = kSTATUS_INIT

            ref_status = self._api.get_status( ds_status( self._ref_project, run, subrun, 0 ) )

            if not ref_status._status == 0:
                self.warning('Reference project (%s) not yet finished for run=%d subrun=%d' % (self._ref_project,run,subrun))
                continue

            if not ref_status._data:
                self.error('Checksum from project %s unknown for run=%d subrun=%d' % (self._ref_project,run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_REFERENCE_PROJECT_DATA ) )

            file_,checksum_ = ref_status._data.split(':')
            self.info("Fetched file: %s and checksum: %s"%(file_,checksum_))
            filelist = [file_]

            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND ) )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE ) )

                continue

            checksum_v.append(checksum_)
            infile_v.append(filelist[0])
            out_file_name = insert_sebname(infile_v[-1],self._seb)
            outfile_v.append('%s.json' % out_file_name)
            runid_v.append((run,subrun))
            ctr -= 1

        action = self.get_action()

        status_v = action(infile_v,runid_v,checksum_v)

        if not len(status_v) == len(runid_v):
            raise DSException('Logic error: status vector from %s must match # of run ids!' % str(action))
        
        for index_run in xrange(len(status_v)):

            run, subrun  = runid_v[index_run]
            status, data = status_v[index_run]
            
            if data is None: data = ""

            fout = None

            if data and type(data) == type(dict()):
                fname = os.path.basename(infile_v[index_run])
                out_file_name = insert_sebname(fname,self._seb)
                fout = open('%s/%s/%s.json' % (self._json_location,self._seb,out_file_name), 'w+')
                json.dump(data, fout, sort_keys = True, indent = 4, ensure_ascii=False)
                data = ''

            # Create a status object to be logged to DB (if necessary)
            data = infile_v[index_run]
            self.log_status ( ds_status( project = self._project,
                                         run     = run,
                                         subrun  = subrun,
                                         seq     = 0,
                                         status  = status,
                                         data    = data ) )
        

    def process_ubdaq_files(self,in_file_v,runid_v,checksum_v=[]):

        if not len(in_file_v) == len(runid_v):
            raise DSException('Input file list and runid list has different length!')

        # Now extract MetaData for successful ones
        status_v = []

        for index_run in xrange(len(in_file_v)):

            status_v.append((3,None))
            in_file = in_file_v[index_run]
            in_file_split = os.path.basename(in_file).split('-')
            
            if in_file_split[0]=='PhysicsRun':
                run_type='physics'
            elif in_file_split[0]=='NoiseRun':
                run_type='noise'
            elif in_file_split[0]=='CalibrationRun':
                run_type='calibration'
            elif in_file_split[0]=='PMTCalibrationRun':
                run_type='pmtcalibration'
            elif in_file_split[0]=='TPCCalibrationRun':
                run_type='tpccalibration'
            elif in_file_split[0]=='LaserCalibrationRun':
                run_type='lasercalibration'
            elif in_file_split[0]=='BeamOffRun':
                run_type='beamoff'
            elif in_file_split[0]=='BeamOff':
                run_type='beamoff'
            elif in_file_split[0]=='TestRun':
                run_type='test'
            elif in_file_split[0]=="2StreamTest":
                run_type='test'
            else:
                run_type='unknown'
                
            SS= "stat -c %%s %s" %in_file
            file_size = long(exec_ssh("vgenty",self._remote_host,SS)[0])

            run, subrun = runid_v[index_run]

            checksum = ""
            if checksum_v:
                checksum = checksum_v[index_run]
            else:
                ref_status = self._api.get_status( ds_status( self._ref_project, run, subrun, 0 ) )

                if not ref_status._status == 0:
                    self.warning('Reference project (%s) not yet finished for run=%d subrun=%d' % (self._ref_project,run,subrun))
                    status_v[index_run] = (kSTATUS_INIT,None)
                    continue

                if not ref_status._data:
                    self.error('Checksum from project %s unknown for run=%d subrun=%d' % (self._ref_project,run,subrun))
                    status_v[index_run] = (kSTATUS_ERROR_REFERENCE_PROJECT_DATA,None)
                    continue

                checksum = ref_status._data.split(":")[1]

            # Can we put a "bad" metadata as a default contents for "bad file"? If we can, comment out continue
            badJsonData = { 'file_name': os.path.basename(in_file), 
                            'file_type': "data", 
                            'file_size': file_size, 
                            'file_format': "binaryraw-compressed", 
                            'runs': [ [run,  subrun, run_type] ], 
                            'first_event': 0,
                            'start_time': '1970-01-01T00:00:00',
                            'end_time': '1970-01-01T00:00:00',
                            'last_event': 0,
                            'group': 'uboone', 
                            "crc": { "crc_value":str(checksum),  "crc_type" : "adler 32 crc type" }, 
                            "application": {  "family": "online",  "name": "assembler", "version": 'unknown' }, 
                            "data_tier": "raw", "event_count": 0,
                            "ub_project.name": "online", 
                            "ub_project.stage": "assembler", 
                            "ub_project.version": self._pubsver,
                            'online.start_time_usec': '-1',
                            'online.end_time_usec': '-1'}
            
            last_event_cout,first_event_cout = (-1,-1)
            ver = 'unknown'
            sevt = eevt = 20
            stime = etime = '1970-01-01:T00:00:00'
            stime_usec = etime_usec  = -1
            stime_secs = etime_secs = -1
            sdaqclock = edaqclock = -1

            gps_etime = gps_etime_usec = gps_etime_secs = -1
            gps_stime = gps_stime_usec = gps_etime_usec = -1
            invalid_format = False
            num_events = eevt - sevt + 1
            if num_events < 0: num_events = 0

            # set the input file and output file

            in_file_name  = os.path.basename(in_file)
            out_file_name = insert_sebname(in_file_name,self._seb)
            
            run_type='test'
            first_event=1

            #TIME_FORMAT='%d-%02d-%02d:T%02d:%02d:%02d'                                                                                                                                                  
            tmp_str = str("")
            tmp_str = in_file_name.split("-")[2].split('_')

            start_time = datetime_obj = datetime.datetime.strptime(" ".join(tmp_str),'%Y %m %d %H %M %S')
            tmp_str = str("")

            start_time = start_time.isoformat()
            end_time=1
            last_event=1
            daq_version="v6_21_05"
            event_count=1
            ub_project_version="v6_00_00"
            gps_end_time_usec=1
            gps_start_time_usec=2
            
            jsonData = { 'file_name': out_file_name,
                         'file_type': "data",
                         'file_size': file_size,
                         'file_format': "snbinaryraw-suppressed",
                         'runs': [ [run,  subrun, run_type] ],
                         'first_event': first_event,
                         'start_time': start_time,
                         'end_time': end_time,
                         'last_event':last_event,
                         'group': 'uboone',
                         "crc": { "crc_value" : str(checksum),  "crc_type":"adler 32 crc type" },
                         "application": {  "family": "online",  "name": "sn_daq", "version": daq_version },
                         "data_tier": "raw", "event_count": event_count,
                         "ub_project.name": "online",
                         "ub_project.stage": "sn_binary",
                         "ub_project.version": ub_project_version,
                         'online.start_time_usec': str(gps_start_time_usec),
                         'online.end_time_usec': str(gps_end_time_usec) }

            status_v[index_run] = (kSTATUS_TO_BE_VALIDATED,jsonData)

        return status_v


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

            status = kSTATUS_INIT

            ref_status = self._api.get_status( ds_status( self._ref_project, run, subrun, 0 ) )
            file_ = ref_status._data.split(":")[0]
            filelist=[ file_ ]

            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND ) )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_OUTPUT_FILE_NOT_UNIQUE ) )


            in_file_name = os.path.basename(filelist[0])
            out_file_name=insert_sebname(in_file_name,self._seb)

            out_file_path = '%s/%s/%s.json' % (self._json_location,self._seb,out_file_name)

            if os.path.isfile(out_file_path):
                self.info("Ok see in_file %s and out_file %s"%(in_file_name,out_file_name))
                status = 0
            else:
                self.info("JSON dump failed!")
                status = 100

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


# A unit test section
if __name__ == '__main__':
    proj_name = sys.argv[1]
    test_obj = get_metadata( proj_name )
    test_obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
    test_obj.get_resource()
    test_obj.process_newruns()
    test_obj.validate()
    test_obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
