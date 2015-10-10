## @namespace dstream_online.get_metadata
#  @ingroup dstream_online
#  @brief Defines a project dstream_online.get_metadata
#  @author echurch, yuntse

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
from ROOT import *
import datetime, json
import samweb_cli
import samweb_client.utility
import extractor_dict
import pdb
import subprocess
# script module tools
from scripts import find_run

## @Class dstream_online.get_metadata
#  @brief Get metadata from a binary or a swizzled file
#  @details
#  This project opens daq bin files mv'd by mv_assembler_daq_files project, 
#  opens it and extracts some metadata,\n
#  stuffs this into and writes out a json file.
#  Next process registers the file with samweb *.ubdaq and mv's it to a dropbox directory for SAM to whisk it away...
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
#        self._out_dir = ''
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
        self._jver = -12
        self._pubsver = "v6_00_00"
        #Kirby - I think this should actually be the assembler version
        # since this is the version of the ub_project and it's online/assembler/v6_00_00 in the pnfs area
        # and it's stored based on that, but the storage location is independent of the pubs version 

        self._action_map = { kUBDAQ_METADATA    : self.process_ubdaq_files,
                             kSWIZZLED_METADATA : self.process_swizzled_files }
        self._metadata_type = kMAXTYPE_METADATA        
        self._max_proc_time = 50
        self._parallelize = 0
        self._min_run = 0

        self._nretrial = 5

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._nruns = 1
#        self._out_dir = '%s' % (resource['OUTDIR'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
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

        self._ref_project = resource['REF_PROJECT']

        if ( 'NSKIP' in resource and
             'SKIP_REF_PROJECT' in resource and
             'SKIP_REF_STATUS' in resource and
             'SKIP_STATUS' in resource ):
            self._nskip = int(resource['NSKIP'])
            self._skip_ref_project = resource['SKIP_REF_PROJECT']
            exec('self._skip_ref_status=int(%s)' % resource['SKIP_REF_STATUS'])
            exec('self._skip_status=int(%s)' % resource['SKIP_STATUS'])
            status_name(self._skip_ref_status)
            status_name(self._skip_status)        

        self._nretrial = int(resource['NRETRIAL'])

    def get_action(self):

        if not self._metadata_type in self._action_map:
            raise DSException('Specified action type not supported! (%d)' % self._metadata_type)

        return self._action_map[self._metadata_type]
    
    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # self.info('Here, self._nruns=%d ... ' % (self._nruns))

        if self._nskip and self._skip_ref_project:
            ctr = self._nskip
            for x in self.get_xtable_runs([self._project,self._skip_ref_project],
                                          [kSTATUS_INIT,self._skip_ref_status]):
                if ctr<=0: break
                self.log_status( ds_status( project = self._project,
                                            run     = int(x[0]),
                                            subrun  = int(x[1]),
                                            seq     = 0,
                                            status  = self._skip_status) )
                ctr -= 1

            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._project)
            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._skip_ref_project)
        

        action = self.get_action()

        runid_v   = []
        infile_v  = []
        outfile_v = []
        checksum_v = []
        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [kSTATUS_INIT,kSTATUS_DONE]):

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: break
            # Counter decreases by 1
            if ctr <= 0: break

            # Report starting
            #self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            status = kSTATUS_INIT

            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
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
                continue

            checksum_v.append(ref_status._data)
            infile_v.append(filelist[0])
            outfile_v.append('%s.json' % infile_v[-1])
            runid_v.append((run,subrun))
            ctr -= 1

        action = self.get_action()

        status_v = action(infile_v,runid_v,checksum_v)

        if not len(status_v) == len(runid_v):
            raise DSException('Logic error: status vector from %s must match # of run ids!' % str(action))

        for i in xrange(len(status_v)):

            run,subrun = runid_v[i]
            status,data = status_v[i]
            if data is None:
                data = ''
            if data and type(data) == type(dict()):
                fout = open('%s.json' % infile_v[i], 'w')
                json.dump(data, fout, sort_keys = True, indent = 4, ensure_ascii=False)
                data = ''
            # Create a status object to be logged to DB (if necessary)
            self.log_status ( ds_status( project = self._project,
                                         run     = run,
                                         subrun  = subrun,
                                         seq     = 0,
                                         status  = status,
                                         data    = data ) )
        
    def process_swizzled_files(self,in_file_v,runid_v,checksum_v=[]):
        
        status_v=[]
        for in_file in in_file_v:

            status_v.append((0,''))
            jsonData = None
            try:
                jsonData = extractor_dict.getmetadata( in_file )
                status = 3
                self.info('Successfully extract metadata from the swizzled file.')
            except:
                status = 100
                self.error('Failed extracting metadata from the swizzled file.')
                    
            status_v[-1]=(status,jsonData)

        return status_v
            
    def process_ubdaq_files(self,in_file_v,runid_v,checksum_v=[]):

        if not len(in_file_v) == len(runid_v):
            raise DSException('Input file list and runid list has different length!')

        cmd_template =   "dumpEventHeaders %s 1000000; echo SPLIT_HERE; dumpEventHeaders %s 1;"

        mp = ds_multiprocess(self._project)

        for i in xrange(len(in_file_v)):

            in_file = in_file_v[i]
            
            cmd = cmd_template % (in_file,in_file)
            index, active_counter = mp.execute(cmd)

            if not self._parallelize:
                mp.communicate()
            else:

                time_slept = 0
                while active_counter >= self._parallelize:

                    if time_slept > self._max_proc_time:
                        self.error('Exceeding process limit time (%s sec). Killing processes...' % self._max_proc_time)
                        mp.kill()
                        break
                    time.sleep(0.2)
                    time_slept += 0.2

                    if (int(time_slept*10)%50) == 0:
                        self.info('Parallel processing %d runs (%d/%d left)...' % (active_counter,
                                                                                   i-active_counter,
                                                                                   len(in_file_v)))

                    active_counter = mp.active_count()

        time_slept = 0
        active_counter = mp.active_count()
        while active_counter:
            
            if time_slept > self._max_proc_time:
                self.error('Exceeding process limit time (%s sec). Killing processes...' % self._max_proc_time)
                mp.kill()
                break
            time.sleep(0.2)
            time_slept += 0.2

            if (int(time_slept*10)%50) == 0:
                self.info('Parallel processing %d runs (%d/%d left)...' % (active_counter,
                                                                           len(in_file_v)-active_counter,
                                                                           len(in_file_v)))
            active_counter = mp.active_count()

        # Now extract MetaData for successful ones
        status_v=[]
        for i in xrange(len(in_file_v)):

            status_v.append((3,None))
            in_file = in_file_v[i]
            out,err = mp.communicate(i)
            fsize = os.path.getsize(in_file)
            run,subrun = runid_v[i]

            checksum = ''
            if checksum_v:
                checksum = checksum_v[i]
            else:
                ref_status = self._api.get_status( ds_status( self._ref_project, run, subrun, 0 ) )

                if not ref_status._status == 0:
                    self.warning('Reference project (%s) not yet finished for run=%d subrun=%d' % (self._ref_project,run,subrun))
                    status_v[i] = (kSTATUS_INIT,None)
                    continue

                if not ref_status._data:
                    self.error('Checksum from project %s unknown for run=%d subrun=%d' % (self._ref_project,run,subrun))
                    status_v[i] = (kSTATUS_ERROR_REFERENCE_PROJECT_DATA,None)
                    continue

                checksum = ref_status._data

            # Can we put a "bad" metadata as a default contents for "bad file"? If we can, comment out continue
            badJsonData = { 'file_name': os.path.basename(in_file), 
                            'file_type': "data", 
                            'file_size': fsize, 
                            'file_format': "binaryraw-uncompressed", 
                            'runs': [ [run,  subrun, 'test'] ], 
                            'first_event': 0,
                            'start_time': '1970-01-01T00:00:00',
                            'end_time': '1970-01-01T00:00:00',
                            'last_event': 0,
                            'group': 'uboone', 
                            "crc": { "crc_value":str(checksum),  "crc_type":"adler 32 crc type" }, 
                            "application": {  "family": "online",  "name": "assembler", "version": 'unknown' }, 
                            "data_tier": "raw", "event_count": 0,
                            "ub_project.name": "online", 
                            "ub_project.stage": "assembler", 
                            "ub_project.version": self._pubsver,
                            'online.start_time_usec': '-1',
                            'online.end_time_usec': '-1'}
            
            if mp.poll(i):
                self.error('Metadata extraction failed on %s w/ return code %d.' % (in_file,mp.poll(i)))
                ref_status = self._api.get_status( ds_status( self._project, run, subrun, 0 ) )
                ntrial_past = 0
                try:
                    ntrial_past = int(ref_status._data)
                    ntrial_past +=1
                except Exception:
                    ntrial_past = 0
                if ntrial_past <= self._nretrial:
                    self.info('Will give a re-trial later (%d/%d)' % (ntrial_past,self._nretrial))
                    status_v[i] = (kSTATUS_INIT,ntrial_past)
                    continue
                if not mp.poll(i) in [2,-9]:
                    status_v[i] = (kSTATUS_ERROR_CANNOT_MAKE_BIN_METADATA,None)
                    continue
                self.info('Return code = 2... will provide invalid metadata and move on!')
                status_v[i] = (kSTATUS_TO_BE_VALIDATED,badJsonData)
                continue
                               
            last_event_cout,first_event_cout = out.split('SPLIT_HERE')
            ver = 'unknown'
            sevt = eevt = 0
            stime = etime = '1970-01-01:T00:00:00'
            stime_usec=-1
            etime_usec=-1
            try:
                read_gps=False
                for line in last_event_cout.split('\n'):
                    if "run_number=" in line and "subrun" not in line :
                        self.debug('Extracting run_number... %s' % line.split('=')[-1])
                        if not run == int(line.split('=')[-1].replace(' ','')):
                            self.warning('Detected un-matching run number: content says run=%s for file %s' % (int(line.split('=')[-1]),in_file))
                    if "subrun_number=" in line:
                        self.debug('Extracting subrun_number... %s' % line.split('=')[-1])
                        if not subrun == int(line.split('=')[-1].replace(' ','')):
                            self.warning('Detected un-matching subrun number: content says subrun=%s for file %s' % (int(line.split('=')[-1]),in_file))
                    if "event_number=" in line:
                        self.debug('Extracting event_number... %s' % line.split('=')[-1])
                        eevt = int(line.split('=')[-1])
                    if "GPS Time FROM EVENT:Object" in line:
                        read_gps=True
                    if "GPS time (second,micro,nano)" in line and read_gps:
                        words=[]
                        for w in line.split():
                            if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                        if not len(words) == 3:
                            self.error('GPS time stamp format could not be interpreted...')
                            self.error(line)
                            raise DSException()
                        self.debug('Extracted GPS Time... %s' % words)
                        etime = datetime.datetime.fromtimestamp(words[0]).replace(microsecond=0).isoformat()
                        etime_usec = words[1]
                        read_gps=False
                    if "daq_version_label=" in line:
                        self.debug('DAQ version... %s' % line.split('=')[-1])
                        ver = line.split('=')[-1]
                        
                read_gps=False
                for line in first_event_cout.split('\n'):
                    if "event_number=" in line:
                        self.debug('Extracting event_number... %s' % line.split('=')[-1])
                        sevt = int(line.split('=')[-1].replace(' ',''))
                    if "GPS Time FROM EVENT:Object" in line:
                        read_gps=True
                    if "GPS time (second,micro,nano)" in line and read_gps:
                        words=[]
                        for w in line.split():
                            if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                        if not len(words) == 3:
                            self.error('GPS time stamp format could not be interpreted...')
                            self.error(line)
                            raise DSException()
                        self.debug('Extracted GPS Time... %s' % words)
                        stime = datetime.datetime.fromtimestamp(words[0]).replace(microsecond=0).isoformat()
                        stime_usec = words[1]
                        read_gps=False

                status_v[i] = (3,None)
                self.info('Successfully extract metadata for run=%d subrun=%d: %s @ %s' % (run,subrun,in_file,time.strftime('%Y-%m-%d %H:%M:%S')))

            except ValueError,TypeError:
                self.error ("Unexpected error: %s" % sys.exc_info()[0] )
                self.error ("1st event:\n%s" % first_event_cout)
                self.error ("Last event:\n%s" % last_event_cout)
                status_v[i] = (kSTATUS_ERROR_CANNOT_MAKE_BIN_METADATA,badJsonData)
                self.error('Failed extracting metadata: %s' % in_file)
                continue

            # run number and subrun number in the metadata seem to be funny,
            # and currently we are using the values in the file name.
            # Also add ub_project.name/stage/version, and data_tier by hand
            jsonData = { 'file_name': os.path.basename(in_file), 
                         'file_type': "data", 
                         'file_size': fsize, 
                         'file_format': "binaryraw-uncompressed", 
                         'runs': [ [run,  subrun, 'test'] ], 
                         'first_event': sevt, 
                         'start_time': stime, 
                         'end_time': etime, 
                         'last_event':eevt, 
                         'group': 'uboone', 
                         "crc": { "crc_value":str(checksum),  "crc_type":"adler 32 crc type" }, 
                         "application": {  "family": "online",  "name": "assembler", "version": ver }, 
                         "data_tier": "raw", "event_count": eevt - sevt + 1 ,
                         "ub_project.name": "online", 
                         "ub_project.stage": "assembler", 
                         "ub_project.version": self._pubsver,
                         'online.start_time_usec': str(stime_usec),
                         'online.end_time_usec': str(etime_usec)}
            status_v[i] = (kSTATUS_TO_BE_VALIDATED,jsonData)

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
            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
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

            in_file = filelist[0]
            out_file = '%s.json' % in_file

            if os.path.isfile(out_file):
#                os.system('rm %s' % in_file)
                status = 0
            else:
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

            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('List of files found %s' % filelist)

            in_file = filelist[0]
            out_file = '%s.json' % in_file

            if os.path.isfile(out_file):
                os.system('rm %s' % out_file)

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

    # def get_ubdaq_metadata()

# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    test_obj = get_metadata( proj_name )

    test_obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    test_obj.get_resource()

    test_obj.process_newruns()

#    test_obj.error_handle()

    test_obj.validate()

    test_obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
