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
from ds_online_env import *
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
        self._nruns_to_postpone = 0
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
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
#        self._out_dir = '%s' % (resource['OUTDIR'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']
        try:
            self._nruns_to_postpone = int(resource['NRUNS_POSTPONE'])
            self.info('Will process %d runs to be postponed (status=%d)' % (self._nruns_to_postpone,kSTATUS_POSTPONE))
        except KeyError,ValueError:
            pass

        if not 'METADATA_TYPE' in resource or not is_valid_metadata_type(resource['METADATA_TYPE']):
            raise DSException('Invalid metadata type or not specified...')

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

        #
        # Process Postpone first
        #
        ctr_postpone = 0
        for parent in [self._parent_project]:
            if ctr_postpone >= self._nruns_to_postpone: break
            if parent == self._project: continue
            
            postpone_name_list = [self._project, parent]
            postpone_status_list = [kSTATUS_INIT, kSTATUS_POSTPONE]
            target_runs = self.get_xtable_runs(postpone_name_list,postpone_status_list)
            self.info('Found %d runs to be postponed due to parent %s...' % (len(target_runs),parent))
            for x in target_runs:

                if int(x[0]) < self._min_run: continue
                status = ds_status( project = self._project,
                                    run     = int(x[0]),
                                    subrun  = int(x[1]),
                                    seq     = 0,
                                    status  = kSTATUS_POSTPONE )
                self.log_status(status)
                ctr_postpone += 1
                if ctr_postpone > self._nruns_to_postpone: break

        action = self.get_action()

        runid_v   = []
        infile_v  = []
        outfile_v = []
        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [1,0]):

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: break
            # Counter decreases by 1
            ctr -= 1
            if ctr < 0: break

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

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

            infile_v.append(filelist[0])
            outfile_v.append('%s.json' % infile_v[-1])
            runid_v.append((run,subrun))

        action = self.get_action()

        status_v = action(infile_v)

        if not len(status_v) == len(runid_v):
            raise DSException('Logic error: status vector from %s must match # of run ids!' % str(action))

        for i in xrange(len(status_v)):

            run,subrun = runid_v[i]
            status,jsonData = status_v[i]
            if jsonData:
                fout = open('%s.json' % infile_v[i], 'w')
                json.dump(jsonData, fout, sort_keys = True, indent = 4, ensure_ascii=False)

            # Create a status object to be logged to DB (if necessary)
            self.log_status ( ds_status( project = self._project,
                                         run     = run,
                                         subrun  = subrun,
                                         seq     = 0,
                                         status  = status) )
        
    def process_swizzled_files(self,in_file_v):
        
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
            
    def process_ubdaq_files(self,in_file_v):

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

                    if int(time_slept)%5 == 0:
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

            if int(time_slept)%5 == 0:
                self.info('Parallel processing %d runs (%d/%d left)...' % (active_counter,
                                                                           len(in_file_v)-active_counter,
                                                                           len(in_file_v)))
            active_counter = mp.active_count()

        # Now extract MetaData for successful ones
        status_v=[]
        for i in xrange(len(in_file_v)):

            status_v.append((3,None))
            out,err = mp.communicate(i)
            if mp.poll(i):
                self.error('Return status from dumpEventHeaders for last event is not successful. It is %d .' % mp.poll(i))
                status_v[i] = (kSTATUS_ERROR_CANNOT_MAKE_BIN_METADATA,None)
                continue
            in_file = in_file_v[i]

            last_event_cout,first_event_cout = out.split('SPLIT_HERE')
            run = subrun = 0
            ver = -12
            sevt = eevt = -12
            stime = etime = -12
            try:
                for line in last_event_cout.split('\n'):
                    
                    if "run_number=" in line and "subrun" not in line :
                        run = int(line.split('=')[-1])
                    if "subrun_number=" in line:
                        subrun = int(line.split('=')[-1])
                    if "event_number=" in line:
                        eevt = int(line.split('=')[-1])
                    if "Localhost Time: (sec,usec)" in line:
                        etime = datetime.datetime.fromtimestamp(float(line.split(')')[-1].split(',')[0])).replace(microsecond=0).isoformat()
                    if "daq_version_label=" in line:
                        ver = line.split('=')[-1]

                for line in first_event_cout.split('\n'):
                    if "event_number=" in line:
                        sevt = int(line.split('=')[-1])
                    if "Localhost Time: (sec,usec)" in line:
                        stime = datetime.datetime.fromtimestamp(float(line.split(')')[-1].split(',')[0])).replace(microsecond=0).isoformat()

                status_v[i] = (3,None)
                self.info('Successfully extract metadata from the ubdaq file (%d/%d) @ %s' % (i,len(in_file_v),time.strftime('%Y-%m-%d %H:%M:%S')))

            except:
                self.error ("Unexpected error:", sys.exc_info()[0] )
                status_v[i] = (kSTATUS_ERROR_CANNOT_MAKE_BIN_METADATA,None)
                self.error('Failed extracting metadata from the ubdaq file. (%d/%d)' % (i,len(in_file_v)))
                continue

            fsize = os.path.getsize(in_file)

            ref_status = self._api.get_status( ds_status( self._ref_project, run, subrun, 0 ) )

            if not ref_status._status == 0:
                self.warning('Reference project (%s) not yet finished for run=%d subrun=%d' % (self._ref_project,run,subrun))
                status_v[i] = (kSTATUS_INIT,None)
                continue

            if not ref_status._data:
                self.error('Checksum from project %s unknown for run=%d subrun=%d' % (self._ref_project,run,subrun))
                status_v[i] = (kSTATUS_ERROR_REFERENCE_PROJECT_DATA,None)
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
                         "crc": { "crc_value":ref_status._data,  "crc_type":"adler 32 crc type" }, 
                         "application": {  "family": "online",  "name": "assembler", "version": ver }, 
                         "data_tier": "raw", "event_count": eevt - sevt + 1 ,
                         "ub_project.name": "online", 
                         "ub_project.stage": "assembler", 
                         "ub_project.version": self._pubsver }
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
