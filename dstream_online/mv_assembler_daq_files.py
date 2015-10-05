## @namespace dummy_dstream.dummy_daq
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_daq
#  @author echurch

# python include
import time,os
import subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from ds_online_util import *
# script module tools
from scripts import find_run

## @class dummy_daq
#  @brief A fake DAQ process that makes a fake nu bin file.
#  @details
#  This assembler_daq project grabs nu bin data file under $PUB_TOP_DIR/data directory
class mv_assembler_daq_files(ds_project_base):

    # Define project name as class attribute
    _project = 'mv_binary_evb'
    _nruns   = 0
    _in_dir  = ''
    _out_dir = ''
    _infile_foramt  = ''
    _outfile_format = ''
    _parallelize = 0
    _max_proc_time = 600
    _parent = ''

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(mv_assembler_daq_files,self).__init__()
        if not self.load_params():
            raise Exception()
        
    ## @brief load project parameters
    def load_params(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
            return False
        try:
            resource = self._api.get_resource(self._project)
            self._nruns = int(resource['NRUNS'])
            self._out_dir = '%s' % (resource['OUTDIR'])
            self._in_dir = '%s' % (resource['INDIR'])
            self._infile_format = resource['INFILE_FORMAT']
            self._outfile_format = resource['OUTFILE_FORMAT']
            if 'PARALLELIZE' in resource:
                self._parallelize = int(resource['PARALLELIZE'])
            self._max_proc_time = int(resource['MAX_PROC_TIME'])

            self._parent_project = resource['PARENT_PROJECT']
            exec('self._parent_status  = int(%s)' % resource[PARENT_STATUS])
            status_name(self._parent_status)
        except Exception:
            return False

        return True

    ## @brief access DB and retrieves new runs
    def process_newruns(self):
        if self._parallelize:
            self.info('Starting a parallel (%d) transfer process for %d runs...' % (self._parallelize,self._nruns))
        else:
            self.info('Starting a sequential transfer process for %d runs...' % self._nruns)
        ctr = self._nruns

        runid_v=[]
        infile_v=[]

        for x in self.get_runs([self._project,self._parent_project],
                               [kSTATUS_INIT,self._parent_status]):

            # Counter decreases by 1
            ctr -=1
            if ctr < 0: break

            run    = int(x[0])
            subrun = int(x[1])

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))
            status_code=kSTATUS_INIT
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
                continue
            
            in_file = filelist[0]
            in_file_segments = os.path.basename(in_file).split('-')
            if len(in_file_segments)<2:
                self.error('The file %s does not contain the - character' % in_file)
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FORMATED ) )
                continue

            infile_v.append(in_file_segments[0])
            runid_v.append((run,subrun))

        mp = self.process_files(infile_v)

        self.info('Finished all @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
        for i in xrange(len(infile_v)):
            run,subrun = runid_v[i]
            self.log_status( ds_status( project = self._project,
                                        run = run,
                                        subrun = subrun,
                                        seq = 0,
                                        status = kSTATUS_TO_BE_VALIDATED ) )
            

    def process_files(self,in_filelist_v):

        mp = ds_multiprocess(self._project)

        for i in xrange(len(in_filelist_v)):

            in_file  = in_filelist_v[i]
            out_file = '%s/%s' % (self._out_dir,in_file.split('/')[-1])

            cmd = ['cp', '-v', in_file, out_file]
            self.info('Copying %s @ %s' % (in_file,time.strftime('%Y-%m-%d %H:%M:%S')))

            index,active_ctr = mp.execute(cmd)

            if not self._parallelize:
                mp.communicate(index)
            else:
                time_slept = 0
                while active_ctr > self._parallelize:
                    time.sleep(0.2)
                    time_slept += 0.2
                    active_ctr = mp.active_count()

                    if time_slept > self._max_proc_time:
                        self.error('Exceeding time limit %s ... killing %d jobs...' % (self._max_proc_time,active_ctr))
                        mp.kill()
                        break
                    if int(time_slept) and (int(time_slept*10)%50) == 0:
                        self.info('Waiting for %d/%d process to finish...' % (active_ctr,len(in_filelist_v)))
        time_slept=0
        while mp.active_count():
            time.sleep(0.2)
            time_slept += 0.2
            if time_slept > self._max_proc_time:
                mp.kill()
                break
        return mp

    ## @brief access DB and validate finished runs
    def validate(self):

        ctr = self._nruns
        # see if the status=2 files we've processed are indeed where they should be.
        for x in self.get_runs(self._project,kSTATUS_TO_BE_VALIDATED): 

            # Counter decreases by 1
            ctr -=1
            if ctr<0: break
            
            run    = int(x[0])
            subrun = int(x[1])
            status_code = kSTATUS_TO_BE_VALIDATED

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
                continue

            in_file = filelist[0]
            in_file_segments = os.path.basename(in_file).split('-')
            if (len(in_file_segments)<2):
                self.error('The file %s does not contain the - character' % in_file)
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FORMATED ) )
                continue

            out_file_prefix = in_file_segments[0]
            out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (out_file_prefix,run,subrun) )

            size_diff = -1
            try:
                size_diff = os.path.getsize(in_file) - os.path.getsize(out_file)
            except Exception:
                size_diff = -1
            
            #res = subprocess.call(['ssh', 'ubdaq-prod-near1', '-x', 'ls', out_file])
            #res = subprocess.call(['ls', out_file])
            if size_diff:
                self.error('Size mis-match on run: run=%d, subrun=%d ...' % (run,subrun))
                status_code = kSTATUS_ERROR_TRANSFER_FAILED
            else:
                self.info('validated run: run=%d, subrun=%d ...' % (run,subrun))
                status_code = kSTATUS_DONE
                
            # Create a status object to be logged to DB (if necessary)
            self.log_status( ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = status_code )
            
# A unit test section
if __name__ == '__main__':

    test_obj = mv_assembler_daq_files()

    test_obj.process_newruns()

    test_obj.validate()



