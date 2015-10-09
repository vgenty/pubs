## @namespace dummy_dstream.dummy_daq
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_daq
#  @author echurch

# python include
import time,os,sys
import subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_multiprocess
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
    _parallelize = 0
    _max_proc_time = 600
    _parent = ''
    _parent_status = kSTATUS_DONE
    _satellite_extension = ''

    _nskip = 0
    _skip_ref_project = []
    _skip_ref_status = None
    _skip_status = None    

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super(mv_assembler_daq_files,self).__init__(arg)

        self._project = str(arg)
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
            if 'PARALLELIZE' in resource:
                self._parallelize = int(resource['PARALLELIZE'])
            self._max_proc_time = int(resource['MAX_PROC_TIME'])
            if 'SATELLITE_EXTENSION' in resource:
                self._satellite_extension = resource['SATELLITE_EXTENSION']
            self._parent_project = resource['PARENT_PROJECT']
            exec('self._parent_status  = int(%s)' % resource['PARENT_STATUS'])
            status_name(self._parent_status)

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
            
        except Exception:
            return False

        return True

    ## @brief access DB and retrieves new runs
    def process_newruns(self):
        if self._parallelize:
            self.info('Starting a parallel (%d) transfer process for %d runs...' % (self._parallelize,self._nruns))
        else:
            self.info('Starting a sequential transfer process for %d runs...' % self._nruns)

        if self._nskip and self._skip_ref_project:
            ctr = self._nskip
            for x in self.get_xtable_runs([self._project,self._skip_ref_project],
                                          [kSTATUS_INIT,self._skip_ref_status]):
                if ctr<=0: break
                self.log_status( ds_status( project = self._project,
                                            run     = int(x[0]),
                                            subrun  = int(x[1]),
                                            seq     = 0,
                                            status  = self._skip_status ) )
                ctr -= 1

            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._project)
            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._skip_ref_project)

        ctr = self._nruns
        runid_v=[]
        infile_v=[]
        satellite_v=[]
        for x in self.get_xtable_runs([self._project,self._parent_project],
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

            if self._satellite_extension:
                #satellite_file = str(in_file[0:in_file.rfind('.')]) + '.' + self._satellite_extension
                satellite_file = in_file + '.' + self._satellite_extension
                if not os.path.isfile(satellite_file):
                    self.error('The satellite file not found: %s' % satellite_file)
                    continue
                satellite_v.append(satellite_file)
                
            infile_v.append(in_file)
            runid_v.append((run,subrun))

        mp = self.process_files(infile_v)
        mp_satellite = None
        if satellite_v:
            mp_satellite = self.process_files(satellite_v)

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
            out_file = '%s/%s' % ( self._out_dir, in_file.split('/')[-1] )

            cmd = ['cp', '-v', in_file, out_file]
            self.info('Copying %s @ %s' % (in_file,time.strftime('%Y-%m-%d %H:%M:%S')))
            print cmd

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

            out_file = '%s/%s' % ( self._out_dir, in_file.split('/')[-1] )

            if not os.path.isfile(out_file):
                self.error('The output file not found: %s' % out_file)
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND) )

            size_diff = -1
            try:
                size_diff = os.path.getsize(in_file) - os.path.getsize(out_file)
            except Exception:
                size_diff = -1
            
            #res = subprocess.call(['ssh', 'ubdaq-prod-near1', '-x', 'ls', out_file])
            #res = subprocess.call(['ls', out_file])
            if size_diff:
                self.error('Size mis-match on input file: %s' % in_file)
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_TRANSFER_FAILED) )
                continue

            if self._satellite_extension:
                #satellite_in = str(in_file[0:in_file.rfind('.')]) + '.' + self._satellite_extension
                satellite_in = in_file + '.' + self._satellite_extension
                satellite_out = '%s/%s' % ( self._out_dir, satellite_in.split('/')[-1])
                if not os.path.isfile(satellite_out):
                    self.error('The satellite file not found: %s' % satellite_out)
                    self.log_status( ds_status( project = self._project,
                                                run     = run,
                                                subrun  = subrun,
                                                seq     = 0,
                                                status  = kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND) )
                size_diff = -1
                try:
                    size_diff = os.path.getsize(satellite_in) - os.path.getsize(satellite_out)
                except Exception:
                    size_diff = -1
                if size_diff:
                    self.error('Size mis-match on input file: %s' % satellite_in)
                    self.log_status( ds_status( project = self._project,
                                                run     = run,
                                                subrun  = subrun,
                                                seq     = 0,
                                                status  = kSTATUS_ERROR_TRANSFER_FAILED) )
                    continue

            self.info('validated run: run=%d, subrun=%d ...' % (run,subrun))
            self.log_status( ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = kSTATUS_DONE ) )

            
# A unit test section
if __name__ == '__main__':

    test_obj = mv_assembler_daq_files(sys.argv[1])

    test_obj.process_newruns()

    test_obj.validate()



