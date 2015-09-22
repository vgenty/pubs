## @namespace dummy_dstream.dummy_daq
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_daq
#  @author echurch

# python include
import time,os,glob
import subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

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
    _parallelize = False
    _max_wait = 600
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
#            self._nruns = int(resource['NRUNS'])
            self._nruns = 2
            self._out_dir = '%s' % (resource['OUTDIR'])
            self._in_dir = '%s' % (resource['INDIR'])
            self._infile_format = resource['INFILE_FORMAT']
            self._outfile_format = resource['OUTFILE_FORMAT']
            exec('self._parallelize = bool(%s)' % resource['PARALLELIZE'])
            self._max_wait = int(resource['MAX_WAIT'])
        except Exception:
            return False

        return True

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        ctr = self._nruns
        proc_list=[]
        done_list=[]
        run_id=[]
        for x in self.get_runs(self._project,1):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))
            status_code=1
            in_file_holder = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            filelist = glob.glob( in_file_holder )
            if (len(filelist)<1):
                self.error('ERROR: Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('ERROR: Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('ERROR: List of files found %s' % filelist)
            
            if (len(filelist)>0):
                in_file = filelist[0]
                in_file_segments = os.path.basename(in_file).split('-')
                if (len(in_file_segments)<2):
                    self.error('ERROR: The file %s does not contain the - character' % in_file)
                    self.error('ERROR: So have no idea what to do.')
                    break
                out_file_prefix = in_file_segments[0]
                out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (out_file_prefix,run,subrun) )
                #cmd = ['rsync', '-v', in_file, 'ubdaq-prod-near1:%s' % out_file]
                #cmd = ['rsync', '-v', in_file, out_file]
                cmd = ['cp', '-v', in_file, out_file]
                proc_list.append(subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE))
                done_list.append(False)
                run_id.append((run,subrun))
                self.info('Started copy (run,subrun)=%s @ %s' % (run_id[-1],time.strftime('%Y-%m-%d %H:%M:%S')))
                # Create a status object to be logged to DB (if necessary)
                status_code=3

                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )

                self.log_status( status )

            # if not parallelized, wait till proc is done
                if not self._parallelize:
                    time_spent = 0
                    while ((len(proc_list)>0) and (proc_list[-1].poll() is None)):
                        time.sleep(1)
                        time_spent +=1

                        if time_spent > self._max_wait:
                            self.error('Exceeding the max wait time (%d sec). Terminating the process...' % self._max_wait)
                            proc_list[-1].kill()
                            time.sleep(5)

                            if proc_list[-1].poll() is None:
                                self.error('Process termination failed. Hard-killing it (kill -9 %d)' % proc_list[-1].pid)
                                subprocess.call(['kill','-9',str(proc_list[-1].pid)])
                                break

                    self.info('Finished copy [%s] @ %s' % (run_id[-1],time.strftime('%Y-%m-%d %H:%M:%S')))
                    status = ds_status( project = self._project,
                                        run     = run_id[-1][0],
                                        subrun  = run_id[-1][1],
                                        seq     = 0,
                                        status  = 2 )
                    self.log_status( status )

            # if parallelized, just sleep 5 sec and go next run
                else:
                    time.sleep(5)

            if not ctr: break
        
        # if not parallelized, done
        if not self._parallelize:
            return

        finished = False
        time_spent = 0
        while not finished:
            finished = True
            time.sleep(1)
            time_spent += 1
            active_counter = 0
            for x in xrange(len(proc_list)):
                if done_list[x]: continue
                if not proc_list[x].poll() is None:
                    self.info('Finished copy [%s] @ %s' % (run_id[x],time.strftime('%Y-%m-%d %H:%M:%S')))
                    status_code = 2
                    status = ds_status( project = self._project,
                                        run     = run_id[x][0],
                                        subrun  = run_id[x][1],
                                        seq     = 0,
                                        status  = status_code )
                    self.log_status( status )
                    done_list[x] = True
                else:
                    active_counter += 1
                    finished = False
            if time_spent%10:
                self.info('Waiting for copy to be done... (%d/%d processes) ... %d [sec]' % (active_counter,len(proc_list),time_spent))
            if time_spent > self._max_wait:
                self.error('Exceeding the max wait time (%d sec). Terminating the processes...' % self._max_wait)
                for x in xrange(len(proc_list)):
                    proc_list[x].kill()

                    status_code = 101
                    status = ds_status( project = self._project,
                                        run     = run_id[x][0],
                                        subrun  = run_id[x][1],
                                        seq     = 0,
                                        status  = status_code )
                    self.log_status( status )

                    # hard kill if still alive
                    time.sleep(5)
                    if proc_list[x].poll() is None:
                        self.error('Process termination failed. Hard-killing it (kill -9 %d)' % proc_list[x].pid)
                        subprocess.call(['kill','-9',str(proc_list[x].pid)])
                break
        self.info('All finished @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    ## @brief access DB and validate finished runs
    def validate(self):

        ctr = self._nruns
        # see if the status=2 files we've processed are indeed where they should be.
        for x in self.get_runs(self._project,2): 

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])
            status_code = 2

            in_file_holder = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            filelist = glob.glob( in_file_holder )
            if (len(filelist)<1):
                self.error('ERROR: Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                

            if (len(filelist)>1):
                self.error('ERROR: Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('ERROR: List of files found %s' % filelist)
            
            if (len(filelist)>0):
                in_file = filelist[0]
                in_file_segments = os.path.basename(in_file).split('-')
                if (len(in_file_segments)<2):
                    self.error('ERROR: The file %s does not contain the - character' % in_file)
                    self.error('ERROR: So have no idea what to do.')
                    break
                out_file_prefix = in_file_segments[0]

                out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (out_file_prefix,run,subrun) )
            
                #res = subprocess.call(['ssh', 'ubdaq-prod-near1', '-x', 'ls', out_file])
                res = subprocess.call(['ls', out_file])
                if res:
                    self.error('error on run: run=%d, subrun=%d ...' % (run,subrun))
                    status_code = 102
                else:
                    self.info('validated run: run=%d, subrun=%d ...' % (run,subrun))
                    status_code = 0
                
                    # Create a status object to be logged to DB (if necessary)
                    status = ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = status_code )
            
                    # Log status
                    self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = mv_assembler_daq_files()

    test_obj.process_newruns()

    test_obj.validate()



