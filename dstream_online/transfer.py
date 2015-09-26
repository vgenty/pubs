## @namespace dstream_online.transfer
#  @ingroup dstream_online
#  @brief Defines a project transfer
#  @author echurch,yuntse

# python include
import time, os, sys, subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from ds_online_env import *
# ifdh
import ifdh
import subprocess as sub
import samweb_cli, extractor_dict
import pdb, json
import glob

## @class transfer
#  @brief Transferring files
#  @details
#  This process mv's a file to a dropbox directory for SAM to whisk it away...
#  Status codes:
#    2: Copied the file to dropbox

class transfer( ds_project_base ):

    # Define project name as class attribute
    _project = 'transfer'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( transfer, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._nruns = None
        self._out_dir = ''
        #self._outfile_format = ''
        self._in_dir = ''
        #self._meta_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._nruns_to_postpone = 0
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        #self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        #self._meta_dir = '%s' % (resource['METADIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']
        self._max_wait = int(resource['MAX_WAIT'])
        exec('self._parallelize = bool(%s)' % resource['PARALLELIZE'])

        try:
            self._nruns_to_postpone = int(resource['NRUNS_POSTPONE'])
            self.info('Will process %d runs to be postponed (status=%d)' % (self._nruns_to_postpone,kSTATUS_POSTPONE))
        except KeyError,ValueError:
            pass

    ## @brief Transfer files to dropbox
    def transfer_file( self ):

        proc_list=[]
        done_list=[]
        run_id=[]
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Start transfer_file @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
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
                status = ds_status( project = self._project,
                                    run     = int(x[0]),
                                    subrun  = int(x[1]),
                                    seq     = 0,
                                    status  = kSTATUS_POSTPONE )
                self.log_status(status)
                ctr_postpone += 1
                if ctr_postpone > self._nruns_to_postpone: break
                
        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project, self._parent_project],
                                      [1, 0]):

            # Counter decreases by 1
            ctr -= 1
            
            (run, subrun) = (int(x[0]), int(x[1]))
            
            # Report starting
            self.info('Transferring a file: run=%d, subrun=%d ...' % (run,subrun) )
            
            status = 1
            
            # Check input file exists. Otherwise report error
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

            in_file = filelist[0]
            in_json = '%s.json' % in_file
            in_file_base = os.path.basename(in_file)
            out_file = '%s/%s' % ( self._out_dir, in_file_base)
            out_json = '%s/%s.json' % ( self._out_dir, in_file_base)
            
            # construct ifdh object
            #ih = ifdh.ifdh()
            #we're gonna use subprocess to parallelize these transfers and construct an ifdh command by hand
            
            if (os.path.isfile( in_file ) and (os.path.isfile( in_json ))):
                self.info('Found %s' % (in_file) )
                self.info('Found %s' % (in_json) )

                try:
                    cmd = ['ifdh', 'cp','-D', in_file, in_json, self._out_dir]
                    proc_list.append(subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE))
                    done_list.append(False)
                    run_id.append((run,subrun))
                    self.info('Started transfer for (run,subrun)=%s @ %s' % (run_id[-1], time.strftime('%Y-%m-%d %H:%M:%s')))
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
                                status = ds_status( project = self._project,
                                                    run     = run_id[-1][0],
                                                    subrun  = run_id[-1][1],
                                                    seq     = 0,
                                                    status  = 555 )
                                self.log_status( status )
                                time.sleep(5)
                                
                                if proc_list[-1].poll() is None:
                                    self.error('Process termination failed. Hard-killing it (kill -9 %d)' % proc_list[-1].pid)
                                    subprocess.call(['kill','-9',str(proc_list[-1].pid)])
                                    status = ds_status( project = self._project,
                                                        run     = run_id[-1][0],
                                                        subrun  = run_id[-1][1],
                                                        seq     = 0,
                                                        status  = 666 )
                                    self.log_status( status )
                                break

                        self.info('Finished copy [%s] @ %s' % (run_id[-1],time.strftime('%Y-%m-%d %H:%M:%S')))
                        status = ds_status( project = self._project,
                                            run     = run_id[-1][0],
                                            subrun  = run_id[-1][1],
                                            seq     = 0,
                                            status  = 0 )
                        self.log_status( status )

                    else:
                        time.sleep(1)
                
                except:
                    self.error('Caught the exception and setting the status back to 1 for (run,subrun) = (%s, %s)' % (x[0],x[1]))
                    status = 1
                    status = ds_status( project = self._project,
                                        run     = int(x[0]),
                                        subrun  = int(x[1]),
                                        seq     = 0,
                                        status  = status )
                    self.log_status( status )

            else:
                self.error('Did not find the files that you told me to look for (run,subrun) = (%s, %s)' % (x[0],x[1]))
                self.error('Not found: %s' % (in_file) )
                self.error('Or not found: %s' % (in_json) )
                status = 100
                status = ds_status( project = self._project,
                                    run     = int(x[0]),
                                    subrun  = int(x[1]),
                                    seq     = 0,
                                    status  = status )
                self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

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
                    status = ds_status( project = self._project,
                                        run     = run_id[x][0],
                                        subrun  = run_id[x][1],
                                        seq     = 0,
                                        status  = 0 )
                    self.log_status( status )
                    done_list[x] = True
                else:
                    active_counter += 1
                    finished = False
            if finished: break
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


    ## @brief Validate the dropbox
    def validate_outfile( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs( self._project, 2 ):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Validating a file in the output directory: run=%d, subrun=%d ...' % (run,subrun))

            status = 2
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

            in_file = filelist[0]
            if_file_basename = os.path.basename(in_file)
            out_file = '%s/%s' % ( self._out_dir, in_file_base)
            out_json = '%s/%s.json' %( self._out_dir, in_file_base)

            # construct ifdh object
            ih = ifdh.ifdh()

            try:
                ih.locateFile( out_file )
                ih.locateFile( out_json )
                status = 0
            except:
                status = 1

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

    obj = transfer( proj_name )

    obj.transfer_file()

    #if "pnnl" not in proj_name:
    #    obj.validate_outfile()
