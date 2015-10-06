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
from dstream import ds_multiprocess
from ds_online_util import *
# ifdh
import ifdh
import subprocess as sub
import samweb_cli, extractor_dict
import pdb, json
import glob
# script module tools
from scripts import find_run

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
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._parallelize = 0
        self._max_proc_time = 120
        self._min_run = 0

        self._ntrials = 0
        
        self._success_status = kSTATUS_DONE
        self._error_handle_status = None
        self._bypass_status = None
        self._bypass = False

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None
        
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

        if 'PARALLELIZE' in resource:
            self._parallelize = int(resource['PARALLELIZE'])
            
        if 'MAX_PROC_TIME' in resource:
            self._max_proc_time = int(resource['MAX_PROC_TIME'])
        
        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])

        if 'NUM_RETRIAL' in resource:
            self._ntrials = int(resource['NUM_RETRIAL'])

        exec('self._success_status=int(resource[\'COMPLETE_STATUS\'])')
        exec('self._error_handle_status=int(resource[\'ERROR_HANDLE_STATUS\'])')
        status_name(self._success_status)
        status_name(self._error_handle_status)

        # get bypass status
        exec('self._bypass = bool(%s)' % resource['BYPASS'])
        exec('self._bypass_status = int(%s)' % resource['BYPASS_STATUS'])
        status_name(self._bypass_status)

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
            
    ## @brief a function that should be used to set status
    def set_transfer_status(self,run,subrun,status,data=''):

        status_name(status)

        self.log_status( ds_status( project = self._project,
                                    run = run,
                                    subrun = subrun,
                                    seq = 0,
                                    status = status,
                                    data = data ) )

            
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

        if self._nskip and self._skip_ref_project:
            ctr = self._nskip
            for x in self.get_xtable_runs([self._project,self._skip_ref_project],
                                          [kSTATUS_INIT,self._skip_ref_status]):
                if ctr<=0 break;
                set_transfer_status(run=int(x[0]),subrun=int(x[1]),status=self._skip_status)
                ctr -= 1

            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._project)
            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._skip_ref_project)        
                
        # Fetch runs from DB and process for # runs specified for this instance.
        args_v  = []
        runid_v = []
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project, self._parent_project],
                                      [kSTATUS_INIT, kSTATUS_DONE]):
            if ctr <=0: break

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            # Counter decreases by 1
            ctr -= 1

            if self._bypass:
                self.info('Configured to bypass transfer: run=%d, subrun=%d ...' % (run,subrun))
                self.set_transfer_status(run=run,subrun=subrun,status=self._bypass_status)
                continue

            # Report starting
            self.info('Transferring a file: run=%d, subrun=%d ...' % (run,subrun))
            
            status = kSTATUS_INIT
            
            # Check input file exists. Otherwise report error
            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.set_transfer_status(run=run,subrun=subrun,status=kSTATUS_ERROR_INPUT_FILE_NOT_FOUND)
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.set_transfer_status(run=run,subrun=subrun,status=kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE)
                continue

            in_file = filelist[0]
            in_json = '%s.json' % in_file
            in_file_base = os.path.basename(in_file)
            out_file = '%s/%s' % ( self._out_dir, in_file_base)
            out_json = '%s/%s.json' % ( self._out_dir, in_file_base)
            
            # construct ifdh object
            #ih = ifdh.ifdh()
            #we're gonna use subprocess to parallelize these transfers and construct an ifdh command by hand

            if not os.path.isfile( in_json ):
                self.error('Did not find json file: %s' % in_json)
                self.set_transfer_status(run=run,subrun=subrun,status=kSTATUS_ERROR_INPUT_FILE_NOT_FOUND)
                continue

            args_v.append((in_file, in_json, self._out_dir))
            runid_v.append((run,subrun))

        mp = self.process_files(args_v)

        for i in xrange(len(args_v)):

            run = runid_v[i][0]
            subrun = runid_v[i][1]
            
            if mp.poll(i):
                self.error('Failed copy %s @ %s' % (runid_v[i],time.strftime('%Y-%m-%d %H:%M:%S')))
                old_status = self._api.get_status( ds_status(self._project, run, subrun, 0) )
                if self._ntrials and (not old_status._data or int(old_status._data) < self._ntrials):
                    self.info('Will retry later...')
                    past_trials = 1
                    if old_status._data:
                        past_trials = int(old_status._data) + 1
                        
                    self.set_transfer_status( run = run,
                                              subrun = subrun,
                                              status = kSTATUS_INIT,
                                              data = str(past_trials) )
                else:
                     self.set_transfer_status( run = run,
                                               subrun = subrun,
                                               status =  self._error_handle_status )
            else:
                self.info('Finished copy %s @ %s' % (runid_v[i],time.strftime('%Y-%m-%d %H:%M:%S')))
                self.set_transfer_status( run = run,
                                          subrun = subrun,
                                          status = self._success_status )

        self.info('All finished @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
                
    def process_files(self,in_filelist_v):

        mp = ds_multiprocess(self._project)

        for i in xrange(len(in_filelist_v)):

            in_file,in_json,out_dir = in_filelist_v[i]
            cmd = ['ifdh','cp','-D',in_file,in_json,out_dir]

            self.info('Transferring %s @ %s' % (in_file,time.strftime('%Y-%m-%d %H:%M:%S')))

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

# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = transfer( proj_name )

    obj.transfer_file()

