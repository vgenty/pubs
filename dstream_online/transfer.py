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
        
        self._child_trigger_status=[]
        self._child_projects=[]
        self._child_status=[]

        self._success_status = kSTATUS_DONE
        self._error_handle_status = None
        
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

        exec('self._success_status=int(resource[\'SUCCESS_STATUS\'])')
        exec('self._error_handle_status=int(resource[\'ERROR_STATUS\'])')
        status_name(self._success_status)
        status_name(self._error_handle_status)

        # Get child status sets
        self._child_trigger_status=[]
        for child_trigger_status in resource['CHILD_TRIGGER_STATUS'].split(':'):

            status = None
            exec('status = int(%s)' % child_trigger_status)
            status_name(status)
            self._child_trigger_status.append(status)

        # Get child list
        self._child_projects=[]
        for child_project in resource['CHILD_PROJECT'].split('::'):

            self._child_projects.append( child_project.split(':') )

        # Get child status
        self._child_status=[]
        for child_status in resource['CHILD_STATUS'].split(':'):

            status = None
            exec('status = int(%s)' % child_status)
            status_name(status)
            self._child_status.append(status)

        # Validate sanity of child status list
        if not len(self._child_trigger_status) == len(self._child_projects):
            raise DSException('Child trigger status and child projects have diferent length!')

        if not len(self._child_trigger_status) == len(self._child_status):
            raise DSException('Child trigger status and child status have diferent length!')

    ## @brief a function that should be used to set status
    def set_transfer_status(self,run,subrun,status,data=''):

        status_name(status)

        child_list   = None
        child_status = None
        if status in self._child_trigger_status:
            child_index = None
            for i in xrange(len(self._child_trigger_status)):
                if status == self._child_trigger_status[i]:
                    child_index = i
                    break
            child_list   = self._child_project[child_index]
            child_status = self._child_status[child_index]
        
        self.log_status( ds_status( project = self._project,
                                    run = run,
                                    subrun = subrun,
                                    seq = 0,
                                    status = status,
                                    data = data ) )
        if child_list:

            for child in child_list:
                self.log_status( ds_status( project = child,
                                            run = run,
                                            subrun = subrun,
                                            seq = 0
                                            status = child_status ) )
            
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
                
        # Fetch runs from DB and process for # runs specified for this instance.
        args_v  = []
        runid_v = []
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project, self._parent_project],
                                      [1, 0]):
            if ctr <=0: break

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            # Counter decreases by 1
            ctr -= 1
            
            # Report starting
            self.info('Transferring a file: run=%d, subrun=%d ...' % (run,subrun))
            
            status = 1
            
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
                    if int(time_slept) and int(time_slept)%3 < 0.3 == 0:
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

