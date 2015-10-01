## @namespace dummy_dstream.ds_clean
#  @ingroup dummy_dstream
#  @brief Script for removing old files
#  @author zarko

# python include
import sys
import time, os, shutil
import glob
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from ds_online_env import *
import glob
# script module tools
from scripts import find_run

## @class ds_clean
#  @brief Script for removing old files
#  @details
#  Script checks finished jobs and deletes old files

class ds_clean(ds_project_base):

    # Define project name as class attribute
    #_project = 'ds_clean'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, project_name ):

        self._project = project_name
        # Call base class ctor
        super(ds_clean,self).__init__( project_name )

        # self.info('Running cleaning project %s'%self._project)
        if (self._project==''):
            self.error('Missing project name argument')
            return

        self._nruns = None
        self._min_run = 0
        self._in_dir = ''
        self._name_pattern = ''
        self._parent_project = []
        self._parent_status = []
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['DIR'])
        self._name_pattern = resource['NAME_PATTERN']
        self._disk_frac_limit = int(resource['USED_DISK_FRAC_LIMIT'].strip("%"))

        try:
            self._parent_project = resource['PARENT_PROJECT'].split(':')
            self._parent_status  = [int(x) for x in resource['PARENT_STATUS'].split(':')]
            if not len(self._parent_project) == len(self._parent_status):
                raise ValueError
        except Exception:
            self.error('Failed to load parent projects...')
            return False

        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])

        #this constructs the list of projects and their status codes
        #we want the project to be status 1, while the dependent projects to
        # be status 0
        self._project_list = [self._project ]
        self._project_requirement = [ kSTATUS_INIT ]

        self._project_list += self._parent_project
        self._project_requirement += self._parent_status

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Check available space
        if ":" in self._in_dir:
            disk_frac_used=int(os.popen('ssh -x %s "df %s" | tail -n1'%tuple(self._in_dir.split(":"))).read().split()[4].strip("%"))
        else:
            disk_frac_used=int(os.popen('df %s | tail -n1'%(self._in_dir)).read().split()[4].strip("%"))

        self.info("%i%% of disk used. Removing files to get down to %i%%."%(disk_frac_used, self._disk_frac_limit))
        if (disk_frac_used < self._disk_frac_limit):
            self.info('Only %i%% of disk space used (%s), skip cleaning until %i%% is reached.'%(disk_frac_used, self._in_dir, self._disk_frac_limit))
            return

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        #we want the last argument of this list get_xtable_runs call to be False
        #that way the list is old files first to newew files last and clean up that way
        #target_runs = self.get_xtable_runs([self._project, self._parent_project], 
        #                                   [            1,                    0],False)

        for p in self._project_list:
            self._api.commit('DROP TABLE IF EXISTS temp%s' % p)

        target_runs = self.get_xtable_runs(self._project_list, self._project_requirement, False)
        self.info('Found %d runs to be processed (from project %s ... requirement %s)' % (len(target_runs),self._parent_project,self._parent_status))
        for x in target_runs:

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: 
                break

            # Counter decreases by 1
            ctr -=1
            if ctr <0: break

            tmp_status = kSTATUS_INIT
            rm_status = kSTATUS_INIT

            # Check input file exists. Otherwise report error
            filelist = find_run.find_file(self._in_dir,self._name_pattern,run,subrun)

            if (len(filelist)>1):
                self.error('There is more than one file matching that pattern: %s' % filelist)
                self.log_status( ds_status( project = self._project,
                                            run     = int(x[0]),
                                            subrun  = int(x[1]),
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE) )
                continue

            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = int(x[0]),
                                            subrun  = int(x[1]),
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND) )
                continue

            in_file = filelist[0]
            self.info('Removing %s'%in_file)

            if ":" in in_file:
                #check that out_file is a file before trying to remove 
                #(hopefully should avoid unintentional rm with bad out_dir/name_pattern combo)
                if not os.system('ssh -x %s "test -f %s"'%(tuple(in_file.split(":")))):
                    rm_status=os.system('ssh -x %s "rm -f %s"' % tuple(in_file.split(":")))
                    tmp_status=kSTATUS_TO_BE_VALIDATED
            else:
                self.info('Looks like the file is local on this node')
                if not os.path.isfile(in_file):
                    self.info("ERROR: os.path.isfile('%s') returned false?!"%in_file)
                self.info('Going to remove the file with rm...')
                rm_status=os.system('rm -f %s' % in_file)
                tmp_status=kSTATUS_TO_BE_VALIDATED

            if rm_status:
                self.info('Failed to remove the file %s' % in_file)
                tmp_status=kSTATUS_ERROR_CANNOT_REMOVE_FILE
            # Create a status object to be logged to DB (if necessary)
            self.log_status ( ds_status( project = self._project,
                                         run     = int(x[0]),
                                         subrun  = int(x[1]),
                                         seq     = 0,
                                         status  = tmp_status ) )
            
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
        for x in self.get_runs(self._project,kSTATUS_TO_BE_VALIDATED,False):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            self.info('Check if file %s was deleted.' % in_file)

            tmp_status=kSTATUS_DONE
            if ":" in in_file:
                if not os.system('ssh -x %s "ls %s"'%(tuple(in_file.split(":")))):
                    tmp_status=kSTATUS_CANNOT_REMOVE_FILE
            else:
                if (len(glob.glob(in_file))>0):
                    tmp_status=kSTATUS_CANNOT_REMOVE_FILE

            if not tmp_status == kSTATUS_DONE:
                self.error('Failed to remove %s'%in_file)

            # Create a status object to be logged to DB (if necessary)
            self.log_status ( ds_status( project = self._project,
                                         run     = int(x[0]),
                                         subrun  = int(x[1]),
                                         seq     = int(x[2]),
                                         status  = tmp_status ) )
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
        for x in self.get_runs(self._project,kSTATUS_ERROR_CANNOT_REMOVE_FILE,False):

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: continue

            # Counter decreases by 1
            ctr -=1

            # Report starting
            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            self.info('Will try removing %s again later.' % (in_file))

            tmp_status = kSTATUS_ERROR_CANNOT_REMOVE_FILE

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = tmp_status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = ds_clean(sys.argv[1])
    
    test_obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
    
    test_obj.process_newruns()
    
    #test_obj.error_handle()
    
    test_obj.validate()
    
    test_obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
