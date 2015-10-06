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
from ds_online_util import *
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
        self._satellite_extension = ''

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['DIR'])
        self._name_pattern = resource['NAME_PATTERN']
        self._disk_frac_limit = int(resource['USED_DISK_FRAC_LIMIT'].strip("%"))
        if 'SATTELITE_EXT' in resource:
            self._satellite_extension = resource['SATTELITE_EXT']
        try:
            self._parent_project = []

            # Get parent project sets
            for parent_list in resource['PARENT_PROJECT'].split('::'):

                self._parent_project.append( parent_list.split(':') )

            self._parent_status = []

            # Get parent status sets
            for parent_status in resource['PARENT_STATUS'].split('::'):

                self._parent_status.append( parent_status.split(':') )

            # Convert parent status to known integers
            for x in xrange(len(self._parent_status)):

                status_list = self._parent_status[i]
                int_status_list = []
                for x in status_list:
                    try:
                        exec('int_status_list.append( int(%s) )' % x)
                        status_name(int_status_list[-1])
                    except Exception:
                        self.error('Parent set %s has invalid status representation %s' % (self._parent_project[i],x))
                        raise ValueError

                self._parent_status[i] = int_status_list

            if not len(self._parent_project):
                self.error('No parent registered!')
                raise ValueError                
        
            if not len(self._parent_project) == len(self._parent_status):
                self.error('Number set of parent and its status entry do not match!')
                raise ValueError

            for x in xrange(len(self._parent_project)):

                if not len(self._parent_project[i]) == len(self._parent_status[i]):
                    self.error('Parent set %s has only %d status entries!' % (self._parent_project[i],len(self._parent_status[i])))
                    raise ValueError

                if not len(self._parent_project[i]):
                    self.error('Empty parent set given!')
                    raise ValueError
                
        except Exception:
            self.error('Failed to load parent projects...')
            return False

        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])

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


    ## @brief internal function to organize a list of runs based on self._parent_project and self._parent_status (not to be called in public)
    def get_run_list(self):

        run_list = []

        for i in xrange(len(self._parent_project)):

            parent_list   = self._parent_project[i]
            parent_status = self._parent_status[i]
            
            project_list   = [ self._project ] + parent_list
            project_status = [ kSTATUS_INIT  ] + parent_status

            run_list.append(self._api.get_xtable_runs( project_list, project_status, False ))
        
        return run_list

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        if self._nskip and self._skip_ref_project:
            ctr = self._nskip
            for x in self.get_xtable_runs([self._project,self._skip_ref_project],
                                          [kSTATUS_INIT,self._skip_ref_status]):
                if ctr<=0: break
                set_transfer_status(run=int(x[0]),subrun=int(x[1]),status=self._skip_status)
                ctr -= 1

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

        target_runs = self.get_run_list()
        for i in len(xrange(self._parent_project)):
            self.info('Found %d runs to be processed (from project %s ... requirement %s)' % (len(target_runs[i]),
                                                                                              self._parent_project[i],
                                                                                              self._parent_status[i]))
        combined_runs = []
        for x in target_runs:
            combined_runs += x
            
        for x in combined_runs:

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

            if not os.path.isfile(in_file):
                rm_status=os.system('rm -f %s' % in_file)
                if self._satellite_extension:
                    satellite_file = str(in_file)
                    satellite_file = satellite_file[0:in_file.rfind('.')] + '.' + self._satellite_extension
                    os.system('rm -f %s' % satellite_file)
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
