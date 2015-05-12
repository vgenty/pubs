## @namespace dummy_dstream.ds_clean
#  @ingroup dummy_dstream
#  @brief Script for removing old files
#  @author zarko

# python include
import sys
import time, os, shutil
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class ds_clean
#  @brief Script for removing old files
#  @details
#  Script checks finished jobs and deletes old files

class ds_clean(ds_project_base):

    # Define project name as class attribute
    _project = 'ds_clean'

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
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['DIR'])
        self._name_pattern = resource['NAME_PATTERN']
        self._parent_project = resource['PARENT_PROJECT']
        self._disk_frac_limit = int(resource['USED_DISK_FRAC_LIMIT'].strip("%"))

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
            # return

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project, self._parent_project], 
                                      [            1,                    0]):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            status = 1
            
            # Check input file exists. Otherwise report error
            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            self.info('Removing %s'%in_file)

            if ":" in in_file:
                #check that out_file is a file before trying to remove 
                #(hopefully should avoid unintentional rm with bad out_dir/name_pattern combo)
                if not os.system('ssh -x %s "test -f %s"'%(tuple(in_file.split(":")))):
                    os.system('ssh -x %s "rm %s"' % tuple(in_file.split(":")))
                    status=2
            else:
                if os.path.isfile(in_file):
                    os.system('rm %s' % in_file)
                    status=2

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

            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            self.info('Check if file %s was deleted.' % in_file)

            status=0
            if ":" in in_file:
                if not os.system('ssh -x %s "test -f %s"'%(tuple(in_file.split(":")))):
                    status=100
            else:
                if os.path.isfile(in_file):
                    status=100

            if (status==100):
                self.error('Failed to remove %s'%in_file)

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
            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            self.info('Will try removing %s again later.' % (in_file))

            status = 1

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

# A unit test section
if __name__ == '__main__':

    if (len(sys.argv)==2):
        test_obj = ds_clean(sys.argv[1])
        
        test_obj.process_newruns()
        
        test_obj.error_handle()
        
        test_obj.validate()
    else:
        test_obj = ds_clean()
