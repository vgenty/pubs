## @namespace dstream.REGISTER_NEW_RUN
#  @ingroup dstream
#  @brief Defines a project register_new_run
#  @author david caratelli

# python include
import time
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class register_new_run
#  @brief register a new run number in the DB
#  @details
#  check closed files written to disk by DAQ
#  and save run/subrun info for this file
#  to DB.
#  DAQ creates new run/subrun numbers but does
#  not log this info to DB (to reduce dependency)
#  this project bridges the gap and makes sure
#  information for new runs/subruns is promptly
#  stored in the database

class register_new_run(ds_project_base):

    # Define project name as class attribute
    _project = 'register_new_run'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super(register_new_run,self).__init__()

        if not arg:
            self.error('No project name specified!')
            raise Exception
            
        self._project = arg

        self._nruns = None
        self._data_dir = ''
        self._experts = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._data_dir = '%s' % (resource['DATADIR'])
        self._experts = resource['EXPERTS']

        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # I Think this is not necessary for this project
        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # get ALL closed data files in DATADIR
        if (os.path.isdir(self._data_dir) == false):
            self.error('DATA DIR %s does not exist'%self._data_dir)
            return

        dircontents = os.listdir(self._data_dir)

        # create a dictionary to keep track of
        # - file name ----- NAME
        # - run number ---- RUN
        # - subrun number - SUBRUN
        # - time-create --- TIMEC
        # - time-modify --- TIMEM
        # : dictionary key: ------ (RUN,SUBRUN)
        # : dictionary content: -- (NAME,TIMEC,TIMEM)
        file_info = {}

        for f in dircontents:
            
            filepath = self._data_dir+'/'+f

            # check that this is a file
            if (os.path.isfile(filepath) == false):
                continue
            
            try:

                time_create  = os.path.getctime(filepath)
                time_modify = os.path.getmtime(filepath)
                
                # file format:
                # NoiseRun-YYYY_M_DD_HH_MM_SS-RUN-SUBRUN.ubdaq
                run    = int(f.replace('.ubdaq','').split('-')[-2])
                subrun = int(f.replace('.ubdaq','').split('-')[-1])

                file_info[tuple((run,subrun))] = [f,time_create,time_modify]

            except:
                
                # if file-name is .ubdaq then we have a problem
                # were not able to read run-subrun info from file
                if (f.find('.ubdaq')):
                    self.info('Could not read RUN/SUBRUN info for file %s'%f)

        # sort the dictionary
        # we want to ignore the largest run/subrun information
        # this will prevent us from potentially logging info
        # for a file that has not yet been closed
        sorted_file_info = sorted(file_info)
        max_file_info = max(sorted_file_info.iterkeys())

        # fetch from database the last run/subrun number recorded
        last_recorded_info = (100,100) # get_last_run
        
        # loop through dictionary keys and write to DB info
        # for runs/subruns not yet stored
        for info in sorted_file_info:
            
            # this key needs to be larger than the last logged value
            # but less than the last element in the dictionary
            if (info >= max_file_info):
                continue;
            if (info <= last_recorded_info):
                continue;

            # if we made it this far the file info needs to be
            # recorded to the database DO THAT!!!

            # Report starting
            self.info('recording info for new run: run=%d, subrun=%d ...' % (int(x[0]),int(x[1])))



# A unit test section
if __name__ == '__main__':

    test_obj.process_newruns()



