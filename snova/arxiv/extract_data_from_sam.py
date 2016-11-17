import time, os
from pub_dbi import DBException
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

class extract_data_from_sam(ds_project_base):

    # Define project name as class attribute
    _project = 'extract_data_from_sam'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(extract_data_from_sam,self).__init__()

        self._nruns = None
        self._parent_project = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._parent_project = str(resource['SOURCE_PROJECT'])

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [1,0]):

            # Counter decreases by 1
            ctr -=1

            (run, subrun, seq) = (int(x[0]), int(x[1]), 0)

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            parent_status = self._api.get_status( ds_status( self._parent_project, run, subrun, seq ))

            self.info('received data %s'%parent_status._data)

            f=open(parent_status._data,'rb')
            run_   = f.read(1)
            subrun_= f.read(1)
            flag_  = f.read(1)
            f.close()

            run_   = int(run_.encode('hex'),16)
            subrun_= int(subrun_.encode('hex'),16)
            flag_  = int(flag_.encode('hex'),16)

            self.info('found flag %d'%flag_)
            
            status = 0

            # Check input file exists. Otherwise report error
            
            time.sleep(0.5)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = 0,
                                data    = flag_)

            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = extract_data_from_sam()

    test_obj.process_newruns()
