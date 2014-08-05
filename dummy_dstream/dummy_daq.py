## @namespace dummy.dummy_daq
#  @ingroup dummy
#  @brief Defines a project dummy_daq
#  @author kazuhiro

# python include
import time,os
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class dummy_daq
#  @brief kazuhiro should give a brief comment here
#  @details
#  kazuhiro should give a detailed comment here
class dummy_daq(ds_project_base):

    # Define project name as class attribute
    _project = 'dummy_daq'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(dummy_daq,self).__init__()

        self._out_file_format = ''
        self._out_dir = ''
        self._nruns   = None

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns = int(resource['NRUNS'])
            self._out_dir = '%s/%s' % (os.environ['PUB_TOP_DIR'],resource['OUTDIR'])
            self._outfile_format = resource['OUTFILE_FORMAT']

        ctr = self._nruns
        for x in self.get_runs(self._project,1):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            f = open('%s/%s' % (self._out_dir, self._outfile_format % (run,subrun)),'w')
            f.write('Dummy data for run %d, subrun %d' % (run,subrun))
            f.close()

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = 2 )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break


    ## @brief access DB and retrieves new runs
    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns = int(resource['NRUNS'])
            self._out_dir = '%s/%s' % (os.environ['PUB_TOP_DIR'],resource['OUTDIR'])
            self._outfile_format = resource['OUTFILE_FORMAT']

        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])
            status = 0
            if os.path.isfile('%s/%s' % (self._out_dir, self._outfile_format % (run,subrun))):

                self.info('validated run: run=%d, subrun=%d ...' % (run,subrun))
                
            else:

                self.error('error on run: run=%d, subrun=%d ...' % (run,subrun))

                status = 1

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = dummy_daq()

    test_obj.process_newruns()

    test_obj.validate()



