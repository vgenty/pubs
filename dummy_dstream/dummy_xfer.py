## @namespace dummy_dstream.dummy_xfer
#  @ingroup dummy_dstream
#  @brief Defines a toy project class called dummy_xfer

# python include
import time
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
# pub_dbi package include
from pub_dbi import DBException

## @class dummy_xfer
#  @brief A fake project class to serve as a demo
#  @details
#  This class has only 2 methods to serve as a demo. It reads in run/subrun from\n
#  database with status=1 (i.e. new run), change status to 10. Another function\n
#  of this class can read in run/subrun with status 10 and change their status\n
#  to 0.
class dummy_xfer(ds_project_base):

    ## @brief Default ctor defines project name & # of runs to process at a time
    def __init__(self,nruns):

        super(dummy_xfer,self).__init__()
        self._project = self.__class__.__name__
        self._nruns   = int(nruns)
        
    ##
    # @brief One dummy function to change status.
    # It process # runs specified @ ctor. Access those runs with status = 1,\n
    # and change status = 10
    def process_newruns(self):

        try:
            self.connect()
        except DBException as e:
            self.error('Connection failed! Aborting...')
            return
        
        ctr = self._nruns
        for x in self.get_runs(self._project,1):
            print 'processing new runs...',x
            if not x[0]: break
            ctr -=1
            self.log_status(ds_status(self._project,
                                      x[0],
                                      x[1],
                                      x[2],
                                      10))
            time.sleep(0.5)
            if not ctr: break
    ##
    # @brief Another dummy function to change status.
    # It process # runs specified @ ctor. Access those runs with status = 10,\n
    # and change status = 0
    def process_ongoing_runs(self):

        try:
            self.connect()
        except DBException as e:
            self.error('Connection failed! Aborting...')
            return

        ctr = self._nruns
        for x in self.get_runs(self._project,10):
            print 'processing on-going runs...',x
            if not x[0]: break
            ctr -=1
            self.log_status(ds_status(self._project,
                                      x[0],
                                      x[1],
                                      x[2],
                                      0))

            time.sleep(0.5)
            if not ctr: break

if __name__ == '__main__':
    k=dummy_xfer(5)
    k.process_newruns()
    k.process_ongoing_runs()


