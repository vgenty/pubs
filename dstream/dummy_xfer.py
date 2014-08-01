# python include
import time
# dstream class include
from ds_exception import DSException
from ds_proc_base import ds_proc_base
from ds_data import ds_status
# pub_dbi package include
from pub_dbi import DBException


class dummy_xfer(ds_proc_base):

    def __init__(self,nruns):

        super(dummy_xfer,self).__init__()
        self._project = self.__class__.__name__
        self._nruns   = int(nruns)

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


