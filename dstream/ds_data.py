import inspect
from pub_util import pub_logger

class ds_status(object):

    def __init__ (self, 
                  project = '',
                  run     = -1,
                  subrun  = -1,
                  seq     = -1,
                  status  = -1):
        
        try:
            self._project = str(project)
            self._run     = int(run)
            self._subrun  = int(subrun)
            self._seq     = int(seq)
            self._status  = int(status)
        except ValueError:
            name   = '%s' % inspect.stack()[1][3]
            pub_logger.get_logger(name).critical('Invalid value type!')
            self._project = ''
            self._run = self._subrun = self._seq = self._status = -1
            raise DSException()

    def is_valid(self):
        
        if ( not self._project or 
             self._run    < 0  or 
             self._subrun < 0  or 
             self._seq    < 0  or 
             self._seq    < 0 ):

            return False

        else: return True

