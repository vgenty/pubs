## @namespace dstream.ds_data
#  @ingroup dstream
#  @brief Defines data holder class for dstream package
#  @details
#  Contains two data classes:\n
#  0) ds_status is used for logging the project status\n
#  1) ds_project is used to pass project execution information\n

# python import
import inspect, copy
# pub_util package import
from pub_util import pub_logger

## @class ds_status
# @brief Holds status of a project, and is used to log a status in DB.
# @details
# Instance holds a project name, run, sub-run, seq, and status read from DB.
class ds_status(object):

    ## @brief default ctor to specify all data members
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
    
    ## A method to make sure the instance is at least not stupid
    def is_valid(self):
        
        if ( not self._project or 
             self._run    < 0  or 
             self._subrun < 0  or 
             self._seq    < 0  or 
             self._seq    < 0 ):

            return False

        else: return True

## @class ds_project
# @brief This class holds a project information registered in ProcessTable.
# @details
# Stored information include name, command, start run number, start sub-run number,\n
# contact email, period between execution, version number, enable-flag, and resources\n
class ds_project(object):

    ## @brief default ctor to specify all data members
    def __init__ (self, project, command='', run=0, subrun=0,
                  email='', period=100, enable=True, resource={}, ver=-1):
        if not resource: resource = {}
        try:
            if not type(resource) == type(dict()):
                print type(resource)
                print type({})
                print 'aho'
                raise ValueError
            self._project  = str(project)
            self._command  = str(command)
            self._run      = int(run)
            self._subrun   = int(subrun)
            self._email    = str(email)
            self._period   = int(period)
            self._enable   = bool(enable)
            self._resource = copy.copy(resource)
            self._ver      = int(ver)
        except ValueError:
            name   = '%s' % inspect.stack()[1][3]
            pub_logger.get_logger(name).critical('Invalid value type!')
            raise DSException()

    def __str__(self):
        msg = ''
        msg += 'Project : %s\n' % self._project
        msg += 'Command : %s\n' % self._command
        msg += 'Run     : %d\n' % self._run
        msg += 'SubRun  : %d\n' % self._subrun
        msg += 'Email   : %s\n' % self._email
        return msg
    
    ## A method to make sure the instance is at least not stupid
    def is_valid(self):
        
        if ( not self._project or 
             not self._command or
             self._run    < 0  or 
             self._subrun < 0  or 
             not self._email   or
             self._period < 0 ):

            return False

        else: return True
    
