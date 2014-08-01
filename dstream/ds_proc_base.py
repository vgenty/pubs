## @package ds_proc_base
# Defines some base class used in dstream package.
# - ds_base is a simple class with a logger & exception implementation
# - ds_proc_base further implements ds_writer API, and is suitable for project base class

# pub_util package import
from pub_util import pub_logger
# pub_dbi package import
from pub_dbi  import pubdb_conn_info
# dstream module import
from ds_api   import ds_writer

## @class ds_base
#  This class implements a common logger feature in dstream. Such feature includes
#  a logger with name = class name, and exception throwing DSException.
class ds_base(object):

    ## Default ctor
    # CTor is where it implements a common logger feature.
    # Note logger functions (debug, info, warning, error, critical) are imported
    # directly.
    def __init__(self):

        # Create attribute instances
        self._logger  = pub_logger.get_logger(self.__class__.__name__)
        
        # Import message functions
        self.debug    = self._logger.debug
        self.info     = self._logger.info
        self.warning  = self._logger.warning
        self.error    = self._logger.error
        self.critical = self._logger.critical

    ## Exception log & throw method
    # This sends message in exception format + raise actual DSException
    def exception(self,msg):
        self.critical(msg)
        raise DSException()

## @class ds_proc_base
# @brief This is a base class for a project.
# Suitable for a project base class. 
# In addition to the base ds_base class, this class implements API to interact
# with database. As it is for a project, ds_writer API is used. Instead of
# requiring inherited class to learn about how to use ds_writer API, this class
# implements a practical usage of ds_writer and hence hide a complexity of 
# database API.
class ds_project_base(ds_base):

    ## default ctor
    # Constructor implements database API on top of base class logger feature.
    # Note it also imports connect method directly from ds_writer API.
    def __init__(self):

        super(ds_proc_base,self).__init__()

        self._api = ds_writer(pubdb_conn_info.writer_info(),
                              logger=self._logger)

        # Import some of API function as is
        self.log_status = self._api.log_status
    
        self.connect = self._api.connect

    ## Get a list of run/subrun for a specified project with status
    # Use ds_reader::get_runs() function to fetch run/subrun combination for
    # a specified project with a specified status. Return is an array of int
    # representing (Run, SubRun, Sequence, ProjectVersion).
    def get_runs(self,project,status):

        runs =[]
        self._api.get_runs(project,status)
        if self._api.nrows():
            for x in self._api:
                runs.append(x)
        return runs

        

        
    
