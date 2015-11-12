## @namespace dstream.ds_proc_base
#  @ingroup dstream 
#  @brief defines some base class used in dstream package.
#  @details 
#  Include two base classes handy for dstream classes to inherit from\n
#  - ds_base is a simple class with a logger & exception implementation\n
#  - ds_proc_base further implements ds_writer API, and is suitable for project base class\n

# pub_util package import
from pub_util import pub_logger
# pub_dbi package import
from pub_dbi  import pubdb_conn_info
# dstream module import
from ds_api   import ds_writer
from pub_dbi import DBException

## @class ds_base
#  @brief Base of ds_project_base (and some other dstream classes)
#  @details
#  This class implements a common logger feature in dstream. Such feature includes\n
#  a logger with name = class name, and exception throwing DSException.
class ds_base(object):

    ## @brief Default ctor
    #  @details
    #  CTor is where it implements a common logger feature.\n
    #  Note logger functions (debug, info, warning, error, critical) are imported\n
    #  directly.
    def __init__(self,logger_name=None):

        ## Attach a logger for each instance with a default name = class name
        if not logger_name:
            self._logger = pub_logger.get_logger(self.__class__.__name__)
        else:
            self._logger = pub_logger.get_logger(logger_name)

        # Import message functions from logger

        ## @brief pub_logger.debug function. 
        #  @details Takes a string as an input for debug message
        self.debug    = self._logger.debug

        ## @brief pub_logger.info function. 
        #  @details Takes a string as an input for info message
        self.info     = self._logger.info

        ## @brief pub_logger.warning function. 
        #  @details Takes a string as an input for warning message
        self.warning  = self._logger.warning

        ## @brief pub_logger.error function. 
        #  @details Takes a string as an input for error message
        self.error    = self._logger.error

        ## @brief pub_logger.critical function. 
        #  @details Takes a string as an input for critical message
        self.critical = self._logger.critical

    ## @brief Exception log & throw method
    #  @details
    #  This sends message in exception format + raise actual DSException
    def exception(self,msg):
        self.critical(msg)
        raise DSException()

## @class ds_project_base
#  @brief Recommended base class for a dstream project
#  @details
#  Suitable for a project base class. \n
#  In addition to the base ds_base class, this class implements API to interact\n
#  with database. As it is for a project, ds_writer API is used. Instead of\n
#  requiring inherited class to learn about how to use ds_writer API, this class\n
#  implements a practical usage of ds_writer and hence hide a complexity of \n
#  database API.
class ds_project_base(ds_base):

    ## @brief default ctor
    #  @details
    #  Constructor implements database API on top of base class logger feature.\n
    #  Note it also imports connect method directly from ds_writer API.
    def __init__(self,arg=None):

        super(ds_project_base,self).__init__(arg)

        ## @brief Use ds_writer API so inherit classes (projects) can log status
        self._api = ds_writer(pubdb_conn_info.writer_info(),
                              logger=self._logger)

        ## @brief Import ds_reader.log_status as is.
        self.log_status = self._api.log_status

        ## @brief Import API's ds_reader.get_runs as is.
        self.get_runs = self._api.get_runs

        ## @brief Import API's ds_reader.get_xtable_runs as is.
        self.get_xtable_runs = self._api.get_xtable_runs

        ## @brief Import API's ds_reader.run_timestamp as is.
        self.run_timestamp = self._api.run_timestamp

    ## @brief wrapper for connect() method in ds_writer
    #  @details
    #  A simple wrapper that returns True/False. This avoids a downstream class/function\n
    #  to learn about underneath class structure (though if doesn't hide if one's interested in).
    def connect(self):

        try:
            self._api.connect()
            return True
        except DBException as e:
            self.error('Failed to connect DB!')
            return False

        

        
    
