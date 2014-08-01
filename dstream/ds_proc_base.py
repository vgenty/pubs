from pub_util import pub_logger
from pub_dbi  import pubdb_conn_info
from ds_api   import ds_writer

class ds_base(object):

    def __init__(self):

        # Create attribute instances
        self._logger  = pub_logger.get_logger(self.__class__.__name__)
        
        # Import message functions
        self.debug    = self._logger.debug
        self.info     = self._logger.info
        self.warning  = self._logger.warning
        self.error    = self._logger.error
        self.critical = self._logger.critical

    def exception(self,msg):
        self.critical(msg)
        raise DSException()

class ds_proc_base(ds_base):

    def __init__(self):

        super(ds_proc_base,self).__init__()

        self._api = ds_writer(pubdb_conn_info.writer_info(),
                              logger=self._logger)

        # Import some of API function as is
        self.log_status = self._api.log_status
    
        self.connect = self._api.connect
        
    def get_runs(self,project,status):

        runs =[]
        self._api.get_runs(project,status)
        for x in self._api:
            runs.append(x)
        return runs

    


        

        
    
