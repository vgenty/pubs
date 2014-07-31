from pub_dbi import pubdb_conn_info
from dstream import ds_reader,ds_writer
from dstream import ds_status

class ds_proc_base:

    def __init__(self):
        
        self._api    = ds_writer(pubdb_conn_info.writer_info())
        self._logger = self.__class__.__name__


