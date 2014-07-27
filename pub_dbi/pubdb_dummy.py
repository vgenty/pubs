from pubdb_data import pubdb_conn_info, pubdb_status_info
from pubdb_api import pubdb_reader, pubdb_writer

class pubdb_dummy:

    def __init__(self, conn_info = None):

        if not conn_info:
            conn_info = pubdb_conn_info.writer_info()

        self.db_api = pubdb_writer(conn_info)
        
        
        

