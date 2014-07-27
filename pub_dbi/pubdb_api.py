import inspect
from pub_util import pub_logger
from pubdb_conn import pubdb_conn, pubdb_conn_info
from pubdb_data import pubdb_status_info

class pubdb_reader:

    def __init__(self,conn_info):
        self._name   = '%s' % inspect.stack()[1][3]
        self._cursor = pubdb_conn.cursor(conn_info)
        self._logger = pub_logger.get_logger(self._name)
        self._row_id = -1

    def __iter__(self):
        return self._cursor

    def next(self):
        return self._cursor.next()        

    def __del__(self):
        self._cursor.close()
        self._logger.debug('%s reader cursor destroyed.' % self._name)

    def get_runs(self,table_v, status_v):
        pass
        
class pubdb_writer(pubdb_reader):

    def __init__(self,conn_info):

        self._name   = '%s' % inspect.stack()[1][3]
        self._cursor = pubdb_conn.cursor(conn_info)
        self._logger = pub_logger.get_logger(self._name)

    def __del__(self):
        self._cursor.close()
        self._logger.debug('%s writer cursor destroyed.' % self._name)

    def log_status(self,run,subrun,status,project):
        pass

class pubdb_master(pubdb_reader):

    def __init__(self,conn_info):

        self._name   = '%s' % inspect.stack()[1][3]
        self._cursor = pubdb_conn.cursor(conn_info)
        self._logger = pub_logger.get_logger(self._name)

    def __del__(self):
        self._cursor.close()
        self._logger.debug('%s master cursor destroyed.' % self._name)

    def query(self,query):
        self._logger.warning('Executing: \'%s\'' % query)
        self._cursor.execute(query)
#        self._cursor.commit()

    def insert_newrun(self, info):

        if not isinstance(info,pubdb_status_info):
            self._logger.exception('Must provide pubdb_status_info data type instance!')
            raise DBException()
    
        
