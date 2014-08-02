import inspect,logging,psycopg2
from pub_util import pub_logger
from pubdb_exception import DBException
from pubdb_conn import pubdb_conn
from pubdb_data import pubdb_conn_info

class pubdb_reader(object):

    def __init__(self,conn_info,logger=None):
        self._cursor    = None
        self._conn_info = conn_info
        self._logger    = logger
          
        if not self._logger:
            self._logger = pub_logger.get_logger('pubdb')
            
        elif not isinstance(logger,logging.Logger):
            pub_logger.get_logger('pubdb').critical('Invalid logger!')
            raise DBException()

    def fetchone(self):
        if not self._cursor: return None
        return self._cursor.fetchone()

    def connect(self):
        if self._cursor: return True
        self._cursor = pubdb_conn.cursor(self._conn_info)
        return bool(self._cursor)
            
    def _raise_cursor_exception(self):
        if not self._cursor:
            self._logger.critical('Connection not established!')
            raise DBException()

    def __iter__(self):
        self._raise_cursor_exception()
        return self._cursor

    def next(self):
        self._raise_cursor_exception()
        return self._cursor.next()        

    def __del__(self):
        if self._cursor:
            self._cursor.close()
        self._logger.debug('Reader cursor destroyed.')

    def nrows(self):
        self._raise_cursor_exception()
        return self._cursor.rowcount

    def execute(self,query,throw=False):
        self._raise_cursor_exception()
        try:
            self._cursor.execute(query)
        except psycopg2.ProgrammingError as e:
            self._logger.error(e.pgerror)
            if throw: raise
            return False
        except psycopg2.InternalError as e:
            self._logger.error(e.pgerror)
            if throw: raise
            return False
        return True

class pubdb_writer(pubdb_reader):

    def commit(self,query,throw=False):
        self._raise_cursor_exception()
        try:
            self.execute(query)
            pubdb_conn.commit(self._conn_info)
        except psycopg2.ProgrammingError as e:
            self._logger.error(e.pgerror)
            if throw: raise
            return False
        except psycopg2.InternalError as e:
            self._logger.error(e.pgerror)
            if throw: raise
            return False
        return True

    
