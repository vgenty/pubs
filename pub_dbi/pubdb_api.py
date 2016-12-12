import inspect,logging,psycopg2,time
from pub_util import pub_logger
from pubdb_exception import DBException
from pubdb_conn import pubdb_conn
from pubdb_data import pubdb_conn_info

class pubdb_reader(object):

    _conn_info = pubdb_conn_info.reader_info()

    def __init__(self,
                 conn_info = None,
                 logger    = None):

        if not conn_info :
            conn_info = self.__class__._conn_info

        self._cursor    = None
        self._conn_info = conn_info
        self._logger    = logger
        self._n_retrial = 10
        self._sleep     = 10

        if not self._logger:
            self._logger = pub_logger.get_logger(self.__class__.__name__)
            
        elif not isinstance(logger,logging.Logger):
            pub_logger.get_logger('pubdb').critical('Invalid logger!')
            raise DBException()

        self.debug    = self._logger.debug
        self.info     = self._logger.info
        self.warning  = self._logger.warning
        self.error    = self._logger.error
        self.critical = self._logger.critical

    def fetchone(self):
        if not self._cursor: return None
        return self._cursor.fetchone()

    def fetchall(self):
        if not self._cursor: return None
        return self._cursor.fetchall()

    def is_cursor_connected(self):
        if not self._cursor: return None
        if self._cursor.closed: return False
        # Check if connection is still valid
        self._cursor.execute('SELECT 1;')
        return bool(self._cursor.rowcount)

    def is_conn_alive(self):
        if not self.is_cursor_connected(): return False
        return pubdb_conn.is_connected(self._conn_info)

    def connect(self):
        if self.is_cursor_connected(): return True

        if self._cursor:
            self._cursor.close()

        self._cursor = pubdb_conn.cursor(self._conn_info)

        if not self._cursor:
            raise DBException('Failed to obtain a cursor (connection not established)')

        return True

    def reconnect(self):
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if not pubdb_conn.reconnect(self._conn_info):
            return False
        self._cursor = pubdb_conn.cursor(self._conn_info)
        return bool(self._cursor)

    def _raise_cursor_exception(self,check_conn=False):
        if not self._cursor:
            raise DBException('Connection has never been established yet!')
        if check_conn and not self.is_connected():
            if not self.connect():
                raise DBException('Connection disappeared and cannot re-connect!')
            
    def __iter__(self):
        self._raise_cursor_exception()
        return self._cursor

    def next(self):
        self._raise_cursor_exception()
        return self._cursor.next()        

    def __del__(self):
        if self._cursor:
            self._cursor.close()
        self.debug('Reader cursor destroyed.')

    def nrows(self):
        self._raise_cursor_exception()
        return self._cursor.rowcount

    def execute(self,query,throw=False):
        if not self.connect():
            self.error("Failed to connect the DB...")
            return False
        try:
            self.debug(query)
            self._cursor.execute(query)
        except psycopg2.ProgrammingError as e:
            self.error(e.pgerror)
            if throw: raise
            return False
        except psycopg2.InternalError as e:
            self.error(e.pgerror)
            if throw: raise
            return False
        return True

class pubdb_writer(pubdb_reader):

    _conn_info = pubdb_conn_info.writer_info()
    
    def commit(self,query,throw=False):
        if not self.connect():
            self.error("Failed to connect the DB...")
            return False
        try:
            status = self.execute(query)
            pubdb_conn.commit(self._conn_info)
        except psycopg2.ProgrammingError as e:
            self.error(e.pgerror)
            if throw: raise
            status = False
        except psycopg2.InternalError as e:
            self.error(e.pgerror)
            if throw: raise
            status = False
        return status
