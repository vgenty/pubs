import inspect
import copy
import psycopg2
import pubdb_env
from pub_util        import pub_logger,pub_exception
from pubdb_exception import DBException
from pubdb_data      import pubdb_conn_info

class pubdb_conn(object):

    _conn_v = []
    _conn_info_v = []
    _logger = None

    def __init__(self):
        self.__class__._check_logger()
        self._logger = self.__class__._logger
        self._logger.warning('%s instance created by %s ' % (self.__class__,
                                                             inspect.stack()[1][3])
                         )
        self._logger.warning('This is not needed as this is a factory class!')

    @classmethod
    def _check_conn_info_type(cls,conn_info):
        if not isinstance(conn_info,pubdb_conn_info):
            cls._logger.critical('Input conn_info: %s not pubdb_conn_info type!' % conn_info)
            raise DBException()

    @classmethod
    def _check_logger(cls):
        if not cls._logger:
            cls._logger = pub_logger.get_logger(cls.__name__)

    @classmethod
    def _check_conn_info_exist(cls,conn_info):
        cls._check_conn_info_type(conn_info)
        conn_index=-1
        for x in xrange(len(cls._conn_info_v)):
        
            if cls._conn_info_v[x] == conn_info: conn_index = x
        return conn_index

    @classmethod
    def _closed(cls,conn_info):
        cls._check_logger()
        conn_index = cls._check_conn_info_exist(conn_info)
        if conn_index < 0:
            cls._logger.critical('Never existed connection: (%s,%s,%s,%s,%s)' % (conn_info._host,
                                                                                 conn_info._port,
                                                                                 conn_info._db,
                                                                                 conn_info._user,
                                                                                 conn_info._passwd))
            raise DBException()
        else:
            return cls._conn_v[conn_index].closed()

    @classmethod
    def _connect(cls,conn_info):
        cls._check_logger()
        conn_index=cls._check_conn_info_exist(conn_info)

        if conn_index < 0:

            try:
                conn = psycopg2.connect(host=conn_info._host,
                                        port=conn_info._port,
                                        database=conn_info._db,
                                        user=conn_info._user,
                                        password=conn_info._passwd)
                cls._conn_v.append(conn)
                cls._conn_info_v.append(copy.copy(conn_info))
                cls._logger.info('Connected to DB: (%s,%s,%s,%s,XXX)' % (conn_info._host,
                                                                        conn_info._port,
                                                                        conn_info._db,
                                                                        conn_info._user))
                if conn_info._role:
                    cursor = cls.cursor(conn_info)
                    try:
                        cursor.execute('SET ROLE %s;' % conn_info._role)
                        cursor.close()
                        del cursor
                    except psycopg2.ProgrammingError as e:
                        cls._logger.error(e.pgerror)
                        cursor.close()
                        del cursor
                        raise DBException()

            except psycopg2.OperationalError as e:
                cls._logger.critical('Connection failed (%s,%s,%s,%s,XXX)' % (conn_info._host,
                                                                              conn_info._port,
                                                                              conn_info._db,
                                                                              conn_info._user) )
                
                raise DBException()
            
        return conn_index

    @classmethod
    def _reconnect(cls,conn_info):
        cls._check_logger()
        conn_index = cls._check_conn_info_exist(conn_info)
        if conn_index < 0:
            cls._logger.critical('Never existed connection: (%s,%s,%s,%s,XXX)' % (conn_info._host,
                                                                                  conn_info._port,
                                                                                  conn_info._db,
                                                                                  conn_info._user))
            raise DBException()

        cls._conn_v[x].close()
        del cls._conn_v[x]
        return cls._connect(conn_info)

    @classmethod
    def commit(cls,conn_info):
        cls._check_logger()
        cls._check_conn_info_type(conn_info)

        conn_index = cls._check_conn_info_exist(conn_info)
        if conn_index < 0:
            cls._logger.critical('Never existed connection: (%s,%s,%s,%s,XXX)' % (conn_info._host,
                                                                                  conn_info._port,
                                                                                  conn_info._db,
                                                                                  conn_info._user))
            raise DBException()
        cls._conn_v[conn_index].commit()
        return True

    @classmethod
    def cursor(cls,conn_info):
        cls._check_logger()
        conn_index = cls._connect(conn_info)
        return cls._conn_v[conn_index].cursor()


