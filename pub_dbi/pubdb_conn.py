import inspect
import copy
import psycopg2
import pubdb_env
from pub_util        import pub_logger,pub_exception
from pubdb_exception import DBException
from pubdb_data      import pubdb_conn_info

class pubdb_conn:

    _conn_v = []
    _conn_info_v = []
    _logger = pub_logger.get_logger(__name__)

    def __init__(self):
        self._logger = self.__class__._logger
        self._logger.warning('%s instance created by %s ' % (self.__class__,
                                                             inspect.stack()[1][3])
                         )
        self._logger.warning('This is not needed as this is a factory class!')

    @classmethod
    def cursor(cls,conn_info):

        if not isinstance(conn_info,pubdb_conn_info):
            cls._logger.exception('Input conn_info: %s not pubdb_conn_info type!' % conn_info)
            raise DBException()

        conn_index=len(cls._conn_v)
        for x in xrange(len(cls._conn_info_v)):
        
            if cls._conn_info_v[x] == conn_info:
                cls._logger.debug('Connection already exists: (%s,%s,%s,%s)' % (conn_info._host,
                                                                                conn_info._db,
                                                                                conn_info._user,
                                                                                conn_info._passwd))
                conn_index = x

        if conn_index == len(cls._conn_v):

            try:
                conn = psycopg2.connect(host=conn_info._host,
                                        database=conn_info._db,
                                        user=conn_info._user,
                                        password=conn_info._passwd)
                cls._conn_v.append(conn)
                cls._conn_info_v.append(copy.copy(conn_info))
                cls._logger.info('Connected to DB: (%s,%s,%s,%s)' % (conn_info._host,
                                                                     conn_info._db,
                                                                     conn_info._user,
                                                                     conn_info._passwd))
            except psycopg2.OperationalError as e:
                cls._logger.exception('Connection failed (%s,%s,%s,%s)' % (conn_info._host,
                                                                           conn_info._db,
                                                                           conn_info._user,
                                                                           conn_info._passwd))
                
                raise DBException()
            
        return cls._conn_v[conn_index].cursor()


