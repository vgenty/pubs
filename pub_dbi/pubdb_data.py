import inspect
import pubdb_env
from pub_util        import pub_logger 
from pubdb_exception import DBException

class pubdb_conn_info:

    _logger = pub_logger.get_logger(__name__)

    def __init__ (self,host,db,user,passwd):
        self._host   = str( host   )
        self._db     = str( db     )
        self._user   = str( user   )
        self._passwd = str( passwd )
        self._logger = self.__class__._logger

    @classmethod
    def reader_info(cls):
        return cls(pubdb_env.kREADER_HOST,
                   pubdb_env.kREADER_DB,
                   pubdb_env.kREADER_USER,
                   pubdb_env.kREADER_PASS)
    @classmethod
    def writer_info(cls):
        return cls(pubdb_env.kWRITER_HOST,
                   pubdb_env.kWRITER_DB,
                   pubdb_env.kWRITER_USER,
                   pubdb_env.kWRITER_PASS)

    def __eq__(self,other):
        if isinstance(other,pubdb_conn_info):
            issame = ( self._host   == other._host and
                       self._db     == other._db and
                       self._user   == other._user )
            if issame and not self._passwd == other._passwd:
                issame = False
                self._logger.exception('Same configuration but different password!')
                raise DBException()
            return issame
            
        else:
            self._logger.exception('Invalid type comparison!')
            raise DBException()

    def __ne__(self,other):
        result = self.__eq__(other)
        return not result


class pubdb_status_info:

    _logger = pub_logger.get_logger(__name__)

    def __init__ (self, 
                  project = '',
                  run     = -1,
                  subrun  = -1,
                  status  = -1,
                  seq     = -1):
        self._project = project
        self._run     = run
        self._subrun  = subrun
        self._seq     = seq
        self._status  = status
        self._logger  = self.__class__._logger





