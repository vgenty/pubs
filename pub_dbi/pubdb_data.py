import inspect
import pubdb_env
from pub_util        import pub_logger 
from pubdb_exception import DBException

class pubdb_conn_info(object):

    def __init__ (self,host,port,db,user,passwd,ntrial,sleep,role):

        if not port:
            self._port = None
        else:
            self._port   = int( port   )
        
        self._host   = str( host   )
        self._db     = str( db     )
        self._user   = str( user   )
        self._role   = str( role   )
        self._passwd = str( passwd )
        self._ntrial = int( ntrial )
        self._sleep  = int( sleep  )

    @classmethod
    def reader_info(cls):
        return cls(pubdb_env.kREADER_HOST,
                   pubdb_env.kREADER_PORT,
                   pubdb_env.kREADER_DB,
                   pubdb_env.kREADER_USER,
                   pubdb_env.kREADER_PASS,
                   pubdb_env.kREADER_CONN_NTRY,
                   pubdb_env.kREADER_CONN_SLEEP,
                   pubdb_env.kREADER_ROLE)

    @classmethod
    def writer_info(cls):
        return cls(pubdb_env.kWRITER_HOST,
                   pubdb_env.kWRITER_PORT,
                   pubdb_env.kWRITER_DB,
                   pubdb_env.kWRITER_USER,
                   pubdb_env.kWRITER_PASS,
                   pubdb_env.kWRITER_CONN_NTRY,
                   pubdb_env.kWRITER_CONN_SLEEP,
                   pubdb_env.kWRITER_ROLE)

    @classmethod
    def admin_info(cls):
        return cls(pubdb_env.kADMIN_HOST,
                   pubdb_env.kADMIN_PORT,
                   pubdb_env.kADMIN_DB,
                   pubdb_env.kADMIN_USER,
                   pubdb_env.kADMIN_PASS,
                   pubdb_env.kADMIN_CONN_NTRY,
                   pubdb_env.kADMIN_CONN_SLEEP,
                   pubdb_env.kADMIN_ROLE)

    def __eq__(self,other):
        if isinstance(other,pubdb_conn_info):
            issame = ( self._host   == other._host   and
                       self._port   == other._port   and
                       self._db     == other._db     and
                       self._user   == other._user   and
                       self._ntrial == other._ntrial and
                       self._sleep  == other._sleep  and
                       self._role   == other._role)
            if issame and not self._passwd == other._passwd:
                issame = False
                pub_logger.get_logger('pubdb').exception('Same configuration but different password!')
                raise DBException()
            return issame
            
        else:
            pub_logger.get_logger('pubdb').exception('Invalid type comparison!')
            raise DBException()

    def __ne__(self,other):
        result = self.__eq__(other)
        return not result






