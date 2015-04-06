import inspect
import pubdb_env
from pub_util        import pub_logger 
from pubdb_exception import DBException

class pubdb_conn_info(object):

    def __init__ (self,host,port,db,user,passwd,role=''):
        try:
            self._host   = str( host   )
            if port:
                self._port = int( port   )
            else:
                self._port = None
            self._db     = str( db     )
            self._user   = str( user   )
            self._role   = str( role   )
            self._passwd = str( passwd )
        except ValueError:
            print "PORT # is non-integer..."
            raise

    @classmethod
    def reader_info(cls):
        return cls(pubdb_env.kREADER_HOST,
                   pubdb_env.kREADER_PORT,
                   pubdb_env.kREADER_DB,
                   pubdb_env.kREADER_USER,
                   pubdb_env.kREADER_PASS,
                   pubdb_env.kREADER_ROLE)

    @classmethod
    def writer_info(cls):
        print pubdb_env.kWRITER_PASS
        return cls(pubdb_env.kWRITER_HOST,
                   pubdb_env.kWRITER_PORT,
                   pubdb_env.kWRITER_DB,
                   pubdb_env.kWRITER_USER,
                   pubdb_env.kWRITER_PASS,
                   pubdb_env.kWRITER_ROLE)

    def __eq__(self,other):
        if isinstance(other,pubdb_conn_info):
            issame = ( self._host   == other._host and
                       self._port   == other._port and
                       self._db     == other._db and
                       self._user   == other._user and
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






