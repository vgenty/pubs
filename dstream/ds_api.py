import psycopg2
from pub_util import pub_logger
from pub_dbi import pubdb_reader, pubdb_writer
from ds_data import ds_status
from ds_exception import DSException

class ds_reader(pubdb_reader):

    def project_exist(self,project):
        query = 'SELECT DoesTableExist(\'%s\')' % project
        exist = False
        if self.execute(query):
            for x in self._cursor:
                exist = x[0]
        return exist

    def get_runs(self,tname,status):
        if not type(tname)==type(str()) or not type(status)==type(int()):
            self._logger.critical('Invalid input data type!')
            return False
        query = 'SELECT Run,SubRun,Seq,ProjectVer FROM GetRuns(\'%s\',%d);' % (tname,status)
        return self._cursor.execute(query)

    def get_xtable_runs(self,table_v, status_v):

        if not isinstance(table_v,list) or not isinstance(status_v,list):
            self._logger.critical('Invalid input data type!')
            raise DSException()
           
        if not len(table_v) == len(status_v):
            self._logger.critical('Input tuples with different length!')
            raise DSException()

        query_table = ''
        for x in table_v:
            if x == table_v[0]:  
                query_table += 'ARRAY[\'%s\'::TEXT' % x
            else:
                query_table += (',%s::TEXT' % x )
        query_table += ']'

        query_status = ''
        for x in status_v:
            if x == status_v[0]:  
                query_status += 'ARRAY[%d::SMALLINT' % x
            else:
                query_status += (',' + '%d::SMALLINT' % x )
        query_status += ']'

        query = 'SELECT Run, SubRun FROM GetRuns(%s,%s);' % (query_table,query_status)

        return self.execute(query)

class ds_writer(pubdb_writer,ds_reader):


    def _check_info(self,info):

        #if not isinstance(info,ds_status):
        #if not isinstance(info,ds_status) and not issubclass(ds_status,info.__class__):
        #if not type(info) == type(ds_status()):
        if not 'ds_status' in str(type(info)):
            self._logger.error('Must provide ds_status data type instance!')
            return False

        if not info.is_valid():
            
            self._logger.error('Invalid ds_status value contents!')
            return False
        
        return True

    def log_status(self, info):
        
        if not self._check_info(info): return False

        query = 'SELECT UpdateProjStatus(\'%s\',%d,%d,%d::SMALLINT,%d::SMALLINT);'
        query = query % ( info._project,
                          info._run,
                          info._subrun,
                          info._seq,
                          info._status )

        return self.commit(query)

class ds_master(pubdb_writer,ds_reader):
    
    def list_projects(self):

        query = 'SELECT Project,Command,Frequency,Email,Resource FROM ListEnabledProject()'
    
        return self.execute(query)
