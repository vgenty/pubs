## @package ds_api defines database API for dstream
# The module defines three classes: 0) ds_reader, 1) ds_writer, 2) ds_master
# These are API to interact with database, and not meant to be exposed to
# an end-user.

# python import
import psycopg2, sys
# pub_util package import
from pub_util import pub_logger
# pub_dbi package import
from pub_dbi import pubdb_reader, pubdb_writer
# dstream import
from ds_data import ds_status
from ds_exception import DSException

## @class ds_reader 
# @brief This is a read-only API for dstream database
# ds_reader implements dstream project specific read function to fetch
# information from the database. This API may be used in some class/functions
# that only read from database.
class ds_reader(pubdb_reader):

    ## Checks if a specified project exists or not.
    def project_exist(self,project):

        query = 'SELECT DoesTableExist(\'%s\')' % project
        exist = False
        if self.execute(query):
            for x in self._cursor:
                exist = x[0]
        return exist

    ## Fetch run/subrun for a specified project with status
    # Fetch run & sub-runs for a specified project (tname) with a specified status.
    # Upon success, the underneath psycopg2 cursor contains returned rows.
    # If you are writing a project implementation class, see ds_proc_base.
    def get_runs(self,tname,status):

        if not type(tname)==type(str()) or not type(status)==type(int()):
            self._logger.critical('Invalid input data type!')
            return False
        query = 'SELECT Run,SubRun,Seq,ProjectVer FROM GetRuns(\'%s\',%d);' % (tname,status)
        return self._cursor.execute(query)

    ## Fetch run/subrun for a set of specified project with status
    # Fetch run & sub-runs for a group of projects and status.
    # The first argument should be a list of strings each representing a name of project.
    # The second argument should be a list of integers each representing a status of project.
    # Upon success, the underneath psycopg2 cursor contains returned rows.
    # If you are writing a project implementation class, see ds_proc_base.
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

## @class ds_writer
# @brief This is a suitable API for project implementation class
# ds_reader implements dstream project specific write function to fetch
# information from the database. As it inherits from ds_reader, it has
# all read functions implemented in that base class.
# This is the API for project classes.
class ds_writer(pubdb_writer,ds_reader):

    ## ds_status object validity checker
    # An internal function to check if a provided status info is the right type,
    # namely ds_status class instance.
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

    ## Update the database table contents with a provided info (ds_status instance).
    def log_status(self, info):

        if not self._check_info(info): return False

        query = 'SELECT UpdateProjStatus(\'%s\',%d,%d,%d::SMALLINT,%d::SMALLINT);'
        query = query % ( info._project,
                          info._run,
                          info._subrun,
                          info._seq,
                          info._status )

        return self.commit(query)

## @class ds_writer
# @brief This is a database API specialized for master scheduler process
# ds_writer implements read/write functions needed for ds_daemon class specifically.
# This should not be used by project class instances.
class ds_master(pubdb_writer,ds_reader):

    ## Fetch a list of projects for execution
    def list_projects(self):

        query = 'SELECT Project,Command,Frequency,StartRun,StartSubRun,Email,Resource FROM ListEnabledProject()'
    
        return self.execute(query)

    ## @brief Define a new project
    # project .... string, name of a project
    # command .... string, command to be executed
    # frequency .. integer, period between command executions
    # email ...... string, email address to which message is sent upon error
    def define_project(self,project,command,frequency,email):
        
        query = ' SELECT DefineProject(\'%s\',\'%s\',%d,\'%s\');' % (project,command,frequency,email)
        
        return self.commit(query)

    ## @brief remove project from process database
    # project .... string, name of a project
    def remove_project(self,project):

        if not self.project_exist(project):
            self._logger.error('Project %s not found!' % project)
            return

        self._logger.warning('About to remove project %s from DB. Really want to proceed? ' % project)
        user_input = ''
        while not user_input:
            sys.stdout.write('Answer [y/n]: ')
            sys.stdout.flush()
            user_input=sys.stdin.readline().rstrip('\n').rstrip(' ')
            if user_input in ['y','Y']:
                break
            elif user_input in ['n','N']:
                return
            else:
                self._logger.error('Invalid input command: %s' % user_input)
                user_input=''
                continue
                
        query = ' SELECT RemoveProject(\'%s\');' % project
        
        return self.commit(query)


    ## @brief Define a new project
    # project .... string, name of a project
    # command .... string, command to be executed
    # frequency .. integer, period between command execution
    # email ...... string, email address to which message is sent upon error
    def update_project(self, project, command, frequency, email):

        if not self.project_exist(project):
            self._logger.error('Project %s not found!' % project)
            return
        
        query = ' SELECT UpdateProjectConfig(\'%s\',\'%s\',%d,\'%s\');' % (project,command,frequency,email)
        
        return self.commit(query)
