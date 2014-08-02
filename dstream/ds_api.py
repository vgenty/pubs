## @namespace dstream.ds_api 
#  @ingroup dstream 
#  @brief dstream dedicated database interface module
#  @details
#  The module defines three classes: 0) ds_reader, 1) ds_writer, 2) ds_master\n
#  These are API to interact with database, and not meant to be exposed to\n
#  an end-user.


# python import
import psycopg2, sys
# pub_util package import
from pub_util     import pub_logger
# pub_dbi package import
from pub_dbi      import pubdb_reader, pubdb_writer
# dstream import
from ds_data      import ds_status, ds_project
from ds_exception import DSException

## @class ds_reader 
# @brief Most basic read-only API for dstream database
# @details
# ds_reader implements dstream project specific read function to fetch\n
# information from the database. This API may be used in some class/functions\n
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

    ## @brief Fetch run/subrun for a specified project with status
    # Fetch run & sub-runs for a specified project (tname) with a specified status.\n
    # Upon success, the underneath psycopg2 cursor contains returned rows.\n
    # If you are writing a project implementation class, see ds_proc_base.\n
    def get_runs(self,tname,status):

        if not type(tname)==type(str()) or not type(status)==type(int()):
            self._logger.critical('Invalid input data type!')
            return False
        query = 'SELECT Run,SubRun,Seq,ProjectVer FROM GetRuns(\'%s\',%d);' % (tname,status)
        return self._cursor.execute(query)

    ## @brief Fetch run/subrun for a set of specified project with status
    # Fetch run & sub-runs for a group of projects and status.\n
    # The first argument should be a list of strings each representing a name of project.\n
    # The second argument should be a list of integers each representing a status of project.\n
    # Upon success, the underneath psycopg2 cursor contains returned rows.\n
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

    ## Fetch project information. Return is a ds_project data holder instance.
    def project_info(self,project,field_name=None):
        
        project = str(project)
        if not self.project_exist(project):
            self._logger.error('Project %s does not exist!' % project)

        query = 'SELECT Command, Frequency, StartRun, StartSubRun, Email, Resource, ProjectVer'

        if field_name:
            
            query += ' FROM ProjectInfo(\'%s\',\'%s\')' % (project,str(field_name))

        else:
            
            query += ' FROM ProjectInfo(\'%s\')' % project

        self.execute(query)

        x = self.fetchone()
        
        return ds_project(project  = project,
                          command  = x[0],
                          period   = x[1],
                          run      = x[2],
                          subrun   = x[3],
                          email    = x[4],
                          resource = x[5],
                          ver      = x[6])
        
    ## Fetch a list of enabled projects for execution. Return is an array of ds_project.
    def list_projects(self):

        query  = ' SELECT Project,Command,Frequency,StartRun,StartSubRun,Email,Resource'
        query += ' FROM ListEnabledProject()'

        info_array = []

        self.execute(query)

        if not self.nrows() : return info_array

        for x in self:
            info_array.append( ds_project( project  = x[0],
                                           command  = x[1],
                                           period   = int(x[2]),
                                           run      = int(x[3]),
                                           subrun   = int(x[4]),
                                           email    = x[5],
                                           resource = x[6],
                                           enable   = True ) )
        return info_array

## @class ds_writer
# @brief Database API for dstream projects with partial write permission
# @details
# ds_reader implements dstream project specific write function to fetch\n
# information from the database. As it inherits from ds_reader, it has\n
# all read functions implemented in that base class.\n
# This is the API for project classes.
class ds_writer(pubdb_writer,ds_reader):

    ## @brief ds_status object validity checker
    # An internal function to check if a provided status info is the right type,\n
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

## @class ds_master
# @brief Database API specialized for master scheduler, not for projects
# @details
# ds_writer implements read/write functions needed for ds_daemon class specifically.\n
# This should not be used by project class instances.
class ds_master(pubdb_writer,ds_reader):

    ## @brief Define a new project. Input is ds_project object.
    def define_project(self,project_info):

        if not isinstance(project_info,ds_project):

            self._logger.error('Invalid arg type: you must provide ds_project object!')
            return False

        elif not project_info.is_valid():
            
            self._logger.error('Provided project info contains invalid values!')
            return False
        
        query = 'SELECT DefineProject(\'%s\',\'%s\',%d,\'%s\',%d,%d,\'%s\');'
        
        resource = ''
        for x in project_info._resource.keys():
            
            resource += '\'%s\'=>\'%s\',' % (x, project_info._resource[x])
            
        resource = resource.rstrip(',')

        query = query % ( project_info._project,
                          project_info._command,
                          project_info._period,
                          project_info._email,
                          project_info._run,
                          project_info._subrun,
                          resource )
        
        return self.commit(query)

    ## @brief remove project from process database.
    def remove_project(self,project):

        if not self.project_exist(project):
            self._logger.error('Project %s not found!' % project)
            return False

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


    ## @brief Update existing project. Input is a ds_project object.
    def update_project(self, info):

        if not isinstance(info,ds_project):

            self._logger.error('Invalid arg type: you must provide ds_project object!')
            return False

        elif not info.is_valid():
            
            self._logger.error('Provided project info contains invalid values!')
            return False

        if not self.project_exist(info._project):
            self._logger.error('Project %s not found!' % info._project)
            return False

        query = ' SELECT ProjectInfo(\'%s\')' % info._project

        self.execute(query)

        if not self.nrows():
            self._logger.error('Failed to fetch project information for %s!',
                               info._project)

        orig_info = self.project_info(info._project)

        info._run    = orig_info._run
        info._subrun = orig_info._subrun
        info._ver    = orig_info._ver

        self._logger.warning('Attempting to alter project configuration...')
        self._logger.info('Command : %s => %s' % (orig_info._command, info._command))
        self._logger.info('Period  : %d => %d' % (orig_info._period,  info._period ))
        self._logger.info('Email   : %s => %s' % (orig_info._email,   info._email  ))
        self._logger.info('Enabled : %s => %s' % (orig_info._enable,  info._enable))

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
        
        query = ' SELECT UpdateProjectConfig(\'%s\',\'%s\',%d,\'%s\');'
        query = query % ( info._project, info._command, info._period, info._email )

        return self.commit(query)


