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

        query = 'SELECT DoesProjectExist(\'%s\')' % project
        exist = False
        if self.execute(query,True):
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
            return []
        if not self.project_exist(tname):
            self._logger.critical('Project %s does not exist!' % tname)
            return []

        query = 'SELECT Run,SubRun,Seq,ProjectVer FROM GetRuns(\'%s\',%d);' % (tname,status)

        runs =[]
        if not self.execute(query): return runs

        if self.nrows():
            for x in self:
                runs.append(x)
        return runs

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

        runs = []
        if not self.execute(query): return runs
        
        if self.nrows():
            for x in self:
                runs.append(x)
        return x

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

        self.execute(query)
        
        info_array = []

        if not self.nrows() or self.nrows() <= 0: return info_array

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

    ## Fetch DAQ run start/end time stamp
    def run_timestamp(self,run,subrun):

        try:
            run    = int(run)
            subrun = int(subrun)
            if run<= 0 or subrun <= 0:
                raise ValueError
        except ValueError:
            self._logger.error('Run/SubRun must be positive integers!')
            return (None,None)

        query = 'SELECT TimeStart, TimeStop FROM GetRunTimeStamp(%d,%d)' % (run,subrun)

        if not self.execute(query): return (None,None)

        if self.nrows():
            return self.fetch_one();
        else:
            return (None,None)

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

    ## @brief internal method to ask a binary question to a user
    def _ask_binary(self):

        user_input=''
        while not user_input:
            sys.stdout.write('Proceed? [y/n]: ')
            sys.stdout.flush()
            user_input=sys.stdin.readline().rstrip('\n').rstrip(' ')
            if user_input in ['y','Y']:
                break
            elif user_input in ['n','N']:
                return False
            else:
                self._logger.error('Invalid input command: %s' % user_input)
                user_input=''
                continue
        return True

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
        
        if not self._ask_binary(): return False
                
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

        if not self.nrows() or self.nrows() <= 0:
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

        if not self._ask_binary(): return False;
        
        query = ' SELECT UpdateProjectConfig(\'%s\',\'%s\',%d,\'%s\');'
        query = query % ( info._project, info._command, info._period, info._email )

        return self.commit(query)

    ## @brief Method to synchronize all project tables with MainRun table
    def runsynch(self):

        query = ' SELECT AllProjectRunSynch(); '
        
        return self.commit(query)


## @class death_star
# @brief Database API for the dark lord. Don't use it if you don't understand.
# @details
# Don't use it. Even if you know what you are doing, I recommend you don't use it. OK?
class death_star(ds_master):

    ## @brief Destroys an earth-sized planet in one shot. Your projects all disappear. Amen.
    #  @details
    #  Remove all projects and re-make an empty ProcessTable. MainRun is not touched.
    def superbeam(self):

        self._logger.warning('Attempting to remove ALL project & ProcessTable...')

        if not self._ask_binary(): return False
        
        query = ' SELECT RemoveProcessDB();'

        result = self.commit(query)

        if result:

            query = 'SELECT CreateProcessTable();'

            result = self.commit(query)

        if result:

            self._logger.warning('Destroyed the planet Alderaan!')
            
        else:

            self._logger.warning('SuperBeam broken and didn\'t work...')

        return result

    ## @brief Dark-side never disappears. Rebuild a new Anakin from scratch.
    #  @details
    #  Drop & re-create MainRun table. This requires to drop all projects.\n
    #  Outcome is an empty MainRun and ProcessTable.
    def recreate_death_star(self):

        self._logger.warning('Attempting to re-build MainRunTable! (will be empty)...')
        
        if not self._ask_binary(): return False
        
        query = 'SELECT CreateTestRunTable();'

        result = self.commit(query)

        if result:

            query = 'SELECT RemoveProcessDB();'

            result = self.commit(query)

        if result:

            query = 'SELECT CreateProcessTable();'

            result = self.commit(query)

        if result:

            self._logger.warning('Death Star is re-born.')

        else:

            self._logger.warning('Failed re-building the Death Star.')

        return result

    ## @brief Dark-side can create anything, even real data, and you will not notice.
    #  @detail
    #  Recreate MainRun table. This requires to drop all projects first.\n
    #  Outcome is a newly filled MainRun table with an empty ProcessTable
    def refill_death_star(self,run,subrun):

        try:
            run=int(run)
            subrun=int(subrun)
            if run <= 0 or subrun <= 0:
                raise ValueError
        except ValueError:
            self._logger.error('Provided (run,subrun) in an wrong type!' % (str(run),str(subrun)))
            return False

        self._logger.warning('Attempting to re-build MainRun table.')
        self._logger.warning('Requires to drop all projects as well.')
        self._logger.warning('Will be filled with %d runs (%d sub-runs each).' % (run,subrun))

        if not self._ask_binary(): return False

        query = 'SELECT RemoveProcessDB();'

        result = self.commit(query)

        if result:

            query = 'SELECT FillTestRunTable(%d,%d);' % (run,subrun)

            result = self.commit(query)

        if result:

            query = 'SELECT CreateProcessTable();'

            result = self.commit(query)

        if result:

            self._logger.warning('Death Star is re-built and complete.')

        else:

            self._logger.warning('Death Star re-built failed. My god.')
        
        return result

    ## @brief Contribute to a dark-side run-by-run.
    #  @details Fill the MainRun table with a specified run/sub-run number
    def insert_into_death_star(self,run,subrun,ts_start,ts_end):

        try:
            time.strptime(str(ts_start),"%Y-%m-%d %H:%M:%S")
            time.strptime(str(ts_end),"%Y-%m-%d %H:%M:%S")
        except ValueError:
            self.error('TimeStamp not in the right format! (must be %Y-%m-%d %H:%M:%S)')
            return False

        query = 'SELECT InsertIntoTestRunTable(%d,%d,\'%s\'::TIMESTAMP,\'%s\'::TIMESTAMP'
        
        query = query % (run,subrun,str(ts_start),str(ts_end))

        result = self.commit(query)
        
        if result:

            self._logger.warning('Thank you. Death Star became 1 run bigger.')

        else:

            self._logger.warning('Sorry. Seems you failed to grow the Death Star.')

        return result





