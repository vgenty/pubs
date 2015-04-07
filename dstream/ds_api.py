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
from ds_data      import ds_status, ds_project, ds_daemon, ds_daemon_log
from ds_exception import DSException
from datetime     import datetime

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

    ## Checks if a specified daemon server exists or not.
    def daemon_exist(self,daemon):

        query = 'SELECT DoesDaemonExist(\'%s\');' % daemon
        exist = False
        if self.execute(query,True):
            for x in self._cursor:
                exist = x[0]
        return exist

    ## Function to get project's resource
    def get_resource(self,project):
        
        query = 'SELECT ProjectResource(\'%s\');' % str(project)

        resource = {}

        if not self.execute(query):
            return resource
        if not self.nrows() or self.nrows()<0:
            return resource

        res = self.fetchone()

        # handle resource string conversion into a map
        if res[0]:
        
            for y in res[0].split(','):
        
                tmp = y.split("=>")
                
                exec('resource[%s]=%s' % (tmp[0],tmp[1]))

        return resource

    ## @brief Fetch project information from run/sub-run/seq IDs.
    #  @details
    #  For a specified project, run, sub-run, seq numbers, return ds_status data product\n
    #  filled with data & status.
    def get_status(self,info):

        if not isinstance(info,ds_status):
            self._logger.error('Input argument must be ds_status data type!')
            raise DSException()
        elif not info.is_valid():
            self._logger.error('ds_status::is_valid() returned False!')
            raise DSException()

        query = 'SELECT Status, ProjectData FROM GetProjectData(\'%s\',%d,%d,%d::SMALLINT);'

        query = query % (info._project, info._run, info._subrun, info._seq)

        if not self.execute(query):
            self._logger.error('Failed querying project status')
            return info
            
        if not self.nrows() or self.nrows() < 0:
            self._logger.error('No result for project %s (run,subrun,seq) = (%d,%d,%d)' % ( info._project,
                                                                                            info._run,
                                                                                            info._subrun,
                                                                                            info._seq ) )
            return info
        
        (status,data) = self.fetchone()

        info._status = int(status)
        info._data   = data
        return info

    ## @brief Fetch run/subrun for a specified project with status
    #  @details
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
    #  @details
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
        for index in xrange(len(table_v)):
            if not index:
                query_table += 'ARRAY[\'%s\'::TEXT' % table_v[index]
            else:
                query_table += (',\'%s\'::TEXT' % table_v[index] )
        query_table += ']'

        query_status = ''
        for index in xrange(len(status_v)):
            if not index:
                query_status += 'ARRAY[%d::SMALLINT' % status_v[index]
            else:
                query_status += (',%d::SMALLINT' % status_v[index] )
        query_status += ']'

        query = 'SELECT Run, SubRun FROM GetRuns(%s,%s);' % (query_table,query_status)

        runs = []
        if not self.execute(query): return runs
        
        if self.nrows():
            for x in self:
                runs.append(x)
        return runs

    ## Fetch a daemon configuration for the specified server. Returns ds_daemon if found. Else None.
    def daemon_info(self,server):

        server = str(server)

        query  = ' SELECT MaxProjCtr, LifeTime, LogRange, EMail, Enabled'
        query += ' FROM DaemonTable'
        query += ' WHERE Server=\'%s\'; ' % server

        self.execute(query)

        if self.nrows() < 1:
            return None

        x = self.fetchone()

        return ds_daemon(server, x[0], x[1], x[2], x[3], x[4], x[5])

    ## Fetch a daemon log for the specified server. Returns a tuple of ds_daemon_log if found. Else None.
    def daemon_log(self,server):

        server = str(server)

        query  = ' SELECT MaxProjCtr, LifeTime, ProjCtr, UpTime, LogItem, LogTime'
        query += ' FROM DaemonLogTable'
        query += ' WHERE Server=\'%s\';' % server

        self.execute(query)

        if self.nrows() < 1:
            self._logger.info('No daemon log found for a server \'%s\'' % server)
            return None

        log_array=[]
        for x in self:
            log_array.append( ds_daemon_log(server, x[0], x[1], x[2], x[3], x[5]) )
            if x[6]:
                for y in x[6].split(','):
        
                    tmp = y.split("=>")

                    log_array[-1].add_log(tmp[0],tmp[1])

        return tuple(log_array)
    
    ## Fetch project information. Return is a ds_project data holder instance.
    def project_info(self,project,field_name=None):
        
        project = str(project)
        if not self.project_exist(project):
            self._logger.error('Project %s does not exist!' % project)

        query = 'SELECT Command, Frequency, Server, SleepAfter, RunTable, StartRun, StartSubRun, Email, Resource, ProjectVer, Enabled'

        if field_name:
            
            query += ' FROM ProjectInfo(\'%s\',\'%s\')' % (project,str(field_name))

        else:
            
            query += ' FROM ProjectInfo(\'%s\')' % project

        self.execute(query)

        x = self.fetchone()

        resource = {}
        
        # handle resource string conversion into a map
        if x[8]:
        
            for y in x[8].split(','):
        
                tmp = y.split("=>")
                
                exec('resource[%s]=%s' % (tmp[0],tmp[1]))

        return ds_project(project  = project,
                          command  = x[0],
                          period   = x[1],
                          server   = x[2],
                          sleep    = x[3],
                          runtable = x[4],
                          run      = x[5],
                          subrun   = x[6],
                          email    = x[7],
                          resource = resource,
                          ver      = x[9],
                          enable   = x[10])

    ## Fetch a list of enabled projects for execution. Return is an array of ds_project.
    def list_projects(self):

        query  = ' SELECT Project,Command,Frequency,Server,SleepAfter,RunTable,StartRun,StartSubRun,Email,Resource'
        query += ' FROM ListEnabledProject()'

        self.execute(query)
        
        info_array = []

        if not self.nrows() or self.nrows() <= 0: return info_array

        for x in self:

            resource = {}

            # handle resource string conversion into a map
            if x[9]:

                for y in x[9].split(','):

                    tmp = y.split("=>")

                    exec('resource[%s]=%s' % (tmp[0],tmp[1]))

            info_array.append( ds_project( project  = x[0],
                                           command  = x[1],
                                           period   = int(x[2]),
                                           server   = x[3],
                                           sleep    = x[4],
                                           runtable = x[5],
                                           run      = int(x[6]),
                                           subrun   = int(x[7]),
                                           email    = x[8],
                                           resource = resource,
                                           enable   = True ) )
        return info_array

    ## Fetch a list of all projects for execution. Return is an array of ds_project.
    def list_all_projects(self):

        query  = ' SELECT Project,Command,Frequency,StartRun,StartSubRun,Email,Enabled,Resource'
        query += ' FROM ListProject()'

        self.execute(query)
        
        info_array = []

        if not self.nrows() or self.nrows() <= 0: return info_array

        for x in self:

            resource = {}

            # handle resource string conversion into a map
            if x[7]:

                for y in x[7].split(','):

                    tmp = y.split("=>")

                    exec('resource[%s]=%s' % (tmp[0],tmp[1]))

            info_array.append( ds_project( project  = x[0],
                                           command  = x[1],
                                           period   = int(x[2]),
                                           run      = int(x[3]),
                                           subrun   = int(x[4]),
                                           email    = x[5],
                                           enable   = x[6], 
                                           resource = resource ) )

        return info_array

    ## Fetch DAQ run start/end time stamp
    def run_timestamp(self,run,subrun):

        try:
            run    = int(run)
            subrun = int(subrun)
            if run< 0 or subrun < 0:
                raise ValueError
        except ValueError:
            self._logger.error('Run/SubRun must be positive integers!')
            return (None,None)

        query = 'SELECT TimeStart, TimeStop FROM GetRunTimeStamp(%d,%d)' % (run,subrun)

        if not self.execute(query): return (None,None)

        if self.nrows() and self.nrows()>=0:
            return self.fetchone();
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
    #  @details
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
        
        query = ''
        if not info._data:
        
            query = 'SELECT UpdateProjStatus(\'%s\',%d,%d,%d::SMALLINT,%d::SMALLINT);'
            query = query % ( info._project,
                              info._run,
                              info._subrun,
                              info._seq,
                              info._status )
        else:
            query = 'SELECT UpdateProjStatus(\'%s\',%d,%d,%d::SMALLINT,%d::SMALLINT,\'%s\');'
            query = query % ( info._project,
                              info._run,
                              info._subrun,
                              info._seq,
                              info._status,
                              info._data)

        return self.commit(query)

    ## @brief Log daemon status into DaemonLogTable. Input is ds_daemon_log object.
    def log_daemon(self,daemon_log):

        if not isinstance(daemon_log,ds_daemon_log):

            self._logger.error('Invalid arg type: you must provide ds_daemon_log object!')
            return False

        elif not daemon_log.is_valid():

            self._logger.error('Provided daemon log contains invalid values!')
            return False

        query = 'SELECT UpdateDaemonLog(\'%s\',%d,%d,%d,%d,\'%s\');'

        log_item = ''
        for x in daemon_log._log.keys():
            
            log_item += '%s=>%s,' % (x, daemon_log._log[x])
            
        log_item = log_item.rstrip(',')
        
        query = query % (daemon_log._server,
                         daemon_log._max_proj_ctr,
                         daemon_log._lifetime,
                         daemon_log._proj_ctr,
                         daemon_log._uptime,
                         log_item)

        return self.commit(query)

## @class ds_master
# @brief Database API specialized for master scheduler, not for projects
# @details
# ds_writer implements read/write functions needed for ds_daemon class specifically.\n
# This should not be used by project class instances.
class ds_master(ds_writer,ds_reader):

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

    ## @brief Define a daemon. Input is ds_daemon object.
    def define_daemon(self,daemon_info):

        if not isinstance(daemon_info,ds_daemon):

            self._logger.error('Invalid arg type: you must provide ds_daemon object!')
            return False

        elif not daemon_info.is_valid():

            self._logger.error('Provided daemon info contains invalid values!')
            return False

        query = 'SELECT UpdateDaemonTable(\'%s\',%d,%d,%d,%d,%d,%d,\'%s\',%s);'

        query = query % (daemon_info._server,
                         daemon_info._max_proj_ctr,
                         daemon_info._lifetime,
                         daemon_info._log_lifetime,
                         daemon_info._runsync_time,
                         daemon_info._update_time,
                         daemon_info._cleanup_time,
                         daemon_info._email,
                         str(daemon_info._enable).upper() )

        return self.commit(query)

    ## @brief Define a new project. Input is ds_project object.
    def define_project(self,project_info):

        if not isinstance(project_info,ds_project):

            self._logger.error('Invalid arg type: you must provide ds_project object!')
            return False

        elif not project_info.is_valid():
            
            self._logger.error('Provided project info contains invalid values!')
            return False
        
        query = 'SELECT DefineProject(\'%s\',\'%s\',%d,\'%s\',%d,\'%s\',\'%s\',%d,%d,\'%s\');'
        
        resource = ''
        for x in project_info._resource.keys():
            
            resource += '%s=>%s,' % (x, project_info._resource[x])
            
        resource = resource.rstrip(',')

        query = query % ( project_info._project,
                          project_info._command,
                          project_info._period,
                          project_info._email,
                          project_info._sleep,
                          project_info._server,
                          project_info._runtable,
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
    def update_project(self, info, check=True):

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

        resource = ''
        for x in info._resource.keys():

            resource += '%s=>%s,' % (x, info._resource[x])

        resource = resource.rstrip(',')

        if check:
            self._logger.warning('Attempting to alter project configuration...')
            self._logger.info('Server  : %s => %s' % (orig_info._server,  info._server ))
            self._logger.info('Command : %s => %s' % (orig_info._command, info._command))
            self._logger.info('Period  : %d => %d' % (orig_info._period,  info._period ))
            self._logger.info('Sleep   : %d => %d' % (orig_info._sleep,   info._sleep  ))
            self._logger.info('Email   : %s => %s' % (orig_info._email,   info._email  ))
            self._logger.info('Enabled : %s => %s' % (orig_info._enable,  info._enable ))
            self._logger.info('New Resource: %s' % resource )
            
            if not self._ask_binary(): return False;
        
        query = ' SELECT UpdateProjectConfig(\'%s\',\'%s\',%d,%d,\'%s\',\'%s\',\'%s\',%s);'
        query = query % ( info._project,
                          info._command,
                          info._period,
                          info._sleep,
                          info._email,
                          info._server,
                          resource,
                          str(info._enable).upper() )

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
        
        query = 'SELECT RemoveProcessDB();'

        result = self.commit(query)

        if result:

            query = 'SELECT CreateProcessTable();'

            result = self.commit(query)

        if result:

            query = 'SELECT CreateDaemonTable();'

            result = self.commit(query)

        if result:

            query = 'SELECT CreateDaemonLogTable();'

            result = self.commit(query)

        if result:

            self._logger.warning('Death Star is re-born.')

        else:

            self._logger.warning('Failed re-building the Death Star.')

        return result

    ## @brief Dark-side can create anything, even real data, and you will not notice.
    #  @details
    #  Recreate MainRun table. This requires to drop all projects first.\n
    #  Outcome is a newly filled MainRun table with an empty ProcessTable
    def refill_death_star(self,name,run,subrun,recreate=False):

        try:
            run=int(run)
            subrun=int(subrun)
            if run < 0 or subrun < 0:
                raise ValueError
        except ValueError:
            self._logger.error('Provided (run,subrun) in an wrong type!' % (str(run),str(subrun)))
            return False

        self._logger.warning('Attempting to re-build MainRun table.')
        self._logger.warning('Requires to drop all projects as well.')
        self._logger.warning('Will be filled with %d runs (%d sub-runs each).' % (run,subrun))

        if not self._ask_binary():
            self._logger.warning('Death Star re-built failed. My god.')
            return False

        query = 'SELECT RemoveProcessDB();'

        if not self.commit(query):
            self._logger.warning('Death Star re-built failed. My god.')
            return False

        if recreate:
            query = 'DROP TABLE IF EXISTS %s;' % name

            if not self.commit(query):
                self._logger.warning('Death Star re-built failed. My god.')
                return False

            query = 'SELECT CreateTestRunTable(\'%s\')' % name

            if not self.commit(query):
                self._logger.warning('Death Star re-built failed. My god.')
                return False

        query = 'SELECT FillTestRunTable(\'%s\',%d,%d);' % (name,run,subrun)

        if not self.commit(query):
            self._logger.warning('Death Star re-built failed. My god.')
            return False

        query = 'SELECT CreateProcessTable();'
        if not self.commit(query):
            self._logger.warning('Death Star re-built failed. My god.')
            return False

        self._logger.warning('Death Star is re-built and complete.')
        
        return True

    ## @brief Contribute to a dark-side run-by-run.
    #  @details Fill the MainRun table with a specified run/sub-run number
    def insert_into_death_star(self,run,subrun,ts_start,ts_end):

        try:
            datetime.strptime(str(ts_start),"%Y-%m-%d %H:%M:%S")
            datetime.strptime(str(ts_end),"%Y-%m-%d %H:%M:%S")
        except ValueError:
            self.error('TimeStamp not in the right format! (must be %Y-%m-%d %H:%M:%S)')
            return False

        query = 'SELECT InsertIntoTestRunTable(%d,%d,\'%s\'::TIMESTAMP,\'%s\'::TIMESTAMP)'
        
        query = query % (run,subrun,str(ts_start),str(ts_end))

        result = self.commit(query)
        
        if result:

            self._logger.warning('Thank you. Death Star became 1 run bigger.')

        else:

            self._logger.warning('Sorry. Seems you failed to grow the Death Star.')

        return result





