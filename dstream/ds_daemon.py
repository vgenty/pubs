## @namespace dstream.ds_daemon
#  @ingroup dstream
#  @brief Introduce simple daemon toolkit for executing projects
#  @details
#  Includes class ds_action (a unit process executor) and ds_daemon (a process manager)

# Python include
import time, copy
from subprocess   import Popen, PIPE
# dstream include
from ds_exception import DSException
from ds_proc_base import ds_base
from ds_api       import ds_master
from ds_data      import ds_project
# pub_dbi package include
from pub_dbi      import pubdb_conn_info, DBException
# pub_util package include
from pub_util     import pub_logger, pub_smtp
# dstream module include

## @class ds_action
#  @brief A single process executor for a project
#  @details
#  Instantiated with project information, ds_action executes a project.\n
#  Currently executed process's stdout and stderr are simply handled via pipe.\n
#  One may improve this simple design to manage memory/cpu/wall-time of the process.\n
class ds_action(object):

    ## default ctor accepting ds_project instance
    def __init__(self, project_info):

        if not isinstance(project_info,ds_project):
            raise ValueError

        ## Project information
        self._info = copy.copy(project_info)

        ## Process handle
        self._proc = None

    ## Simple method to access name of a project
    def name(self): return self._info._project

    ## Boolean function to check if the project's execution process is alive or not
    def active(self):
        if self._proc is None: return False
        else: return (self._proc.poll() is None)

    ## @brief Clears a project process, if exists, and returns (stdout,stderr). 
    #  @details If process is active, it waits to finish. Use active() function\n
    #  before calling this to avoid waiting time.
    def clear(self):
        if self._proc is None: return (None,None,None)
        (out,err) = self._proc.communicate()
        code = self._proc.poll()
        del self._proc
        self._proc = None
        return (code,out,err)

    ## Opens a sub-process to execute a project
    def execute(self):
        try:
            self._proc = Popen(self._info._command.split(None),
                               stdout = PIPE,
                               stderr = PIPE)
        except OSError as e:
            pub_smtp(receiver = self._info._email,
                     subject  = 'Failed execution of project %s' % self.name(),
                     text     = e.strerror)
            raise DSException(e.strerror)

## @class ds_daemon
#  @brief Simple daemon tool to run registered projects
#  @details
#  Contains one function to continue an indefinite loop of loading project information\n
#  and execution at requested time. Loading of a project from DB is done in the other\n
#  function.
class ds_daemon(ds_base):

    ## default ctor does not require any input
    def __init__(self):

        super(ds_daemon,self).__init__()

        ## API for ds_daemon is ds_master to interact with ProcessTable
        self._api = ds_master(pubdb_conn_info.writer_info(),
                              logger=self._logger)

        ## Array of ds_action instance (one per project)
        self._project_v  = {}

        ## Array of execution timestamp (one per project)
        self._exe_time_v = {}

        ## Constant time period [s] to update project information from database
        self._load_period = int(120)

    ## Access DB and load projects for execution + update pre-loaded project information
    def load_projects(self):

        # First, remove project that is not active
        for x in self._project_v.keys():
            if not self._project_v[x].active():
                self._project_v.pop(x)

        # Second, load new/updated projects
        for x in self._api.list_projects():

            if x._project in self._project_v.keys():
                self.debug('Skipping update on project %s (still active)',x._project)
                continue

            self.debug('Updating project %s information' % x._project)
            self._project_v[x._project] = ds_action(x)
            if not x._project in self._exe_time_v:
                self._exe_time_v[x._project] = None

    ## Initiate an indefinite loop of projects' info-loading & execution 
    def routine(self):

        ctr=0
        sleep=0
        while ctr >= 0 :

            ctr+=1
            time.sleep(1)
            now_str  = time.strftime('%Y-%m-%d %H:%M:%S')
            now_ts   = time.time()
            self.debug(now_str)
            
            if sleep: 
                sleep -=1
                continue

            try:
                self._api.connect()
            except DBException as e:
                self.error('Failed connection to DB @ %s ... Retry in 1 minute' % now_str)
                sleep = 60
                continue

            if (ctr-1) % self._load_period == 0:
                
                self.info('Routine project update @ %s ' % now_str)
                self.load_projects()

            for x in self._project_v.keys():

                proj_ptr = self._project_v[x]
                last_ts  = self._exe_time_v[x]

                if not proj_ptr.active():

                    (code,out,err) = proj_ptr.clear()

                    if out or err:

                        self.info(' %s returned %s @ %s' % (x,code,now_str))
                        print out,err
                        
                    if ( last_ts is None or 
                         last_ts < ( now_ts - proj_ptr._info._period) ):

                        self._exe_time_v[x] = now_ts                        
                        try:
                            self.info('Execute %s @ %s' % (x,now_str))
                            proj_ptr.execute()

                        except DSException as e:
                            self.error('Received error from %s!' % x)
                            self.error('Email report should have been sent.')
                            self.error('%s' % e)


if __name__ == '__main__':
    k=ds_daemon()
    k.routine()
                    
                    
            


        

