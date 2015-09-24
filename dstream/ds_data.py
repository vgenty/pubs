## @namespace dstream.ds_data
#  @ingroup dstream
#  @brief Defines data holder class for dstream package
#  @details
#  Contains two data classes:\n
#  0) ds_status is used for logging the project status\n
#  1) ds_project is used to pass project execution information\n

# python import
import inspect, copy
# pub_util package import
from pub_util import pub_logger
from datetime import tzinfo, timedelta, datetime

## @class ds_status
# @brief Holds status of a project, and is used to log a status in DB.
# @details
# Instance holds a project name, run, sub-run, seq, and status read from DB.
class ds_status(object):

    ## @brief default ctor to specify all data members
    def __init__ (self, 
                  project = '',
                  run     = -1,
                  subrun  = -1,
                  seq     = -1,
                  status  = -1,
                  enable  = True,
                  data    = ''):
        try:
            self._project = str(project)
            self._run     = int(run)
            self._subrun  = int(subrun)
            self._seq     = int(seq)
            self._status  = int(status)
            self._enable  = int(enable)
            self._data    = str(data)
        except ValueError:
            name   = '%s' % inspect.stack()[1][3]
            pub_logger.get_logger(name).critical('Invalid value type!')
            self._project = ''
            self._run = self._subrun = self._seq = self._status = -1
            raise DSException()
    
    ## A method to make sure the instance is at least not stupid
    def is_valid(self):
        
        if ( not self._project or 
             self._run    < 0  or 
             self._subrun < 0  or 
             self._seq    < 0  or 
             self._seq    < 0 ):

            return False

        else: return True

## @class ds_daemon
# @brief This class holds a daemon configuration information registered in DaemonTable
# @details
# Stored information include target server name, max ctr for simultaneously run projects,\n
# daemon's lifetime, daemon log lifetime, contact emails, and daemon enable boolean.
class ds_daemon(object):

    ## @brief default ctor to specify all data members
    def __init__ (self, server,
                  max_proj_ctr = 10,
                  lifetime     = 3600*24,
                  log_lifetime = 3600*24,
                  runsync_time = 120,
                  update_time  = 60,
                  cleanup_time = 120,
                  email        = '',
                  enable       = True):

        try:
            self._server       = str(server)
            self._max_proj_ctr = int(max_proj_ctr)
            self._lifetime     = int(lifetime)
            self._log_lifetime = int(log_lifetime)
            self._runsync_time = int(runsync_time)
            self._update_time  = int(update_time)
            self._cleanup_time = int(cleanup_time)
            self._email        = str(email)
            self._enable      = bool(enable)
            if self._lifetime < 0 or self._log_lifetime < 0:
                raise ValueError
        except ValueError:
            name   = '%s' % inspect.stack()[1][3]
            pub_logger.get_logger(name).critical('Invalid value type!')
            raise DSException()

    ## @brief default == operator override
    def __eq__(self,rhs):
        if ( not self._server == rhs._server or 
             not self._max_proj_ctr == rhs._max_proj_ctr or
             not self._lifetime == rhs._lifetime or
             not self._log_lifetime == rhs._log_lifetime or
             not self._runsync_time == rhs._runsync_time or
             not self._update_time == rhs._update_time or
             not self._cleanup_time == rhs._cleanup_time or
             not self._email == rhs._email or
             not self._enable == rhs._enable) :
            return False
        return True

    ## @brief default != operator override
    def __neq__(self,rhs):
        return not self == rhs

    ## @brief take a diff & return string which reports a difference
    def diff(self,info):

        if not isinstance(info,ds_daemon):
            raise ValueError
        if self == info: return ''
            
        msg = ''
        if not self._server == info._server:
            msg += 'Server       : %s => %s\n' % (self._server, info._server)
        if not self._max_proj_ctr == info._max_proj_ctr:
            msg += 'Proj. Ctr    : %d => %d\n' % (self._max_proj_ctr, info._max_proj_ctr)
        if not self._lifetime == info._lifetime:
            msg += 'LifeTime     : %d => %d\n' % (self._lifetime, info._lifetime)
        if not self._log_lifetime == info._log_lifetime:
            msg += 'Log Period   : %d => %d\n' % (self._log_lifetime, info._log_lifetime)
        if not self._runsync_time == info._runsync_time:
            msg += 'RunSync Time : %d => %d\n' % (self._runsync_time, info._runsync_time)
        if not self._update_time == info._update_time:
            msg += 'Update Time  : %d => %d\n' % (self._update_time, info._update_time)
        if not self._cleanup_time == info._cleanup_time:
            msg += 'CleanUp Time : %d => %d\n' % (self._cleanup_time, info._cleanup_time)
        if not self._email == info._email:
            msg += 'Email      : %s => %s\n' % (self._email, info._email)
        if not self._enable == info._enable:
            msg += 'Enabled : %s => %s\n' % (self._enable,info._enable)

        return msg

    def __str__(self):
        msg = ''
        msg += 'Server       : %s\n' % self._server
        msg += 'Proj. Ctr    : %d\n' % self._max_proj_ctr
        msg += 'LifeTime     : %d [s]\n' % self._lifetime
        msg += 'Log Period   : %d [s]\n' % self._log_lifetime
        msg += 'RunSync Time : %d [s]\n' % self._runsync_time
        msg += 'Update Time  : %d [s]\n' % self._update_time
        msg += 'CleanUp Time : %d [s]\n' % self._cleanup_time
        msg += 'Email        : %s\n' % self._email
        msg += 'Enabled      : %s\n' % self._enable

        return msg
    
    ## A method to make sure the instance is at least not stupid
    def is_valid(self):
        
        if ( not self._server or 
             self._lifetime <= 0 or
             self._runsync_time <= 0 or
             self._update_time <= 0 ):
            return False

        else: return True
        
## @class ds_daemon_log
# @brief This class holds a daemon log information registered in DaemonLogTable
# @details
# Stored information include DaemonTable contents @ log time such as a server, max project \n
# ctr, daemon lifetime. In addition it logs number of running projects, up-time of the daemon,\n
# and any additional log information provided in a python dict (in DB HSTORE)
class ds_daemon_log(object):

    ## @brief default ctor
    def __init__(self, server, max_proj_ctr, lifetime,
                 proj_ctr=0, uptime=0, log=dict()):

        self._server  = server
        self._logtime = None
        
        self.log( proj_ctr=proj_ctr,
                  uptime = uptime,
                  log = log,
                  logtime = None,
                  max_proj_ctr = max_proj_ctr,
                  lifetime = lifetime )

    ## @brief __str__ override
    def __str__(self):
        msg = '%s | Project %-2d/%-2d | UpTime %-6d/%-6d | %s | %d log variables'
        msg = msg % (self._server, 
                     self._proj_ctr, self._max_proj_ctr,
                     self._uptime, self._lifetime,
                     self._logtime,
                     len(self._log) )
        return msg

    ## @brief dump custom log variables
    def dump_log(self):
        if not self._log: return ''
        msg = ' %s ' % self._logtime
        for key in self._log:
            msg += '| %s : %s ' % (key,self._log[key])
        return msg

    ## @brief get log timestamp
    def get_log_time(self):
        if not self._logtime: return ''
        return '%s'%self._logtime

    ## @brief get log dictionary
    def get_log_dict(self):
        
        if not self._log: return dict()
        return self._log

    ## @brief initializer from ds_daemon
    def init(self, conf):

        if not isinstance(conf,ds_daemon):
            raise ValueError

        self._server = conf._server

        self.log( proj_ctr = 0,
                  uptime   = 0,
                  log      = dict(),
                  logtime  = None,
                  max_proj_ctr = conf._max_proj_ctr,
                  lifetime = conf._lifetime )

    def log(self, proj_ctr, uptime, log,
            logtime=None,max_proj_ctr=None,lifetime=None):

        try:
            self._proj_ctr = int(proj_ctr)
            self._uptime   = int(uptime)
            self._log      = dict(log)

            if not max_proj_ctr is None:
                self._max_proj_ctr = int(max_proj_ctr)

            if not lifetime is None:
                self._lifetime = int(lifetime)

            if not logtime is None:
                self._logtime  = float(logtime)                

        except ValueError:
            name   = '%s' % inspect.stack()[1][3]
            pub_logger.get_logger(name).critical('Invalid value type!')
            raise DSException()
        
    def is_valid(self):
        if ( not self._server or
             self._max_proj_ctr < 0 or
             self._lifetime < 0 or
             self._proj_ctr < 0 or
             self._uptime < 0 or
             not type(self._log) == type(dict()) or
             self._logtime and not type(self._logtime) == type(datetime.now())):
            return False
        return True

## @class ds_project
# @brief This class holds a project information registered in ProcessTable.
# @details
# Stored information include project name, command, start run number, start sub-run number,\n
# contact email, execution server name, sleep time after project instantiation, reference  \n
# run table name, period between execution, version number, enable-flag, and resources.    \n
class ds_project(object):

    ## @brief default ctor to specify all data members
    def __init__ (self, project, command='', run=0, subrun=0,
                  email='', server='', runtable='', sleep=0,
                  period=0, enable=True, resource={}, ver=-1):
        if not resource: resource = {}
        try:
            if not type(resource) == type(dict()):
                raise ValueError
            self._project  = str(project)
            self._command  = str(command)
            self._run      = int(run)
            self._subrun   = int(subrun)
            self._email    = str(email)
            self._server   = str(server)
            self._sleep    = int(sleep)
            self._runtable = str(runtable)
            self._period   = int(period)
            self._enable   = bool(enable)
            self._resource = copy.copy(resource)
            self._ver      = int(ver)
            if self._run < 0 or self._subrun < 0 or self._sleep < 0 or self._period < 0:
                raise ValueError
        except ValueError:
            name   = '%s' % inspect.stack()[1][3]
            pub_logger.get_logger(name).critical('Invalid value type!')
            raise DSException()

    ## @brief default == operator override
    def __eq__(self,rhs):
        if ( not self._project == rhs._project or
             not self._command == rhs._command or
             not self._run     == rhs._run or
             not self._subrun  == rhs._subrun or
             not self._email   == rhs._email or
             not self._server  == rhs._server or
             not self._sleep   == rhs._sleep or
             not self._runtable == rhs._runtable or
             not self._period  == rhs._period or
             not self._enable  == rhs._enable or
             not self._ver     == rhs._ver ):
             return False
        for x in self._resource.keys():
            if not x in rhs._resource.keys(): return False
            elif not self._resource[x] == rhs._resource[x]: return False
        for x in rhs._resource.keys():
            if not x in self._resource.keys(): return False

        return True

    ## @brief default != operator override
    def __neq__(self,rhs):
        return not self == rhs
    
    ## @brief take a diff & return string which reports a difference
    def diff(self,info):
    
        if not isinstance(info,ds_project):
            raise ValueError
        if self == info: return ''
            
        msg = ''
        if not self._project == info._project:
            msg += 'Name     : %s => %s\n' % (self._project, info._project)
        if not self._command == info._command:
            msg += 'Command  : %s => %s\n' % (self._command, info._command)
        if not self._period  == info._period:
            msg += 'Period   : %d => %d\n' % (self._period, info._period)
        if not self._sleep   == info._sleep:
            msg += 'Sleep    : %d => %d\n' % (self._sleep, info._sleep)
        if not self._run     == info._run:
            msg += 'Run      : %d => %d\n' % (self._run, info._run)
        if not self._subrun  == info._subrun:
            msg += 'SubRun   : %d => %d\n' % (self._subrun, info._subrun)
        if not self._email   == info._email:
            msg += 'Email    : %s => %s\n' % (self._email,info._email)
        if not self._server  == info._server:
            msg += 'Server   : %s => %s\n' % (self._server,info._server)
        if not self._runtable == info._runtable:
            msg += 'RunTable : %s => %s\n' % (self._runtable,info._runtable)
        if not self._enable == info._enable:
            msg += 'Enabled : %s => %s\n' % (self._enable,info._enable)

        for x in self._resource:
            if not x in info._resource:
                msg += 'Resource: Missing %s => %s\n' % (x,self._resource[x])
            elif not self._resource[x] == info._resource[x]:
                msg += 'Resource: Change  %s : %s => %s\n' % (x,self._resource[x],info._resource[x])
        for x in info._resource:
            if not x in self._resource:
                msg += 'Resource: New %s => %s\n' % (x, info._resource[x])
        return msg

    def __str__(self):
        msg = ''
        msg += 'Project  : %s\n' % self._project
        msg += 'Command  : %s\n' % self._command
        msg += 'Period   : %d\n' % self._period
        msg += 'Sleep    : %d\n' % self._sleep
        msg += 'RunTable : %s\n' % self._runtable
        msg += 'Run      : %d\n' % self._run
        msg += 'SubRun   : %d\n' % self._subrun
        msg += 'Server   : %s\n' % self._server
        msg += 'Email    : %s\n' % self._email
        msg += 'Enabled  : %s\n' % self._enable
        msg += 'Resource list below...\n'
        for x in self._resource.keys():
            msg += "%s => %s\n" % (x,self._resource[x])

        return msg
    
    ## A method to make sure the instance is at least not stupid
    def is_valid(self):
        
        if ( not self._project or 
             not self._command or
             self._run    < 0  or 
             self._subrun < 0  or 
             not self._email   or
             self._period <= 0 ):

            return False

        else: return True
    
