try:
    from pyqtgraph.Qt import QtGui, QtCore
except ImportError:
    raise ImportError('Ruh roh. You need to set up pyqtgraph before you can use this GUI.')

# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info

import datetime
import time
import copy
import math

import threading

class GuiUtilsAPI():

  # Class that handles the API instance to the database (since more than
  #just the main GUI window will use it, as well as a few utility functions
  #that are independent of the database. '''
  #Note: the database connection is done in a separate thread!
  class myQueryThread(threading.Thread):
    def __init__(self, threadID, name, threadlock):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name  
      # DB interface:
      self.dbi = ds_reader(pubdb_conn_info.reader_info())
    
      #Establish a connection to the database through the DBI
      try:
        self.dbi.connect()
      except:
        print "Unable to connect to database in query thread... womp womp :("

      self.queried_proj_dict = self.dbi.list_status()
      self.projects = self.dbi.list_projects()
      self.relevant_daemons = GuiUtils().getRelevantDaemons()
      self.threadLock = threadlock
      self.daemon_last_logtimes = dict.fromkeys(self.relevant_daemons,None)
      self.daemon_is_enabled = dict.fromkeys(self.relevant_daemons,False)

      #the querythread itSELF is a "daemon" (different than PUBS daemon)
      #the reason for this is to make sure the thread closes when the main program closes
      self.setDaemon(True)

    def run(self):
      #self.threadLock.acquire()
      while True:
        self.queried_proj_dict = self.dbi.list_status()
        self.projects = self.dbi.list_projects()
        for servername in self.relevant_daemons:
          self.daemon_last_logtimes[servername] = self.dbi.list_daemon_log(servername)[-1]._logtime
          self.daemon_is_enabled[servername] = self.dbi.daemon_info(servername)._enable

        #self.threadLock.release()
        time.sleep(10)

    def getProjDict(self):
      self.threadLock.acquire()
      tmpdict = self.queried_proj_dict.copy()
      self.threadLock.release()
      return tmpdict

    def getProjects(self):
      self.threadLock.acquire()
      tmplist = copy.copy(self.projects)
      self.threadLock.release()
      return tmplist

    def getDaemonEnabled(self,servername):
      try:
        self.threadLock.acquire()
        tmp_isenabled = self.daemon_is_enabled[servername]
        self.threadLock.release()
      except: 
        self.threadLock.release()
        KeyError('What?? Server not in daemon_is_enabled.keys() inside of gui_utils_api thread.')
      return tmp_isenabled

    def getDaemonLastLogtime(self,servername):
      try:
        self.threadLock.acquire()
        tmp_lastlogtime = self.daemon_last_logtimes[servername]
        self.threadLock.release()
      except: 
        self.threadLock.release()
        KeyError('What?? Server not in daemon_last_logtimes.keys() inside of gui_utils_api thread.')
      return tmp_lastlogtime

  def __init__(self):

    threadLock = threading.Lock()
    #Create thread that does the DB querying:
    self.querythread = self.myQueryThread(1, 'guit_querythread', threadLock)
    #Start the thread that does the DB querying:
    self.querythread.start()
    #Initialize proj_dict
    #Dictionary that contains projects as keys, and arrays of statuses as values. ex:
    #{'dummy_daq': [(1, 109),(2,23)], 'dummy_nubin_xfer': [(0,144), (1, 109)]}
    #At this point, the project dict isn't filled yet...
    self.proj_dict = self.querythread.getProjDict()
    if self.querythread.getProjects():
      self.enabled_projects = [ x._project for x in self.querythread.getProjects() ]
    else:
      self.enabled_projects = ['']
    self.my_utils = GuiUtils()
    self.colors = self.my_utils.getColors()

  #on destructor, kill the thread
  def __del__(self):
    print "Destructing gui utils api!"
    self.querythread.exit()
  def update(self):
    self.proj_dict = self.querythread.getProjDict()
    self.enabled_projects = [ x._project for x in self.querythread.getProjects() ]

  def getAllProjectNames(self):
    return self.proj_dict.keys()

  def getEnabledProjectNames(self):
    return self.enabled_projects

  def computePieSlices(self,projname):

    if projname not in self.proj_dict.keys():
      print "Uh oh projname is not in dictionary. Figure out what happened..."
      return [ (1., 'r') ]

    tot_n = self.getTotNRunSubruns(projname)
    statuses = self.proj_dict[projname]
    #statuses looks like [(0,15),(1,23),(2,333), (status, number_of_that_status)]

    slices = []
    #one giant green slice for fully completed project
    if len(statuses) == 1 and self.my_utils.isGoodStatus(statuses[0][0]):
      return [ ( 1., 'g' ) ]

    for x in statuses:
      #Don't care about good status values
      if self.my_utils.isGoodStatus(x[0]): continue
      elif self.my_utils.isErrorStatus(x[0]): mycolor = 'r'
      elif self.my_utils.isIntermediateStatus(x[0]): mycolor = [255, 140, 0] #dark orange
      # if x[0] in self.colors.keys(): 
      #   mycolor = self.colors[x[0]]
      # elif self.my_utils.isErrorStatus(x[0]): 
      #   mycolor = 'r'
      # elif self.my_utils.isIntermediateStatus(x[0]): 
      #   mycolor = [255, 140, 0] #dark orange
      else:
        mycolor = 'w'
        print "wtf happened, status %d for project %s"%(x[0],projname)
      slices.append( ( (float(x[1])/tot_n), mycolor ) )
    
    return slices
      
  def computePieRadius(self, projname, max_radius, tot_n=0):

    #Right now use radius = (Rmax/2log(5))log(n_total_runsubruns)
    #unless radius > Rmax, in which case use radius = Rmax
    #This function has r = 0.5*Rmax when n = 5
    #radius = (float(max_radius)/(2 * math.log(5.) )) * math.log(float(tot_n))

    #Quick overwrite to make all pie charts maximum size
    radius = 999999.

    #Double check the radius isn't bigger than the max allowed
    return radius if radius <= max_radius else max_radius
    

  #Get the number of run/subruns that are relevant for pie charts
  # (don't care about fully completed run/subruns)
  def getTotNRunSubruns(self,projname):

    statuses = self.proj_dict[projname]
    #statuses looks like [(0,15),(1,23),(2,333), (status, number_of_that_status)]
    #tot_n does not include "good" statuses
    tot_n = sum( [ x[1] for x in statuses if not self.my_utils.isGoodStatus(x[0]) ] )
    return tot_n

  def getNRunSubruns(self,projname):
    #don't include "good" statuses in this
    #return [ x for x in self.proj_dict[projname] if not self.my_utils.isGoodStatus(x[0]) ]
    #Let's try including "good" statuses...
    return [ x for x in self.proj_dict[projname] ]
    
  def getDaemonStatuses(self, servername):
    #Returns [enabled/disabled, running/dead]
    max_daemon_log_lag = 600 #seconds
    is_enabled = self.querythread.getDaemonEnabled(servername)
    last_logtime = self.querythread.getDaemonLastLogtime(servername)
    #If you just use list_daemon_log(servername) you get an ENORMOUS list back
    #Use the list_daemon_log(servername, starttime, endtime) when it is fixed (those are datetime stamps)
    #starttime = datetime.datetime.today()# - datetime.timedelta(seconds=max_daemon_log_lag)
    time_since_log_update = self.my_utils.getTimeSinceInSeconds(last_logtime)
    is_running = True if time_since_log_update < max_daemon_log_lag else False
    return (is_enabled, is_running)
  
  def genDaemonTextAndWarnings(self):
    #Add text to bottom of GUI showing if daemons are running and enabled
    text_content = ''
    warning_content = ''
    for dname in self.my_utils.getRelevantDaemons():
        d_enabled, d_running = self.getDaemonStatuses(dname)
        text_content += 'Daemon: %s. Enabled = %d, Running = %d.\n' % (dname, d_enabled, d_running)
        if not d_enabled:
            warning_content += 'Daemon %s is DISABLED as of %s\n'%(dname,datetime.datetime.today().strftime("%A, %d. %B %Y %I:%M%p"))
        if not d_running:
            warning_content += 'Daemon %s is NOT RUNNING as of %s!\n'%(dname,datetime.datetime.today().strftime("%A, %d. %B %Y %I:%M%p"))
    if warning_content: warning_content += "Tell an expert!"

    #if daemon_warning actually had nothing in it, return no daemon warning
    return (text_content,warning_content) if warning_content else (text_content, '')

class GuiUtils():
  #Class that does NOT connect to any DB but just holds various constants/utility functions
  def __init__(self):
    #r, g, b, c, m, y, k, w    
    #self.colors = { 1:'b', 2:'g', 3: 'w', 4:'w', 100:'y', 102:'m', 65:'r', 101:'c', 404:'k', 999:'w', -9:'y' }
    #empire strikes back color themes!
    #self.colors={ 1:[47,74,101],
    #              2:[18,59,142],
    #              3:[205,180,101],
    #              4:[205,180,101],
    #              100:[235,160,113],
    #              102:[117,21,41],
    #              65:[165,17,26],
    #              101:[144,149,38],
    #              404:[11,20,40],
    #              999:[19,33,71],
    #              -9:[206,211,124],
    #              1000:[18,59,142],
    #              4112:[47,75,101]
    #              }
    self.colors={ 1:[72, 118, 255] }#47, 75, 101] }#[0, 255, 255] } #[47, 75, 101] }
    self.update_period = 10 #seconds
    self.relevant_daemons = [ 'ubdaq-prod-evb.fnal.gov', 'ubdaq-prod-near1.fnal.gov' ]

  def getColors(self):
    return self.colors

  def getUpdatePeriod(self):
    return self.update_period

  def getTimeSinceInSeconds(self,timestamp):
    now = datetime.datetime.today()
    if not timestamp: return 0
    return (now - timestamp).total_seconds()

  def getRelevantDaemons(self):
    return self.relevant_daemons

  def isGoodStatus(self, status):
    return True if status == 0 or status >= 1000 else False

  def isIntermediateStatus(self, status):
    return True if status >= 1 and status < 100 else False

  def isErrorStatus(self,status):
    return True if status >= 100 and status < 1000 else False

  def getNGoodInterError(self,history):
    n_good, n_inter, n_error = 0, 0, 0
    latest_data = []
    for status, ihistory in history.iteritems():
      latest_data.append( (status, ihistory[-1]) )

    for status, current_value in latest_data:
      if self.isGoodStatus(status): n_good += current_value
      elif self.isErrorStatus(status): n_error += current_value
      elif self.isIntermediateStatus(status): n_inter += current_value
      else:
        print "something has gone horribly wrong. status %d for project %s"%(x[0],projname)
    return (n_good, n_inter, n_error)
