# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info

import datetime
import math

class GuiUtilsAPI():

  # Class that handles the API instance to the database (since more than
  #just the main GUI window will use it, as well as a few utility functions
  #that are independent of the database. '''
  
  def __init__(self):

    # DB interface:
    self.dbi = ds_reader(pubdb_conn_info.reader_info())
    
    #Establish a connection to the database through the DBI
    try:
      self.dbi.connect()
    except:
      print "Unable to connect to database... womp womp :("

    #Dictionary that contains projects as keys, and arrays of statuses as values. ex:
    #{'dummy_daq': [(1, 109),(2,23)], 'dummy_nubin_xfer': [(0,144), (1, 109)]}
    self.proj_dict = self.dbi.list_status()
    self.enabled_projects = [ x._project for x in self.dbi.list_projects() ]
    self.colors = GuiUtils().getColors()

  def update(self):
    self.proj_dict = self.dbi.list_status()
    self.enabled_projects = [ x._project for x in self.dbi.list_projects() ]

  def getAllProjectNames(self):
    return self.proj_dict.keys()

  def getEnabledProjectNames(self):
    return self.enabled_projects

  def computePieSlices(self,projname):

    if projname not in self.proj_dict.keys():
      print "Uh oh projname is not in dictionary. Figure out what happened..."
      return [ (1., 'r') ]

    statuses = self.proj_dict[projname]
    #statuses looks like [(0,15),(1,23),(2,333), (status, number_of_that_status)]
    #tot_n does not include status == 0
    tot_n = sum([x[1] for x in statuses if x[0]])

    if len(statuses) > len(self.colors):
      print "Uh oh, more different statuses than colors! Increase number of colors!"
      return [ (1., 'r') ]

    slices = []
    for x in statuses:
      #No slice for status == 0
      if not x[0]: continue
      if x[0] not in self.colors.keys():
        print "uh oh! Status %d for project %s is not in my color dictionary. Adding it as red." % (x[0],projname)

      mycolor = self.colors[x[0]] if x[0] in self.colors.keys() else 'r'
      slices.append( ( (float(x[1])/tot_n), mycolor ) )
    
    return slices
      
  def computePieRadius(self, projname, max_radius, tot_n=0):

    #If piechart has no entries, give it a zero radius
    if not tot_n:
      return 0.

    #Right now use radius = (Rmax/2log(5))log(n_total_runsubruns)
    #unless radius > Rmax, in which case use radius = Rmax
    #This function has r = 0.5*Rmax when n = 5
    radius = (float(max_radius)/(2 * math.log(5.) )) * math.log(float(tot_n))
    #Double check the radius isn't bigger than the max allowed
    return radius if radius <= max_radius else max_radius

  #Get the number of run/subruns that are relevant for pie charts
  # (don't care about fully completed run/subruns)
  def getTotNRunSubruns(self,projname):

    statuses = self.proj_dict[projname]
    #statuses looks like [(0,15),(1,23),(2,333), (status, number_of_that_status)]
    #tot_n does not include status == 0
    tot_n = sum([x[1] for x in statuses if x[0]])
    return tot_n

  def getNRunSubruns(self,projname):
    #don't include status == 0 in any of this
    return [x for x in self.proj_dict[projname] if x[0]]
    
  def getDaemonStatuses(self, servername):
    #Returns [enabled or disabled, running or dead]
    is_enabled = self.dbi.daemon_info(servername)._enable
    d_logs = self.dbi.list_daemon_log(servername)
    time_since_log_update = min([getTimeSinceInSeconds(x._logtime) for x in d_logs])
    is_running = True if time_since_log_update < max_daemon_log_lag else False
    return (is_enabled, is_running)

class GuiUtils():
  #Class that does NOT connect to any DB but just holds various constants/utility functions
  def __init__(self):
    #r, g, b, c, m, y, k, w    
    self.colors = { 1:'b', 2:'g', 3: 'w', 4:'k', 100:'y', 102:'m', 65:'r', 101:'c', 404:'k', 999:'w', -9:'y' }
    self.update_period = 10 #seconds

  def getColors(self):
    return self.colors

  def getUpdatePeriod(self):
    return self.update_period

  def getTimeSinceInSeconds(self,timestamp):
    #timestamp is a string of the form '2015-08-03 15:34:57.773465'
    t0  = datetime.datetime.strptime(timestamp,"%Y-%m-%d %H:%M:%S.%f")
    now = datetime.datetime.today()
    return (now - t0).seconds
