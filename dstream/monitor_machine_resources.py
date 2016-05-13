## @namespace dstream.MONITOR_MACHINE_RESOURCES
#  @ingroup dstream
#  @brief Defines a project monitor_machine_resources
#  @author david caratelli

# python include
import time, os, sys
# pub_dbi package include
from pub_dbi import DBException, pubdb_conn_info
# pub_util import
from pub_util import pub_logger, pub_env, pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_api
from dstream.get_machine_info import getDISKSize

# function that takes rate information and smooths it
def smooth_rate(dsize,times,occupancy,n):

    r_times = []
    rates   = []

    # if the time and occupancy vectors don't agree in length: -> return empty lists
    if (len(times) != len(occupancy)):
        return r_times,rates

    # calculate rate at the beginning of the n-element long averaging
    dt_start  = times[1]-times[0]
    dt_start  = dt_start.seconds + float(dt_start.microseconds)/1.e6
    dMB_start = dsize*(occupancy[1]-occupancy[0])/100.
    r_start   = dMB_start/dt_start

    # calculate the avg. rate for the first n elements
    rate = 0.
    for i in xrange(n):
        dt  = times[n+1]-times[n]
        dt  = dt.seconds + float(dt.microseconds)/1.e6
        dMB = dsize*(occupancy[n+1]-occupancy[n])/100.
        rate += dMB/dt
    rate /= n

    #print 'filling time : %03i w/ rate : %i'%(n,int(rate))
    r_times.append(times[n])
    rates.append(rate)

    # now calculate the avrerage rate for the remaining times
    for i in xrange(n+1,len(times)):
        rate *= n
        rate -= r_start
        # re-calculate the starting rate
        dt_start  = times[i-n]-times[i-n-1]
        dt_start  = dt_start.seconds + float(dt_start.microseconds)/1.e6
        dMB_start = dsize*(occupancy[i-n]-occupancy[i-n-1])/100.
        r_start   = dMB_start/dt_start
        # calculate the end_rate to add
        dt_end  = times[i]-times[i-1]
        dt_end  = dt_end.seconds + float(dt_end.microseconds)/1.e6
        dMB_end = dsize*(occupancy[i]-occupancy[i-1])/100.
        r_end   = dMB_end/dt_end
        rate += r_end
        #print 'adding rate: %i'%(int(r_end))
        rate /= n
        #print 'filling time : %03i w/ rate : %i'%(i,int(rate))
        r_times.append(times[i])
        rates.append(rate)

    return r_times,rates

# function that decides if to send out an email
def disk_usage_alert(proj,max_disk,emails):

    try:
        import datetime
    except ImportError:
        return 'failed import of datetime'

    # if we are at a round 5-minute interval
    timenow = datetime.datetime.now()
    print timenow
    print timenow.minute
    # send email only on the hour
    if ( timenow.minute != 0 ):
        return

    last_entry = proj[-1]
    second_entry = proj[-2]
    
    last_log_time = last_entry.get_log_time()
    last_log_dict = last_entry.get_log_dict()

    second_log_dict = second_entry.get_log_dict()

    # check /data/ disk usage
    lastDISK = 0
    for key in last_log_dict:
        if (str(key) == 'DISK_USAGE_DATA'):
            lastDISK   = float(last_log_dict[key])*100
            secondDISK = float(second_log_dict[key])*100
            
    # check:
    #     above disk-limit     and    positive slope!!
    if ( (lastDISK > max_disk) and (lastDISK > secondDISK) ):
        
        pub_smtp(receiver = emails,
                 subject  = 'PUBS ALERT: Disk usage is above %i percent!' %( max_disk) ,
                 text     = 'Current disk usage on %s @ path %s is at %.02f percent. Please take action and clear disk space!'%(pub_env.kSERVER_NAME, '/data/', lastDISK))

    # check /datalocal/ disk usage
    lastDISK = 0
    for key in last_log_dict:
        if (str(key) == 'DISK_USAGE_DATALOCAL'):
            lastDISK = float(last_log_dict[key])*100
            secondDISK = float(second_log_dict[key])*100

    if ( (lastDISK > max_disk) and (lastDISK > secondDISK) ):
        
        pub_smtp(receiver = emails,
                 subject  = 'PUBS ALERT: Disk usage is above %i percent!' %( max_disk) ,
                 text     = 'Current disk usage on %s @ path %s is at %.02f percent. Please take action and clear disk space!'%(pub_env.kSERVER_NAME, '/datalocal/', lastDISK))

    # check /home/ disk-usage
    lastDISK = 0
    for key in last_log_dict:
        if (str(key) == 'DISK_USAGE_HOME'):
            lastDISK = float(last_log_dict[key])*100
            secondDISK = float(second_log_dict[key])*100

    if ( (lastDISK > max_disk) and (lastDISK > secondDISK) ):
        
        pub_smtp(receiver = emails,
                 subject  = 'PUBS ALERT: Disk usage is above %i percent!' %( max_disk) ,
                 text     = 'Current disk usage on %s @ path %s is at %.02f percent. Please take action and clear disk space!'%(pub_env.kSERVER_NAME, '/home/', lastDISK))

    return

# function to be used to plot machine resources saved to logger
def plot_resource_usage(proj,outpath):

    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.dates as dts
        from mpl_toolkits.axes_grid1 import host_subplot
        import mpl_toolkits.axisartist as AA
        import datetime
        import numpy as np
    except ImportError:
        return 'failed import'

    tCPU  = [] # cpu-usage times
    CPU   = [] # cpu-usage %
    tRAM  = [] # RAM-usage times
    RAM   = [] # RAM-usage %
    tDISK = [] # DISK-usage times
    DISK  = [] # DISK-usage %
    rDISK = [] # DISK rate of filling/draining
    tDLOC = [] # LOCAL DATA-usage times
    DLOC  = [] # LOCAL DATA-usage %
    rDLOC = [] # LOCAL rate of filling/draining
    tHOME = [] # HOME-usage times
    HOME  = [] # HOME-usage %
    rHOME = [] # HOME rate of filling/draining
    tPROJ = [] # number of projects running times
    NPROJ = [] # number of projects running
    rTHOME = [] # times for rates
    rTDISK = []
    rTDLOC = []

    lastDISK = -100
    lastDLOC = -100
    lastHOME = -100
    lastRAM  = -100
    lastCPU  = -100

    servername = pub_env.kSERVER_NAME

    # number of entries scanned
    cntr = 0
    # number of total entries in DB
    totentries = float(len(proj))

    # interval between which to plots point for variables
    # so that plot is not over-crowded
    spacing = 5

    # current time
    currentTime = datetime.datetime.now()
    # max time to scan
    maxTime = datetime.timedelta(hours=4)
    
    for x in proj:

        log_time = x.get_log_time()
        log_dict = x.get_log_dict()

        if ( (log_time != '') and (log_dict) ):

            # get time in python datetime format
            time = datetime.datetime.strptime(log_time[:19],'%Y-%m-%d %H:%M:%S')

            if ( (currentTime-time) > maxTime): continue

            NPROJ.append(x._proj_ctr)
            tPROJ.append(time)

            # keep track of last entry for each curve
            for key in log_dict:
                if (str(key) == 'DISK_USAGE_DATA'):
                    if (cntr%spacing >= 0):
                        rDISK.append(float(log_dict[key])*100)
                        rTDISK.append(time)
                    if ( (abs(float(log_dict[key])*100-lastDISK) > 1) or ((cntr+1)/totentries == 1) or (cntr%spacing == 0) ):
                        lastDISK = float(log_dict[key])*100
                        tDISK.append(time)
                        DISK.append(float(log_dict[key])*100)
                if (str(key) == 'DISK_USAGE_DATALOCAL'):
                    if (cntr%spacing >= 0):
                        rDLOC.append(float(log_dict[key])*100)
                        rTDLOC.append(time)
                    if ( (abs(float(log_dict[key])*100-lastDLOC) > 1) or ((cntr+1)/totentries == 1) or (cntr%spacing == 0) ):
                        lastDLOC = float(log_dict[key])*100
                        tDLOC.append(time)
                        DLOC.append(float(log_dict[key])*100)
                if (str(key) == 'DISK_USAGE_HOME'):
                    if (cntr%spacing >= 0):
                        rHOME.append(float(log_dict[key])*100)
                        rTHOME.append(time)
                    if ( (abs(float(log_dict[key])*100-lastHOME) > 1) or ((cntr+1)/totentries == 1) or (cntr%spacing == 0) ):
                        lastHOME = float(log_dict[key])*100
                        tHOME.append(time)
                        HOME.append(float(log_dict[key])*100)
                if (str(key) == 'RAM_PERCENT'):
                    if ( (abs(float(log_dict[key]) - lastRAM) > 1) or ((cntr+1)/totentries == 1) or (cntr%spacing == 0) ):
                        lastRAM = float(log_dict[key])
                        tRAM.append(time)
                        RAM.append(float(log_dict[key]))
                if (str(key) == 'CPU_PERCENT'):
                    if ( (abs(float(log_dict[key]) - lastCPU) > 1) or ((cntr+1)/totentries == 1) or (cntr%spacing == 0) ):
                        lastCPU = float(log_dict[key])
                        tCPU.append(time)
                        CPU.append(float(log_dict[key]))

        cntr += 1

    maxTime = datetime.timedelta(hours=3)

    datesCPU  = dts.date2num(tCPU)
    datesRAM  = dts.date2num(tRAM)
    datesDISK = dts.date2num(tDISK)
    datesDLOC = dts.date2num(tDLOC)
    datesHOME = dts.date2num(tHOME)
            
    # example for multi-axes (i.e. >= 3) plot here:
    # http://stackoverflow.com/questions/9103166/multiple-axis-in-matplotlib-with-different-scales

    fig, ax = plt.subplots(1,figsize=(12,8))

    ax.set_xlabel('Time',fontsize=20)
    ax.set_ylabel('Usage %',fontsize=20)
    ax.set_ylim([0,100])

    if (len(datesCPU) == len(CPU) and (len(CPU) != 0)):
        cpuPlot  = ax.plot_date(datesCPU,CPU, fmt='o', color='k', label='CPU usage', markersize=7)    
    if (len(datesDISK) == len(DISK) and (len(DISK) != 0)):
        diskPlot = ax.plot_date(datesDISK,DISK, fmt='o--', color='r',label='DISK usage @ /data/', markersize=7)
    if (len(datesRAM) == len(RAM) and (len(RAM) != 0)):
        ramPlot  = ax.plot_date(datesRAM,RAM, fmt='o', color='b', label='RAM usage', markersize=7)
    if (len(datesDLOC) == len(DLOC) and (len(DLOC) != 0)):
        diskPlot = ax.plot_date(datesDLOC,DLOC, fmt='*--', color='m',label='DISK usage @ /datalocal/', markersize=7)
    if (len(datesHOME) == len(HOME) and (len(HOME) != 0)):
        diskPlot = ax.plot_date(datesHOME,HOME, fmt='^--', color='c',label='DISK usage @ /home/', markersize=7)


    years    = dts.YearLocator()   # every year
    months   = dts.MonthLocator()  # every month
    days     = dts.DayLocator()
    hours    = dts.HourLocator()
    daysFmt  = dts.DateFormatter('%m-%d %H:%M')

    # format the ticks
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(daysFmt)
    #ax.xaxis.set_minor_locator(hours)

    ax.set_xlim([datetime.datetime.now()-maxTime, datetime.datetime.now()])

    # if the last entry in Disk Usage is above a threshold that indicates that the usage is too high
    # change the background color of the plot
    diskMax = 95

    if (len(DISK) > 0):
        if ( DISK[-1] > diskMax ):
            xlim = plt.xlim()
            ax.axvspan(xlim[0],xlim[1],color='r',alpha=0.3,lw=0,
                       label='DISK USAGE @ /data/ ABOVE %i PERCENT -> CALL DM EXPERT'%diskMax)

    if (len(DLOC) > 0):
        if ( DLOC[-1] > diskMax ):
            xlim = plt.xlim()
            ax.axvspan(xlim[0],xlim[1],color='r',alpha=0.3,lw=0,
                       label='DISK USAGE @ /datalocal/ ABOVE %i PERCENT -> CALL DM EXPERT'%diskMax)

    if (len(HOME) > 0):
        if ( HOME[-1] > diskMax ):
            xlim = plt.xlim()
            ax.axvspan(xlim[0],xlim[1],color='r',alpha=0.3,lw=0,
                       label='DISK USAGE @ /home/ ABOVE %i PERCENT -> CALL DM EXPERT'%diskMax)


    ax.format_xdata = dts.DateFormatter('%m-%d %H:%M')
    fig.autofmt_xdate()
            
    #host.axis["left"].label.set_color('r')
    #pltRAM.axis["right"].label.set_color('b')
    #pltCPU.axis["right"].label.set_color('k')

    #plt.figure.autofmt_xdate()    
    plt.grid()
    plt.title('Resource Usage on %s'%(servername), fontsize=20)
    plt.legend(loc=3,fontsize=20,framealpha=0.9)

    outpathResource = outpath+"resource_monitoring_%s.png"%(servername)
    plt.savefig(outpathResource)

    # --------------------------------------
    # second plot : save rates of disk-usage
    # --------------------------------------

    rateTHOME = []
    rateHOME = []
    rateTDATA = []
    rateDATA = []
    rateTDLOC = []
    rateDLOC = []

    fig, ax = plt.subplots(1,figsize=(12,8))

    # number of samples to average over
    Navg = 7

    #print len(rTHOME)
    # 1st step is to calculate and smooth the rates
    if (len(rTHOME) > Navg):
        dsize = getDISKSize('/home/')
        #datesRHOME, rateHOME = smooth_rate(dsize,rTHOME,rHOME,Navg)
        for i in xrange(Navg,len(rTHOME)):
            rhome = 0
            for x in xrange(-Navg,0):
                # get the time elapsed between measurements
                dt = rTHOME[i+x+1]-rTHOME[i+x]
                dt = dt.seconds + float(dt.microseconds)/1e6
                dMB = dsize*(rHOME[i+x+1]-rHOME[i+x])/100.
                rhome += dMB/dt
            rateTHOME.append(rTHOME[i])
            rateHOME.append(rhome/Navg)
        datesRHOME  = dts.date2num(rateTHOME)
        plt.plot(datesRHOME,rateHOME,'o--',color='c',label='Rate @ /home/')

    #print len(rTDISK)
    if (len(rTDISK) > Navg):
        dsize = getDISKSize('/data/')
        #datesRDATA, rateDATA = smooth_rate(dsize,rTDISK,rDISK,Navg)
        for i in xrange(Navg,len(rTDISK)):
            rdata = 0
            for x in xrange(-Navg,0):
                # get the time elapsed between measurements
                dt = rTDISK[i+x+1]-rTDISK[i+x]
                dt = dt.seconds + float(dt.microseconds)/1e6
                dMB = dsize*(rDISK[i+x+1]-rDISK[i+x])/100.
                rdata += dMB/dt
            rateTDATA.append(rTDISK[i])
            rateDATA.append(rdata/Navg)
        datesRDATA  = dts.date2num(rateTDATA)
        plt.plot(datesRDATA,rateDATA,'o--',color='r',label='Rate @ /data/')

    #print len(rTDLOC)
    if (len(rTDLOC) > Navg):
        dsize = getDISKSize('/datalocal/')
        #datesRDLOC, rateDLOC = smooth_rate(dsize,rTDLOC,rDLOC,Navg)
        for i in xrange(10,len(rTDLOC)):
            rdloc = 0
            for x in xrange(-Navg,0):
                # get the time elapsed between measurements
                dt = rTDLOC[i+x+1]-rTDLOC[i+x]
                dt = dt.seconds + float(dt.microseconds)/1e6
                dMB = dsize*(rDLOC[i+x+1]-rDLOC[i+x])/100.
                rdloc += dMB/dt
            rateTDLOC.append(rTDLOC[i])
            rateDLOC.append(rdloc/Navg)
        datesRDLOC  = dts.date2num(rateTDLOC)
        plt.plot(datesRDLOC,rateDLOC,'o--',color='m',label='Rate @ /datalocal/')

        
    Rmin = -50
    Rmax = 50
    if (len(rateDLOC) > 0):
        rmin = np.amin(np.array(rateDLOC))
        rmax = np.amax(np.array(rateDLOC))
        if (rmin < Rmin): Rmin = rmin
        if (rmax > Rmax): Rmax = rmax
    if (len(rateDATA) > 0):
        rmin = np.amin(np.array(rateDATA))
        rmax = np.amax(np.array(rateDATA))
        if (rmin < Rmin): Rmin = rmin
        if (rmax > Rmax): Rmax = rmax
    if (len(rateHOME) > 0):
        rmin = np.amin(np.array(rateHOME))
        rmax = np.amax(np.array(rateHOME))
        if (rmin < Rmin): Rmin = rmin
        if (rmax > Rmax): Rmax = rmax

    ax.set_xlabel('Time',fontsize=20)
    ax.set_ylabel('Disk Filling/Draining Rate [ MB/sec ]',fontsize=20)
    ax.set_ylim([Rmin-20,Rmax+20])

    # format the ticks
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(daysFmt)
    ax.set_xlim([datetime.datetime.now()-maxTime, datetime.datetime.now()])
    
    plt.grid()
    plt.title('Disk draining rate on %s'%(servername), fontsize=20)
    plt.legend(loc=3,fontsize=20,framealpha=0.9)
    # get y-limits for arrow boundaries
    #plt.annotate("Filling",xy=(1.05, 0.55), xycoords='axes fraction',
    #             xytext=(1.02, 0.9),arrowprops=dict(arrowstyle="<-", color="r"))
    #plt.annotate("Draining",xy=(1.05, 0.45), xycoords='axes fraction',
    #             xytext=(1.01, 0.1),arrowprops=dict(arrowstyle="<-", color="g"))



    outpathResource = outpath+"disk_rate_monitoring_%s.png"%(servername)
    plt.savefig(outpathResource)


    # -------------------------------------------------------
    #  third plot : number of projects running simulatneously
    # -------------------------------------------------------
    
    fig, ax = plt.subplots(1,figsize=(12,8))
    plt.plot(tPROJ[2:],NPROJ[2:],'ro')
    # get max number of projects to set axes accordingly
    try:
        nmax = np.amax(np.array(NPROJ[2:]))
    except:
        nmax = 1
    plt.title('PUBS Projects Running on %s'%(servername), fontsize=20)
    ax.set_xlabel('Time',fontsize=20)
    ax.set_ylabel('Number of Projects Running',fontsize=20)
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(daysFmt)
    ax.set_xlim([datetime.datetime.now()-maxTime, datetime.datetime.now()])
    #ax.set_xlim([datetime.datetime.now()-datetime.timedelta(days=1), datetime.datetime.now()])
    ax.set_ylim([0,nmax+1])
    ax.format_xdata = dts.DateFormatter('%m-%d %H:%M')
    fig.autofmt_xdate()
    outpathProjs = outpath+"numproj_monitoring_%s.png"%(servername)
    plt.grid()
    plt.savefig(outpathProjs)
    return outpath
    
## @class monitor_machine_resources
#  @brief this project produces monitoring plots
#  @details
#  this project executes a plotting script
#  that monitors machine resources such as
#  CPU usage, RAM usage and DISK usage
class monitor_machine_resources(ds_project_base):

    # Define project name as class attribute
    _project = 'monitor_machine_resources'


    ## @brief default ctor can take # runs to process for this instance
    def __init__(self,arg = ''):

        # Call base class ctor
        super(monitor_machine_resources,self).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._data_dir = ''
        self._experts  = ''
        self._email_disk_percent = ''
        self._email_disk_users   = ''
        self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._data_dir = '%s' % (resource['DATADIR'])
        self._experts = resource['EXPERTS']
        self._email_disk_percent = int(resource['EMAILDISKPERCENT'])
        self._email_disk_users = resource['EMAILDISKUSERS']
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # get daemon_log
        try:
            #logger = pub_logger.get_logger('list_log')
            #k = self._api.ds_reader(pubdb_conn_info.reader_info(),logger)
            #k.connect()
            project = self._api.list_daemon_log(server=pub_env.kSERVER_NAME)
        except:
            self.error('could not get logger')
            return

        # if no projects found
        if not project:
            self.info('no projects found...exiting @ %s'%(time.strftime("%Y-%m-%d %H:%M:%S")))
            return
            
        # directory where to store plots
        pubstop = str(os.environ.get('PUB_TOP_DIR'))

        plotpath = pubstop+'/'+self._data_dir+'/'
        self.info('saving plot to path: %s @ %s ...'%(plotpath,time.strftime("%Y-%m-%d %H:%M:%S")))
        outpath = plot_resource_usage(project,plotpath)
        self.info('...done')
        if (outpath == 'failed import'):
            self.error('could not complete import...plot not produced...')
        if (outpath == None):
            self.error('No plot produced...')

        disk_usage_alert(project,self._email_disk_percent,self._email_disk_users)
        self.info('done calling alert function')

        

# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    test_obj = monitor_machine_resources(proj_name)

    test_obj.process_newruns()



