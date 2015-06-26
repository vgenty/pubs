## @namespace dstream.MONITOR_MACHINE_RESOURCES
#  @ingroup dstream
#  @brief Defines a project monitor_machine_resources
#  @author david caratelli

# python include
import time, os
# pub_dbi package include
from pub_dbi import DBException, pubdb_conn_info
# pub_util import
from pub_util import pub_logger, pub_env
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_api
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
    except ImportError:
        return 'failed import'

    tCPU  = []
    CPU   = []
    tRAM  = []
    RAM   = []
    tDISK = []
    DISK  = []

    lastDISK = -100
    lastRAM  = -100
    lastCPU  = -100

    servername = pub_env.kSERVER_NAME#proj[0]._server

    # number of entries scanned
    cntr = 0
    # number of total entries in DB
    totentries = float(len(proj))
    
    for x in proj:

        log_time = x.get_log_time()
        log_dict = x.get_log_dict()

        if ( (log_time != '') and (log_dict) ):
            # get time in python datetime format
            time = datetime.datetime.strptime(log_time,'%Y-%m-%d %H:%M:%S.%f')
            # keep track of last entry for each curve
            for key in log_dict:
                if (str(key) == 'DISK_USAGE_HOME'):
                    #if ( cntr%100 == 0 ):
                    if ( (abs(float(log_dict[key])*100-lastDISK) > 1) or ((cntr+1)/totentries == 1) or (cntr%100 == 0) ):
                        lastDISK = float(log_dict[key])*100
                        tDISK.append(time)
                        DISK.append(float(log_dict[key])*100)
                if (str(key) == 'RAM_PERCENT'):
                    if ( (abs(float(log_dict[key]) - lastRAM) > 1) or ((cntr+1)/totentries == 1) or (cntr%100 == 0) ):
                        lastRAM = float(log_dict[key])
                        tRAM.append(time)
                        RAM.append(float(log_dict[key]))
                if (str(key) == 'CPU_PERCENT'):
                    if ( (abs(float(log_dict[key]) - lastCPU) > 1) or ((cntr+1)/totentries == 1) or (cntr%100 == 0) ):
                        lastCPU = float(log_dict[key])
                        tCPU.append(time)
                        CPU.append(float(log_dict[key]))

        cntr += 1

    datesCPU  = dts.date2num(tCPU)
    datesRAM  = dts.date2num(tRAM)
    datesDISK = dts.date2num(tDISK)

            
    # example for multi-axes (i.e. >= 3) plot here:
    # http://stackoverflow.com/questions/9103166/multiple-axis-in-matplotlib-with-different-scales

    fig, ax = plt.subplots(1,figsize=(12,8))


    
    '''
    host = host_subplot(111,axes_class=AA.Axes)

    plt.subplots_adjust(right=0.75)
    pltRAM = host.twinx()
    pltCPU = host.twinx()
    
    offset = 60
    new_fixed_axis = pltCPU.get_grid_helper().new_fixed_axis
    pltCPU.axis['right'] = new_fixed_axis(loc='right',
                                          axes=pltCPU,
                                          offset=(offset,0))
    
    pltCPU.axis['right'].toggle(all=True)
    
    host.set_xlabel('Time', fontsize=20)
    host.set_ylabel('DISK usage Frac.', fontsize=18, color='r')
    host.set_ylim([0,1])
    
    pltRAM.set_ylabel('RAM Usage %', fontsize=18, color='b')
    pltRAM.set_ylim([0,100])
    pltCPU.set_ylabel('CPU Usage %', fontsize=18, color='k')
    pltCPU.set_ylim([0,100])
    '''

    ax.set_xlabel('Time',fontsize=20)
    ax.set_ylabel('Usage %',fontsize=20)
    ax.set_ylim([0,100])

    if (len(datesCPU) == len(CPU)):
        cpuPlot  = ax.plot_date(datesCPU,CPU, fmt='o:', color='k', label='CPU usage', markersize=7)    
    if (len(datesDISK) == len(DISK)):
        diskPlot = ax.plot_date(datesDISK,DISK, fmt='o:', color='r',label='DISK usage', markersize=7)
    if (len(datesRAM) == len(RAM)):
        ramPlot  = ax.plot_date(datesRAM,RAM, fmt='o:', color='b', label='RAM usage', markersize=7)



    years    = dts.YearLocator()   # every year
    months   = dts.MonthLocator()  # every month
    days     = dts.DayLocator()
    hours    = dts.HourLocator()
    daysFmt  = dts.DateFormatter('%m-%d %H:%M')

    # format the ticks
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(daysFmt)
    #ax.xaxis.set_minor_locator(hours)

    ax.set_xlim([datetime.datetime.now()-datetime.timedelta(days=1), datetime.datetime.now()])

    ax.format_xdata = dts.DateFormatter('%m-%d %H:%M')
    fig.autofmt_xdate()
            
    #host.axis["left"].label.set_color('r')
    #pltRAM.axis["right"].label.set_color('b')
    #pltCPU.axis["right"].label.set_color('k')

    #plt.figure.autofmt_xdate()    
    plt.grid()
    plt.title('Resource Usage on %s'%(servername), fontsize=20)
    plt.legend(loc=2,fontsize=20)

    outpath += "resource_monitoring_%s.png"%(servername)
    plt.savefig(outpath)
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
    def __init__(self,nruns=None):

        # Call base class ctor
        super(monitor_machine_resources,self).__init__()

        self._data_dir = ''
        self._experts  = ''
        self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._data_dir = '%s' % (resource['DATADIR'])
        self._experts = resource['EXPERTS']
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # get daemon_log
        try:
            logger = pub_logger.get_logger('list_log')
            k = ds_api.ds_reader(pubdb_conn_info.reader_info(),logger)
            k.connect()
            project = k.list_daemon_log()
        except:
            self.error('could not get logger')
            return

        # if no projects found
        if not project:
            self.info('no projects found...exiting')
            return
            
        # directory where to store plots
        pubstop = str(os.environ.get('PUB_TOP_DIR'))

        ctr = 0
        #try:
        plotpath = pubstop+'/'+self._data_dir+'/'
        self.info('saving plot to path: %s'%plotpath)
        outpath = plot_resource_usage(project,plotpath)
        self.info('saving plot to path: %s'%outpath)
        if (outpath == 'failed import'):
            self.error('could not complete import...plot not produced...')
        if (outpath == None):
            self.error('No plot produced...')
        ctr += 1
        #except:
        #    self.info('could not produce plot') 

        

# A unit test section
if __name__ == '__main__':

    test_obj = monitor_machine_resources(5)

    test_obj.process_newruns()



