## @namespace dstream.DAQ_UPTIME_MONITOR
#  @ingroup dstream
#  @brief Defines a project daq_uptime_monitor
#  @author kterao

# python include
import time,sys,datetime
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
import samweb_cli

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mpc
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
import datetime
import numpy as np

## @class daq_uptime_monitor
#  @brief kterao should give a brief comment here
#  @details
#  kterao should give a detailed comment here
class daq_uptime_monitor(ds_project_base):

    # Define project name as class attribute
    _project = 'daq_uptime_monitor'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self,name=None):

        # Call base class ctor
        super(daq_uptime_monitor,self).__init__(name)

        self._run_table='MainRun'
        self._boundary_past = 3600*24*7

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        #resource = self._api.get_resource( self._project )

        #self._run_table =  resource['DAQRunTable']
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        if not self._run_table:
            self.get_resource()

        run,subrun = self._api.get_last_run_subrun(self._run_table)

        samweb = samweb_cli.SAMWebClient(experiment="uboone")

        timerange_v=[]
        for i in xrange(run):

            current_run = run - i
            
            try:
                run_files = [x for x in samweb.listFiles('run_number %d' % current_run) if x.endswith('.ubdaq')]
                if not run_files: continue
                subrun_max=0
                subrun_min=1e9
                fname_max = ''
                fname_min = ''
                for f in run_files:
                    subrun = int(f.replace('.ubdaq','').split('-')[-1])
                    if subrun_max < subrun: 
                        subrun_max = subrun
                        fname_max = f
                    if subrun_min > subrun: 
                        subrun_min = subrun
                        fname_min = f
                meta_max = samweb.getMetadata(fname_max)
                meta_min = samweb.getMetadata(fname_min)

                time_max = meta_max['end_time'].replace('T',' ').split('+')[0]
                time_min = meta_min['start_time'].replace('T',' ').split('+')[0]

                time_max = datetime.datetime.strptime(time_max,'%Y-%m-%d %H:%M:%S')
                time_min = datetime.datetime.strptime(time_min,'%Y-%m-%d %H:%M:%S')

                timerange_v.append( (current_run,time_min,time_max) )

            except Exception as e:
                continue

            if timerange_v and (int(timerange_v[-1][1].strftime('%s')) + self._boundary_past) < time.time():
                break
        
        hour_frac_map = {}
        for timerange in timerange_v:
            run,start,end = timerange
            duration = (end - start).total_seconds()
            if duration < 1: continue

            year,month,day,hour,minute = (start.year,start.month,start.day,start.hour,start.minute)
            key_datetime = datetime.datetime.strptime('%s-%02d-%02d %02d:00:00' % (year,month,day,hour), '%Y-%m-%d %H:%M:%S')

            if not key_datetime in hour_frac_map:
                hour_frac_map[key_datetime] = 0

            # take care of the first hour bounday
            if duration < (3600 - minute*60):
                hour_frac_map[key_datetime] += duration / 3600.
                continue

            sec_in_first_hour = 3600 - minute*60

            hour_frac_map[key_datetime] += sec_in_first_hour / 3600.
            
            duration -= sec_in_first_hour

            while duration >0:

                key_datetime += datetime.timedelta(0,3600,0)

                if not key_datetime in hour_frac_map:
                    hour_frac_map[key_datetime] = 0

                if duration >= 3600:
                    hour_frac_map[key_datetime] += 1.
                    duration -= 3600
            
                else:
                    hour_frac_map[key_datetime] += duration/3600.
                    break

        min_key = None
        max_key = None
        for d in hour_frac_map:
            if not min_key: min_key = d
            if not max_key: max_key = d
            if min_key > d: min_key = d
            if max_key < d: max_key = d

        if not min_key or not max_key: return

        dt = max_key - min_key
        num_hours = int(dt.days*24 + dt.seconds/3600)
        for x in xrange(num_hours):
            every_hour = min_key + datetime.timedelta(0,3600*x,0)
            if not every_hour in hour_frac_map:
                hour_frac_map[every_hour] = 0

        dates = hour_frac_map.keys()
        dates.sort()

        #
        # 1 week
        #
        values=[]
        for date in dates:
            values.append(hour_frac_map[date])

        fig, ax = plt.subplots(figsize=(12, 8))

        plt.plot_date(dates,values,label='DAQ UpTime Fraction (7 Days)',marker='o',linestyle='-',color='blue')
        ax.legend(prop={'size':20})
        ax.set_xlim(dates[0],dates[-1])
        ax.set_ylim(0,1.2)
        ax.xaxis.set_major_locator( DayLocator() )
        #ax.xaxis.set_major_locator( HourLocator(np.arange(0,25,6)) )
        ax.xaxis.set_minor_locator( HourLocator(np.arange(0,25,6)) )
        ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d %H:%M:%S') )
        ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
        plt.ylabel('DAQ UpTime Fraction',fontsize=20)
        fig.autofmt_xdate()
        plt.title('DAQ UpTime (Last 7 Days)')
        plt.tick_params(labelsize=15)
        plt.grid()
        plt.show()
        plt.savefig('%s/data/UpTimeLong.png' % os.environ['PUB_TOP_DIR'])


        #
        # 24 hours
        #
        fig, ax = plt.subplots(figsize=(12, 8))

        plt.plot_date(dates,values,label='DAQ UpTime Fraction (24 hour)',marker='o',linestyle='-',color='blue')
        ax.legend(prop={'size':20})
        ax.set_xlim(dates[-24],dates[-1])
        ax.set_ylim(0,1.2)
        ax.xaxis.set_major_locator( HourLocator(np.arange(0,25,6)) )
        ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d %H:%M:%S') )
        ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
        plt.ylabel('DAQ UpTime Fraction',fontsize=20)
        fig.autofmt_xdate()
        plt.title('DAQ UpTime (Last 24 Hours)')
        plt.tick_params(labelsize=15)
        plt.grid()
        plt.show()
        plt.savefig('%s/data/UpTimeShort.png' % os.environ['PUB_TOP_DIR'])

# A unit test section
if __name__ == '__main__':

    test_obj = daq_uptime_monitor(sys.argv[1])

    test_obj.process_newruns()



