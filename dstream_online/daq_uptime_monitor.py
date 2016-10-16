## @namespace dstream.DAQ_UPTIME_MONITOR
#  @ingroup dstream
#  @brief Defines a project daq_uptime_monitor
#  @author kterao

# python include
import time,sys,datetime,os,copy,pytz
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
import matplotlib.dates as mpd
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
import numpy as np
import dstream_online.POTvsTime.POTvsTime as POTvsTime

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
        self._runinfo_v = []
        self._update_period_ppp_vs_intensity = 1800
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):
        
        #resource = self._api.get_resource( self._project )

        #self._run_table =  resource['DAQRunTable']

        pass
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        if not self._run_table:
            self.get_resource()

        #
        # PPP vs. Intensity
        #
        intensity_png = '%s/data/%s' % (os.environ['PUB_TOP_DIR'],POTvsTime.outfile)
        update = not os.path.isfile(intensity_png)
        if not update:
            update = (time.time() - os.path.getmtime(intensity_png)) > self._update_period_ppp_vs_intensity

        if update:
            POTvsTime.getRunsVsIntensity('%s/data' % os.environ['PUB_TOP_DIR'],True)
        
        run,subrun = self._api.get_last_run_subrun(self._run_table)

        samweb = samweb_cli.SAMWebClient(experiment="uboone")

        self._runinfo_v = []
        utc_timezone = None
        for i in xrange(run):

            current_run = run - i

            last_subrun = self._api.get_last_subrun(self._run_table,current_run)

            if last_subrun<0: continue

            run_start_time_s,run_start_time_e = self._api.run_timestamp(self._run_table,current_run,0)

            run_end_time_s,run_end_time_e = self._api.run_timestamp(self._run_table,current_run,last_subrun)

            #if not utc_timezone:
                
            utc_timezone_s = copy.copy(run_start_time_s.tzinfo)
            utc_timezone_e = copy.copy(run_end_time_e.tzinfo)

            run_start_time_s = utc_timezone_s.fromutc(run_start_time_s).replace(tzinfo=None)
            run_end_time_e = utc_timezone_e.fromutc(run_end_time_e).replace(tzinfo=None)

            try:

                run_files = [x for x in samweb.listFiles('run_number %d' % current_run) if x.endswith('.ubdaq')]

                if not run_files: 
                    self.info('Run %d has no data file: skipping' % current_run)
                    continue

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

                if not time_max.startswith('19'):

                    time_max = datetime.datetime.strptime(time_max,'%Y-%m-%d %H:%M:%S')#.replace(tzinfo=pytz.utc)
                    if time_max > run_end_time_e:
                        run_end_time_e = time_max

                if not time_min.startswith('19'):
                    time_min = datetime.datetime.strptime(time_min,'%Y-%m-%d %H:%M:%S')#.replace(tzinfo=pytz.utc)

                    if time_min < run_start_time_s:
                        run_start_time_s = time_min

                self.debug('Run %d ... Start @ %s ... End @ %s' % (current_run, run_start_time_s, run_end_time_s))

                self._runinfo_v.append((current_run,last_subrun,run_start_time_s,run_end_time_e))
            except Exception as e:
                self.error('Failed to extract metadta from run=%d' % current_run)
                continue
            if self._runinfo_v and (int(self._runinfo_v[-1][2].strftime('%s')) + self._boundary_past) < time.time():
                break

        hour_frac_map = {}
        for runinfo in self._runinfo_v:
            run,subrun,start,end = runinfo
            duration = (end - start).total_seconds()
            if duration < 1: continue

            year,month,day,hour,minute = (start.year,start.month,start.day,start.hour,start.minute)
            key_datetime = datetime.datetime.strptime('%s-%02d-%02d %02d:00:00' % (year,month,day,hour), '%Y-%m-%d %H:%M:%S')#.replace(tzinfo=utc_timezone)

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

        # Correct "this hour fraction"
        #print hour_frac_map[max_key]
        current_hour = (datetime.datetime.now() - max_key)
        hour_frac_map[max_key] *= ( 3600. / current_hour.total_seconds())
        #print hour_frac_map[max_key]
        # Analysis
        dates = hour_frac_map.keys()
        dates.sort()

        #
        # 1 week
        #
        values=[]
        for date in dates:
            values.append(hour_frac_map[date])

        fig, ax = plt.subplots(figsize=(12, 8))

        overlay_range = [dates[0] + datetime.timedelta(0,-3600,0),dates[-1] + datetime.timedelta(0,3600,0)]
        datenum = mpd.date2num(dates)

        plt.plot(overlay_range, [1,1], marker='',linestyle='--',linewidth=2,color='black')
        plt.axvspan(xmin=dates[0],xmax=dates[-1],ymin=0.,ymax=1./1.3,color='gray',alpha=0.1)
        plt.plot_date(dates,values,label='DAQ UpTime Fraction\nHourly Average (7 Days)',marker='o',linestyle='-',color='blue')
        plt.fill_between(dates,values,color='#81bef7')
        #plt.hist(datenum,weights=values,bins=len(datenum),label='DAQ UpTime Fraction\nHourly Average (7 Days)',color='orange')
        ax.legend(prop={'size':20})
        ax.set_xlim(dates[0],dates[-1]+datetime.timedelta(0,60,0))
        ax.set_ylim(0,1.3)
        ax.xaxis.set_major_locator( DayLocator() )
        #ax.xaxis.set_major_locator( HourLocator(np.arange(0,25,6)) )
        #ax.xaxis.set_minor_locator( HourLocator(np.arange(0,24,6)) )
        ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d %H:%M:%S') )
        ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
        plt.ylabel('Hourly UpTime Fraction',fontsize=20)
        fig.autofmt_xdate()
        plt.title('')
        plt.tick_params(labelsize=15)
        plt.grid()
        plt.show()
        plt.savefig('%s/data/UpTimeLong.png' % os.environ['PUB_TOP_DIR'])


        #
        # 24 hours
        #
        fig, ax = plt.subplots(figsize=(12, 8))
#        datesnum = mpd.date2num(dates[0:25])
#        values   = values[0:25]
        plt.plot(overlay_range, [1,1], marker='',linestyle='--',linewidth=2,color='black')
        plt.axvspan(xmin=dates[0],xmax=dates[-1],ymin=0.,ymax=1./1.3,color='gray',alpha=0.1)
        plt.hist(datenum,weights=values,bins=len(datenum),label='DAQ UpTime Fraction\nHourly Average (24 Hours)',color='cyan')
        #plt.plot_date(dates,values,label='DAQ UpTime Fraction (24 hour)',marker='o',linestyle='-',color='blue')
        ax.legend(prop={'size':20})
        ax.set_xlim(dates[-24],dates[-1]+datetime.timedelta(0,1,0))
        ax.set_ylim(0,1.3)
        ax.xaxis.set_major_locator( HourLocator(np.arange(0,24,6)) )
        ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d %H:%M:%S') )
        ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
        plt.ylabel('Hourly UpTime Fraction',fontsize=20)
        fig.autofmt_xdate()
        plt.title('')
        plt.tick_params(labelsize=15)
        plt.grid()
        plt.show()
        plt.savefig('%s/data/UpTimeShort.png' % os.environ['PUB_TOP_DIR'])

        #
        # 1 week (daily)
        #
        days_frac_map={}
        first_date = None
        end_date = None
        for date in dates:
            year,month,day = (date.year,date.month,date.day)
            day_key = datetime.datetime.strptime('%s-%02d-%02d 00:00:00' % (year,month,day), '%Y-%m-%d %H:%M:%S')#.replace(tzinfo=utc_timezone)
            if not first_date:
                first_date = copy.copy(day_key)
                end_date = copy.copy(day_key)
            if first_date < day_key: first_date = copy.copy(day_key)
            if end_date < day_key: end_date = copy.copy(day_key)

            #day_key -= datetime.timedelta(0,24*3600,0)
            if not day_key in days_frac_map:
                days_frac_map[day_key] = []
            days_frac_map[day_key].append(hour_frac_map[date])

            #print date,' => ',day_key

        for day in days_frac_map:
            ar = np.array(days_frac_map[day])
            days_frac_map[day] = ar.mean()

        dates = days_frac_map.keys()
        dates.sort()

        #print dates
        values = []
        for date in dates:
            values.append(days_frac_map[date])

        fig, ax = plt.subplots(figsize=(12, 8))

        overlay_range = [dates[0] + datetime.timedelta(0,-3600,0),dates[-1] + datetime.timedelta(0,3600,0)]
        datenum = mpd.date2num(dates)
        #print datenum



        first_date = dates[0]  - datetime.timedelta(0,12*3600)
        end_date   = dates[-1] + datetime.timedelta(0,12*3600)

        #plt.plot(overlay_range, [1,1], marker='',linestyle='--',linewidth=2,color='black')
        #plt.axvspan(xmin=dates[0],xmax=dates[-1],ymin=0.,ymax=1./1.3,color='gray',alpha=0.1)
        plt.hist(datenum,weights=values,bins=len(datenum-1),range=mpd.date2num((first_date,end_date)),label='DAQ UpTime Fraction\nDaily Average (7 days)',color='#81bef7')
        ax.legend(prop={'size':20})
        ax.set_xlim(first_date + datetime.timedelta(0,-10,0),
                    end_date + datetime.timedelta(0,-10,0))
        # include uptime value per day
        for i,date in enumerate(dates):
            ax.text(date,0.22,
                    '%.01f %%'%(100*values[i]),
                    weight='bold',
                    fontsize=16, color='k',horizontalalignment='center',
                    verticalalignment='bottom',rotation='vertical')
        #print dates[0],dates[-1]
        ax.set_ylim(0,1.3)
        ax.xaxis.set_major_locator( DayLocator() )
        #ax.xaxis.set_major_locator( HourLocator(np.arange(0,25,6)) )
        #ax.xaxis.set_minor_locator( HourLocator(np.arange(0,24,6)) )
        ax.xaxis.set_major_formatter( DateFormatter('%Y-%m-%d') )
        ax.fmt_xdata = DateFormatter('%Y-%m-%d %H:%M:%S')
        plt.ylabel('Daily UpTime Fraction',fontsize=20)
        #ax.xaxis.labelpad=200
        fig.autofmt_xdate()
        #ax.tick_params(direction='right', pad=15)
        plt.title('')
        plt.tick_params(labelsize=15)
        plt.grid()
        plt.show()
        plt.savefig('%s/data/UpTimeLongDaily.png' % os.environ['PUB_TOP_DIR'])


    def make_html(self):
        web_contents = \
        """
        <!DOCTYPE html>
        <head>
        <meta http-equiv="refresh" content="30">
        </head>
        <html>
        <body>

        <h1> DAQ Uptime Statistics </h1>
        Brief run statistics summary page for recently taken runs.<br>
        The page is created by online PUBS and maintained by DataManagement group.
        """

        now = datetime.datetime.now()
        this_shift_start = datetime.datetime(now.year, now.month, now.day, (int(now.hour)/8)*8, 0, 0)
        this_shift_end = this_shift_start + datetime.timedelta(0,3600*8,0)
        shift_start_v=[]
        shift_end_v=[]
        for x in xrange(3):
            start_ts = this_shift_start - datetime.timedelta(0,3600*8*x,0)
            end_ts   = this_shift_end - datetime.timedelta(0,3600*8*x,0)
            shift_start_v.append( start_ts )
            shift_end_v.append( end_ts )

        web_contents += '<h2>Run Statistics Summary for Last 24 Hours</h2>\n'
        web_contents += 'Table below shows the list of runs that is taken during your shift period.<br>\n'
        web_contents += 'DAQ up-time during the current shift is shown in the next table (right).<br>\n'
        web_contents += '<br><br>\n'

        web_contents += "<table style=\"width:100%\"><tr><td>\n"
        web_contents += \
        """
        <table border='1' width=600>
        <tr>
        <th> Run           </th>
        <th> SubRun Counts </th>
        <th> Start Time    </th>
        <th> Run Length    </th>
        </tr>
        """

        current_shift  = 0
        previous_shift = 1
        sum_time_v=[0] * len(shift_start_v)
        duration_v=[0] * len(shift_start_v)
        for i in xrange(len(self._runinfo_v)):

            #run,subrun,start,end = self._runinfo_v[ -1 - i ]
            run,subrun,start,end = self._runinfo_v[ i ]

            break_loop = False
            while end < shift_start_v[current_shift]:
                current_shift  += 1
                previous_shift += 1
                if current_shift >= len(shift_start_v):
                    break_loop = True
                    break
            if break_loop: break

            for i in xrange(len(duration_v)):

                duration_v[i] = 0
            
            # this run "ends within current shift" and either "start within current shift" or "started within previous shift"

            if shift_start_v[current_shift] < start:
                duration_v[current_shift] = (end - start).total_seconds()
            else:
                duration_v[current_shift] = (end - shift_start_v[current_shift]).total_seconds()
                if previous_shift < len(shift_start_v):
                    duration_v[previous_shift] = (shift_end_v[previous_shift] - start).total_seconds()

            for i in xrange(len(duration_v)):
                
                sum_time_v[i] += duration_v[i]

            start_date = start.isoformat().split('T')[0]
            start_date = start_date[start_date.find('-')+1:len(start_date)]
            start_time = start.isoformat().split('T')[1]
            web_contents += "<tr>\n"
            web_contents += "<td align=\"center\"> %d</td>\n" % run
            web_contents += "<td align=\"center\"> %d</td>\n" % subrun
            web_contents += "<td align=\"center\"> %s %s</td>\n" % (start_date,start_time)
            web_contents += "<td align=\"center\"> %g [min.]</td>\n" % (float((end - start).total_seconds())/60.)

            web_contents += "</tr>\n"

        web_contents += "</table> </td>\n"

        web_contents += "<td valign=\"top\"><table border='1' width=600>\n"
        web_contents += "<tr>\n"
        web_contents += "<th></th>\n"
        for i in xrange(len(sum_time_v)):
            index = len(sum_time_v) - i - 1
            web_contents += "<th> Shift %s %s to %s </th>\n" % (shift_start_v[index].isoformat().split('T')[0],
                                                                shift_start_v[index].isoformat().split('T')[1][0:5],
                                                                shift_end_v[index].isoformat().split('T')[1][0:5])
        web_contents += "</tr>\n"
        web_contents += "<tr>\n"
        web_contents += "<td align=\"center\"><b> Cumulative Run Length </b></td>\n"
        for i in xrange(len(sum_time_v)):
            index = len(sum_time_v) - i - 1
            up_time  = sum_time_v[index]
            web_contents += "<td align=\"center\"><b>%d min.</td>\n" % int(up_time/60.)
            
        web_contents += "</tr>\n"
        web_contents += "<td align=\"center\"><b> UpTime Fraction </b></td>\n"

        for i in xrange(len(sum_time_v)):
            index = len(sum_time_v) - i - 1
            up_time  = sum_time_v[index]
            tot_time = (shift_end_v[index] - shift_start_v[index]).total_seconds()
            if shift_end_v[index] > now:
                tot_time = (now - shift_start_v[index]).total_seconds()
            frac = float(up_time)/float(tot_time) 
            frac = int(frac*10000)
            frac /= 100.
            if not i:
                tot_time = (now - shift_start_v[index]).total_seconds()
            color = ''
            if frac > 85.:
                color = 'blue'
            elif frac > 65.:
                color = '#FF00FF'
            else:
                color = 'FF0000'
                
            web_contents += "<td align=\"center\"><b><font color=\"%s\"> %g%% </font></td>\n" % (color,frac)
        
        web_contents += "</tr>\n"
        web_contents += "</table>\n"
        web_contents += "</td></tr></table>\n"

        web_contents += "<br>Last Updated: %s %s<br>\n" % tuple(now.replace(microsecond=0).isoformat().split("T"))

        web_contents += \
        """
        <h2> DAQ UpTime Plots </h2>
        <center>
        <table style="width:100%">
        <tr>
        <figure>
        <td>
        <img src="UpTimeShort.png" alt="DAQ UpTime (Hourly, 24 hour)" style="width:600px;height:400px;" border="2"/>
        <center><figcaption><font size=4 color="0080ff"><b> DAQ UpTime (Hourly, Last 24 Hours) </b></font></figcaption></center>
        </td>
        <td>
        <img src="UpTimeLong.png" alt="DAQ UpTime (Hourly, 7 days)" style="width:600px;height:400px;" border="2"/>
        <center><figcaption><font size=4 color="0080ff"><b> DAQ UpTime (Hourly, Past Week) </b></font></figcaption></center>
        </td>
        <td>
        <img src="UpTimeLongDaily.png" alt="DAQ UpTime (Daily, 24 hour)" style="width:600px;height:400px;" border="2"/>
        <center><figcaption><font size=4 color="0080ff"><b> DAQ UpTime (Daily, Past Week) </b></font></figcaption></center>
        </td>
        </figure>
        </tr>        
        </table>
        </center>
        """

        web_contents += \
        """
        <h2>Beam Intensity Summary for Last 24 Hours</h2>
        <center>
        <figure>
        """
        web_contents += '<img src="%s" alt="Beam Intensity Plot" style="width:1200px;height:666px;" border="2"/>\n' % POTvsTime.outfile
        web_contents += \
        """
        <figcaption><font size=4 color="0080ff"><b>PPP Intensity (24 Hours) </b></font></figcaption>
        </figure>
        </center>        
        <br> Link to RunSummary page with statistics per run: <br>
        <a href='http://ubdaq-prod-near2.fnal.gov/Pubs/RunSummary.html'>RunSummary Page</a>
        </body>
        </html>
        """
        fout=open('%s/data/RunStat.html' % os.environ['PUB_TOP_DIR'],'w')
        fout.write(web_contents)
        fout.close()

# A unit test section
if __name__ == '__main__':

    test_obj = daq_uptime_monitor(sys.argv[1])

    test_obj.info('Start @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    test_obj.process_newruns()

    test_obj.make_html()

    test_obj.info('End @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

