import sys,os
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as dts
import datetime
import numpy as np

# verbose flag
VERBOSE = False

# import specific get* modules
import get_beamdata, get_runtimes

# import function to update HTML page
import runsummarytemplate

get_beamdata.VERBOSE = VERBOSE
get_runtimes.VERBOSE = VERBOSE

# start and end-time to consider
end   = datetime.datetime.now()
#end   = datetime.datetime.strptime('2015-10-21 23:59:59','%Y-%m-%d %H:%M:%S')
start = end - datetime.timedelta(hours=24)
outfile = 'ppp_vs_intensity.png'

# beam data file name getter
def getBeamDataFileName(): return get_beamdata.outfile

# runtime data file name getter
def getRunTimesDataFileName(): return get_runtimes.outfile

#regenearte variable decides if run info and beam info should
# be downloaded again or not
def getRunsVsIntensity(outdir='',regenerate=True):

    beamdata_file = getBeamDataFileName()
    runtimes_file = getRunTimesDataFileName()

    # name of csv file where to save run summary info
    runsummaryCSV = 'RunSummary.csv'

    # open file in which to save RunSummary information
    runSummary_file = open('%s/%s'%(outdir,runsummaryCSV),'r')
    # get the latest run with info on the RunSummary page
    lastSummaryRun = 0
    old_run_summary = []
    for idx, line in enumerate(runSummary_file):
        if (idx == 0):
            lastSummaryRun = int(line.split()[0])
        old_run_summary.append(line)
    runSummary_file.close()
    runSummary_file = open('%s/%s'%(outdir,runsummaryCSV),'w+')

    if outdir:
        beamdata_file = '%s/%s' % (outdir,beamdata_file)
        runtimes_file = '%s/%s' % (outdir,runtimes_file)
        runstats_file = '%s/%s' % (outdir,runtimes_file)

    if regenerate or not os.path.isfile(beamdata_file) or not os.path.isfile(runtimes_file):
        # first, collect beam information
        get_beamdata.getBeamData(start,end,beamdata_file)
        # second, get run info for the times of interest
        get_runtimes.getRunTimes(start,end,runtimes_file)

    # focus on run-time information first (get start/end time for runs)
    run_file = open(runtimes_file,'r')
    runs = []
    rstart = []
    rend = []
    run_sec = []
    run_ppp = [] # summed ppp for this run
    run_ctr = [] # number of ppp values added
    events  = []
    for line in run_file:
        words = line.split(',')
        if (words[1] == 'None'):
            continue
        run        = int(words[0])
        rstart_str = str(words[1])
        rend_str   = str(words[2])
        subruns    = int((words[3]).split('/n')[0])
        nevents    = subruns*50
        if VERBOSE:
            print 'run : %i subrun : %i events : %i'%(run,subruns,nevents)

        runstart = datetime.datetime.strptime(rstart_str,'%Y-%m-%d %H:%M:%S')
        runend   = datetime.datetime.strptime(rend_str,  '%Y-%m-%d %H:%M:%S')

        dt = runend - runstart
        runs.append(run)
        rstart.append(runstart)
        rend.append(runend)
        run_ppp.append(0)
        run_ctr.append(0)
        run_sec.append(dt.seconds)
        events.append(nevents)

    # now look at beam information
    beam_file = open(beamdata_file,'r')
        
    Tmin = 2000000000000
    Tmax = 0

    pppTot = 0
    ppp_v  = []
    time_v = []

    cntr = 0
    plt_ctr = 0

    sampling = 50

    # current run: which run time-range am I in?
    current_run_idx = len(runs)-1

    for line in beam_file:
        words = line.split(',')
        if (words[0] == "Event"):
            continue

        cntr += 1
        plt_ctr += 1

        time = int(words[3])
        date = datetime.datetime.fromtimestamp(time/1000.)
        pppstr = words[5].split('\n')[0]
        ppp  = float(pppstr)

        if (plt_ctr%sampling == 0):
            ppp_v.append(ppp)
            time_v.append(date)

        # are we in the current run?
        if ( (current_run_idx < len(rstart)) and (current_run_idx >= 0) ):
            if ( (date > rend[current_run_idx]) ):
                current_run_idx -= 1

        # if in the current run interval
        if ( (current_run_idx < len(rstart)) and (current_run_idx >= 0) ):
            if ( (date > rstart[current_run_idx]) and (date < rend[current_run_idx])):
                run_ppp[current_run_idx] += ppp
                run_ctr[current_run_idx] += 1
    
        # update bounds for time of plot
        if (time < Tmin):
            Tmin = time
        if (time > Tmax):
            Tmax = time
        
        pppTot += ppp


    # start working on the actual plot!
    years    = dts.YearLocator()   # every year
    months   = dts.MonthLocator()  # every month
    days     = dts.DayLocator()
    hours    = dts.HourLocator()
    daysFmt  = dts.DateFormatter('%m-%d %H:%M')

    mintime = datetime.datetime.fromtimestamp(Tmin/1000.)
    maxtime = datetime.datetime.fromtimestamp(Tmax/1000.)

    dt = (Tmax-Tmin)/(1000.*3600.) # hours

    if VERBOSE:
        print 'total time [hrs] : %.02f'%dt
        print 'total ppp  [E12] : %i'%pppTot
        print 'avg ppp    [E12] : %.02f'%(pppTot/cntr)
    
    fig,ax = plt.subplots(figsize=(21,10))
    
    dates = dts.date2num(time_v)
    plt.scatter(dates,ppp_v,edgecolor=None,marker='.',lw=0)
    
    years    = dts.YearLocator()   # every year
    months   = dts.MonthLocator()  # every month
    days     = dts.DayLocator()
    hours    = dts.HourLocator()
    daysFmt  = dts.DateFormatter('%m-%d %H:%M')
    
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(daysFmt)
    ax.set_xlim([mintime, maxtime])
    if (mintime > maxtime):
        return
    #print 'Min Time : ',mintime
    #print 'Max Time : ',maxtime
    fig.autofmt_xdate()
    
    lblctr = 0
    for label in ax.xaxis.get_ticklabels():
        lblctr += 1
        if (lblctr%3 != 0):
            label.set_visible(False)

    if VERBOSE:
        print 'mintime: ',mintime
        print 'maxtime: ',maxtime

    # plot time-intervals for runs
    for i in xrange(len(runs)):
        # save run info to RunSummary page if
        # this is a "new" run
        # also, ignore the current run (i==0)
        if ( (runs[i] > lastSummaryRun) and i!=0 ):
            ppp_avg = 0
            if (events[i] != 0):
                ppp_avg = run_ppp[i]/events[i]
            # calculate event rate (Hz)
            run_start_sec = (rstart[i]-datetime.datetime(1970,1,1)).total_seconds()
            run_end_sec   = (rend[i]-datetime.datetime(1970,1,1)).total_seconds()
            run_time_sec = float(run_end_sec - run_start_sec)
            run_time_hrs = run_time_sec / 3600.
            rate = 0
            if run_time_sec != 0:
                rate = events[i]/run_time_sec
            runStats = '%i %s %s %.02f %i %i %.02f %.02f %.02f\n'%(runs[i],rstart[i],rend[i],run_time_hrs,int(events[i]/50.),events[i],run_ppp[i],ppp_avg,rate)
            runSummary_file.write(runStats)

        rstart_date = dts.date2num(rstart[i])
        rend_date   = dts.date2num(rend[i])

        if VERBOSE:
            print 'run % i : [%s,%s]'%(runs[i],rstart[i],rend[i])
            print 'run ctr: ',run_ctr[i]

        if (rend_date < dts.date2num(mintime)):
            continue
        if (rstart_date > dts.date2num(maxtime)):
            continue
        if (run_ctr[i] == 0):
            continue
        pppavg      = run_ppp[i]/run_ctr[i] 
        
        plt.axvspan(rstart_date, rend_date, color='orange', alpha=0.3, lw=4)

        if (run_sec[i] > 3600):
            xpos = (rstart_date+rend_date)/2.
            # if within time-bounds of axes
            if ( (xpos < dts.date2num(maxtime)) and (xpos > dts.date2num(mintime)) ):
                ypos = 1.8
                if pppavg > 1.6: ypos = 0.2
                ax.text(xpos,
                        ypos,
                        'run %i : avg. ppp %.02f'%(runs[i],pppavg),
                        weight='bold',
                        fontsize=16, color='k',horizontalalignment='center',
                        verticalalignment='bottom',rotation='vertical')
    plt.ylim([-0.1,5.5])
    plt.xlabel('Date (US/Central)',fontsize=20)
    plt.ylabel('Intensity [ppp E12]',fontsize=20,color='b')
    plt.title('Daily Run and Beam Intensity Summary',fontsize=20)
    plt.grid()
    plt.tick_params(labelsize=18)
    out_png_name = outfile
    if outdir: out_png_name = '%s/%s' % (outdir,outfile)
    plt.savefig(out_png_name)
    plt.show()

    # add the old run summary info back to the file
    for line in old_run_summary:
        runSummary_file.write(line)
    runSummary_file.close()

    # now generate HTML page with run stats
    runsummarytemplate.generateRunSummaryPage('%s/%s'%(outdir,runsummaryCSV),'%s/RunSummary.html'%outdir)

if __name__ == '__main__' :

    regenerate = True
    if ( len(sys.argv) == 2):
        if (sys.argv[1] == '0'):
            regenerate = False
    
    getRunsVsIntensity(os.environ['PUB_TOP_DIR'],regenerate)
