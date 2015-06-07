#!/usr/bin/env python
# python import
import sys
# dstream import
from dstream.ds_api import ds_reader
# pub_util import
from pub_util import pub_logger
# pub_dbi import
from pub_dbi  import pubdb_conn_info
# matplotlib import (for plotting)
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as dts
    from mpl_toolkits.axes_grid1 import host_subplot
    import mpl_toolkits.axisartist as AA
except ImportError:
    print 'Matplotlib not available... aborting!'
    sys.exit(1)
# datetime (to plot vs. time)
import datetime

logger = pub_logger.get_logger('list_log')

# DB interface for altering ProcessTable
k=ds_reader(pubdb_conn_info.reader_info(), logger)

# Connect to DB
k.connect()

# Define a project
projects = k.list_daemon_log()

if not projects: 
    print 'No project found... aborting!'
    print
    sys.exit(1)

times = []
CPU   = []
RAM   = []
DISK  = []

for x in projects:

    log_time = x.get_log_time()
    log_dict = x.get_log_dict()
    if ( (log_time != '') and (log_dict) ):
        # get time in python datetime format
        time = datetime.datetime.strptime(log_time,'%Y-%m-%d %H:%M:%S.%f')
        times.append(time)
    for key in log_dict:
        if (str(key) == 'DISK_USAGE_HOME'):
            DISK.append(float(log_dict[key]))
        if (str(key) == 'RAM_PERCENT'):
            RAM.append(float(log_dict[key]))
        if (str(key) == 'CPU_PERCENT'):
            CPU.append(float(log_dict[key]))

dates = dts.date2num(times)


# example for multi-axes (i.e. >= 3) plot here:
# http://stackoverflow.com/questions/9103166/multiple-axis-in-matplotlib-with-different-scales

host = host_subplot(111,axes_class=AA.Axes)
#plt.subplots_adjust(0.75)

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
pltCPU.set_ylabel('CPU Usage %', fontsize=18, color='k')

host.plot_date(dates,DISK, fmt='o--', color='r')
pltRAM.plot_date(dates,RAM, fmt='o--', color='b', label='RAM')
pltCPU.plot_date(dates,CPU, fmt='o--', color='k', label='CPU')

host.axis["left"].label.set_color('r')
pltRAM.axis["right"].label.set_color('b')
pltCPU.axis["right"].label.set_color('k')

plt.draw()

plt.grid()
plt.title('Computer Resource Monitoring', fontsize=18)
plt.show()

print
sys.exit(0)

