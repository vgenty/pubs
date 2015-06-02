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

fig, ax1 = plt.subplots()
ax1.plot_date(dates,DISK,fmt='o--',color='b')
ax1.set_ylabel('DISK usage Frac.',fontsize=16,color='b')
ax1.set_xlabel('Time',fontsize=16)
ax1.set_ylim([0,1])

ax2 = ax1.twinx()
ax2.plot_date(dates,RAM,fmt='o--',color='k',label='RAM')
ax2.plot_date(dates,CPU,fmt='^--',color='k',label='CPU')
ax2.legend()
ax2.set_ylabel('Resource Usage %',fontsize=16,color='k')
#ax2.set_ylim()

plt.grid()
plt.title('Computer Resource Monitoring',fontsize=16)
plt.show()

print
sys.exit(0)

