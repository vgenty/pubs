import commands
from pub_util import pub_watch
from ds_messenger import daemon_messenger as d_msg
from get_machine_info import getRAMusage, getCPUusage, getDISKusage
import os

def dummy_logger():

    result = {}

    # keep track of status (if anything went wrong)
    status = 0

    '''
    dt = pub_watch.time('dummy_logger')
    if not dt:
        pub_watch.start('dummy_logger')
        return result

    if dt < 40.:
        return result
        
    lines = [x for x in commands.getoutput('df').split('\n') if len(x.split())==5]
    for l in lines:
        words=l.split(None)
        result[words[-1]] = float(words[1])/float(words[0])
    '''
    # check disk usage:
    
    homedir = '/home'
    datadir = '/data'

    if (os.path.isdir(homedir)):
        diskUsage = getDISKusage(homedir)
        result['DISK_USAGE_HOME'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            msg = "disk usage in %s above 90-percent..."%homedir
            d_msg.email('proc_daemon','dummy_logger',msg)
    else:
        # log the fact that /home is not recognized as dir
        #print "/home not recognized as directory..."
        status = -1

    if (os.path.isdir(datadir)):
        diskUsage = getDISKusage(datadir)
        result['DISK_USAGE_DATA'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            msg = "disk usage in %s above 90-percent..."%datadir
            d_msg.email('proc_daemon','dummy_logger',msg)

    else:
        # log the fact that /data is not recognized as dir
        #print "/data not recognized as directory..."
        status = -1
        
    mempath = '/proc/meminfo'
    if (os.path.isfile(mempath)):
        RAMused = getRAMusage(mempath)
        result['RAM_PERCENT'] = RAMused
    else:
        # log the fact that we cannot access /proc/meminfo...
        #print "cannot access /proc/meminfo file..."
        status = -1
    

    statpath = '/proc/stat'
    if (os.path.isfile(statpath)):
        CPUpercent = getCPUusage(statpath)
        result['CPU_PERCENT'] = CPUpercent
    else:
        # log the fact that we cannot access /proc/stat
        #print "cannot access /proc/stat file..."
        status = -1


    #d_msg.email('proc_daemon','hello world','executed dummy_logger! dt=%g' % dt)
    pub_watch.start('dummy_logger')

    return result

def dummy_handler():
    return (True,'')    
