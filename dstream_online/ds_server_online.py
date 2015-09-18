import commands,os
#import psutil
from get_machine_info import getRAMusage, getCPUusage, getDISKusage

def ubdaq_logger_smc():
    lines = [x for x in commands.getoutput('df').split('\n') if len(x.split())==5]
    result = {}
    for l in lines:
        words=l.split(None)
        result[words[-1]] = float(words[1])/float(words[0])
    return result

def ubdaq_handler_smc():
    return (True,'')

def near1_handler():
    return (True,'')

def near2_handler():
    return (True,'')

def near1_logger():

    result = {}

    # keep track of status (if something goes wrong)
    status = 0

    log_dir = os.environ['PUB_LOGGER_FILE_LOCATION']
    if not os.path.isdir(log_dir): return result

    # check disk usage:
    
    homedir = '/home'
    datadir = '/data'
    datalocal = '/datalocal'

    if (os.path.isdir(homedir)):
        diskUsage = getDISKusage(homedir)
        result['DISK_USAGE_HOME'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            msg = "disk usage in %s above 90-percent..."%homedir
            print msg
            #self.info(msg)
            #d_msg.email('proc_daemon','near1_logger',msg)
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
            print msg
            #self.info(msg)
            #d_msg.email('proc_daemon','near1_logger',msg)
    else:
        # log the fact that /data is not recognized as dir
        #print "/data not recognized as directory..."
        status = -1


    if (os.path.isdir(datalocal)):
        diskUsage = getDISKusage(datalocal)
        result['DISK_USAGE_DATALOCAL'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            msg = "disk usage in %s above 90-percent..."%datalocal
            print msg
            #self.info(msg)
            #d_msg.email('proc_daemon','near1_logger',msg)
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
        
        
    '''
    # CPU and RAM information accessed
    # using the psutil library
    # info: https://pypi.python.org/pypi/psutil
    # documentation: https://pythonhosted.org/psutil/
    
    # check CPU usage
    CPUpercent = psutil.cpu_percent()
    result['CPU_PERCENT'] = CPUpercent
    
    # check memory usage
    RAMpercent = psutil.virtual_memory().percent
    result['RAM_PERCENT'] = RAMpercent
    '''

    print "**** RESULT IS %i in LENGTH ****"%len(result)

    return result

def near2_logger():
    return near1_logger()

def evb_handler():
    return (True,'')

def evb_logger():

    # keep track of status
    status = 0

    result = {}

    log_dir = os.environ['PUB_LOGGER_FILE_LOCATION']
    if not os.path.isdir(log_dir): return result

    # check disk usage:
    
    homedir = '/home'
    datadir = '/data'

    if (os.path.isdir(homedir)):
        diskUsage = getDISKusage(homedir)
        result['DISK_USAGE_HOME'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            msg = "disk usage in %s above 90-percent..."%homedir
            print msg
            #self.info(msg)
            #d_msg.email('proc_daemon','evb_logger',msg)

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
            print msg
            #self.info(msg)
            #d_msg.email('proc_daemon','evb_logger',msg)

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

    print result
    return result

def smc_handler():
    return (True,'')

def smc_logger():

    # keep track of status
    status = 0

    result = {}

    log_dir = os.environ['PUB_LOGGER_FILE_LOCATION']
    if not os.path.isdir(log_dir): return result

    # check disk usage:
    
    homedir = '/home'
    datadir = '/data'

    if (os.path.isdir(homedir)):
        diskUsage = getDISKusage(homedir)
        result['DISK_USAGE_HOME'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            print "disk usage in /home above 90-percent..."
    else:
        # log the fact that /home is not recognized as dir
        #print "/home not recognized as directory..."
        status = -1

    if (os.path.isdir(datadir)):
        diskUsage = getDISKusage(datadir)
        result['DISK_USAGE_DATA'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            print "disk usage in /data above 90-percent..."
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
        status =-1
        
        
    '''
    # CPU and RAM information accessed
    # using the psutil library
    # info: https://pypi.python.org/pypi/psutil
    # documentation: https://pythonhosted.org/psutil/
    
    # check CPU usage
    CPUpercent = psutil.cpu_percent()
    result['CPU_PERCENT'] = CPUpercent
    
    # check memory usage
    RAMpercent = psutil.virtual_memory().percent
    result['RAM_PERCENT'] = RAMpercent
    '''

    return result
