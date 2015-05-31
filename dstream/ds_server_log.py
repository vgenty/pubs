import commands,os
#import psutil
from get_machine_info import getRAMusage, getCPUusage

def ubdaq_logger_smc():
    lines = [x for x in commands.getoutput('df').split('\n') if len(x.split())==5]
    result = {}
    for l in lines:
        words=l.split(None)
        result[words[-1]] = float(words[1])/float(words[0])
    return result

def ubdaq_handler_smc():
    return (True,'')

def gpvm_logger():
    result = {}
    fout = None
    log_dir = os.environ['PUB_LOGGER_FILE_LOCATION']
    if not os.path.isdir(log_dir): return result

    try:
        fout = open('%s/joblist.txt' % log_dir,'w')
    except Exception:
        return result
    out = commands.getoutput('jobsub_q --user %s' % os.environ['USER'])
    fout.write(out)
    fout.close()

    jobids = []
    jobctr = 0
    njob_running = 0
    njob_idle = 0
    njob_hold = 0
    njob_unknown = 0
    for line in out.split('\n'):
        words = line.split()
        if not len(words) > 5: continue
        if not words[0].find('@') >= 0: continue
        if not words[0][0:words[0].find('.')].isdigit(): continue

        jobid = words[0]
        jobid = jobid[0:jobid.find('.')]
        if not jobid in jobids: jobids.append(jobid)
        jobctr += 1

        status = words[5]
        if status.upper() == 'R': njob_running +=1
        elif status.upper() == 'I': njob_idle +=1
        elif status.upper() == 'H': njob_hold +=1
        else: njob_unknown +=1

    result['NUM_SUBMITTED'] = len(jobids)
    result['NUM_JOBS'] = jobctr
    result['NUM_RUNNING'] = njob_running
    result['NUM_IDLE'] = njob_idle
    result['NUM_HOLD'] = njob_hold
    result['NUM_UNKNOWN'] = njob_unknown
    return result
    
    
def near1_logger():

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
        print "/home not recognized as directory..."

    if (os.path.isdir(datadir)):
        diskUsage = getDISKusage(datadir)
        result['DISK_USAGE_DATA'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            print "disk usage in /data above 90-percent..."
    else:
        # log the fact that /data is not recognized as dir
        print "/data not recognized as directory..."
        
    mempath = '/proc/meminfo'
    if (os.path.isfile(mempath)):
        RAMused = getRAMusage(mempath)
        result['RAM_PERCENT'] = RAMused
    else:
        # log the fact that we cannot access /proc/meminfo...
        print "cannot access /proc/meminfo file..."
    

    statpath = '/proc/stat'
    if (os.isfile(statpath)):
        CPUpercent = getCPUusage(statpath)
        result['CPU_PERCENT'] = CPUpercent
    else:
        # log the fact that we cannot access /proc/stat
        print "cannot access /proc/stat file..."
        
        
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


def evb_logger():

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
        print "/home not recognized as directory..."

    if (os.path.isdir(datadir)):
        diskUsage = getDISKusage(datadir)
        result['DISK_USAGE_DATA'] = diskUsage
        if (diskUsage > 0.9):
            # send email...
            print "disk usage in /data above 90-percent..."
    else:
        # log the fact that /data is not recognized as dir
        print "/data not recognized as directory..."
        
    mempath = '/proc/meminfo'
    if (os.path.isfile(mempath)):
        RAMused = getRAMusage(mempath)
        result['RAM_PERCENT'] = RAMused
    else:
        # log the fact that we cannot access /proc/meminfo...
        print "cannot access /proc/meminfo file..."
    

    statpath = '/proc/stat'
    if (os.isfile(statpath)):
        CPUpercent = getCPUusage(statpath)
        result['CPU_PERCENT'] = CPUpercent
    else:
        # log the fact that we cannot access /proc/stat
        print "cannot access /proc/stat file..."


    return result
