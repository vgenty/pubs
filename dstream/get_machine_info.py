import time, os

def getCPUusage(statpath):

    # check CPU upage from /proc/stat file
    # method:
    # http://unix.stackexchange.com/questions/27076/how-can-i-receive-top-like-cpu-statistics-from-the-shell
    CPUpercent = 0
    # measure CPU percentage 10 times and average
    Nchecks = 100
    for x in xrange(Nchecks):
        statfile = open(statpath,'r')
        line0 = statfile.readline()
        statfile.close()
        words = line0.split()
        totPre = float ( int(words[1]) + int(words[2]) + int(words[3]) + int(words[4]) )
        idlePre = float(words[4])
        time.sleep(0.01)
        statfile = open(statpath,'r')
        line0 = statfile.readline()
        statfile.close()
        words = line0.split()
        totPost = float( int(words[1]) + int(words[2]) + int(words[3]) + int(words[4]) )
        idlePost = float(words[4])
        CPUpercent += 100 * ( (totPost-totPre) - (idlePost-idlePre) ) / (totPost-totPre)
    CPUpercent /= float(Nchecks)
    return CPUpercent

    
def getRAMusage(mempath):

    # Check RAM usage
    # look at /proc/meminfo file to find information
    meminfo = open(mempath,'r')
    memTotal = 0 # total available memory (to be found)
    memFree  = 0 # total free memory (to be found)
    buffers  = 0 # memory in buffers
    cached   = 0 # memory chached
    for line in meminfo:
        words = line.split()
        if (words[0] == 'MemTotal:'):
            memTotal = int(words[1])
        if (words[0] == 'MemFree:'):
            memFree  = int(words[1])
        if (words[0] == 'Buffers:'):
            buffers  = int(words[1])
        if (words[0] == 'Cached:'):
            cached  = int(words[1])
    meminfo.close()
    RAMused = 100 * float(memTotal-memFree-buffers-cached)/memTotal
    return RAMused


def getDISKSize(dirpath):

    stat = os.statvfs(dirpath)
    pathSize = stat.f_frsize * stat.f_blocks
    totlMB = pathSize/1.e6
    return totlMB

def getDISKusage(dirpath):

    # get disk-space usage
    # use python os.statvfs
    # see https://docs.python.org/2/library/os.html#os.statvfs
    # /home dir first
    statHome = os.statvfs(dirpath)
    freeBytes = statHome.f_bfree
    totlBytes = statHome.f_blocks
    if totlBytes:
        return  1.-float(freeBytes)/float(totlBytes)    
    return 0

def getNetworkUsage(port):

    return
    # use ifconfig command to measure the network
    # transfer speeds.
    
    
