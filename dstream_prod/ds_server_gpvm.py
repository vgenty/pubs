import commands,os

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
    
