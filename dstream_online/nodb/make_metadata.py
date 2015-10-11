from dstream import ds_multiprocess,ds_api,ds_status
import sys,time,os,json,datetime

flist_v = [ ( int(x.split()[0]), int(x.split()[1]), x.split()[2] ) for x in open(sys.argv[1],'r').read().split('\n') if len(x.split()) == 3 ]

reader = ds_api.ds_reader()

mp2 = ds_multiprocess()
checksum_v=[]
mp2_index_v=[]
for i in xrange(len(flist_v)):

    run,subrun,fname = flist_v[i]

    checksum = -1
    try:
        ref_status = reader.get_status( ds_status('get_binary_checksum_evb', run, subrun, 0 ) )
        checksum = int(ref_status._data)
    except Exception:
        print 'Calculating checksum:',fname

    checksum_v.append(-1)
    if checksum >0:
        checksum_v[-1]=checksum
        print 'Got checksum:',checksum,'for',fname
        continue

    mp2_index_v.append(i)
    cmd = "python -c \"import samweb_client.utility;print samweb_client.utility.fileEnstoreChecksum('%s')\"" % fname

    index,active_ctr = mp2.execute(cmd)
    time_slept = 0
    while active_ctr >= 5:

        if time_slept and (int(time_slept*10))%50 == 0:
            print '... Waiting parallel processes to be done...'
        
        time.sleep(0.2)
        time_slept += 0.2

        active_ctr = mp2.active_count()

        if time_slept > 180:
            print '... Killing...'
            mp2.kill()

while mp2.active_count():
        
    time.sleep(0.2)
    time_slept += 0.2
    
    if (int(time_slept*10))%50 == 0:
        print '... Waiting parallel processes to be done...'
        
    if time_slept > 210:
        print '... Killing...'
        mp2.kill()

for i in xrange(len(mp2_index_v)):

    checksum_index = mp2_index_v[i]
    mp2_index = i
    
    cout,cerr = mp2.communicate(mp2_index)

    checksum_dict = None
    checksum = -1
    try:
        exec('checksum_dict = %s' % cout)
        checksum = int(checksum_dict['crc_value'])
    except:
        checksum_dict = None
        checksum = -1
        print 'Checksum calculation failed!'
        print cout,cerr
        sys.exit(1)
        continue
    checksum_v[checksum_index] = checksum

    run,subrun,fname = flist_v[checksum_index]
    print 'Got checksum:',checksum,'for',fname

#
# Check
#
if not len(checksum_v) == len(flist_v):
    print "Error! length of checksum & flist don't match!"
    sys.exit(1)
for checksum in checksum_v:
    if checksum < 0:
        print "Bad checksum found!",checksum
        sys.exit(1)
#
# Event header
#

mp = ds_multiprocess()

cmd_template =   "dumpEventHeaders %s 1000000; echo SPLIT_HERE; dumpEventHeaders %s 1;"

for run,subrun,fname in flist_v:
    
    print 'processing',run,subrun,fname

    cmd = cmd_template % (fname,fname)

#    if run in [278,279,280,281,282] or subrun == 37:
#        cmd = 'echo bad data'
    
    index, active_ctr = mp.execute(cmd)

    time_slept = 0
    while active_ctr >= 5:

        if time_slept and (int(time_slept*10))%50 == 0:
            print '... Waiting parallel processes to be done...'
        
        time.sleep(0.2)
        time_slept += 0.2

        active_ctr = mp.active_count()

        if time_slept > 180:
            print '... Killing...'
            mp.kill()

while mp.active_count():
        
    time.sleep(0.2)
    time_slept += 0.2
    
    if (int(time_slept*10))%50 == 0:
        print '... Waiting parallel processes to be done...'
        
    if time_slept > 210:
        print '... Killing...'
        mp.kill()

print 'Making json...'
bad_checksum_v=[]
bad_json_v=[]
good_json_v=[]
zerosize_v=[]
for i in xrange(len(flist_v)):

    run,subrun,fname = flist_v[i]
    cout,cerr = mp.communicate(i)
    checksum = checksum_v[i]

    if checksum <0:
        print 'Checksum unknown:',fname
        bad_checksum_v.append((run,subrun,fname))
        continue

    if not checksum:
        zerosize_v.append((run,subrun,fname))

    fsize = os.path.getsize(fname)

    # Can we put a "bad" metadata as a default contents for "bad file"? If we can, comment out continue                                    
    jsonData = { 'file_name': os.path.basename(fname),
                 'file_type': "data",
                 'file_size': fsize,
                 'file_format': "binaryraw-uncompressed",
                 'runs': [ [run,  subrun, 'test'] ],
                 'first_event': 0,
                 'start_time': '1970-01-01T00:00:00',
                 'end_time': '1970-01-01T00:00:00',
                 'last_event': 0,
                 'group': 'uboone',
                 "crc": { "crc_value":str(checksum),  "crc_type":"adler 32 crc type" },
                 "application": {  "family": "online",  "name": "assembler", "version": 'unknown' },
                 "data_tier": "raw", "event_count": 0,
                 "ub_project.name": "online",
                 "ub_project.stage": "assembler",
                 "ub_project.version": 'v6_00_00',
                 'online.start_time_usec': '-1',
                 'online.end_time_usec': '-1' }
    
    try:
        last_event_cout,first_event_cout = cout.split('SPLIT_HERE')
        ver = 'unknown'
        sevt = eevt = 0
        stime = etime = '1970-01-01T00:00:00'
        stime_usec = etime_usec = -1
        read_gps = False
        for line in last_event_cout.split('\n'):
            if "run_number=" in line and "subrun" not in line :
                if not run == int(line.split('=')[-1].replace(' ','')):
                    print 'Detected un-matching run number: content says run=%s for file %s' % (int(line.split('=')[-1]),fname)
            if "subrun_number=" in line:
                if not subrun == int(line.split('=')[-1].replace(' ','')):
                    print 'Detected un-matching subrun number: content says subrun=%s for file %s' % (int(line.split('=')[-1]),fname)
            if "event_number=" in line:
                eevt = int(line.split('=')[-1])

            if "GPS Time FROM EVENT:Object" in line:
                read_gps=True
            if "GPS time (second,micro,nano)" in line:
                if not read_gps:
                    continue
                words=[]
                for w in line.split():
                    if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                if not len(words) == 3:
                    print 'GPS time stamp format could not be interpreted...'
                    print line
                    raise DSException()
                #print 'Extracted GPS Time... %s' % words
                etime = datetime.datetime.fromtimestamp(words[0]).replace(microsecond=0).isoformat()
                etime_usec = words[1]
                read_gps=False
            if "daq_version_label=" in line:
                ver = line.split('=')[-1]

        read_gps = False
        for line in first_event_cout.split('\n'):
            if "event_number=" in line:
                sevt = int(line.split('=')[-1].replace(' ',''))

            if "GPS Time FROM EVENT:Object" in line:
                read_gps=True

            if "GPS time (second,micro,nano)" in line and read_gps:
                if not read_gps:
                    continue
                words=[]
                for w in line.split():
                    if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                if not len(words) == 3:
                    print 'GPS time stamp format could not be interpreted...'
                    print line
                    raise DSException()
                #print 'Extracted GPS Time... %s' % words
                stime = datetime.datetime.fromtimestamp(words[0]).replace(microsecond=0).isoformat()
                stime_usec = words[1]
                read_gps=False

        print 'Successfully extract metadata for run=%d subrun=%d: %s @ %s' % (run,subrun,fname,time.strftime('%Y-%m-%d %H:%M:%S'))
        num_events = eevt - sevt + 1
        if num_events<0: num_events = 0
        jsonData = { 'file_name': os.path.basename(fname),
                     'file_type': "data",
                     'file_size': fsize,
                     'file_format': "binaryraw-uncompressed",
                     'runs': [ [run,  subrun, 'test'] ],
                     'first_event': sevt,
                     'start_time': stime,
                     'end_time': etime,
                     'last_event':eevt,
                     'group': 'uboone',
                     "crc": { "crc_value":str(checksum),  "crc_type":"adler 32 crc type" },
                     "application": {  "family": "online",  "name": "assembler", "version": ver },
                     "data_tier": "raw", "event_count": num_events,
                     "ub_project.name": "online",
                     "ub_project.stage": "assembler",
                     "ub_project.version": 'v6_00_00',
                     'online.start_time_usec': str(stime_usec),
                     'online.end_time_usec': str(etime_usec) }
    except Exception:
        print 'failed...'
        bad_json_v.append((run,subrun,fname))

    fout = open('%s.json' % fname, 'w')
    json.dump(jsonData, fout, sort_keys = True, indent = 4, ensure_ascii=False)

    good_json_v.append((run,subrun,fname))

fout = open('%s_bad_checksum.txt' % sys.argv[1],'w')
for f in bad_checksum_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()

fout = open('%s_bad_json.txt' % sys.argv[1],'w')
for f in bad_json_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()

fout = open('%s_good_json.txt' % sys.argv[1],'w')
for f in good_json_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()
    
fout = open('%s_zerosize.txt' % sys.argv[1],'w')
for f in zerosize_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()
