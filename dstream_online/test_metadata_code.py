
## @namespace dstream_online.get_metadata
#  @ingroup dstream_online
#  @brief Defines a project dstream_online.get_metadata
#  @author echurch, yuntse

# python include
import time, os, shutil, sys, gc
# pub_dbi package include
import datetime, json
import pdb
import subprocess
import glob

class get_metadata() :

    def debug(str):
        sys.strerr.write(str)
    
    def error(str):
        sys.strerr.write(str)
    
    def process_ubdaq_files(self,in_file_v):

        cmd_template =   "dumpEventHeaders %s 1000000; echo SPLIT_HERE; dumpEventHeaders %s 1;"
        
        #mp = ds_multiprocess(self._project)
        print(in_file_v)
        
        out =[""]*len(in_file_v)
        err = [""]*len(in_file_v)
        
        for index_run in xrange(len(in_file_v)):
            
            in_file = in_file_v[index_run]
            
            cmd = cmd_template % (in_file,in_file)
            
            p = subprocess.Popen(cmd, shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

            out[index_run],err[index_run] = p.communicate()
            
            if p.returncode!=0 :
                print "File failed to parse with dumpEventHeaders: %s" % in_file
                print "The return value was: %s" % p
                return
            
            
        status_v=[]
        for index_run in xrange(len(in_file_v)):
            
            status_v.append((3,None))
            in_file = in_file_v[index_run]
            in_file_split = in_file.split('-')
            
            if in_file_split[0]=='PhysicsRun':
                run_type='physics'
            elif in_file_split[0]=='NoiseRun':
                run_type='noise'
            elif in_file_split[0]=='CalibrationRun':
                run_type='calibration'
            elif in_file_split[0]=='PMTCalibrationRun':
                run_type='pmtcalibration'
            elif in_file_split[0]=='TPCCalibrationRun':
                run_type='tpccalibration'
            elif in_file_split[0]=='LaserCalibrationRun':
                run_type='lasercalibration'
            elif in_file_split[0]=='BeamOffRun':
                run_type='beamoff'
            elif in_file_split[0]=='BeamOff':
                run_type='beamoff'
            elif in_file_split[0]=='TestRun':
                run_type='test'
            else:
                run_type='unknown'

            fsize = os.path.getsize(in_file)
            run = int(in_file_split[2])
            subrun = int(in_file_split[3].split('.')[0])
        
            # Can we put a "bad" metadata as a default contents for "bad file"? If we can, comment out continue
            badJsonData = { 'file_name': os.path.basename(in_file),
                'file_type': "data",
                    'file_size': fsize,
                        'file_format': "binaryraw-compressed",
                            'runs': [ [run,  subrun, run_type] ],
                            'first_event': 0,
                            'start_time': '1970-01-01T00:00:00',
                            'end_time': '1970-01-01T00:00:00',
                            'last_event': 0,
                            'group': 'uboone',
                            "crc": { "crc_value":"123456",  "crc_type":"adler 32 crc type" },
                            "application": {  "family": "online",  "name": "assembler", "version": 'unknown' },
                            "data_tier": "raw", "event_count": 0,
                            "ub_project.name": "online",
                            "ub_project.stage": "assembler",
                            "ub_project.version": "v6_0_0",
                            'online.start_time_usec': '-1',
                            'online.end_time_usec': '-1'}
                        
            last_event_cout,first_event_cout = out[index_run].split('SPLIT_HERE')
            ver = 'unknown'
            sevt = eevt = 0
            stime = etime = '1970-01-01:T00:00:00'
            stime_usec = etime_usec  = -1
            stime_secs = etime_secs = -1
            sdaqclock = edaqclock = -1
            
            try:
                read_gps=False
                read_daqclock=False
                read_event_time=False
                utc_offset = 0
                if (time.daylight):
                    utc_offset=time.altzone
                else:
                    utc_offset=time.timezone
                
                for line in last_event_cout.split('\n'):
                    if "run_number=" in line and "subrun" not in line :
                        print('Extracting run_number... %s' % line.split('=')[-1])
                        if not run == int(line.split('=')[-1].replace(' ','')):
                            print('Detected un-matching run number: content says run=%s for file %s' % (int(line.split('=')[-1]),in_file))
                    if "subrun_number=" in line:
                        print('Extracting subrun_number... %s' % line.split('=')[-1])
                        if not subrun == int(line.split('=')[-1].replace(' ','')):
                            print('Detected un-matching subrun number: content says subrun=%s for file %s' % (int(line.split('=')[-1]),in_file))
                    if "event_number=" in line:
                        print('Extracting event_number... %s' % line.split('=')[-1])
                        eevt = int(line.split('=')[-1])
                    if "Event Time:" in line:
                        read_event_time=True
                    if "seconds" in line and "micro_seconds" in line and "nano_seconds" in line and read_event_time:
                        words=line.split(' ')
                        print(words)
                        for w in words:
                            if "seconds" in w and "micro_seconds" not in w and "nano_seconds" not in w:
                                print('last etime_secs_tmp is %d' % int(w.split('=')[1]))
                                etime_secs_tmp = int(w.split('=')[1]) + utc_offset
                            if "micro_seconds" in w:
                                etime_usec = int(w.split('=')[1])
                    
                        print('Extracted Last Event Time... %d %d' % (etime_secs_tmp, etime_usec) )
                        etime = datetime.datetime.fromtimestamp(etime_secs_tmp).replace(microsecond=0).isoformat()
                        etime_secs = (etime_secs_tmp, etime_usec)
                        read_event_time=False
                    if "GPS Time FROM EVENT:Object" in line:
                        read_gps=True
                    if "GPS time (second,micro,nano)" in line and read_gps:
                        words=[]
                        for w in line.split():
                            if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                        if not len(words) == 3:
                            print('GPS last time stamp format could not be interpreted...')
                            print(line)
                            raise Exception()
                        print('Extracted Last GPS Time... %s' % words)
                        gps_etime = datetime.datetime.fromtimestamp(words[0] + utc_offset).replace(microsecond=0).isoformat()
                        gps_etime_usec = words[1]
                        gps_etime_secs = (words[0] + utc_offset, words[1])
                        read_gps=False
                    if "Trigger Board Clock Time FROM EVENT:Object" in line:
                        read_daqclock=True
                    if "Trigger Clock: (frame,sample,div)" in line and read_daqclock:
                        words=[]
                        for w in line.split():
                            if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                        if not len(words) == 3:
                            print('DAQClock time last-event format could not be interpreted...')
                            print(line)
                            raise Exception()
                        print('Extracted Last DAQClock Time... %s' % words)
                        edaqclock = (words[0] + utc_offset,words[1],words[2])
                        read_daqclock=False
                    if "daq_version_label=" in line:
                        print('DAQ version... %s' % line.split('=')[-1])
                        ver = line.split('=')[-1]
                                        
                read_gps=False
                read_daqclock=False
                read_event_time=False
                for line in first_event_cout.split('\n'):
                    if "event_number=" in line:
                        print('Extracting Start event_number... %s' % line.split('=')[-1])
                        sevt = int(line.split('=')[-1].replace(' ',''))
                    if "Event Time:" in line:
                        read_event_time=True
                    if "seconds" in line and "micro_seconds" in line and "nano_seconds" in line and read_event_time:
                        words=line.split(' ')
                        for w in words:
                            if "seconds" in w and "micro_seconds" not in w and "nano_seconds" not in w:
                                stime_secs_tmp = int(w.split('=')[1]) + utc_offset
                            if "micro_seconds" in w:
                                stime_usec = int(w.split('=')[1])
                                                                                                        
                        print('Extracted Start Event Time... %d %d' % (stime_secs_tmp, stime_usec) )
                        stime = datetime.datetime.fromtimestamp(stime_secs_tmp).replace(microsecond=0).isoformat()
                        stime_secs = (stime_secs_tmp, stime_usec)
                        read_event_time=False
                    if "GPS Time FROM EVENT:Object" in line:
                        read_gps=True
                    if "GPS time (second,micro,nano)" in line and read_gps:
                        words=[]
                        for w in line.split():
                            if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                        if not len(words) == 3:
                            print('GPS start time stamp format could not be interpreted...')
                            print(line)
                            raise Exception()
                        print('Extracted Start GPS Time... %s' % words)
                        gps_stime = datetime.datetime.fromtimestamp(words[0] + utc_offset).replace(microsecond=0).isoformat()
                        gps_stime_usec = words[1]
                        gps_stime_secs = (words[0] + utc_offset, words[1])
                        read_gps=False
                    if "Trigger Board Clock Time FROM EVENT:Object" in line:
                        read_daqclock=True
                    if "Trigger Clock: (frame,sample,div)" in line and read_daqclock:
                        words=[]
                        for w in line.split():
                            if w.rstrip(',').isdigit(): words.append(int(w.rstrip(',')))
                        if not len(words) == 3:
                            print('DAQClock time first-event format could not be interpreted...')
                            print(line)
                            raise Exception()
                        print('Extracted Start DAQClock Time... %s' % words)
                        sdaqclock = (words[0] + utc_offset,words[1],words[2])
                        read_daqclock=False
                            
                # Need to fix it cuz it's pre-PPS
                # Gotta get out of isoformat strings to do the maths.
                if datetime.datetime.fromtimestamp(gps_stime_secs[0]).replace(microsecond=0).year < 2015 and edaqclock is not -1 and sdaqclock is not -1 and gps_etime is not -1 and gps_stime is not -1:
                    start_prePPS = gps_stime
                    dt = sum([i*j for (i, j) in zip(tuple(map(lambda x, y: x - y, edaqclock, sdaqclock)) , (1600.0,0.5,0.00624)) ]) # dot-product, musec
                    gps_stime_tmp = gps_etime_secs[0]*1.0E6 + gps_etime_secs[1] - dt
                    #pdb.set_trace()
                    gps_stime = datetime.datetime.fromtimestamp(int(gps_stime_tmp * 1.0E-6)).replace(microsecond=0).isoformat()
                    gps_stime_usec = str ( int (gps_stime_tmp - ( int(gps_stime_tmp *1.0E-6) * 1.0E6)) )
                    print('Changed start time from ' + start_prePPS + ' to ' + gps_stime + ' and ' + gps_stime_usec + ' microseconds.')
                                                        
                status_v[index_run] = (3,None)
                print('Successfully extract metadata for run=%d subrun=%d: %s @ %s' % (run,subrun,in_file,time.strftime('%Y-%m-%d %H:%M:%S')))
                            
            except ValueError,TypeError:
                print ("Unexpected error: %s" % sys.exc_info()[0] )
                print ("1st event:\n%s" % first_event_cout)
                print ("Last event:\n%s" % last_event_cout)
                status_v[index_run] = (999,badJsonData)
                print('Failed extracting metadata: %s' % in_file)
                continue
                                                                                    
            if ( (gps_stime_secs[0] - stime_secs[0]) > 100):
                print("The GPS Event time was more than 100 seconds different than the localhost time from NTP!!!!")
                print("We are gonna use the localhost NTP time for online microseconds")
                gps_stime_usec = stime_usec
                                                                                                    
            if ( ( gps_etime_secs[0] - etime_secs[0]) > 100 ):
                print("The GPS Event time was more than 100 seconds different than the localhost time from NTP!!!!")
                print("We are gonna use the localhost NTP time for online microseconds")
                gps_etime_usec = etime_usec
                                                                                                                    
            num_events = eevt - sevt + 1
            if num_events < 0:
                num_events = 0
            # run number and subrun number in the metadata seem to be funny,
            # and currently we are using the values in the file name.
            # Also add ub_project.name/stage/version, and data_tier by hand
            jsonData = {'file_name': os.path.basename(in_file),
                        'file_type': "data",
                        'file_size': fsize,
                        'file_format': "binaryraw-compressed",
                        'runs': [ [run,  subrun, run_type] ],
                        'first_event': sevt,
                        'start_time': stime,
                        'end_time': etime,
                        'last_event':eevt,
                        'group': 'uboone',
                        "crc": { "crc_value":"123456",  "crc_type":"adler 32 crc type" },
                        "application": {  "family": "online",  "name": "assembler", "version": ver },
                        "data_tier": "raw", "event_count": num_events,
                        "ub_project.name": "online",
                        "ub_project.stage": "assembler",
                        "ub_project.version": "v6_0_0",
                        'online.start_time_usec': str(gps_stime_usec),
                        'online.end_time_usec': str(gps_etime_usec)}
            status_v[index_run] = (999,jsonData)

        for index_run in xrange(len(status_v)):
        
            status,data = status_v[index_run]
            fout = open('%s.json' % in_file_v[index_run], 'w')
            json.dump(data, fout, sort_keys = True, indent = 4, ensure_ascii=False)
            fout.close()

        return status_v


if __name__ == '__main__':

    input_files = glob.glob(sys.argv[1])
    print(input_files)

    test_obj = get_metadata()

    status_v = test_obj.process_ubdaq_files(input_files)
