## @namespace dummy_dstream.dummy_nubin_xfer
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_nubin_xfer
#  @author echurch

# python include
import time, os, shutil, sys
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from ds_online_env import *
import datetime
import subprocess as sub
# script module tools
from scripts import find_run


## @Class swizzle_data
#  @brief The swizzler
#  @details
#  This project swizzles the data that has successfully been declared, validated, signed, sealed delivered to SAM.
#  Eventually we will in fact ask also that the beamdaq has been retrieved and merged, but that's only a config
#  change in this project's config chunk. We submit lar jobs to local node only, by-passsing condor altogether for now.
# bad status = 101 => we tried running this project on this (run,subrun) more than X times and all failed
# bad status = 404 => file path not found. Probably deleted before swizzler could do its job...
class swizzle_data(ds_project_base):


    # Define project name as class attribute
    #_project = 'swizzle_data'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self, project_name):

        self._project = project_name
        # Call base class ctor
        super(swizzle_data,self).__init__( project_name)

        # self.info('Running cleaning project %s'%self._project)
        if (self._project==''):
            self.error('Missing project name argument')
            return

        self._nruns = None
        self._out_dir = ''
        #self._outfile_format = ''
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._proc_lifetime = 600
        self._proc_list = []
        self._proc_active = []
        self._log_file_list = []
        self._run_list = []
        self._subrun_list = []
        self._sampling_scale = 0

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
#        self._nruns = 1
        self._fcl_file = '%s' % (resource['FCLFILE'])
        self._fcl_file = os.environ["PUB_TOP_DIR"] + "/dstream_online/" + self._fcl_file
        self._fcl_file_new  = self._fcl_file.replace(".fcl","_local.fcl")

        self._out_dir = '%s' % (resource['OUTDIR'])
        #self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._log_file =  self._out_dir + "/lar_out_"
        self._parent_project = resource['PARENT_PROJECT']
        self._cpu_frac_limit = resource['USED_CPU_FRAC_LIMIT']
        self._available_memory = resource['AVAIL_MEMORY']
        self._disk_frac_limit = resource['USED_DISK_FRAC_LIMIT']
        self._proc_lifetime = int(resource['LAR_LIFETIME'])
        if 'SAMPLING_SCALE' in resource:
            self._sampling_scale = int(resource['SAMPLING_SCALE'])
        # First update the place-holder version currently in the fcl file
        f = open (self._fcl_file,'r')
        n = open (self._fcl_file_new,'w')
        n.write(f.read().replace('vxx_yy_zz',os.environ["LARSOFT_VERSION"]))
        n.close()
        f.close()

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        #self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [1,0]):
            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # if sampling scale is set, skip some
            if self._sampling_scale and (subrun % self._sampling_scale):
                self.info('Sampling scale (%d) Skipping (run,subrun) = (%d,%d)' % (self._sampling_scale,run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_POSTPONE) )
                if not ctr: break
                continue
            
            self._log_file_local = self._log_file +  str(run) + "_" + str(subrun) + ".txt"
            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            
            # Check input file exists. Otherwise report error
            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('List of files found %s' % filelist)

            in_file = filelist[0]
            in_file_base_no_ext = os.path.splitext(os.path.basename(in_file))[0]
            out_file_base = '%s.root' % in_file_base_no_ext
            out_file = '%s/%s' % (self._out_dir,out_file_base)

#
#
#            print "Looking for ", in_file
# This is a hackity, hacky hack. But for now, run 1409 is the first run taken with the cable swap performed on Thurs Aug 13, 2015.
# I'm putting this hack into place so that we don't swizzle the data before that with uboonecode v04_19_00 since there is not
# yet an interval of validity for the database and channel mapping. Basically, the swizzler_data.py will ignore anything before
#run 1409 and just not process it.
            if not run>1408: continue

            if not os.path.isfile(in_file):
                self.error('Could not find file %s. Assigning status 404' % in_file)
                status = 404

            else:

                self.info('Found %s' % (in_file))

                try:
# setup the LArSoft envt

                    # print "Putting together cmd "
                    # print "\t fcl_file ", self._fcl_file_new
                    # print "\t in_file", in_file
                    # print "\t out_file", out_file
                    # print "\t basename out_file", os.path.basename(out_file)
                    # print "\t _log_file", self._log_file_local

                    cmd = "lar -c "+ self._fcl_file_new + " -s " +in_file + " -o " + out_file + " -T " + self._out_dir +"/" + os.path.basename(out_file).strip(".root") + "_hist.root "
                    # print "cmd is ", cmd
                    self.info('Launch cmd is ' + cmd)

                except:
                    self.error(sys.exc_info()[0])
                    # print "Give some null properties to this meta data"
                    self.error("Give this file a status 100")
                    status = 100
                    

                if not status==100:
# form the lar command

                    # Check available space
                    if ":" in self._in_dir:
                        disk_frac_used=int(os.popen('ssh -x %s "df %s" | tail -n1'%tuple(self._in_dir.split(":"))).read().split()[4].strip("%"))
                    else:
                        disk_frac_used=int(os.popen('df %s | tail -n1'%(self._in_dir)).read().split()[4].strip("%"))
                        
                    if (disk_frac_used > self._disk_frac_limit):
                        self.info('%i%% of disk space used (%s), will not swizzle until %i%% is reached.'%(disk_frac_used, self._in_dir, self._disk_frac_limit))
                        status = 1
                        raise Exception( " raising Exception: not enough disk space." )

                    # Check available cpu
                    cpu_used = float(os.popen("top -bn 2 -d 0.01 | grep '^Cpu.s.' | tail -n 1 | gawk '{print $2+$4+$6}'").read().strip("\n"))
                    if (cpu_used > self._cpu_frac_limit):
                        self.info('%i of cpu used; will not swizzle until %i is reached.'%(cpu_used, self._cpu_frac_limit))
                        status = 1
                        raise Exception( " raising Exception: not enough cpu." )

                    # Check available memory
                    mem_avail = float(os.popen("free -m | grep buffers | tail -n 1|  gawk '{print $4}'").read().strip("\n"))
                    if (mem_avail < int(self._available_memory)):
                        self.info('%d Memory available, will not swizzle until %d is reached.'%(mem_avail, int(self._available_memory)))
                        status = 1
                        raise Exception( " raising Exception: not enough memory available." )


                    self._proc_list.append(sub.Popen(cmd,shell=True,stderr=sub.PIPE,stdout=sub.PIPE))
                    self._log_file_list.append(self._log_file_local)
                    self._run_list.append(x[0])
                    self._subrun_list.append(x[1])
                    self.info( ' Swizzling (run,subrun,processID) = (%d,%d,%d)...' % (run,subrun,self._proc_list[-1].pid))
                    self._proc_active.append(True)
                    status = 3
                    time.sleep (1)

            # Create a status object to be logged to DB (if necessary)
            self.info('logging (run,subrun) = (%i,%i) with status %i'%(int(x[0]),int(x[1]),status))
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from run/subrun loop if counter became 0
            if not ctr: break

#############################################################################################
# NOTE that the below "poll" solution deadlocks with piping. Yet, I can't read to break the deadlock till
# done, so let's for now just block each process till done. I need 'em all anyway before I can proceed.
# The real time cost here seems to be in the reading (out,err) into local files. And then grep'ing 
# for the success mesage -- "Art has completed ...".
#############################################################################################


# Now continually loop over all the running processes and ask for them each to be finished before we break out
#        while (1):
#            proc_alive=False
        time_spent = 0
        while 1:
            active_counter = 0
            time.sleep(5)
            time_spent += 5
            for x in xrange(len(self._proc_list)):

                proc = self._proc_list[x]
                if not self._proc_active[x]:
                    continue

                if not proc.poll() is None: 
                    self._proc_active[x] = False
                    self.info('The return code was %d ' % proc.returncode )
                    self.info('Finished swizzler process %s' % proc.pid)
                    (out,err) = proc.communicate()
                    fout = open(str(self._log_file_list[x]),'w')
                    fout.write(out)
                    fout.close()

                    if proc.returncode != 0:
                        status = proc.returncode
                    else:
                        status = 2

                    status = ds_status( project = self._project,
                                        run     = int(self._run_list[x]),
                                        subrun  = int(self._subrun_list[x]),
                                        seq     = 0,
                                        status  = status )
                    # Log status
                    self.log_status( status )

                else:
                    active_counter += 1
            if not active_counter:
                break
            if time_spent%20 == 0:
                self.info('Swizzling process %d/%d active... @ %d [sec]' % (active_counter,len(self._proc_list),time_spent))
            else:
                self.debug('Swizzling process %d/%d active... @ %d [sec]' % (active_counter,len(self._proc_list),time_spent))

            if time_spent > self._proc_lifetime:
                self.error('Exceeding the allowed time span (%d [sec])! Killing lar jobs...',self._proc_lifetime)
                # Loop over & kill
                for x in xrange(len(self._proc_list)):
                    proc = self._proc_list[x]
                    # ignore already finished ones
                    if not self._proc_active[x]:
                        continue
                    # kill
                    proc.kill()
                    # Log "finished" status
                    status = 2
                    status = ds_status( project = self._project,
                                        run     = int(self._run_list[x]),
                                        subrun  = int(self._subrun_list[x]),
                                        seq     = 0,
                                        status  = status )
                    self.log_status( status )

                # Wait 30 sec and make sure they are dead
                time.sleep(30)
                for x in xrange(len(self._proc_list)):
                    proc = self._proc_list[x]
                    if not self._proc_active[x]:
                        continue
                    if proc.poll() is None:
                        self.warning('Process %d not ending 30 sec after SIGINT... kill -9 now...' % proc.pid)
                        sub.call(['kill','-9',str(proc.pid)])
                    


    ## @brief access DB and retrieves processed run for validation
    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))
            self._log_file_local = self._log_file + str(run) + "_" + str(subrun) + ".txt"
            # Report starting
            self.info('validating run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1

            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('ERROR: Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('ERROR: Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('ERROR: List of files found %s' % filelist)

            in_file = filelist[0]
            in_file_base_no_ext = os.path.splitext(os.path.basename(in_file))[0]
            out_file_base = '%s.root' % in_file_base_no_ext
            out_file = '%s/%s' % (self._out_dir,out_file_base)

            # Get status object
            proj_status = self._api.get_status(ds_status(self._project,
                                                         x[0],x[1],x[2]))
            # get data string for this project for this (run,subrun)
            datastr = proj_status._data
            # variable to hold the number of attempts to run this project on this (run,subrun)
            trial = 0
            if (datastr != ''):
                try:
                    trial = int(datastr)
                    self.info('Trial number is %i'%trial)
                except:
                    self.info('data field in status was neither string nor integer...')
                    
            if os.path.exists(self._log_file_local):
                contents = open(self._log_file_local,'r').read()
                if contents.find('Art has completed and will exit with status 0') > 0:
                    self.info('Swizzling successfully completed for: run=%d, subrun=%d ...' % (run,subrun))
                    status = 0
            else:
                    self.info('Swizzling has no corresponding logfile for: run=%d, subrun=%d ...' % (run,subrun))
                    trial += 1

            # if we tried this (run,subrun) too many times
            # change status to bad status = 
            if (trial > 3):
                self.info('more than 3 trials...changing status to 101')
                status = 101

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = int(x[2]),
                                status  = status,
                                data    = str(trial) )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

    ## @brief access DB and retrieves runs for which 1st process failed. Clean up.
    def error_handle(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project,100):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('cleaning failed run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1

            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('List of files found %s' % filelist)

            in_file = filelist[0]
            in_file_base_no_ext = os.path.splitext(os.path.basename(in_file))[0]
            out_file_base = '%s.root' % in_file_base_no_ext
            out_file = '%s/%s' % (self._out_dir,out_file_base)

            if os.path.isfile(out_file):
                os.system('rm %s' % out_file)

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break
            
        try:
            os.remove(self._fcl_file_new)
        except OSError:
            pass


# A unit test section
if __name__ == '__main__':

    test_obj = swizzle_data(sys.argv[1])

    test_obj.process_newruns()

    test_obj.error_handle()

    test_obj.validate()

