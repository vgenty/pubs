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
import datetime
import subprocess as sub



## @Class swizzle_data
#  @brief The swizzler
#  @details
#  This project swizzles the data that has successfully been declared, validated, signed, sealed delivered to SAM.
#  Eventually we will in fact ask also that the beamdaq has been retrieved and merged, but that's only a config
#  change in this project's config chunk. We submit lar jobs to local node only, by-passsing condor altogether for now.
class swizzle_data(ds_project_base):


    # Define project name as class attribute
    _project = 'swizzle_data'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(swizzle_data,self).__init__()

        self._nruns = None
        self._out_dir = ''
        self._outfile_format = ''
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._fcl_file = '%s' % (resource['FCLFILE'])
        self._log_file = "lar_out_"
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']
        self._cpu_frac_limit = resource['USED_CPU_FRAC_LIMIT']
        self._available_memory = resource['AVAIL_MEMORY']
        self._disk_frac_limit = resource['USED_DISK_FRAC_LIMIT']

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [1,0]):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))
            self._log_file += str(run) + "_" + str(subrun) + ".txt"
            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            
            # Check input file exists. Otherwise report error
            in_file = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

#
#
            if os.path.isfile(in_file):
                self.info('Found %s' % (in_file))

                try:
# setup the LArSoft envt
                    print "Putting together cmd "
                    print "\t fcl_file ", self._fcl_file
                    print "\t in_file", in_file
                    print "\t out_file", out_file
                    print "\t basename out_file", os.path.basename(out_file)
                    print "\t _log_file", self._log_file

                    cmd = "lar -c "+ self._fcl_file + " -s " +in_file + " -o " + out_file + " -T " + os.path.basename(out_file).strip(".root") + "_hist.root >& " + self._log_file 
                    print "cmd is ", cmd

                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    # print "Give some null properties to this meta data"
                    print "Give this file a status 100"
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


                    self._proc = sub.Popen(cmd.split(),stderr=sub.STDOUT,stdout=sub.PIPE)
                    self.info( ' Swizzling (run,subrun) = (%d,%d)...' % (run,subrun))
# This will block! #        (logfile,err) = self._proc.communicate()
                            
                    time.sleep (2)
                    # Set status to 2 only if all is well so far.
                    if not os.path.isfile(self._log_file):
                        status = 1
                        raise Exception( " raising Exception: no log file." )
                    
                    status = 2

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

            # Report starting
            self.info('validating run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            in_file = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

            self._proc.poll()
            if not  self._proc.returncode:
# Check the status of the self._proc. If it's not running self._proc.returncode evaluates to None
# Then check the _logfile for signs of success. If all's well set the status here to 0.

                if open(self._log_file).read().find('Art has completed and will exit with status 0') > 0:
                    self.info('Swizzling successfully completed for: run=%d, subrun=%d ...' % (run,subrun))
                    status = 0
                else:
                    self.info('Swizzling completed with error for: run=%d, subrun=%d ...' % (run,subrun))

            else:
                self.info('Swizzling job %d still running for: run=%d, subrun=%d ...' % (self._proc,run,subrun))
                status = 2


            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = int(x[2]),
                                status  = status )
            
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

            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

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

# A unit test section
if __name__ == '__main__':

    test_obj = swizzle_data()

    test_obj.process_newruns()

    test_obj.error_handle()

    test_obj.validate()

