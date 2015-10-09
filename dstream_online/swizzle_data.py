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
from ds_online_util import *
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
        self._parent_status = kSTATUS_SWIZZLE_DATA
        self._max_proc_time = 600
        self._parallelize = 1
        self._min_run = 0
        self._ntrials = 0

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._min_run = int(resource['MIN_RUN'])
        self._fcl_file = '%s' % (resource['FCLFILE'])
        self._fcl_file = os.environ["PUB_TOP_DIR"] + "/dstream_online/" + self._fcl_file
        self._fcl_file_new  = self._fcl_file.replace(".fcl","_local.fcl")

        self._out_dir = '%s' % (resource['OUTDIR'])
        #self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._log_file =  self._out_dir + "/lar_out_"
        self._cpu_frac_limit = resource['USED_CPU_FRAC_LIMIT']
        self._available_memory = resource['AVAIL_MEMORY']
        self._disk_frac_limit = resource['USED_DISK_FRAC_LIMIT']

        self._parallelize = int(resource['LARALLELIZE'])
        self._max_proc_time = int(resource['MAX_PROC_TIME'])

        self._parent_project = resource['PARENT_PROJECT']
        exec('self._parent_status = int(%s)' % resource[PARENT_STATUS])
        status_name(self._parent_status)

        self._ntrials = int(resource['NUM_RETRIAL'])

        if ( 'NSKIP' in resource and
             'SKIP_REF_PROJECT' in resource and
             'SKIP_REF_STATUS' in resource and
             'SKIP_STATUS' in resource ):
            self._nskip = int(resource['NSKIP'])
            self._skip_ref_project = resource['SKIP_REF_PROJECT']
            exec('self._skip_ref_status=int(%s)' % resource['SKIP_REF_STATUS'])
            exec('self._skip_status=int(%s)' % resource['SKIP_STATUS'])
            status_name(self._skip_ref_status)
            status_name(self._skip_status)

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
        if self._nskip and self._skip_ref_project:
            ctr = self._nskip
            for x in self.get_xtable_runs([self._project,self._skip_ref_project],
                                          [kSTATUS_INIT,self._skip_ref_status]):
                if ctr<=0: break
                self.log_status( ds_status( project = self._project,
                                            run     = int(x[0]),
                                            subrun  = int(x[1]),
                                            seq     = 0,
                                            status  = self._skip_status) )
                ctr -= 1

        # Check available space
        if ":" in self._in_dir:
            disk_frac_used=int(os.popen('ssh -x %s "df %s" | tail -n1'%tuple(self._in_dir.split(":"))).read().split()[4].strip("%"))
        else:
            disk_frac_used=int(os.popen('df %s | tail -n1'%(self._in_dir)).read().split()[4].strip("%"))
                        
        if (disk_frac_used > self._disk_frac_limit):
            self.info('%i%% of disk space used (%s), will not swizzle until %i%% is reached.'%(disk_frac_used, self._in_dir, self._disk_frac_limit))
            return

        # Check available cpu
        cpu_used = float(os.popen("top -bn 2 -d 0.01 | grep '^Cpu.s.' | tail -n 1 | gawk '{print $2+$4+$6}'").read().strip("\n"))
        if (cpu_used > self._cpu_frac_limit):
            self.info('%i of cpu used; will not swizzle until %i is reached.'%(cpu_used, self._cpu_frac_limit))
            return

        # Check available memory
        mem_avail = float(os.popen("free -m | grep buffers | tail -n 1|  gawk '{print $4}'").read().strip("\n"))
        if (mem_avail < int(self._available_memory)):
            self.info('%d Memory available, will not swizzle until %d is reached.'%(mem_avail, int(self._available_memory)))
            return

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        runid_v = []
        infile_v = []
        logfile_v = []
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [kSTATUS_INIT,self._parent_status]):
            # Counter decreases by 1
            ctr -= 1

            if ctr < 0: break

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: break

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            
            # Check input file exists. Otherwise report error
            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND ) )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE ) )

            infile_v.append(in_file)
            runid_v.append((run,subrun)
            logfile_v.append( self._log_file +  str(run) + "_" + str(subrun) + ".txt" )

        mp = self.process_files(infile_v)
        self.info('Finished all @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

        for i in xrange(len(infile_v)):

            run,subrun = runid_v[i]
            fout = open(logfile_v[i],'w')
            out,err = mp.communicate(i)

            fout.write(out)
            fout.write('\n')
            fout.write(err)
            fout.close()

            self.log_status( ds_status( project = self._project,
                                        run = run,
                                        subrun = subrun,
                                        seq = 0,
                                        status = kSTATUS_TO_BE_VALIDATED ) )

    def process_files(self,in_filelist_v):

        mp = ds_multiprocess(self._project)
        for i in xrange(len(in_filelist_v)):

            in_file = in_filelist_v[i]
            in_file_base_no_ext = os.path.splitext(os.path.basename(in_file))[0]
            out_file_base = '%s.root' % in_file_base_no_ext
            out_file = '%s/%s' % (self._out_dir,out_file_base)

            cmd  = "lar -c " + self._fcl_file_new
            cmd += " -s " +in_file
            cmd += " -o " + out_file
            cmd += " -T " + self._out_dir +"/" + os.path.basename(out_file).strip(".root") + "_hist.root "

            self.info('Swizzling %s @ %s' % (in_file,time.strftime('%Y-%m-%d %H:%M:%S')))

            index,active_ctr = mp.execute(cmd)

            if not self._parallelize:
                mp.communicate(index)
            else:
                time_slept = 0
                while active_ctr > self._parallelize:
                    time.sleep(0.2)
                    time_slept += 0.2
                    active_ctr = mp.active_count()

                    if time_slept > self._max_proc_time:
                        self.error('Exceeding time limit %s ... killing %d jobs...' % (self._max_proc_time,active_ctr))
                        mp.kill()
                        break
                    if int(time_slept) and (int(time_slept*10)%50) == 0:
                        self.info('Waiting for %d/%d process to finish...' % (active_ctr,len(in_filelist_v)))
        time_slept=0
        while mp.active_count():
            time.sleep(0.2)
            time_slept += 0.2
            if time_slept > self._max_proc_time:
                mp.kill()
                break
        return mp

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
        for x in self.get_runs(self._project,kSTATUS_TO_BE_VALIDATED):

            # Counter decreases by 1
            ctr -=1
            if ctr < 0: break

            (run, subrun) = (int(x[0]), int(x[1]))

            if run < self._min_run: break
                           
            # Report starting
            self.info('validating run: run=%d, subrun=%d ...' % (run,subrun))

            status = kSTATUS_INIT

            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND ) )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE ) )
                continue

            in_file = filelist[0]
            in_file_base_no_ext = os.path.splitext(os.path.basename(in_file))[0]
            out_file_larsoft = self._out_dir + '/' + '%s.root' % in_file_base_no_ext
            out_file_hist = out_file_larsoft.replace('.root','_hist.root')
            log_file_local = self._log_file + str(run) + "_" + str(subrun) + ".txt"
                           
            if os.path.exists(log_file_local):
                contents = open(self._log_file_local,'r').read()
                if contents.find('Art has completed and will exit with status 0') > 0:
                    self.info('Swizzling successfully completed for: run=%d, subrun=%d ...' % (run,subrun))
                    self.log_status( ds_status( project = self._project,
                                                run = run,
                                                subrun = subrun,
                                                seq = 0,
                                                status = kSTATUS_DONE ) )
                    continue
                else:
                    self.error('Swizzling failed for: run=%d, subrun=%d ...' % (run,subrun))
            else:
                self.error('Swizzling has no corresponding logfile for: run=%d, subrun=%d ...' % (run,subrun))

            # Swizzler failed... re-register for retrial
            os.system('rm -f %s' % out_file_larsoft)
            os.system('rm -f %s' % out_file_hist)
            os.system('rm -f %s' % log_file_local)
                           
            # Get status object
            proj_status = self._api.get_status( ds_status(self._project,run=run,subrun=subrun,seq=0) )
            # get data string for this project for this (run,subrun)
            ntrials = proj_status._data
            if not ntrials: ntrials = 1
            else:
                try:
                    ntrials = int(ntrials)
                    ntrials += 1
                except ValueError:
                    ntrials = 1

            status_code = kSTATUS_ERROR_CANNOT_SWIZZLE
            if ntrials > self._ntrials:
                self.error('More than %d trial made. Flagging as a corresponding status...')
            else:
                status_code = kSTATUS_INIT

            # Create a status object to be logged to DB (if necessary)
            self.log_status( ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = status_code,
                                        data    = str(ntrials) ) )

# A unit test section
if __name__ == '__main__':

    test_obj = swizzle_data(sys.argv[1])

    test_obj.process_newruns()

    test_obj.validate()

