## @namespace dummy_dstream.dummy_daq
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_daq
#  @author echurch

# python include
import time,os,glob
import subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class dummy_daq
#  @brief A fake DAQ process that makes a fake nu bin file.
#  @details
#  This assembler_daq project grabs nu bin data file under $PUB_TOP_DIR/data directory
class mv_assembler_daq_files(ds_project_base):

    # Define project name as class attribute
    _project = 'mv_binary_evb'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(mv_assembler_daq_files,self).__init__()

        self._out_file_format = ''
        self._out_dir = ''
        self._nruns   = None

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns = int(resource['NRUNS'])
            self._out_dir = '%s' % (resource['OUTDIR'])
            self._in_dir = '%s' % (resource['INDIR'])
            self._infile_format = resource['INFILE_FORMAT']
            self._outfile_format = resource['OUTFILE_FORMAT']

        ctr = self._nruns
        for x in self.get_runs(self._project,1):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            in_file_holder = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (run,subrun) )
            filelist = glob.glob( in_file_holder )
            in_file = filelist[0]
            cmd = ['rsync', '-v', in_file, 'ubdaq-prod-near1:%s' % out_file]
            subprocess.call(cmd)
            # os.symlink(in_file, ('%s/%s' % (self._out_dir, self._outfile_format % (run,subrun))))
# In the end, use the line below rather than the one above.
#            os.symlink(glob.glob(in_file)[0],('%s/%s' % (self._out_dir, self._outfile_format % (run,subrun))))


            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = 2 )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break


    ## @brief access DB and validate finished runs
    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns = int(resource['NRUNS'])
            self._out_dir = '%s/%s' % (os.environ['PUB_TOP_DIR'],resource['OUTDIR'])
            self._outfile_format = resource['OUTFILE_FORMAT']

        ctr = self._nruns
# see if the status=2 files we've processed are indeed where they should be.
        for x in self.get_runs(self._project,2): 

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])
            status = 0

            out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (run,subrun) )
            res = subprocess.call(['ssh', 'ubdaq-prod-near1', '-x', 'ls', out_file])
            if res:
                self.error('error on run: run=%d, subrun=%d ...' % (run,subrun))
                status = 1
            else:
                self.info('validated run: run=%d, subrun=%d ...' % (run,subrun))
                status = 0
                

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = status )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = mv_assembler_daq_files()

    test_obj.process_newruns()

    test_obj.validate()



