## @namespace dstream.CHECK_NOISE
#  @ingroup dstream
#  @brief Defines a project check_noise
#  @author david caratelli

# python include
import time
import subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class check_noise
#  @brief check noise on newly created binary file
#  @details
#  check noise values on newly created binary file
class check_noise(ds_project_base):

    # Define project name as class attribute
    _project = 'check_noise'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self,nruns=None):

        # Call base class ctor
        super(check_noise,self).__init__()

        self._nruns = None
        self._in_dir = ''   # where to find the data! data! data! me wants data!
        self._out_dir = ''  # where should output plots/files be placed?
        self._infile_format = ''  # what is the format of the file-name?
        self._experts  = ''  
        self._lowN     = ''  # what counts as low noise? any value below this value is reported
        self._highN    = ''  # what counts as high noise? any value above this value is reported
        self._bashPath = ''  # path to bash script that performs noise check

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        self._nruns  = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']
        self._experts = resource['EXPERTS']
        self._lowN = resource['LOWNOISE']
        self._highN = resource['HIGHNOISE']
        self._bashPath = resource['BASHPATH']
        

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # attempt to setup the DAQ so that we can run the noise-check
        try:
            subprocess.check_call('source /uboonenew/setup')
            subprocess.check_call('setup uboonedaq v6_10_05 -q debug:e7')
        except CalledProcessError:
	    self.error('Cannot setup the DAQ...')
            return

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs( [self._project, self._parent_project], [1, 0] ):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('preparing noisy channel list: run=%d, subrun=%d ...' % (run,subrun))

            # set status code
            statusCode = 1

            # create text file containing path to binary files to be analyzed
            binfilepath = self._in_dir
            # get the file that matches the infile format and has
            # the correct run/subrun numbers
            binfilepath += '/'+self._infile_format % (run, subrun)
            
            # check that this file exists
            if (os.path.isfile(binfilepath) == False):
                statusCode = 666
                self.info('data file not found. Assigning error message %i'%statusCode)
            
            else:

                try:
                    # execute noise_check script
                    # prepare a text file that contains the path to this file
                    subprocess.check_call('ls -r %s > %s/file_list_run_%i_subrun_%i.txt'%binfilepath,self._out_dir,run,subrun)
                    # use subprocess.check_call
                    subprocess.check_call('source /uboonenew/setup')
                    subprocess.check_call('setup uboonedaq v6_10_05 -q debug:e7')
                    noise_check_input = '%s/file_list_run_%i_subrun_%i.txt'%(self._out_dir,run,subrun)
                    noise_check_output = 'noise_check_output_run_%i_subrun_%i'%(run,subrun)
                    subprocess.check_call('noise_check %s %s'% (noise_check_input) )

                except CalledProcessError:
                    statuscode = 666
                    
            # Report finishing
            self.info('done preparing noisy channel list: run=%d, subrun=%d ...' % (run,subrun) )

            # Create a status object to be logged to DB (if necessary)
            # Let's say we set the status to be 10
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = int(x[2]),
                                status  = statusCode )
            
            # Log status
            #self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    test_obj = check_noise(5)

    test_obj.process_newruns()



