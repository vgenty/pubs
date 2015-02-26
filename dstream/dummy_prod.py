## @namespace dstream.dummy_prod
#  @ingroup dstream
#  @brief Defines a project dummy_prod
#  @author yuntse

# The status codes:
# 1: initialized
# 2: to be validated
# 3: job submitted

# python include
import time,os,sys
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

# Parse xml
import xml.etree.ElementTree as ET

## @class dummy_prod
#  @brief A fake job submission process, only printing out the commands
#  @details
#  This dummy project prints out the job submission commands using project.py
class dummy_prod(ds_project_base):

    PROD_STATUS = ( kDONE,
                    kINITIATED,
                    kTOBEVALIDATED,
                    kSUBMITTED,
                    kRUNNING,
                    kFINISHED,
                    kTOBERECOVERED ) = xrange(7)

    def checkNext( self, statusCode, istage ):

        if istage in self._stage_digits:
            statusCode = istage + self.kINITIATED
            print "Next stage, statusCode: %d" % statusCode

        return statusCode

    # def checkNext()

    def submit( self, statusCode, istage ):

        # Report starting
        # self.info()

        # Main command
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--submit' ]
        self.info( 'Submit jobs: xml: %s, stage: %s' %( self._xml_file, stage ) )
        print "submit cmd: %s" % cmd
        # jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
        jobinfo = open( "test/submit.txt", 'r' ) # Here is temporary, for test

        # Grab the JobID
        for line in jobinfo:
            if "JobsubJobId" in line:
                jobno = line.strip().split()[-1]
                try:
                    AtSign = jobno.index('@')
                except ValueError: continue
                self._jobid = jobno[:AtSign]

        # Now grab the parent job id
        self._jobid = self._jobid.split('.')[0]

        statusCode = istage + self.kTOBEVALIDATED
        print "Submitted jobs, jobid: %s, status: %d" % ( self._jobid, statusCode )

        # Pretend I'm doing something
        time.sleep(1)

        # Here we may need some checks
        return statusCode

    # def submit()

    def isSubmitted( self, statusCode, istage ):

        # Main command
        cmd = [ 'jobsub_q', '--jobid=%s' % self._jobid ]
        # jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
        jobinfo = open( "test/query.txt", 'r' ) # Here is temporary, for test

        for line in jobinfo:
            if ( self._jobid in line ) and ( line.split()[1] == os.environ['USER'] ):
                statusCode = self.kSUBMITTED

        statusCode += istage
        print "Validated job submission, jobid: %s, status: %d" % ( self._jobid, statusCode )

        # Pretend I'm doing something
        time.sleep(1)

        return statusCode
    # def isSubmitted()


    def isRunning( self, statusCode, istage ):

        # Main command
        cmd = [ 'jobsub_q', '--jobid=%s' % self._jobid ]
        # jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
        jobinfo = open( "test/query.txt", 'r' ) # Here is temporary, for test

        for line in jobinfo:
            if ( self._jobid in line ) and ( line.split()[1] == os.environ['USER'] ):
                if ( line.split()[5] == "R" ):
                    statusCode = self.kRUNNING
                    break

        statusCode += istage
        print "Checked if the job is running, jobid: %s, status: %d" % ( self._jobid, statusCode )
        # Pretend I'm doing something
        time.sleep(1)

        return statusCode

    # def isRunning()

    def isFinished( self, statusCode, istage ):

        # Main command
        cmd = [ 'jobsub_q', '--jobid=%s' % self._jobid ]
        # jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
        jobinfo = open( "test/query3.txt", 'r' ) # Here is temporary, for test

        for line in jobinfo:
            if not ( ( self._jobid in line ) and ( line.split()[1] == os.environ['USER'] ) ):
                statusCode = self.kFINISHED

        statusCode += istage
        print "Checked if the job is still running, jobid: %s, status: %d" % ( self._jobid, statusCode )
        # Pretend I'm doing something
        time.sleep(1)

        return statusCode

    # def isFinished()

    def check( self, statusCode, istage ):
        nEvents     = None
        nGoodEvents = None

        # Report starting
        # self.info()

        # Parse the xml file for the number of the submitted events
        tree = ET.parse( self._xml_file )
        root = tree.getroot()
        nEvents = int( root.find('numevents').text )

        # Check the finished jobs
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--check' ]
        self.info( cmd )
        # jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
        jobinfo = open( "test/check.txt", 'r' ) # Here is temporary, for test

        # Grab the good jobs and bad jobs
        for line in jobinfo:
            if "good events" in line:
                nGoodEvents = int( line.split()[0] )

        print "nEvents: %d, nGoodEvents: %d" %( nEvents, nGoodEvents )

        # Compare the expected and the good events
        if nGoodEvents == nEvents:
           statusCode = self.kDONE
           istage += 10
        else:
           statusCode = self.kTOBERECOVERED

        statusCode += istage
        print "Checked job, status: %d" % statusCode

        # Pretend I'm doing something
        time.sleep(1)

        # Here we may need some checks

        return statusCode
    # def check()


    def recover( self, statusCode, istage ):

        # Report starting
        # self.info()

        # Main command
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--makeup' ]
        self.info( cmd )
        # jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
        jobinfo = open( "test/makeup.txt", 'r' ) # Here is temporary, for test

        # Grab the JobID
        for line in jobinfo:
            if "JobsubJobId" in line:
                jobno = line.strip().split()[-1]
                try:
                    AtSign = jobno.index('@')
                except ValueError: continue
                self._jobid = jobno[:AtSign]

        # Now grab the parent job id
        self._jobid = self._jobid.split('.')[0]

        statusCode = istage + self.kTOBEVALIDATED
        print "Resubmitted jobs, job id: %s, status: %d" % ( self._jobid, statusCode )

        # Pretend I'm doing something
        time.sleep(1)

        # Here we may need some checks
        return statusCode

    # def recover()
        
    def __decode_status__(self,arg):
        try:
            arg = int(arg)
        except ValueError:
            raise
        arg = arg%10
        if not arg in self.__class__.PROD_STATUS:
            self.error( 'HEY this is not a valid status code!: %d' % arg )
        return arg
    # def __decode_status__()

    # Define project name as class attribute
    _project = 'dummy_prod'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        # Call base class ctor
        super(dummy_prod,self).__init__()

        self.PROD_ACTION = { self.kDONE          : self.checkNext,
                             self.kINITIATED     : self.submit,
                             self.kTOBEVALIDATED : self.isSubmitted,
                             self.kSUBMITTED     : self.isRunning,
                             self.kRUNNING       : self.isFinished,
                             self.kFINISHED      : self.check,
                             self.kTOBERECOVERED : self.recover }

        self._nruns   = None
        self._xml_file = ''
        self._stage_name   = []
        self._stage_digits = []
        self._digit_to_name={}
        self._name_to_digit={}
        self._jobid = "None"

    def loadProjectParams( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._xml_file = resource['XMLFILE']

        try:
            self._stage_names  = resource['STAGE_NAME'].split(':')
            self._stage_digits = [int(x) for x in resource['STAGE_STATUS'].split(':')]
            if not len(self._stage_names) == len(self._stage_digits):
                raise Exception
            for x in xrange(len(self._stage_names)):
                name  = self._stage_names[x]
                digit = self._stage_digits[x]
                self._digit_to_name[digit]=name
                self._name_to_digit[name]=digit

        except Exception:
            self.error('Failed to load project parameters...')
            return False
        return True

    ## @brief access DB and retrieves new runs
    def process( self ):

        self.loadProjectParams()

        ctr = self._nruns
        
        # Kazu's version of submit jobs
        for istage in self._stage_digits:
            for istatus in self.PROD_STATUS:
                fstatus = istage + istatus

                for x in self.get_runs( self._project, fstatus ):
                    run    = int(x[0])
                    subrun = int(x[1])

                    statusCode = self.__decode_status__( fstatus )
                    action = self.PROD_ACTION[statusCode]

                    # Get status object
                    status = self._api.get_status(ds_status(self._project,
                                                            x[0],x[1],x[2]))
                    self._jobid = status._data
                    statusCode = action( statusCode, istage )

                    # Create a status object to be logged to DB (if necessary)
                    status = ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = statusCode,
                                        data    = self._jobid )

                    # Log status
                    self.log_status( status )

                    # Counter decreases by 1
                    ctr -=1
                    # Break from loop if counter became 0
                    if ctr < 0: return
        return


# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    test_obj = dummy_prod(proj_name)

    test_obj.process()

