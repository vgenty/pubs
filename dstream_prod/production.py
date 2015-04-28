## @namespace dstream_prod.production
#  @ingroup dstream_prod
#  @brief Defines a project production
#  @author yuntse

# python include
import time,os,sys,time
import subprocess
# pub_dbi package include
from pub_dbi import DBException
# pub_util package include
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

# Parse xml
import xml.etree.ElementTree as ET

## @class production
#  @brief A fake job submission process, only printing out the commands
#  @details
#  This production project prints out the job submission commands using project.py
class production(ds_project_base):

    PROD_STATUS = ( kDONE,
                    kINITIATED,
                    kTOBEVALIDATED,
                    kSUBMITTED,
                    kRUNNING,
                    kFINISHED,
                    kTOBERECOVERED ) = xrange(7)

    def checkNext( self, statusCode, istage ):
        nEvents = None

        if istage in self._stage_digits:
            statusCode = istage + self.kINITIATED
            self.info( "Next stage, statusCode: %d" % statusCode )

        return statusCode

    # def checkNext()

    def submit( self, statusCode, istage ):

        # Report starting
        # self.info()
        self._data = str( self._data )

        # If there is no numevents in self._data ("data" column in the table),
        # parse the xml file for the number of the submitted events
        if ( "numevents" not in self._data ):
           tree = ET.parse( self._xml_file )
           root = tree.getroot()
           nEvents = root.find('numevents').text
           self._data = "numevents:%s" % nEvents

        # Main command
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--submit' ]
        self.info( 'Submit jobs: xml: %s, stage: %s' %( self._xml_file, stage ) )
        # print "submit cmd: %s" % cmd

        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
            jobout, joberr = jobinfo.communicate()
        except:
            return ( statusCode + istage )

        # Check if te return code is 0
        if jobinfo.poll() != 0:
            self.error( jobinfo )
            return ( statusCode + istage )

        # Check job error
        findResponse = 0
        for line in joberr:
            if "JOBSUB SERVER RESPONSE CODE" in line:
                findResponse = 1
                if not "Success" in line:
                    self.error( jobinfo )
                    return ( statusCode + istage )

        if ( findResponse == 0 ):
            self.error( jobinfo )
            return ( statusCode + istage )

        jobid = ''
        # Grab the JobID
        for line in jobout:
            if "JobsubJobId" in line:
                jobid = line.strip().split()[-1]
                try:
                    AtSign = jobid.index('@')
                except ValueError:
                    self.error('Failed to extract the @ index!')
                    return ( statusCode + istage )
                jobid = jobid[:AtSign]

        # Tentatively do so; need to change!!!
        if not jobid:
            self.error( jobinfo )
            return 1000

        # Now grab the parent job id
        self._data += ":%s" % jobid.split('.')[0]

        statusCode = istage + self.kSUBMITTED
        self.info( "Submitted jobs, jobid: %s, status: %d" % ( self._data, statusCode ) )

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks
        return statusCode

    # def submit()

"""
    def isSubmitted( self, statusCode, istage ):

        self._data = str( self._data )
        jobid = self._data.strip().split(':')[-1]

        # Main command
        cmd = [ 'jobsub_q', '--jobid=%s' % jobid ]
        # print "isSubmitted cmd: %s" % cmd
        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
            jobout, joberr = jobinfo.communicate()
        except:
            return ( statusCode + istage )

        # Check if te return code is 0
        if jobinfo.poll() != 0:
            self.error( jobinfo )
            return ( statusCode + istage )

        # Check job error
        for line in joberr:
            if "JOBSUB SERVER RESPONSE CODE" in line:
                if not "Success" in line:
                    self.error( jobinfo )
                    return ( statusCode + istage )

        for line in jobout:
            if ( jobid in line ):
                if ( line.split()[1] == os.environ['USER'] ):
                    statusCode = self.kSUBMITTED

                if ( line.split()[5] == "R" ):
                    statusCode = self.kRUNNING
                    break


        statusCode += istage
        print "Validated job submission, jobid: %s, status: %d" % ( self._data, statusCode )

        # Pretend I'm doing something
        time.sleep(5)

        return statusCode
    # def isSubmitted()
"""

    def isRunning( self, statusCode, istage ):

        self._data = str( self._data )
        jobid = self._data.strip().split(':')[-1]

        # Main command
        cmd = [ 'jobsub_q', '--jobid=%s' % jobid ]
        # print "isRunning cmd: %s" % cmd
        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
            jobout, joberr = jobinfo.communicate()
        except:
            return ( statusCode + istage )

        # Check if te return code is 0
        if jobinfo.poll() != 0:
            self.error( jobinfo )
            return ( statusCode + istage )

        # Check job error
        findResponse = 0
        for line in joberr:
            if "JOBSUB SERVER RESPONSE CODE" in line:
                findResponse = 1
                if not "Success" in line:
                    self.error( jobinfo )
                    return ( statusCode + istage )

        if ( findResponse == 0 ):
            self.error( jobinfo )
            return ( statusCode + istage )

        nRunning = 0
        for line in jobout:
            if ( jobid in line ):
                if ( line.split()[1] == os.environ['USER'] ) 
                    nRunning += 1
                    statusCode = self.kSUBMITTED
                    if ( line.split()[5] == "R" ):
                        statusCode = self.kRUNNING
                        break

        if ( nRunning == 0 ):
            statusCode = self.kFINISHED

        statusCode += istage
        print "Checked if the job is running, jobid: %s, status: %d" % ( self._data, statusCode )
        # Pretend I'm doing something
        time.sleep(5)

        return statusCode

    # def isRunning()
"""
    def isFinished( self, statusCode, istage ):

        self._data = str( self._data )
        jobid = self._data.strip().split(':')[-1]

        # Main command
        cmd = [ 'jobsub_q', '--jobid=%s' % jobid ]
        # print "isFinished cmd: %s" % cmd
        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE ).stdout
            # jobinfo = open( "test/query3.txt", 'r' ) # Here is temporary, for test
        except:
            return ( statusCode + istage )

        nRunning = 0
        for line in jobinfo:
            if ( jobid in line ):
                if ( line.split()[1] == os.environ['USER'] ):
                    nRunning += 1

        if ( nRunning == 0 ):
            statusCode = self.kFINISHED

        statusCode += istage
        print "Checked if the job is still running, jobid: %s, status: %d" % ( self._data, statusCode )
        # Pretend I'm doing something
        time.sleep(5)

        return statusCode

    # def isFinished()
"""
    def check( self, statusCode, istage ):
        self._data = str( self._data )
        nEvents     = None
        nGoodEvents = None
        nSubmit     = None

        # Get the number of events
        if "numevents" in self._data:
            holder = self._data.split(':')
            try:
                inum = holder.index('numevents') + 1
            except ValueError:
                self.error('Failed to extract the numevents index!')
                return False
            nEvents = int(holder[inum])
            nSubmit = len(holder) - 2
        else:
            self.error('Failed to find numevents!')
            return False

        # Report starting
        # self.info()


        # Check the finished jobs
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--check' ]
        self.info( cmd )
        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
            jobout, joberr = jobinfo.communicate()
        except:
            return ( statusCode + istage )

        # Check if te return code is 0
        if jobinfo.poll() != 0:
            self.error( jobinfo )
            return ( statusCode + istage )

        # Grab the good jobs and bad jobs
        for line in jobout:
            if "good events" in line:
                nGoodEvents = int( line.split()[0] )

        print "nEvents: %d, nGoodEvents: %d" %( nEvents, nGoodEvents )

        # Compare the expected and the good events
        if nGoodEvents == nEvents:
           statusCode = self.kDONE
           istage += 10
           self._data = "numevents:%d" % nEvents

           # If all the stages complete, send an email to experts
           if not istage in self._stage_digits:
               subject = "Completed: MCC sample %s" % self._project
               text = """
Sample     : %s
Stage      : %s
Good events: %d
               """ % ( self._project, self._digit_to_name[istage-10], nGoodEvents )

               pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

        elif nSubmit > self._nresubmission:
           # If the sample has been submitted more than a certain number
           # of times, email the expert, and move on to the next stage
           subject = "MCC jobs fails after %d resubmissions" % nSubmit 
           text = """
Sample     : %s
Stage      : %s
Events     : %d
Good events: %d
Job IDs    : %s
           """ % ( self._project, self._digit_to_name[istage], nEvents, nGoodEvents, self._data.split(':')[2:] )

           pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

           statusCode = self.kDONE
           istage += 10
           self._data = "numevents:%d" % nGoodEvents
        else:
           statusCode = self.kTOBERECOVERED

        statusCode += istage
        print "Checked job, status: %d" % statusCode

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks

        return statusCode
    # def check()


    def recover( self, statusCode, istage ):

        # Report starting
        # self.info()
        self._data = str( self._data )

        # Main command
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--makeup' ]
        self.info( cmd )

        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
            jobout, joberr = jobinfo.communicate()
        except:
            return ( statusCode + istage )

        # Check if te return code is 0
        if jobinfo.poll() != 0:
            self.error( jobinfo )
            return ( statusCode + istage )

        # Check job error
        findResponse = 0
        for line in joberr:
            if "JOBSUB SERVER RESPONSE CODE" in line:
                findResponse = 1
                if not "Success" in line:
                    self.error( jobinfo )
                    return ( statusCode + istage )

        if ( findResponse == 0 ):
            self.error( jobinfo )
            return ( statusCode + istage )

        # Grab the JobID
        jobid = ''
        for line in jobout:
            if "JobsubJobId" in line:
                jobid = line.strip().split()[-1]
                try:
                    AtSign = jobid.index('@')
                except ValueError:
                    self.error('Failed to extract the @ index!')
                    return ( statusCode + istage )

                jobid = jobid[:AtSign]

        # Tentatively do so; need to change!!!
        if not jobid:
            self.error( jobinfo )
            return 1000

        # Now grab the parent job id
        self._data += ":%s" % jobid.split('.')[0]

        statusCode = istage + self.kSUBMITTED
        self.info( "Resubmitted jobs, job id: %s, status: %d" % ( self._data, statusCode ) )

        # Pretend I'm doing something
        time.sleep(5)

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
    _project = 'production'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super(production,self).__init__(arg)

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self.PROD_ACTION = { self.kDONE          : self.checkNext,
                             self.kINITIATED     : self.submit,
                             self.kTOBEVALIDATED : self.isRunning,
                             self.kSUBMITTED     : self.isRunning,
                             self.kRUNNING       : self.isRunning,
                             self.kFINISHED      : self.check,
                             self.kTOBERECOVERED : self.recover }

        self._nruns   = None
        self._xml_file = ''
        self._stage_name   = []
        self._stage_digits = []
        self._nresubmission = 3
        self._digit_to_name = {}
        self._name_to_digit = {}
        self._data = "None"

    def loadProjectParams( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        resource = self._api.get_resource(self._project)

        self._nruns = int(resource['NRUNS'])
        self._xml_file = resource['XMLFILE']
        self._nresubmission = int(resource['NRESUBMISSION'])
        self._experts = resource['EXPERTS']

        if self._nruns > 3:
            self._nruns = 3

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
        #return
        # Kazu's version of submit jobs
        for istage in self._stage_digits:
            # self.warning('Inspecting stage %s' % istage)
            for istatus in self.PROD_STATUS:
                fstatus = istage + istatus
                # self.warning('Inspecting status %s' % istatus)
                for x in self.get_runs( self._project, fstatus ):
                    # self.warning('Inspecting run/subrun: %s/%s' % (x[0],x[1]))
                    run    = int(x[0])
                    subrun = int(x[1])

                    statusCode = self.__decode_status__( fstatus )
                    action = self.PROD_ACTION[statusCode]

                    # Get status object
                    status = self._api.get_status(ds_status(self._project,
                                                            x[0],x[1],x[2]))
                    self._data = status._data
                    statusCode = action( statusCode, istage )

                    # Create a status object to be logged to DB (if necessary)
                    status = ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = statusCode,
                                        data    = self._data )

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

    test_obj = production(proj_name)
    
    now_str = time.strftime('%Y-%m-%d %H:%M:%S')
    test_obj.info("Project %s start @ %s" % (proj_name,now_str))
    
    test_obj.process()
    
    now_str = time.strftime('%Y-%m-%d %H:%M:%S')
    test_obj.info("Project %s end @ %s" % (proj_name,now_str))

    sys.exit(0)
