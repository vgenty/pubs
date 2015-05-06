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

    JOBSUB_LOG = '%s/joblist.txt' % os.environ['PUB_LOGGER_FILE_LOCATION']

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

        if not self.loadProjectParams():
            self.info('Failed to load project @ %s' % self.now_str())

    def loadProjectParams( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return False

        proj_info = self._api.project_info(self._project)

        self._nruns = int(proj_info._resource['NRUNS'])
        self._xml_file = proj_info._resource['XMLFILE']
        self._nresubmission = int(proj_info._resource['NRESUBMISSION'])
        self._experts = proj_info._resource['EXPERTS']
        self._period = proj_info._period

        try:
            self._stage_names  = proj_info._resource['STAGE_NAME'].split(':')
            self._stage_digits = [int(x) for x in proj_info._resource['STAGE_STATUS'].split(':')]
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

        self.info('Project loaded @ %s' % self.now_str())
        return True

    def now_str(self):
        return time.strftime('%Y-%m-%d %H:%M:%S')

    def checkNext( self, statusCode, istage ):
        nEvents = None

        if istage in self._stage_digits:
            statusCode = istage + self.kINITIATED
            self.info( "Next stage, statusCode: %d" % statusCode )

        return statusCode

    def _jobstat_from_log(self):
        result = (False,'')
        if not os.path.isfile(self.JOBSUB_LOG):
            subject = 'Failed fetching job log'
            text  = 'Batch job log file not available... (check daemon, should not happen)'
            text += '\n\n'
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return result

        log_age = time.time() - os.path.getmtime(self.JOBSUB_LOG)
        if log_age + 10 > self._period:
            return result

        contents = open(self.JOBSUB_LOG,'r').read()
        return (self._check_jobstat_str(contents),contents)

    def _jobstat_from_cmd(self,jobid=None):
        cmd = ['jobsub_q', '--user', os.environ['USER'] ]
        if jobid:
            cmd.append('--jobid')
            cmd.append('%s' % jobid)
        try:
            proc = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.STDOUT )
            jobout, joberr = proc.communicate()
        except Exception:
            self.error('Failed to execute a command: %s' % ' '.join(cmd))
            self.error('Reporting Output:\n %s' % jobout)
            return (False,jobout)

        # Check if te return code is 0
        proc_return = proc.poll()
        if proc_return != 0:
            self.error('Non-zero return code (%s) from %s' % (proc_return,cmd))
            self.error('Reporting Output:\n %s' % jobout)
            return (False,jobout)

        return (self._check_jobstat_str(jobout),jobout)

    def _check_jobstat_str(self,stat_str):

        # Check job out
        failure_codes = ['Traceback (most recent call last):',
                         'Failed to fetch']
        success_codes = ['JOBSUBJOBID','JOBSUB SERVER RESPONSE CODE : 200 (Success)']
        for code in failure_codes:
            if stat_str.find(code) >=0:
                print "found:",code
                return False
        for code in success_codes:
            if stat_str.find(code) < 0:
                print "not found:",code
                return False
        return True

    def submit( self, statusCode, istage ):
        current_status = statusCode + istage
        error_status   = current_status + 1000
        
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
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.STDOUT )
            jobout, joberr = jobinfo.communicate()
        except:
            return current_status

        # Check if te return code is 0
        proc_return = jobinfo.poll()
        if proc_return != 0:
            self.error('Non-zero return code (%s) from %s' % (proc_return,cmd))
            self.error('Reporting Output:\n %s' % jobout)
            self.warning('Status code remains same (%d)' % current_status)
            subject = 'Failed executing: %s' % (' '.join(cmd))
            text  = subject
            text += '\n\n'
            text += 'Output:\n%s\n\n' % jobout
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return current_status

        # Check job out
        findResponse = 0
        for line in jobout.split('\n'):
            if "JOBSUB SERVER RESPONSE CODE" in line:
                findResponse = 1
                if not "Success" in line:
                    self.error('Non-successful status return from status query (output below)!')
                    self.error( jobout )
                    return current_status

        if ( findResponse == 0 ):
            self.error('Unexpected format in output return (show below)!')
            self.error( jobout )
            subject = 'Unexpected format string from: %s' % (' '.join(cmd))
            text  = subject
            text += '\n\n'
            text += 'Output:\n%s\n\n' % jobout
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return current_status

        jobid = ''
        # Grab the JobID
        for line in jobout.split('\n'):
            if "JobsubJobId" in line:
                jobid = line.strip().split()[-1]
                try:
                    AtSign = jobid.index('@')
                except ValueError:
                    self.error('Failed to extract the @ index!')
                    return current_status
                jobid = jobid[:AtSign]

        # Tentatively do so; need to change!!!
        if not jobid:
            self.error('Failed to fetch job log id...')
            self.error( jobout )
            subject = 'Failed to fetch job log id from: %s' % (' '.join(cmd))
            text  = subject
            text += '\n'
            text += 'Status code is set to %d!\n\n' % error_status
            text += 'Output:\n%s\n\n' % jobout
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return error_status

        # Now grab the parent job id
        self._data += ":%s" % jobid.split('.')[0]

        statusCode = istage + self.kSUBMITTED
        self.info( "Submitted jobs, jobid: %s, status: %d" % ( self._data, statusCode ) )

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks
        return statusCode

    def isRunning( self, statusCode, istage ):
        current_status = statusCode + istage
        error_status   = current_status + 1000

        self._data = str( self._data )
        jobid = self._data.strip().split(':')[-1]

        # Main command
        jobstat = self._jobstat_from_log()
        if not jobstat[0]:
            self.warning('Fetching job status from log file failed! Try running cmd...')
            jobstat = self._jobstat_from_cmd(jobid)
        
        if not jobstat[0]:
            subject = 'Failed to fetch job status!'
            text = ''
            if not jobstat[1]:
                text = 'No job log found...'
            else:
                text = 'Job log indicates query has failed (see below).\n %s' % jobstat[1]
            text += '\n'
            text += 'PUBS status remains same (%d)' % current_status
            self.error(subject)
            self.error(text)
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return current_status

        is_running = False
        target_jobs = [x for x in jobstat[1].split('\n') if x.startswith(jobid)]
        for line in target_jobs:
            words = line.split()
            job_state = words[5]
            if job_state == 'X': continue
            is_running = True
            if job_state == 'R':
                statusCode = self.kRUNNING
                break

        if not is_running:
            statusCode = self.kFINISHED

        msg = 'jobid: %s ... status: ' % jobid
        if statusCode == self.kRUNNING:
            msg += 'RUNNING'
        elif statusCode == self.kFINISHED:
            msg += 'FINISHED'
        elif statusCode == self.kSUBMITTED:
            msg += 'SUBMITTED'
        statusCode += istage
        msg += ' (%d)' % statusCode
        self.info(msg)

        return statusCode

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

        # Check the finished jobs
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--check' ]
        self.info( cmd )
        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.STDOUT )
            jobout, joberr = jobinfo.communicate()
        except:
            return ( statusCode + istage )

        # Check if te return code is 0
        if jobinfo.poll() != 0:
            self.error( jobinfo )
            return ( statusCode + istage )

        # Grab the good jobs and bad jobs
        for line in jobout.split('\n'):
            if "good events" in line:
                nGoodEvents = int( line.split()[0] )

        self.info("nEvents: %d, nGoodEvents: %d" %( nEvents, nGoodEvents ))

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

           #statusCode = self.kDONE
           #istage += 10
           #self._data = "numevents:%d" % nGoodEvents
           statusCode += 1000
        else:
           statusCode = self.kTOBERECOVERED

        statusCode += istage
        self.info("Checked job, status: %d" % statusCode)

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks

        return statusCode
    # def check()


    def recover( self, statusCode, istage ):
        current_status = statusCode + istage
        error_status   = current_status + 1000
                             
        # Report starting
        # self.info()
        self._data = str( self._data )

        # Main command
        stage = self._digit_to_name[istage]
        cmd = [ 'project.py', '--xml', self._xml_file, '--stage', stage, '--makeup' ]
        self.info( cmd )

        try:
            jobinfo = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.STDOUT )
            jobout, joberr = jobinfo.communicate()
        except:
            return current_status

        # Check if te return code is 0
        proc_return = jobinfo.poll()
        if proc_return != 0:
            self.error('Non-zero return code (%s) from %s' % (proc_return,cmd))
            self.error('Reporting Output:\n %s' % jobout)
            self.warning('Status code remains same (%d)' % current_status )
            subject = 'Failed executing: %s' % (' '.join(cmd))
            text  = subject
            text += '\n\n'
            text += 'Output:\n%s\n\n' % jobout
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return current_status

        # Check job out
        findResponse = 0
        for line in jobout.split('\n'):
            if "JOBSUB SERVER RESPONSE CODE" in line:
                findResponse = 1
                if not "Success" in line:
                    self.error('Non-successful status return from status query (output below)!')
                    self.error( jobout )
                    return current_status

        if ( findResponse == 0 ):
            self.error('Unexpected format in output return (show below)!')
            self.error( jobout )
            subject = 'Unexpected format string from: %s' % (' '.join(cmd))
            text  = subject
            text += '\n\n'
            text += 'Output:\n%s\n\n' % jobout
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return current_status

        # Grab the JobID
        jobid = ''
        for line in jobout.split('\n'):
            if "JobsubJobId" in line:
                jobid = line.strip().split()[-1]
                try:
                    AtSign = jobid.index('@')
                except ValueError:
                    self.error('Failed to extract the @ index!')
                    return current_status

                jobid = jobid[:AtSign]

        # Tentatively do so; need to change!!!
        if not jobid:
            self.error('Failed to fetch job log id...')
            self.error( jobout )
            subject = 'Failed to fetch job log id from: %s' % (' '.join(cmd))
            text  = subject
            text += '\n'
            text += 'Status code is set to %d!\n\n' % error_status
            text += 'Output:\n%s\n\n' % jobout
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return error_status

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

    ## @brief access DB and retrieves new runs
    def process( self ):

        ctr = self._nruns
        #return
        # Kazu's version of submit jobs
        stage_v  = list(self._stage_digits)
        stage_v.reverse()
        status_v = list(self.PROD_STATUS)
        status_v.reverse()
        # temporary fix: record the processed run and only process once per process function call
        processed_run=[]
        for istage in stage_v:
            # self.warning('Inspecting stage %s @ %s' % (istage,self.now_str()))
            for istatus in status_v:
                fstatus = istage + istatus
                self.debug('Inspecting status %s @ %s' % (istatus,self.now_str()))
                for x in self.get_runs( self._project, fstatus ):

                    run    = int(x[0])
                    subrun = int(x[1])
                    runid = (run,subrun)
                    if runid in processed_run: continue
                    processed_run.append(runid)

                    self.info('Found run/subrun: %s/%s ... inspecting @ %s' % (run,subrun,self.now_str()))

                    statusCode = self.__decode_status__( fstatus )
                    action = self.PROD_ACTION[statusCode]

                    # Get status object
                    status = self._api.get_status(ds_status(self._project,
                                                            x[0],x[1],x[2]))
                    self._data = status._data
                    statusCode = action( statusCode, istage )

                    self.info('Finished executing an action: %s @ %s' % (action.__name__,self.now_str()))

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
