## @namespace dstream_prod.production
#  @ingroup dstream_prod
#  @brief Defines a project production
#  @author yuntse

# python include
import time,os,sys,time
import subprocess, traceback
import StringIO
# pub_dbi package include
from pub_dbi import DBException
# pub_util package include
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
# From larbatch
import project
import project_utilities
from project_modules.pubsdeadenderror import PubsDeadEndError
from project_modules.pubsinputerror import PubsInputError

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
                    kTOBERECOVERED,
                    kREADYFORSAM,
                    kDECLARED ) = xrange(9)

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

        # Actions associated with single subruns.

        self.PROD_ACTION = { self.kDONE          : self.checkNext,
                             self.kINITIATED     : None,
                             self.kTOBEVALIDATED : self.isRunning,
                             self.kSUBMITTED     : self.isRunning,
                             self.kRUNNING       : self.isRunning,
                             self.kFINISHED      : self.check,
                             self.kTOBERECOVERED : None,
                             self.kREADYFORSAM   : self.declare,
                             self.kDECLARED      : self.store }
        self.PROD_MULTIACTION = { self.kDONE          : None,
                                  self.kINITIATED     : self.submit,
                                  self.kTOBEVALIDATED : None,
                                  self.kSUBMITTED     : None,
                                  self.kRUNNING       : None,
                                  self.kFINISHED      : None,
                                  self.kTOBERECOVERED : self.recover,
                                  self.kREADYFORSAM   : None,
                                  self.kDECLARED      : None }
        self._max_runid = None
        self._min_runid = None
        self._nruns     = None
        self._nsubruns  = []
        self._xml_file  = ''
        self._stage_name    = []
        self._stage_digits  = []
        self._nresubmission = 3
        self._digit_to_name = {}
        self._name_to_digit = {}
        self._digit_to_nsubruns = {}
        self._data = "None"
        self._runid_status = {}

        if not self.loadProjectParams():
            self.info('Failed to load project @ %s' % self.now_str())
            sys.exit(1)

    def loadProjectParams( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return False

        proj_info = self._api.project_info(self._project)
        
        try:
            self._nruns = int(proj_info._resource['NRUNS'])
            self._xml_file = proj_info._resource['XMLFILE']
            self._nresubmission = int(proj_info._resource['NRESUBMISSION'])
            self._experts = proj_info._resource['EXPERTS']
            self._period = proj_info._period
            self._version = proj_info._ver
            self._stage_names  = proj_info._resource['STAGE_NAME'].split(':')
            self._stage_digits = [int(x) for x in proj_info._resource['STAGE_STATUS'].split(':')]
            if not len(self._stage_names) == len(self._stage_digits):
                raise Exception
            for x in xrange(len(self._stage_names)):
                name  = self._stage_names[x]
                digit = self._stage_digits[x]
                self._digit_to_name[digit]=name
                self._name_to_digit[name]=digit
            self._max_runid = (int(proj_info._resource['MAX_RUN']),int(proj_info._resource['MAX_SUBRUN']))
            if proj_info._resource.has_key('MIN_RUN') and proj_info._resource.has_key('MIN_SUBRUN'):
                self._min_runid = (int(proj_info._resource['MIN_RUN']),int(proj_info._resource['MIN_SUBRUN']))

            # Set subrun multiplicity.

            if proj_info._resource.has_key('NSUBRUNS'):
                self._nsubruns = [int(x) for x in proj_info._resource['NSUBRUNS'].split(':')]
            else:
                self._nsubruns = [12] * len(self._stage_names)
            if len(self._nsubruns) != len(self._stage_digits):
                raise Exception
            for x in xrange(len(self._stage_digits)):
                digit = self._stage_digits[x]
                nsubruns = self._nsubruns[x]
                self._digit_to_nsubruns[digit] = nsubruns

        except Exception:
            self.error('Failed to load project parameters...')
            return False

        self.info('Project loaded @ %s' % self.now_str())
        return True

    def now_str(self):
        return time.strftime('%Y-%m-%d %H:%M:%S')

    def checkNext( self, statusCode, istage, run, subrun ):

        if istage in self._stage_digits:
            statusCode = istage + self.kINITIATED
            self.info( "Next stage, statusCode: %d" % statusCode )
            self._data = ''

        return statusCode

    def _jobstat_from_log(self, submit_time):
        result = (False,'')
        if not os.path.isfile(self.JOBSUB_LOG):
            subject = 'Failed fetching job log'
            text  = 'Batch job log file not available... (check daemon, should not happen)'
            text += '\n\n'
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return result

        # Make sure log has been updated more recently than most recent submit.

        mod_time = os.path.getmtime(self.JOBSUB_LOG)
        self.info('Job log modification time: %s' % time.ctime(mod_time))
        self.info('Submit time: %s' % time.ctime(submit_time))
        if mod_time < submit_time + 60:
            return result

        log_age = time.time() - mod_time
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
        success_codes = ['JOBSUBJOBID']
        for code in failure_codes:
            if stat_str.find(code) >=0:
                print "found:",code
                return False
        for code in success_codes:
            if stat_str.find(code) < 0:
                print "not found:",code
                return False
        return True

    def submit( self, statusCode, istage, run, subruns ):
        current_status = statusCode + istage
        error_status   = current_status + 1000
        
        # Report starting
        # self.info()
        self._data = str( self._data )

        # Main command
        stage = self._digit_to_name[istage]

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self._xml_file, '', stage, run, subruns, self._version)
        except PubsDeadEndError:
            self.info('Exception PubsDeadEndError raised by project.get_pubs_stage')
            return 100
        except PubsInputError:
            self.info('Exception PubsInputError raised by project.get_pubs_stage')
            return current_status
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return current_status

        # Submit job.
        jobid=''
        try:
            jobid = project.dosubmit(probj, stobj)
        except:
            self.error('Exception raised by project.dosubmit:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return current_status
        self.info( 'Submit jobs: xml: %s, stage: %s' %( self._xml_file, stage ) )

        # Tentatively do so; need to change!!!
        if not jobid:
            self.error('Failed to fetch job log id...')
            subject = 'Failed to fetch job log id while submitting project %s stage %s.' % (
                probj.name, stobj.name)
            text = subject
            text += '\n'
            text += 'Status code is set to %d!\n\n' % error_status
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return error_status

        # Now grab the parent job id and submit time.
        single_data = '%s+%f' % (jobid, time.time())

        statusCode = istage + self.kSUBMITTED
        self.info( "Submitted jobs, jobid: %s, status: %d" % ( single_data, statusCode ) )

        # Here we need to convert the job data into a list with one entry for each subrun.
        # In case of input from sam, we keep the original job id and copy it for each job,
        # since we have no way of knowing what the job ids of the parallel workers will
        # be at this point.  This jobid will be the dagman jobid, which will run until
        # every worker is finished.
        # In case of input from a file list, increment the process number encoded in
        # the job id for each subrun, up to stobj.num_jobs jobs.

        n1 = single_data.find('.')
        n2 = single_data.find('@')
        multi_data = []
        if n1 > 0 and n2 > 0 and n2 > n1 and stobj.inputlist != '':
            head = single_data[:n1+1]
            tail = single_data[n2:]
            process=0
            for subrun in subruns:
                if process < stobj.num_jobs:
                    multi_data.append('%s%d%s' % (head, process, tail))
                else:
                    multi_data.append('Merge %d' % subruns[0])
                process += 1
        else:
            for subrun in subruns:
                multi_data.append(single_data)
        self._data = multi_data

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks
        return statusCode

    def isRunning( self, statusCode, istage, run, subrun ):
        current_status = statusCode + istage
        error_status   = current_status + 1000

        self._data = str( self._data )

        # Merged subruns handled here.

        if self._data[:5] == 'Merge':
            merge_subrun = int(self._data[5:])
            merge_runid = (run, merge_subrun)
            if self._runid_status.has_key(merge_runid):
                if self._runid_status[merge_runid] == istage + self.kREADYFORSAM:

                    # If we get here, the merge batch job was successful, and we
                    # can set the status for this subrun to ignore further processing.

                    return 100

                elif self._runid_status[merge_runid] == istage + self.kTOBERECOVERED:

                    # If we get here, the merge batch job failed.  Set the 
                    # status for this subrun to recover.

                    return istage + kTOBERECOVERED

                else:
                    return current_status

            else:

                # This branch of the if shouldn't really ever happen (may require
                # manual intervention to fix).

                msg = 'No merge status for run %d, subrun %d.' % merge_runid
                self.info(msg)
                return current_status

        last_job_data = self._data.strip().split(':')[-1]
        job_data_list = last_job_data.split('+')
        jobid = job_data_list[0]
        if len(job_data_list) > 1:
            submit_time = float(job_data_list[1])
        else:
            submit_time = float(0)

        # Main command
        jobstat = self._jobstat_from_log(submit_time)
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

    def check( self, statusCode, istage, run, subrun ):
        self._data = str( self._data )
        nSubmit     = None

        # Get the number of job submissions.
        if self._data != None and len(self._data) > 0:

            holder = self._data.split(':')
            nSubmit = len(holder)

        # Check the finished jobs
        stage = self._digit_to_name[istage]

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self._xml_file, '', stage, run, [subrun], self._version)
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode + istage 

        # Do check.
        try:
            real_stdout = sys.stdout
            real_stderr = sys.stderr
            sys.stdout = StringIO.StringIO()
            sys.stderr = StringIO.StringIO()
            project.doshorten(stobj)
            check_status = project.docheck(probj, stobj, ana=False)
            strout = sys.stdout.getvalue()
            strerr = sys.stderr.getvalue()
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            if strout:
                self.info(strout)
            if strerr:
                self.warning(strerr)
        except:
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            self.error('Exception raised by project.docheck:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            check_status = 1

        # Update pubs status.
        if check_status == 0:
           statusCode = self.kREADYFORSAM
           self._data = ''

        elif nSubmit > self._nresubmission:
           # If the sample has been submitted more than a certain number
           # of times, email the expert, and move on to the next stage
           subject = "MCC jobs fails after %d resubmissions" % nSubmit 
           text = """
Sample     : %s
Stage      : %s
Job IDs    : %s
           """ % ( self._project, self._digit_to_name[istage], self._data.split(':')[2:] )

           pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

           #statusCode = self.kDONE
           #istage += 10
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


    def recover( self, statusCode, istage, run, subruns ):
        current_status = statusCode + istage
        error_status   = current_status + 1000
                             
        # Report starting
        # self.info()
        self._data = str( self._data )

        # Main command
        stage = self._digit_to_name[istage]

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self._xml_file, '', stage, run, subruns, self._version)
        except PubsInputError:
            self.info('Exception PubsInputError raised by project.get_pubs_stage')
            return current_status
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return current_status

        # Submit job.
        jobid=''
        try:
            jobid = project.dosubmit(probj, stobj)
        except:
            self.error('Exception raised by project.dosubmit:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return current_status
        self.info( 'Resubmit jobs: xml: %s, stage: %s' %( self._xml_file, stage ) )

        # Tentatively do so; need to change!!!
        if not jobid:
            self.error('Failed to fetch job log id...')
            subject = 'Failed to fetch job log id while submitting project %s stage %s.' % (
                probj.name, stobj.name)
            text = subject
            text += '\n'
            text += 'Status code is set to %d!\n\n' % error_status
            pub_smtp(receiver = self._experts,
                     subject = subject,
                     text = text)
            return error_status

        # Now grab the parent job id and submit time
        single_data = '%s+%f' % (jobid, time.time())
        original_data = None
        if self._data != None and self._data != "None" and len(self._data) != 0:
            original_data = self._data

        statusCode = istage + self.kSUBMITTED
        self.info( "Resubmitted jobs, job id: %s, status: %d" % ( self._data, statusCode ) )

        # Here we need to convert the job data into a list with one entry for each subrun.
        # In case of input from sam, we keep the original job id and copy it for each job,
        # since we have no way of knowing what the job ids of the parallel workers will
        # be at this point.  This jobid will be the dagman jobid, which will run until
        # every worker is finished.
        # In case of input from a file list, increment the process number encoded in
        # the job id for each subrun, up to stobj.num_jobs jobs.

        n1 = single_data.find('.')
        n2 = single_data.find('@')
        multi_data = []
        if n1 > 0 and n2 > 0 and n2 > n1 and stobj.inputlist != '':
            head = single_data[:n1+1]
            tail = single_data[n2:]
            process=0
            for subrun in subruns:
                if process < stobj.num_jobs:
                    if original_data != None:
                        multi_data.append('%s:%s%d%s' % (original_data, head, process, tail))
                    else:
                        multi_data.append('%s%d%s' % (head, process, tail))
                else:
                    multi_data.append('Merge %d' % subruns[0])
                process += 1
        else:
            for subrun in subruns:
                multi_data.append(single_data)
        self._data = multi_data



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

    def declare( self, statusCode, istage, run, subrun ):

        # Get stage name.
        stage = self._digit_to_name[istage]

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self._xml_file, '', stage, run, [subrun], self._version)
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode + istage 

        # Do declaration.
        try:
            real_stdout = sys.stdout
            real_stderr = sys.stderr
            sys.stdout = StringIO.StringIO()
            sys.stderr = StringIO.StringIO()

            # Declare artroot files.

            declare_status = project.docheck_declarations(stobj.logdir, stobj.outdir,
                                                          declare=True, ana=False)

            # Declare analysis root files.

            if declare_status == 0:
                declare_status = project.docheck_declarations(stobj.logdir, stobj.outdir,
                                                              declare=True, ana=True)

            # Create artroot dataset definition.

            if declare_status == 0:
                nopubs_stobj = stobj
                nopubs_stobj.pubs_output = 0
                dim = project_utilities.dimensions(probj, nopubs_stobj, ana=False)
                declare_status = project.docheck_definition(stobj.defname, dim, True)

            # Create analysis dataset definition.

            if declare_status == 0:
                dim = project_utilities.dimensions(probj, stobj, ana=True)
                declare_status = project.docheck_definition(stobj.defname, dim, True)

            strout = sys.stdout.getvalue()
            strerr = sys.stderr.getvalue()
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            if strout:
                self.info(strout)
            if strerr:
                self.warning(strerr)
        except:
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            self.error('Exception raised by project.docheck_definitions:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode + istage

        # Update pubs status.
        if declare_status == 0:
           statusCode = self.kDECLARED

        statusCode += istage
        self.info("SAM declarations, status: %d" % statusCode)

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks

        return statusCode
    # def declare( self, statusCode, istage, run, subrun ):


    def store( self, statusCode, istage, run, subrun ):

        # Only store the final stage.
        # If this is not the final stage, advance to the next stage.

        if istage != self._stage_digits[-1]:
            statusCode = self.kDONE
            istage += 10
            return statusCode + istage

        # Get stage name.
        stage = self._digit_to_name[istage]

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self._xml_file, '', stage, run, [subrun], self._version)
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode + istage 

        # Do store.
        try:
            real_stdout = sys.stdout
            real_stderr = sys.stderr
            sys.stdout = StringIO.StringIO()
            sys.stderr = StringIO.StringIO()

            # Store files.

            dim = project_utilities.dimensions(probj, stobj, ana=False)
            store_status = project.docheck_locations(dim, stobj.outdir, 
                                                     add=False,
                                                     clean=False,
                                                     remove=False,
                                                     upload=True)

            if store_status == 0 and stobj.ana_data_tier != '':
                dim = project_utilities.dimensions(probj, stobj, ana=True)
                store_status = project.docheck_locations(dim, stobj.outdir, 
                                                         add=False,
                                                         clean=False,
                                                         remove=False,
                                                         upload=True)

            strout = sys.stdout.getvalue()
            strerr = sys.stderr.getvalue()
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            if strout:
                self.info(strout)
            if strerr:
                self.warning(strerr)
        except:
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            self.error('Exception raised by project.docheck_locations:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode + istage

        # Update pubs status.
        if store_status == 0:
           statusCode = self.kDONE
           istage += 10

        # Pretend I'm doing something
        time.sleep(5)

        # If all the stages complete, send an email to experts
        if not istage in self._stage_digits:
            subject = "Completed: MCC sample %s" % self._project
            text = """
Sample     : %s
Stage      : %s
               """ % ( self._project, self._digit_to_name[istage-10] )

            pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

        statusCode += istage
        self.info("SAM store, status: %d" % statusCode)

        # Pretend I'm doing something
        time.sleep(5)

        # Here we may need some checks

        return statusCode
    # def store( self, statusCode, istage, run, subrun ):


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
                self.debug('Inspecting status %s @ %s' % (fstatus,self.now_str()))

                # Get (run, subrun) pairs from pubs database.

                run_subruns = {}
                for x in self.get_runs( self._project, fstatus ):
                    
                    run    = int(x[0])
                    subrun = int(x[1])
                    runid = (run,subrun)
                    if self._max_runid and runid > self._max_runid:
                        continue
                    if self._min_runid and runid < self._min_runid:
                        continue
                    if runid in processed_run: continue
                    processed_run.append(runid)

                    self.info('Found run/subrun: %s/%s ... inspecting @ %s' % (run,subrun,self.now_str()))
                    if not run_subruns.has_key(run):
                        run_subruns[run] = set()
                    if not subrun in run_subruns[run]:
                        run_subruns[run].add(subrun)

                # Loop over runs.

                for run in run_subruns.keys():
                    all_subruns = run_subruns[run].copy()

                    # Process subruns in groups.

                    subruns = []
                    while len(all_subruns) > 0:
                        subrun = all_subruns.pop()
                        subruns.append(subrun)
                        if len(subruns) >= self._digit_to_nsubruns[istage] or len(all_subruns) == 0:

                            subruns.sort()
                            statusCode = self.__decode_status__( fstatus )

                            # Do actions associated with multiple subruns.

                            multiaction = self.PROD_MULTIACTION[statusCode]
                            if multiaction != None:
                                status = self._api.get_status(ds_status(self._project,
                                                                        run, subruns[0], 0))
                                self._data = status._data
                                self.info('Starting a multiple subrun action: %s @ %s' % (
                                        multiaction.__name__, self.now_str()))
                                statusCode = multiaction( statusCode, istage, run, subruns )
                                self.info('Finished a multiple subrun action: %s @ %s' % (
                                        multiaction.__name__, self.now_str()))

                                # Create a status object to be logged to DB (if necessary)
                                # Do this for each subrun.

                                process = 0
                                for subrun in subruns:
                                    status = ds_status( project = self._project,
                                                        run     = run,
                                                        subrun  = subrun,
                                                        seq     = 0,
                                                        status  = statusCode,
                                                        data    = self._data[process] )
                                    runid = (run, subrun)
                                    self._runid_status[runid] = statusCode

                                    # Log status
                                    self.log_status( status )
                                    process += 1

                                # Counter decreases by number of subruns
                                ctr -= len(subruns)

                                # Break from loop if counter became 0
                                if ctr < 0: return

                            subruns = []


                    # Loop over subruns and do single-subrun actions.

                    for subrun in run_subruns[run]:

                        statusCode = self.__decode_status__( fstatus )
                        action = self.PROD_ACTION[statusCode]
                        if action != None:

                            # Read data.

                            status = self._api.get_status(ds_status(self._project,
                                                                    run, subrun, 0))
                            self._data = status._data

                            self.info('Starting an action: %s @ %s' % (
                                    action.__name__,self.now_str()))
                            statusCode = action( statusCode, istage, run, subrun )
                            self.info('Finished an action: %s @ %s' % (
                                    action.__name__,self.now_str()))

                            # Create a status object to be logged to DB (if necessary)
                            status = ds_status( project = self._project,
                                                run     = run,
                                                subrun  = subrun,
                                                seq     = 0,
                                                status  = statusCode,
                                                data    = self._data )

                            runid = (run, subrun)
                            self._runid_status[runid] = statusCode

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
