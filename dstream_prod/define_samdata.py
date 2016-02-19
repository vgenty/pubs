## @namespace dstream.DEFINE_SAMDATA
#  @ingroup dstream
#  @brief Defines a project define_samdata
#  @author kazuhiro

# python include
import time,sys,commands,os
# pub_dbi package include
from pub_dbi import DBException
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
import samweb_cli

## @class define_samdata
#  @brief kazuhiro should give a brief comment here
#  @details
#  kazuhiro should give a detailed comment here
class define_samdata(ds_project_base):

    # Define project name as class attribute
    _project = 'define_samdata'

    # Define # of runs to process per request
    _nruns   = 5

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self,project_name):

        # Call base class ctor
        super(define_samdata,self).__init__(project_name)

        self._project = project_name

        self._nruns = 0
        self._num_subrun_per_job = 0
        self._runtable = ''
        self._list_format = ''
        self._declare_format = ''
        self._defname_format = ''
        self._input_file_extension = 'ubdaq'
        self._max_run = 1e12
        self._min_run = 0
        
    ## @brief load project parameters
    def get_resource(self):
        proj_info = self._api.project_info(self._project)

        self._nruns = int(proj_info._resource['NRUNS'])
        if 'MAX_RUN' in proj_info._resource:
            self._max_run = int(proj_info._resource['MAX_RUN'])
        if 'MIN_RUN' in proj_info._resource:
            self._min_run = int(proj_info._resource['MIN_RUN'])
        self._num_subrun_per_job = int(proj_info._resource['NUM_SUBRUN_PER_JOB'])
        self._runtable = proj_info._runtable
        self._input_file_extension = proj_info._resource['INPUT_EXTENSION']

        self._declare_format = proj_info._resource['SAM_DECLARE_FORMAT']
        self._defname_format = proj_info._resource['SAM_DEFNAME_FORMAT']
        self._list_format = proj_info._resource['SAM_LIST_FORMAT']
        self._experts = proj_info._resource['EXPERTS']

    ## @brief getter for a formatted string
    def sam_query_formatter(self,format,run,subrun=-1):
        res = str(format)
        res = res.replace('REP_RUN_NUMBER',str(int(run)))
        res = res.replace('REP_ZEROPAD_RUN_NUMBER','%07d' % int(run))
        if subrun>0:
            res = res.replace('REP_SUBRUN_NUMBER',str(int(subrun)))
            res = res.replace('REP_ZEROPAD_SUBRUN_NUMBER','%07d' % int(subrun))
        return res
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        if not self._nruns:
            self.get_resource()

        samweb = samweb_cli.SAMWebClient(experiment="uboone")
        # Fetch runs from DB and process for # runs specified for this instance.
        runsubrun_list = []
        subrun_ctr = {}
        for x in self.get_runs(self._project,1):
            run,subrun = (int(x[0]),int(x[1]))

            if run > self._max_run: continue
            if run < self._min_run: continue
            runsubrun_list.append((run,subrun))
            if not run in subrun_ctr:
                subrun_ctr[run]=0
            subrun_ctr[run] += 1
        self.info('Files to be processed: %d' % len(runsubrun_list))
        for r in subrun_ctr:
            nsubrun = self._api.get_last_subrun(self._runtable,r)+1
            self.debug('Run %d ... %d subruns ... %d subruns to be processed' % (r,nsubrun,subrun_ctr[r]))

        last_run = self._api.get_last_run(self._runtable)
        last_subrun = self._api.get_last_subrun(self._runtable,last_run)
        
        runlist = []
        joblist = {}
        ignore_jobid = (last_run, int(last_subrun) % int(self._num_subrun_per_job))
        
        for run,subrun in runsubrun_list:
            if run in runlist: continue
            runlist.append(run)

            query = self.sam_query_formatter(self._list_format,run)
            self.debug('Query: %s' % query)
            filelist = samweb.listFiles(query)
            self.info('Run %d found %d files...' % (run,len(filelist)))
            for f in filelist:

                tmp_f = f.replace(self._input_file_extension,'')
                subrun = int(tmp_f.split('-')[-1])
                jobid = (run, int(subrun) / int(self._num_subrun_per_job))
                if jobid == ignore_jobid: 
                    self.debug('Skipping Run %d SubRun %d ... Seq %d (this is last sequence)' % (run,subrun,jobid[1]))
                    continue
                
                if not jobid in joblist:
                    joblist[jobid] = 0
                joblist[jobid] += 1
        self.info('Found %d dataset possibilities' % len(joblist))

        missing_runlist=[]

        for j,f_ctr in joblist.iteritems():

            run,seq = j

            num_file_necessary = self._num_subrun_per_job

            subrun_max = self._api.get_last_subrun(self._runtable,run)

            last_seq = int(subrun_max) / int(self._num_subrun_per_job)

            if seq == last_seq:
                num_file_necessary = subrun_max - last_seq * self._num_subrun_per_job + 1
            
            if f_ctr < num_file_necessary:
                self.debug('Skip Run %d Sequence %d (file count %d/%d)' % (run,seq,f_ctr,num_file_necessary))
                continue

            # Check if all subruns in this sequence exists
            subrun_start = seq * self._num_subrun_per_job
            subrun_end = subrun_start + num_file_necessary - 1

            skip = False
            for x in xrange(subrun_end - subrun_start + 1):
                subrun = x + subrun_start

                if not (run,subrun) in runsubrun_list:
                    skip=True
                    if not self._api.subrun_exist(self._runtable,run,subrun):


                        self.error('Skipping Run %d Sequence %d (missing subrun %d from Project table)' % (run,seq,subrun))
                        subject = '%s found missing sub-run in run table' % self._project
                        text = subject
                        text += '\n'
                        text += 'Run=%d SubRun=%d not found in project status table!\n' % (run,subrun)
                        text += 'Please update Offline MainRun table...\n'
                        #try:
                        #    pub_smtp( receiver=self._experts, 
                        #              subject=subject, 
                        #              text=text )
                        #except Exception:
                        #    self.error('Failed to send an email notice about the failure...')
                    break

            if skip: continue
            
            defname = self.sam_query_formatter(self._defname_format,run)
            query   = self.sam_query_formatter(self._declare_format,run)
            try:
                samweb.descDefinition(defname)
            except samweb_cli.DefinitionNotFound:
                samweb.createDefinition(defname,query)

            self.info('Ready Run %d Sequence %d (file count %d)' % (run,seq,f_ctr))

            for x in xrange(subrun_end - subrun_start + 1):
                subrun = x + subrun_start
                try:
                    self.log_status( ds_status( project = self._project,
                                                run     = run,
                                                subrun  = subrun,
                                                seq     = 0,
                                                status  = 0 ) )
                    time.sleep(0.1)
                except Exception:
                    self.error('Run %d SubRun %d does not exist for %s' % (run,subrun,self._project))
# A unit test section
if __name__ == '__main__':

    test_obj = define_samdata(sys.argv[1])

    test_obj.process_newruns()



