## @namespace dstream.DEFINE_SAMDATA
#  @ingroup dstream
#  @brief Defines a project define_samdata
#  @author kazuhiro

# python include
import time,sys,commands
# pub_dbi package include
from pub_dbi import DBException
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
        self._list_format = "data_tier = 'raw' and file_type = data and file_format = binaryraw-uncompressed and run_number = %d and availability: physical"
        self._declare_format = "data_tier = 'raw' and file_type = data and file_format = binaryraw-uncompressed and run_number = %d and run_number >= %d.%d and run_number <= %d.%d"
        self._defname_format = 'prod_assembler_binary_run_%07d_jobseq_%04d'
        self._input_file_extension = 'ubdaq'
        
    ## @brief
    def get_resource(self):
        proj_info = self._api.project_info(self._project)

        self._nruns = int(proj_info._resource['NRUNS'])
        self._num_subrun_per_job = int(proj_info._resource['NUM_SUBRUN_PER_JOB'])
        self._runtable = proj_info._runtable
        self._input_file_extension = proj_info._resource['INPUT_EXTENSION']

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        if not self._nruns:
            self.get_resource()

        samweb = samweb_cli.SAMWebClient(experiment="uboone")
        # Fetch runs from DB and process for # runs specified for this instance.
        runsubrun_list = []
        for x in self.get_runs(self._project,1):
            runsubrun_list.append((int(x[0]),int(x[1])))
        self.info('Files to be processed: %d' % len(runsubrun_list))
        last_run = self._api.get_last_run(self._runtable)
        last_subrun = self._api.get_last_subrun(self._runtable,last_run)
        
        runlist = []
        joblist = {}
        ignore_jobid = (last_run, int(last_subrun) % int(self._num_subrun_per_job))
        
        for run,subrun in runsubrun_list:
            if run in runlist: continue
            runlist.append(run)

            query = self._list_format % run
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
        for j,f_ctr in joblist.iteritems():

            run,seq = j

            num_file_necessary = self._num_subrun_per_job

            subrun_max = self._api.get_last_subrun(self._runtable,run)

            last_seq = int(subrun_max) / int(self._num_subrun_per_job)

            if seq == last_seq:
                num_file_necessary = subrun_max - last_seq * self._num_subrun_per_job
            
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
                    self.warning('Skipping Run %d Sequence %d (missing subrun %d from Project table)' % (run,seq,subrun))
                    skip=True
                    break
            if skip: continue
            
            #defname = self._defname_format % (run,seq)
            #query   = self._declare_format % (run,run,subrun_start,run,subrun_end)
            #continue
            #try:
            #    samweb.createDefinition(defname,query)
            #except Exception:
            #    self.error('Failed to create a definition: %s' % (self._defname_format % (run,seq)))
            #    continue
            self.info('Ready Run %d Sequence %d (file count %d)' % (run,seq,f_ctr))

            for x in xrange(subrun_end - subrun_start + 1):
                subrun = x + subrun_start
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = 0 ) )

# A unit test section
if __name__ == '__main__':

    test_obj = define_samdata(sys.argv[1])

    test_obj.process_newruns()



