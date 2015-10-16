## @namespace dstream.DEFINE_SAMDATA
#  @ingroup dstream
#  @brief Defines a project define_samdata
#  @author kazuhiro

# python include
import time
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

        self._nruns = 0
        self._num_subrun_per_job = 0
        self._runtable = ''
        self._list_cmd = "( data_tier = 'raw' and file_type = data and file_format = binaryraw-uncompressed and run_number = %d)"
        self._input_file_extension = 'ubdaq'
        
    ## @brief
    def get_resource(self):
        proj_info = self._api.project_info(self._project)

        self._nruns = proj_info._resource['NRUNS']
        self._num_subrun_per_job = proj_info._resource['NUM_SUBRUN_PER_JOB']
        self._runtable = proj_info._runtable
        self._list_cmd = proj_info._resource['SAMWEB_LIST_CMD']
        self._input_file_extension = proj_info._resource['INPUT_EXTENSION']
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        if not self._nruns:
            self.get_resource()

        samweb = samweb_cli.SAMWebClient(experiment="uboone")

        # Fetch runs from DB and process for # runs specified for this instance.
        runsubrun_list = self.get_runs(self._project,1)
        last_run = self._api.get_last_run(self._runtable)
        last_subrun = self._api.get_last_subrun(self._runtable,last_run)
        
        joblist = {}
        ignore_jobid = (last_run, last_subrun % self._num_subrun_per_job)
        
        for x in runsubrun_list:
            run = int(x[0])
            if run in runlist: continue
            runlist.append(int(x[0]))

            query = self._list_cmd % run
            filelist = [x for x in command.getoutput(query).split() if x.endswith(self._input_file_extension)]
            for f in filelist:
                tmp_f = f.replace(self._input_file_extension,'')
                subrun = int(tmp_f.split('-')[-1])
                jobid = (run,subrun % self._num_subrun_per_job)
                if jobid = ignore_jobid: continue
                
                if not jobid in joblist:
                    joblist[jobid] = 0
                joblist[jobid] += 1

        for j,f_ctr in jobid.iteritems():

            run,seq = j

            num_file_necessary = self._num_subrun_per_job

            subrun_max = self._api.get_last_subrun(self._runtable,run)

            last_seq = subrun_max % self._num_subrun_per_job

            if seq == last_seq:
                num_file_necessary = subrun_max - last_seq * self._num_subrun_per_job
            
            if f_ctr < num_file_necessary:
                self.debug('Skip Run %d Sequence %d (file count %d/%d)' % (run,seq,f_ctr,num_file_necessary))
                continue

            self.info('Ready Run %d Sequence %d (file count %d)' % (run,seq,f_ctr))

            continue

            self.log_status( ds_status( project = self._project,
                                        run     = int(x[0]),
                                        subrun  = int(x[1]),
                                        seq     = int(x[2]),
                                        status  = 10 ) )

# A unit test section
if __name__ == '__main__':

    test_obj = define_samdata(5)

    test_obj.process_newruns()



