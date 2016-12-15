## @namespace dstream_online.get_checksum
#  @ingroup get_filename
#  @brief Defines a project get_filename
#  @author yuntse

# python include
import time, os, sys
# pub_dbi package include
from pub_dbi import DBException
# pub_util package include
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_multiprocess
from ds_online_util import *
from snova_util import *
import traceback
from collections import OrderedDict
import subprocess

class construct_filename( ds_project_base ):

    _project = 'construct_filename'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( construct_filename, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._nruns = None
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._experts = ''
        self._data = ''
        self._min_run = 0

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None
        self._seb=""
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']

        if 'PARENT_PROJECT' in resource:
            self._parent_project = resource['PARENT_PROJECT']
        self._experts = resource['EXPERTS']

        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])
        self._min_run = 0

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

        self._seb = resource["SEB"]

    ## @brief calculate the filename of a file
    def register_filename( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

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

            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._project)
            self._api.commit('DROP TABLE IF EXISTS temp%s' % self._skip_ref_project)
        
        ctr = 1000
        runlist = self.get_runs(self._project,1,False,ctr)
        in_file_v = []
        runid_v = []
        
        #slice the run list
        sliced_runlist=runlist
        self.info(sliced_runlist)
        # base_cmd="find /datalocal/supernova/ -type f -regex '.*\("
        # run_str=["%07d-%05d"%(r[0],r[1]) for r in sliced_runlist]

        # base_cmd+="\|".join(run_str)
        # base_cmd+="\)'.ubdaq"

        # res_ = exec_system(["ssh", self._seb, base_cmd])

        #execute a single command to get all files in snova directory
        dir_flist=exec_system(["ssh", self._seb, "ls -f -1 /datalocal/supernova/"])[2:]

        file_map=OrderedDict()
        
        for res in dir_flist:
            split_ = res.split('.')[0].split('_')[-1].split('-')
            run_    = int(split_[1])
            subrun_ = int(split_[2])
            file_map[tuple((run_,subrun_))]=os.path.join("/datalocal/supernova/",res)
            
        for x in sliced_runlist:
            # Break from loop if counter became 0
            if ctr <= 0: break

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            # Counter decreases by 1
            ctr -= 1

            # Report starting
            self.info('Calculating the file filename: run=%d subrun=%d @ %s' % (run,subrun,time.strftime('%Y-%m-%d %H:%M:%S')))

            statusCode=kSTATUS_DONE
            self._data=file_map[(run,subrun)]
            self.info("Inserting data... %s... status %s "%(str(self._data),str(kSTATUS_DONE)))
            ret = self.log_status( ds_status( project = self._project,
                                              run     = run,
                                              subrun  = subrun,
                                              seq     = 0,
                                              status  = kSTATUS_DONE,
                                              data    = self._data ) )
            self.info("Return of log status was %s"%str(ret))
            self.info("Checking........")
            seb_status = self._api.get_status( ds_status( self._project , run, subrun, kSTATUS_DONE ))
            self.info("Data is........ %s"%seb_status._data)

if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = construct_filename( proj_name )

    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    obj.register_filename()

    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
