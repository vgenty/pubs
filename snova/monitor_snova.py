## @namespace dstream_online.get_checksum
#  @ingroup get_checksum
#  @brief Defines a project get_checksum
#  @author vgenty

# python include
import time, os, sys
# pub_dbi package include
from pub_dbi import DBException, pubdb_conn_info
# pub_util package include
from pub_util import pub_smtp
from pub_util import pub_logger
from pub_dbi.pubdb_conn import pubdb_conn
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_multiprocess
from dstream import ds_api
from ds_online_util import *
from snova_util import *
import traceback

def query_seb_snova_size(parent_seb_ids,parent_seb_names):
        # go through each of the SEBS and calculate the disk usage, if it's 
        # above 70% on a single SEB we have to start slashing

        seb_datalocal_size_v = [0.0]*len(parent_seb_ids)

        for ix,seb in enumerate(parent_seb_names):
            ret_ = exec_system(["ssh", seb, "df /datalocal/"])
            size_, used_, unused_ = ret_[-1].split(" ")[6:9]
            seb_datalocal_size_v[ix] = float(used_) / float(size_)
        
        max_idx=argmax(seb_datalocal_size_v)
        #self.info("SEB sizes: %s"%str(seb_datalocal_size_v))

        return seb_datalocal_size_v,max_idx

class monitor_snova( ds_project_base ):

    _project = 'monitor_snova'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( monitor_snova, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._nruns = None
        self._in_dir = ''
        self._infile_format = ''
        self._parent_projects = ''
        self._experts = ''
        self._data = ''
        self._parallelize = 0
        self._max_proc_time = 30
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
        
        # if 'REG_PREFIX' in resource:
            #self._parent_prefix = resource['REG_PREFIX']
        self._parent_prefix = "get_binary_filename"

            # if 'FRAGMENT_PREFIX' in resource:
        self._fragment_prefix = "fragmentruntable"#resource['FRAGMENT_PREFIX']
            
        if 'SEBS' in resource:
            self._parent_seb_ids = resource['SEBS'].split("-")

        self._parent_seb_names    = ["seb%02d"%(int(parent_seb_id)) for parent_seb_id in self._parent_seb_ids]
        self._parent_seb_projects = ["%s_%s"%(self._parent_prefix,seb_name) for seb_name in self._parent_seb_names]

        self._experts = resource['EXPERTS']

        if 'PARALLELIZE' in resource:
            self._parallelize = int(resource['PARALLELIZE'])

        if 'MAX_PROC_TIME' in resource:
            self._max_proc_time = int(resource['MAX_PROC_TIME'])

        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])

    ## @brief monitor disk usage of data local
    def monitor_sebs( self ):
        
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()



        seb_datalocal_size_v,max_idx = query_seb_snova_size(self._parent_seb_ids,
                                                            self._parent_seb_names)
        
        self.info("Start of Monitor... Max index: %d which is SEB %s used: %s"%(max_idx,self._parent_seb_names[max_idx],str(seb_datalocal_size_v[max_idx])))

        if seb_datalocal_size_v[max_idx] < 0.7: return

        # Fetch runs from DB and process for # runs specified for this instance.
        status_v=[kSTATUS_DONE]*len(self._parent_seb_projects)
        
        # all sebs runlist
        death_star = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)
        
        #each subrun is 1MB
        #should go through and remove 1G at a time?
        ctr=-1
	self.info("Requesting cross table runs for %s"%str(self._parent_seb_projects))
        for x in reversed(self.get_xtable_runs(self._parent_seb_projects,status_v)):
            ctr+=1
            (run, subrun) = (int(x[0]), int(x[1]))
            # do the deletion
            self.info("ctr: %d ... run: %d ... subrun %d"%(ctr,run,subrun))
            # remove from the runtable
            for seb,seb_project in zip(self._parent_seb_names,self._parent_seb_projects):
                runtable=self._fragment_prefix+"_"+seb
                seb_status = self._api.get_status( ds_status( seb_project, run, subrun, kSTATUS_DONE ) )
                fname = seb_status._data
                ret = exec_system(["ssh", seb, "stat %s"%fname])
                # ret = exec_system(["ssh", seb, "rm -rf %s"%fname])
                #print seb,runtable,fname
                SS="rundbWriter.star_destroyer(%s, %d, %d)"%(runtable,run,subrun)
                #print SS
                #print ret
                #print "~~~~"

            if ctr%100==0:
                seb_datalocal_size_v,max_idx = query_seb_snova_size(self._parent_seb_ids,
                                                                    self._parent_seb_names) 
                self.info("Current max index: %d which is SEB %s used: %s"%(max_idx,self._parent_seb_names[max_idx],str(seb_datalocal_size_v[max_idx])))
                if seb_datalocal_size_v[max_idx] < 0.7:  break

                if ctr==5000: break
         
	self.info("End")
        return

if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = monitor_snova( proj_name )

    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    obj.monitor_sebs()

    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
