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
import collections
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
        self._data = ''

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None
	
	self._seb_occupancy = float(0.5)

        self._parent_prefix = None
	self._fragment_prefix = None
	self._parent_seb_ids = None
	self._ignore_runs = []
	self._max_run = None
	self._lock_file = None
	self._locked = False

	self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        
        self._parent_prefix = resource['REG_PREFIX']
      
 	self._fragment_prefix = resource['FRAGMENT_PREFIX']
      
        self._parent_seb_ids = resource['SEBS'].split("-")

        self._parent_seb_names    = ["seb%02d"%(int(parent_seb_id)) for parent_seb_id in self._parent_seb_ids]
        self._parent_seb_projects = ["%s_%s"%(self._parent_prefix,seb_name) for seb_name in self._parent_seb_names]

        if 'SEB_OCCUPANCY' in resource:
	    self._seb_occupancy = float(resource['SEB_OCCUPANCY'])

	self._ignore_runs = [int(r_) for r_ in resource['IGNORE_RUNS'].split("-")]
	self._max_run     = int(resource['MAX_RUN'])
	self._lock_file = resource['LOCK_FILE']

    ## @brief monitor disk usage of data local
    def monitor_sebs( self ):
        
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

	if self._locked == True: 
	    self.info("Locked.")	
            return 

        seb_datalocal_size_v,max_idx = query_seb_snova_size(self._parent_seb_ids,
                                                            self._parent_seb_names)
        self.info("Start of Monitor... Max index: %d which is SEB %s used: %s"%(max_idx,self._parent_seb_names[max_idx],str(seb_datalocal_size_v[max_idx])))
	
        if seb_datalocal_size_v[max_idx] < self._seb_occupancy: return

        # Fetch runs from DB and process for # runs specified for this instance.
        status_v=[kSTATUS_DONE]*len(self._parent_seb_projects)
        
        # each subrun is 1MB
        # should go through and remove 1G at a time?
        ctr=-1

	seb_del_map = {}

	for seb in self._parent_seb_names:
            seb_del_map[seb] = ""

	self._removed_runs_list = []

        logger = pub_logger.get_logger(self._project)
        reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)

	self.info("Asking for earliest runs...")
	
	run_list_map=collections.OrderedDict()
	
	for seb in self._parent_seb_names:
            runs = reader.get_earliest_runs("fragmentruntable_%s"%seb)

	    if type(runs) is not list: continue

	    runs = [int(run[0]) for run in runs]
	    for run in runs:
                 try:
                      run_list_map[run].append(seb)
		 except KeyError:
		      run_list_map[run] = []
                      run_list_map[run].append(seb)
	    

        if len(run_list_map.keys())<1: return
        run_list_map= collections.OrderedDict(sorted(run_list_map.iteritems()))

	run = None
	for run_ in run_list_map.keys():
            if run_ in self._ignore_runs: continue
	    run = run_
	    break
	    
	self.info("Got run %s"%str(run))

	if run >= self._max_run: return

        sebs_v = run_list_map[run]
	
        death_star = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)

	for seb_ in sebs_v:
            self.info("inspecting seb... %s run... %d"%(seb_,run))

	    seb_del = ""
	    subruns=reader.get_subruns("%s_%s"%(self._fragment_prefix,seb_),run,500)
	    self.info(str(subruns))
	    valid_subruns=[]

	    for subrun_ in subruns:
                seb_status = None
		try:
                    seb_status = self._api.get_status( ds_status( "%s_%s"%(self._parent_prefix,seb_) , run, subrun_, kSTATUS_DONE ) )
		except : 
                    continue
		
		fname = seb_status._data

		if fname is None: 
                    continue

		seb_del += str(fname + " ")
		valid_subruns.append(subrun_)
	    
            if len(valid_subruns) < 1 : 
                continue

	    self.info("removing @ %s..."%seb_)
	    self.info("valid # subruns... %s" % str(valid_subruns))
	    # ret = exec_system(["ssh", "root@%s"%seb_, "rm -rf %s"%seb_del])
	    # self.info(ret)
	    runtable = "%s_%s"%(self._parent_prefix,seb_)
            self.info("...removing %s %d \n%s"%(runtable,run,str(valid_subruns)))
	    self.info("is this a list? : %s"%str(type(valid_subruns) is list))
	    runtable = "%s_%s"%(self._fragment_prefix,seb_)
	    #rundbWriter.star_destroyer(runtable,run,valid_subruns);
	    runtable="get_binary_filename__%s"%seb_
	    #rundbWriter.star_destroyer(runtable,run,valid_subruns);
	    self.info("...done...")

        return

    def lock_observer( self ) :
	
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return
        	
 	death_star = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)

	#check if the lock file exists, if so
	if os.path.isfile(self._lock_file) == False:
            self.info("Lock file does not exist.")
	    for seb_ in self._parent_seb_names:
		runtable = 'lockedruntable_%s'%seb_
	 	res = rundbWriter.clear_death_star(runtable)	
		if res==2: self.info("Already cleared run table: %s"%runtable)
		if res==1: self.info("Clear a valid run table: %s"%runtable)
		if res==0: self.info("No runtable by name %s exist" % runtable)

	    self._locked = False
	    return
	
	# copy over the run tables...

        for seb_ in self._parent_seb_names:
	    inruntable  = 'fragmentruntable_%s' % seb_   
	    outruntable = 'lockedruntable_%s'   % seb_

	    res = rundbWriter.copy_death_star(inruntable,outruntable)
   	    if res==2: self.info("Copy of %s into %s exists" % (inruntable,outruntable))
   	    if res==1: self.info("Created copy of %s into %s" % (inruntable,outruntable))
	    if res==0: self.info("Failure to copy %s into %s" % (inruntable,outruntable))
	
	
	self._locked = True
	return
	

if __name__ == '__main__':
	
    proj_name = sys.argv[1]
	
    obj = monitor_snova( proj_name )

    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
    
    obj.lock_observer()

    obj.monitor_sebs()

    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
