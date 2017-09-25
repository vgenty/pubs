# python include
import time, os, sys, json
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
import threading
import Queue

def thread_worker(q):
    while True:
        data = q.get()
        data[2].info("@ thread worker got data seb: %s for run: %d" % (str(data[0]),int(data[1])))
        copy_subruns(data)
        data[2].info("...complete @ seb: %s for run: %d" % (str(data[0]),int(data[1])))
        q.task_done()
        data[2].info("...called task_done()")
        time.sleep(1)
        data[2].info("...slept")
        data[2].info("q.qsize(): %d" % int(q.qsize()))
        data[2].info("q.empty(): %d" % int(q.empty()))
    return

def copy_subruns(data):
    seb_    = str(data[0])
    run     = int(data[1])
    obj     = data[2]
    subruns = data[3]
    force_overwrite = data[4]

    valid_subruns = list(subruns)

    while len(valid_subruns) > 0:
        subrun_ = valid_subruns.pop(0)

	# get the filenames to transfer for this seb
        obj.connect()
        obj.info("Query status of %s"%("%s_%s"%(obj._parent_prefix,seb_)))
        status = ds_status( "%s_%s"%(obj._parent_prefix,seb_) , run, subrun_, kSTATUS_DONE )
	seb_status = obj._api.get_status(status)
	fname = seb_status._data
        obj.info("Got file name "+str(fname))
	
	if fname is None: 
            obj.error("Cannot lock on %s for run %s and subrun %s. Filename project not complete."% (seb_,str(run),str(subrun_)))
	    valid_subruns.append(subrun_)
	    continue
			
	dst_dir  = os.path.join(obj._file_destination,seb_)
	dst_file = os.path.join(dst_dir,os.path.basename(fname))
	
	# determine if file is already there
        SS = "if [ -e %s ]; then echo 1; else echo 0; fi" % dst_file
	ret = int(exec_ssh("vgenty",obj._remote_host,SS)[0])

	if ret == 1:
           obj.info("File exists already in destination directory @ %s"%seb_)
           
           if force_overwrite:
               # delete whats over there
               obj.info("FORCING OVERWRITE @ %s!"%seb_)
               SS = "rm -rf %s" % dst_file 
               del_ = int(exec_ssh("vgenty",obj._remote_host,SS)[0])

           else:

               # confirm the copied files is the same number of bytes else we have to try again
               obj.info("Confirm the copied size @ remote location")
               SS = "stat -c %%s %s" % dst_file
               dst_fsize = int(exec_ssh("vgenty",obj._remote_host,SS)[0])
               
               SS = "stat -c %%s %s" % fname
               org_fsize = int(exec_ssh("vgenty",seb_,SS)[0])
               
               if dst_fsize != org_fsize:
                   obj.error("Subrun %s could not be copied to seb %s... reinserting"%(subrun_,seb_))
                   # delete whats over there
                   SS = "rm -rf %s" % dst_file
                   del_ = int(exec_ssh("vgenty",obj._remote_host,SS)[0])
                   
                   valid_subruns.append(subrun_)

               else:
                   obj.info("%s valid subruns are %s len=%d"%(seb_,str(valid_subruns),len(valid_subruns)))
                   continue
        
        #
        # copy
        #
	# obj.info(" ==> Copying @ %s..."%seb_)
        # SS = "scp %s vgenty@%s:%s" % (fname,obj._remote_host,dst_dir) 
        # ret = exec_ssh("root",seb_,SS)
        # obj.info(" ==> Copied @ %s..."%seb_)

        #
        # alternate copy
        #
        obj.info(" ==> Alternate copy from %s"%seb_)
        alt_dir = "/build/vgenty/scratch/%s" % seb_

        SS = "scp %s vgenty@%s:%s" % (fname,obj._remote_host,alt_dir) 
        obj.info(SS)
        ret = exec_ssh("root",seb_,SS)

        alt_fout = os.path.join(alt_dir,os.path.basename(fname))
        SS = "scp %s %s" % (alt_fout,dst_dir) 
        obj.info(SS)
        ret = exec_ssh("vgenty",obj._remote_host,SS)

        SS = "rm -rf %s" % alt_fout
        obj.info(SS)
        ret = exec_ssh("vgenty",obj._remote_host,SS)

        #
	# confirm the copied files is the same number of bytes else we have to try again
        #
	obj.info("Confirm the copied size @ remote location")
        SS = "stat -c %%s %s" % dst_file
        dst_fsize = int(exec_ssh("vgenty",obj._remote_host,SS)[0])
	
        SS = "stat -c %%s %s" % fname
        org_fsize = int(exec_ssh("vgenty",seb_,SS)[0])
	
        #
        # is it the same size?
        #
	if dst_fsize != org_fsize:
            obj.error("Subrun %s could not be copied to seb %s... reinserting"%(subrun_,seb_))

	    # delete whats over there
            SS = "rm -rf %s" % dst_file
            del_ = int(exec_ssh("vgenty",obj._remote_host,SS)[0])
	    
	    valid_subruns.append(subrun_)
        else:
            obj.info("Copy complete @ %s valid subruns are %s len=%d"%(seb_,str(valid_subruns),len(valid_subruns)))

    obj.info("Thread @ %s complete"%seb_)
    return

def query_seb_snova_size(seb_ids,seb_names):
        # go through each of the SEBS and calculate the disk usage

        seb_size_v = [0.0]*len(seb_ids)

        for ix,seb in enumerate(seb_names):
            ret_ = exec_ssh("vgenty",seb,"df /datalocal/")
            size_, used_, unused_ = ret_[-1].split(" ")[6:9]
            seb_size_v[ix] = float(used_) / float(size_)
        
        max_idx = argmax(seb_size_v)

        return seb_size_v, max_idx

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

	self._seb_occupancy = float(0.7)

        self._parent_prefix = None
	self._fragment_prefix = None
	self._seb_ids = None
	self._ignore_runs = []
	self._max_run = None
	self._lock_file = None
	self._locked = False
	self._remote_host = None
	self._file_destination = None
	self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        
        self._parent_prefix = resource['REG_PREFIX']
      
 	self._fragment_prefix = resource['FRAGMENT_PREFIX']
      
        self._seb_ids = resource['SEBS'].split("-")

        self._seb_names    = ["seb%02d"%(int(parent_seb_id)) for parent_seb_id in self._seb_ids]
        self._seb_projectss = ["%s_%s"%(self._parent_prefix,seb_name) for seb_name in self._seb_names]

        if 'SEB_OCCUPANCY' in resource:
	    self._seb_occupancy = float(resource['SEB_OCCUPANCY'])

	if 'REMOTE_HOST' in resource:
            self._remote_host = str(resource['REMOTE_HOST'])

	if 'FILE_DESTINATION' in resource:
            self._file_destination = str(resource['FILE_DESTINATION'])

        if resource['IGNORE_RUNS'] != "":
            self._ignore_runs = [int(r_) for r_ in resource['IGNORE_RUNS'].split("-")]

	self._max_run = int(resource['MAX_RUN'])

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

        seb_size_v,max_idx = query_seb_snova_size(self._seb_ids,
						  self._seb_names)

        self.info("Start of Monitor... Max index: %d which is SEB %s used: %s"%(max_idx,self._seb_names[max_idx],str(seb_size_v[max_idx])))
	
	# if the largest SEB size is less than the occupancy don't do anything
	self._seb_occupancy = float(0.7)
	self.info("Comparing %s and %s" % (seb_size_v[max_idx], self._seb_occupancy))
        if seb_size_v[max_idx] < self._seb_occupancy: 
            self.info("Occupancy threshold %s not met" % str(self._seb_occupancy))
            return

        # fetch runs from DB and process for # runs specified for this instance.
        status_v=[kSTATUS_DONE]*len(self._seb_projectss)
        
        # each subrun is 1.5GB 
        ctr=-1

        logger = pub_logger.get_logger(self._project)
        reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)

	# self.info("Asking for earliest runs...")
	
	run_list_map = collections.OrderedDict()
	
	# for each seb get the earliest runs, add them to run_list_map
	for seb in self._seb_names:
            runs = reader.get_earliest_runs( "%s_%s"%(self._fragment_prefix,seb) )

	    if type(runs) is not list: continue

	    runs = [ int(run[0]) for run in runs ]
	    for run in runs:
                 try:
                      run_list_map[run].append(seb)
		 except KeyError:
		      run_list_map[run] = []
                      run_list_map[run].append(seb)
	    
	# no runs yet wait until next call
        if len(run_list_map.keys())<1: return

        # there was something sort the run list
	run_list_map= collections.OrderedDict(sorted(run_list_map.iteritems()))

	run = None
	for run_ in run_list_map.keys():
            if run_ in self._ignore_runs: continue
	    run = run_
	    break
	    
	self.info("Got run %s"%str(run))

	# delete a single run
	self._max_run = 100000
	if run >= self._max_run: return

	# get the sebs associated to this run
        sebs_v = run_list_map[run]
	
        death_star  = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)

	for seb_ in sebs_v:

            # ask a single seb for this runs subruns -- limit the return to 500 subruns
	    seb_del = ""
	    subruns = reader.get_subruns("%s_%s"%(self._fragment_prefix,seb_),run,500)
            
            self.info("Got subruns...")
	    self.info(str(subruns))

	    valid_subruns=[]
	    
	    for subrun_ in subruns:
                seb_status = None

		# try getting the status from file name SEB, if it don't exist, move on
		# to the next subrun
		try:
                    seb_status = self._api.get_status( ds_status( "%s_%s"%(self._parent_prefix,seb_) , run, subrun_, kSTATUS_DONE ) )
		except : 
                    continue
		
		fname = seb_status._data
		
		# filename returned was None, move onto next subrun
		if fname is None: 
                    continue
		
		# add this filename to the deletion string
		seb_del += str(fname + " ")
		
		# was a valid subrun
		valid_subruns.append(subrun_)
	    
	    # at least 1 valid subrun not found, move onto next seb
            if len(valid_subruns) < 1 : 
                continue

	    self.info(" ==> removing @ %s..."%seb_)
	    self.info("valid # subruns... %s" % str(valid_subruns))
            
            SS = "rm -rf %s"%seb_del
            ret = exec_ssh("root",seb_,SS)
	    self.info(ret)
	    runtable = "%s_%s"%(self._parent_prefix,seb_)
            self.info("... removing %s %d \n%s"%(runtable,run,str(valid_subruns)))
	    rundbWriter.star_destroyer(runtable,run,valid_subruns);
	    runtable = "%s_%s"%(self._fragment_prefix,seb_)
	    rundbWriter.star_destroyer(runtable,run,valid_subruns);
	    self.info("...done...")

        return

    def lock_observer( self ) :
	
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return
        	
 	death_star  = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)

	# check if the lock file exists, if so
	if os.path.isfile(self._lock_file) == False:
            self.info("Lock file does not exist.")
	    for seb_ in self._seb_names:
		runtable = 'lockedruntable_%s'%seb_
	 	res = rundbWriter.clear_death_star(runtable)	
		if res==2: self.info("Already cleared run table: %s"%runtable)
		if res==1: self.info("Clear a valid run table: %s"%runtable)
                if res==0: self.info("No runtable by name %s exist" % runtable)

	    self._locked = False
	    return
	
	# read the lock file
	data = None
	with open(self._lock_file,'r') as lf_:
            data = json.load(lf_)

	# get the runs to copy
	copyruns = None
	if data['transferall'] == False:
            copyruns = data['copyruns']

        self.info("Read the lock file and copyruns = %s"%str(copyruns))

        logger = pub_logger.get_logger(self._project)
        reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)

	if data['transfered'] == False:

            for run in copyruns:

                # if run < 12442: continue
                
                self.info("Parallelize @ run %d"%run)
                # q = Queue.Queue()
                # threads = []

                for seb in self._seb_names:
                    data_ = (seb,run,self,reader.get_subruns("%s_%s"%(self._fragment_prefix,seb),run,1000),False)
                    copy_subruns(data_)
                    
                #     q.put(data_)
                #     self.info("Moving onto next seb @ %s qsize %d" % (seb,q.qsize()))
                
                # for ix_ in xrange(len(self._seb_names)):
                #     t = threading.Thread(target=thread_worker,args=(q,))
                #     t.daemon = True
                #     threads.append(t)

                # for iy,thread in enumerate(threads):
                #     thread.start()
                #     self.info("Kicking off thread %d"%iy)
                #     time.sleep(10)

                # self.info("Blocking until complete, calling join...")
                # q.join() 
                self.info("... done copying run %d ?" % run)


        self.info("Done transfering?")
	# copy over the run tables
        for seb_ in self._seb_names:
	    inruntable  = 'fragmentruntable_%s' % seb_   
	    outruntable = 'lockedruntable_%s'   % seb_

	    res = rundbWriter.copy_death_star(inruntable,outruntable,copyruns)

   	    if res==2: self.info("Copy of %s into %s exists"  % (inruntable,outruntable))
   	    if res==1: self.info("Created copy of %s into %s" % (inruntable,outruntable))
	    if res==0: self.info("Failure to copy %s into %s" % (inruntable,outruntable))
        
        data['transfered']=True
	with open(self._lock_file,'w+') as lf_:
            json.dump(data,lf_)

	self._locked = True
	return

if __name__ == '__main__':
	
    proj_name = sys.argv[1]
	
    obj = monitor_snova( proj_name )

    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    obj.info('Observe lock')
    obj.lock_observer()

    obj.info('Monitor sebs')
    obj.monitor_sebs()

    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

