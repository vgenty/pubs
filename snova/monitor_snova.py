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

#
# worker thread for parallel scp
#
def thread_worker(q):
    while True:
        data = q.get()

        seb_ = str(data[0])
        run = int(data[1])
        obj = data[2]

        obj.info("@ thread worker got data seb: %s for run: %d" % (seb_,run))

        copy_subruns(data)

        obj.info("...complete @ seb: %s for run: %d" % (seb_,run))

        q.task_done()
        time.sleep(1)

        obj.info("q.qsize(): %d & q.empty(): %d" % (int(q.qsize()),int(q.empty())))

    return

#
# thread function
#
def copy_subruns(data):
    seb_    = str(data[0])
    run     = int(data[1])
    obj     = data[2]
    subruns = data[3]
    force_overwrite = data[4]

    #
    # copy subruns to valid_subruns
    # one at a time we will copy each one
    #
    valid_subruns = list(subruns)

    while len(valid_subruns) > 0:

        # get one subrun
        subrun_ = valid_subruns.pop(0)

	# get the filename to transfer for this seb into fname
        obj.connect()
        obj.info("Query status of %s"%("%s_%s"%(obj._parent_prefix,seb_)))
        status = ds_status( "%s_%s"%(obj._parent_prefix,seb_) , run, subrun_, kSTATUS_DONE )
	seb_status = obj._api.get_status(status)
	fname = seb_status._data
        obj.info("Got file name "+str(fname))
	
	if fname is None: 
            # subrun does not exist in db yet
            obj.error("Cannot lock on %s for run %s and subrun %s."% (seb_,str(run),str(subrun_)))
	    valid_subruns.append(subrun_)
	    continue
			
	dst_dir  = os.path.join(obj._file_destination,seb_)
	dst_file = os.path.join(dst_dir,os.path.basename(fname))
	
	# determine if file is already there
        SS = "if [ -e %s ]; then echo 1; else echo 0; fi" % dst_file
	ret = int(exec_ssh("vgenty",obj._remote_host,SS)[0])

	if ret == 1:
            # it's already here
           obj.info("File exists already in destination directory @ %s" % seb_)
           
           if force_overwrite:
               # delete whats over there
               obj.info("FORCING OVERWRITE @ %s!"%seb_)
               SS = "rm -rf %s" % dst_file 
               del_ = int(exec_ssh("vgenty",obj._remote_host,SS)[0])

           else:
               # confirm the copied files is the same number of bytes, else we have to try again
               obj.info("Confirm the copied size @ remote location")
               SS = "stat -c %%s %s" % dst_file
               dst_fsize = int(exec_ssh("vgenty",obj._remote_host,SS)[0])
               
               SS = "stat -c %%s %s" % fname
               org_fsize = int(exec_ssh("vgenty",seb_,SS)[0])
               
               if dst_fsize != org_fsize:
                   # delete whats over there
                   obj.error("Subrun %s could not be copied to seb %s... reinserting"%(subrun_,seb_))
                   SS = "rm -rf %s" % dst_file
                   del_ = int(exec_ssh("vgenty",obj._remote_host,SS)[0])
                   
                   valid_subruns.append(subrun_)

               else:
                   # it's fine move on
                   obj.info("%s valid subruns are %s len=%d"%(seb_,str(valid_subruns),len(valid_subruns)))
                   continue


        #
        # copy
        #
        obj.info(" ==> Copying @ %s..."%seb_)
        SS = "nice -19 ionice -c3 scp %s vgenty@%s:%s" % (fname,obj._remote_host,dst_dir) 
        ret = exec_ssh("vgenty",seb_,SS)
        obj.info(" ==> Copied @ %s..."%seb_)

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
            # it's there
            obj.info("Copy complete @ %s valid subruns are %s len=%d"%(seb_,str(valid_subruns),len(valid_subruns)))

    obj.info("Thread @ %s complete"%seb_)
    return


#
# monitor class
#
class monitor_snova( ds_project_base ):

    _project = 'monitor_snova'

    def __init__( self, arg = '' ):

        # Call base class ctor
        super( monitor_snova, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._in_dir = ''
        self._infile_format = ''
        self._data = ''
        self._parent_prefix = str("")
	self._fragment_prefix = str("")
	self._seb_ids = []
	self._ignore_runs = []
	self._lock_file = str("")
	self._locked = False
	self._remote_host = str("")
	self._file_destination = str("")
        self._min_occupancy = float(0.0)
        self._max_occupancy = float(0.0)
        self._drain_file = str("")
        self._user = str("")
	self.get_resource()
        
    #
    # fill class members from hstore
    #
    def get_resource(self):

        resource = self._api.get_resource( self._project )

        self._parent_prefix = resource['REG_PREFIX']
      
 	self._fragment_prefix = resource['FRAGMENT_PREFIX']
      
        self._seb_ids = resource['SEBS'].split("-")

        self._seb_names = ["seb%02d"%(int(parent_seb_id)) for parent_seb_id in self._seb_ids]

        self._min_occupancy = float(resource['SEB_MIN_OCCUPANCY'])
        self._max_occupancy = float(resource['SEB_MAX_OCCUPANCY'])

        self._remote_host = str(resource['REMOTE_HOST'])

        self._file_destination = str(resource['FILE_DESTINATION'])

        if resource['IGNORE_RUNS'] != "":
            self._ignore_runs = [int(r_) for r_ in resource['IGNORE_RUNS'].split("-")]

	self._lock_file = resource['LOCK_FILE']
        
        self._user = "vgenty"

        self._drain_file = resource['DRAIN_FILE']

    #
    # monitor disk usage of supernova datalocal
    #
    def monitor_sebs( self ):
	    
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return
        
        # Locked in copy to persistent space 
	if self._locked == True: 
	    self.info("Locked!")	
            return 

        # query sebs for occupancy
        seb_size_v, max_idx = query_seb_snova_size(self._seb_ids, self._seb_names)
        
        # max occupancy seb
        curr_occupancy = seb_size_v[max_idx]

        self.info("Monitor. max index: %d which is SEB %s used: %s"%(max_idx,
                                                                     self._seb_names[max_idx],
                                                                     str(seb_size_v[max_idx])))
        seb_occupancy = 0.0

        # are we draning?
        drain = read_drain(self._drain_file)
        self.info("Drain: %s" % str(drain))

        if drain == 1:
            # yes drain down to 
            seb_occupancy = float(self._min_occupancy)
            
        if drain == 0: 
            # no let it fill to self._max_occupancy
            seb_occupancy = float(self._max_occupancy)

	self.info("Comparing %s and %s" % (curr_occupancy, seb_occupancy))

        if drain == False and curr_occupancy > self._max_occupancy:
            # start drain
            start_drain(self._drain_file)
            seb_occupancy = float(self._min_occupancy)

        if  curr_occupancy < seb_occupancy: 
            # dont do anything
            self.info("Occupancy threshold %s not met" % str(seb_occupancy))
            
            # stop drain
            if seb_occupancy == self._min_occupancy:
                stop_drain(self._drain_file)

            return
        

        # fetch runs from DB and process for # runs specified for this instance.
        status_v=[kSTATUS_DONE]*len(self._seb_names)
        
        ctr = -1

        logger = pub_logger.get_logger(self._project)
        reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)

	run_list_map = collections.OrderedDict()
	
	# for each seb get the earliest runs, add them to run_list_map
	for seb in self._seb_names:
            runs = reader.get_earliest_runs( "%s_%s"%(self._fragment_prefix,seb) )

	    if type(runs) is not list: continue

	    runs = [int(run[0]) for run in runs]

	    for run in runs:
                 try:
                      run_list_map[run].append(seb)
		 except KeyError:
		      run_list_map[run] = []
                      run_list_map[run].append(seb)
	    
	# no runs yet wait until next call
        if len(run_list_map.keys())<1: return

        # sort the run list by lowest run & subrun
	run_list_map = collections.OrderedDict(sorted(run_list_map.iteritems()))

	run = None
	for run_ in run_list_map.keys():
            if run_ in self._ignore_runs: continue
	    run = run_
	    break
	    
	self.info("Got run %s"%str(run))

	# get the sebs associated to this run
        sebs_v = run_list_map[run]
	
        death_star  = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)

	for seb_ in sebs_v:
            # ask a single seb for this runs subruns -- limit the return to 1e6 subruns
	    seb_del = ""
	    subruns = reader.get_subruns("%s_%s"%(self._fragment_prefix,seb_),run,1e6)

	    valid_subruns = []
	    
	    for subrun_ in subruns:
                seb_status = None

		# try getting the status from file name SEB, if it don't exist, move on
		# to the next subrun
		try:
                    seb_status = self._api.get_status(ds_status("%s_%s"%(self._parent_prefix,seb_), 
                                                                run, 
                                                                subrun_, 
                                                                kSTATUS_DONE))
		except: 
                    continue
		
		fname = seb_status._data
		
		# filename returned was None, move onto next subrun
		if fname is None: continue
		
		# add this filename to the deletion string
		seb_del += str(fname + " ")
		
		# is valid subrun!
		valid_subruns.append(subrun_)
	    
	    # at least 1 valid subrun not found, move onto next seb
            if len(valid_subruns) < 1: continue

	    self.info("Removing @%s..."%seb_)
	    self.info("...valid # subruns: %s" % str(valid_subruns))
            
            # split argument list into a 100 file chunk (to avoid too-long-argument-list)
            split_seb_del = list(seb_del.split(" "))
            n = int(100)
            split_seb_del = [split_seb_del[i:i + n] for i in xrange(0, len(split_seb_del), n)]
            
            # delete
            for seb_del in split_seb_del:
                SD = ""
                for sd in seb_del:
                    SD += "\"" + sd + "\""
                    SD += " "
                SS = "for f in %s; do nice -19 ionice -c3 rm -rf $f; done" % str(SD)
                ret = exec_ssh("root",seb_,SS)

            # get this sebs project table and remove the deleted runs
	    runtable = "%s_%s"%(self._parent_prefix,seb_)
            self.info("... removing %s %d \n%s"%(runtable,run,str(valid_subruns)))
	    rundbWriter.star_destroyer(runtable,run,valid_subruns);

            # get this sebs master runtable and remove the deleted runs
	    runtable = "%s_%s"%(self._fragment_prefix,seb_)
	    rundbWriter.star_destroyer(runtable,run,valid_subruns);
	    self.info("done")
            
        return

    #
    # check if the lock file exists, if so, freeze
    #
    def lock_observer(self) :
	
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return
        	
 	death_star  = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),death_star)

	# check if the lock file exists
	if os.path.isfile(self._lock_file) == False:
            # does not exist
            self.info("Lock file does not exist.")
	    for seb_ in self._seb_names:
                # clear out the frozen run tables (may be empty already)
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

        # has the data been trasnfered already?
	if data['transfered'] == False:

            for run in copyruns:
                
                for seb in self._seb_names:
                    data_ = (seb,
                             run,
                             self,
                             reader.get_subruns("%s_%s"%(self._fragment_prefix,seb),run,1e6),False)

                    copy_subruns(data_)
                 
                #
                # parallelizing was a _sad_ failure, frequent
                # ``** glibc detected *** python: double free or corruption''
                # at random times
                #

                # self.info("Parallelize @ run %d"%run)
                # q = Queue.Queue()
                # threads = []

                # for seb in self._seb_names:
                #     data_ = (seb,run,self,reader.get_subruns("%s_%s"%(self._fragment_prefix,seb),run,100000),False)
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

                self.info("... done copying run %d" % run)


        self.info("Done transfering.")

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

