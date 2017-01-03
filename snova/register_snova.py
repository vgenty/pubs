import time,subprocess
import os, sys
from pub_dbi import DBException, pubdb_conn_info
from pub_util import pub_logger
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_api
from pub_dbi.pubdb_conn import pubdb_conn
from snova_util import *
from collections import OrderedDict

class register_snova(ds_project_base):

    # Define project name as class attribute
    _project = 'register_snova'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' , sebname = ''):

        # Call base class ctor
        super(register_snova,self).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._data_dir = [] # list of directories where to find binary data

        self._runtable = ''

        self._sebname=sebname
        
        self._observed_files = {}
         
        self._locked = False
        self._lock_file = None
      
        self.get_resource()



    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._data_dir  = resource['DATADIR']
        self._runtable  = resource['RUNTABLE']
        self._lock_file = resource['LOCK_FILE']
 
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
            return

        if self._locked == True: 
            self.info("Locked.")
            return

        data_path = self._data_dir

        #execute a single command to get all files in snova directory
        dir_flist=exec_system(["ssh", self._sebname, "ls -f -1 %s"%data_path])[2:]

        od = OrderedDict()
        for res_ in dir_flist:
            split_ = res_.split('.')[0].split('_')[-1].split('-')
            run_    = int(split_[1])
            subrun_ = int(split_[2])
            od[tuple((run_,subrun_))] = [res_,0.0,0.0]
        
        #order by run and subrun
        od = OrderedDict(sorted(od.iteritems()))

        # create a dictionary to keep track of
        # - file name ----- NAME
        # - run number ---- RUN
        # - subrun number - SUBRUN
        # - time-create --- TIMEC
        # - time-modify --- TIMEM
        # : dictionary key: ------ (RUN,SUBRUN)
        # : dictionary content: -- (NAME,TIMEC,TIMEM)

        logger = pub_logger.get_logger(self._project)
        reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)
        last_recorded_info = reader.get_last_run_subrun(self._runtable)

        file_info=OrderedDict()
        
        ik=0
        ikmax=1000
        self.info("Got last recorded info %s"%str(last_recorded_info))

        for k_,v_ in od.iteritems():
            if ( k_[0]  < last_recorded_info[0] ): continue
            if ( k_[0] == last_recorded_info[0] and 
                 k_[1] <= last_recorded_info[1]): continue

            file_info[k_]=v_
            ik+=1
            if ik==ikmax: break
                

        file_info.popitem()

        #lets do a large query for the creation and modified times for these files over ssh
        sshproc = subprocess.Popen(['ssh','-T',self._sebname], 
                                   stdin=subprocess.PIPE, 
                                   stdout = subprocess.PIPE, 
                                   universal_newlines=True,bufsize=0)

        for f_ in file_info:
            filepath=os.path.join(data_path,file_info[f_][0])

            cmd="stat -c %%Y-%%Z %s"%filepath

            sshproc.stdin.write("%s\n"%cmd)
            sshproc.stdin.write("echo END\n")

        sshproc.stdin.close()

        values=[]

        ic=0
        for return_ in sshproc.stdout:
            if return_.rstrip('\n')!="END":
                values.append(return_.rstrip('\n'))
                ic+=1
                
        for ix,run_subrun in enumerate(file_info):
            
            time_create,time_modify=values[ix].split("-")
            
            file_info[run_subrun][1]=time_create
            file_info[run_subrun][2]=time_modify

        logger = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),logger)
            
        # loop through dictionary keys and write to DB info
        # for runs/subruns not yet stored
        for info in file_info:
            
            self.info('Trying to add to RunTable (run,subrun) = (%d,%d)'%(int(info[0]),int(info[1])))

            try:

                # info is key (run,subrun)
                # dictionary value @ key is array
                # [file name, time_crate, time_modify]
                run           = info[0]
                subrun        = info[1]
                run_info      = file_info[info]
                file_creation = time.gmtime(int(run_info[1]))
                file_closing  = time.gmtime(int(run_info[2]))
                file_creation = time.strftime('%Y-%m-%d %H:%M:%S',file_creation)
                file_closing  = time.strftime('%Y-%m-%d %H:%M:%S',file_closing)

                # insert into the death star
                rundbWriter.insert_into_death_star(self._runtable,
                                                   run,
                                                   subrun,
                                                   file_creation,
                                                   file_closing)


                # Report starting
                # self.info('recording info for new run: run=%d, subrun=%d ...' % (int(run),int(subrun)))
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = 0,
                                    data    = os.path.join(data_path,run_info[0]))

                    
            except:
                    
                # we did not succeed in adding this (run,subrun)
                self.info('FAILED to add run=%d, subrun=%d to RunTable'%(int(run),int(subrun)))

    def monitor_lock(self):
        self.info("Observing lock @ %s. Current state: %r"  % (self._lock_file,self._locked))

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # check if the lock file exists, if so
        if os.path.isfile(self._lock_file) == False:
            self.info("Lock does not exist.")
            self._locked = False
	    return

	self._locked = True

        self.info("Lock exists. Current state: %r" % self._locked)

	return
			
 

# A unit test section
if __name__ == '__main__':

    proj_name = 'register_snova_%s'%sys.argv[1]

    test_obj = register_snova( proj_name , sys.argv[1] )
    
    test_obj.monitor_lock()

    test_obj.process_newruns()



