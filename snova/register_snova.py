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

        self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._data_dir  = resource['DATADIR']
        self._runtable  = resource['RUNTABLE']
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
            return

        data_path = self._data_dir

        # self.info('Start access in data directory %s'%data_path)
        # self.info('Looking for data files in: %s'%data_path)
        
        #execute a single command to get all files in snova directory
        dir_flist=exec_system(["ssh", self._sebname, "ls -f -1 %s"%data_path])[2:]

        # self.info("Sorting dir_flist size: %s",str(len(dir_flist)))
        dir_flist.sort(key=lambda x : int("".join(x.split('.')[0].split('_')[-1].split('-')[1:])))

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
        last_recorded_info   = reader.get_last_run_subrun(self._runtable)
        
        file_info = {}
        
        self.info("Got last recorded info %s"%str(last_recorded_info))

        #do how many at a time, 1000?
        ik=0
        imax=10000
        for f_ in dir_flist:
            
            try:
                rs_tmp=f_.split('.')[0].split('_')[-1].split('-')
                run    = int(rs_tmp[1])
                subrun = int(rs_tmp[2])
                
                run_subrun_t=tuple((run,subrun))

                if run_subrun_t <= last_recorded_info: 
                    continue

                file_info[run_subrun_t] = [f_,0.0,0.0]

                ik+=1

                if ik==imax: break

            except:
                
                # if file-name is .ubdaq then we have a problem
                # were not able to read run-subrun info from file
                if (f.find('.ubdaq')):
                    self.info('Could not read RUN/SUBRUN info for file %s'%f)

        # self.info("Sorting file_info size: %s",str(len(file_info)))
        sorted_file_info = sorted(file_info)
        max_file_info = sorted_file_info[-1]
        # self.info("Query seb")
        #lets do a big query for the creation and modified times for these files over ssh
        sshproc = subprocess.Popen(['ssh','-T',self._sebname], 
                                   stdin=subprocess.PIPE, stdout = subprocess.PIPE, 
                                   universal_newlines=True,bufsize=0)

        for f_ in sorted_file_info:

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
                
        for ix,run_subrun in enumerate(sorted_file_info):
            
            time_create,time_modify=values[ix].split("-")
            
            file_info[run_subrun][1]=time_create
            file_info[run_subrun][2]=time_modify

        logger = pub_logger.get_logger('death_star')
        rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),logger)
            
        # loop through dictionary keys and write to DB info
        # for runs/subruns not yet stored
        for info in sorted_file_info:
            
            # this key needs to be larger than the last logged value
            # but less than the last element in the dictionary
            if (info >= max_file_info):
                continue;

            # self.info('Trying to add to RunTable (run,subrun) = (%d,%d)'%(int(info[0]),int(info[1])))

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

                

# A unit test section
if __name__ == '__main__':

    proj_name = 'register_snova_%s'%sys.argv[1]

    test_obj = register_snova( proj_name , sys.argv[1] )

    test_obj.process_newruns()



