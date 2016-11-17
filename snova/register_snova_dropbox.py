
import time
import os, sys
from pub_dbi import DBException, pubdb_conn_info
from pub_util import pub_logger
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_api
from pub_dbi.pubdb_conn import pubdb_conn

class register_snova_dropbox(ds_project_base):

    # Define project name as class attribute
    _project = 'register_snova_dropbox'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super(register_snova_dropbox,self).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._data_dir = [] # list of directories where to find binary data

        self._runtable = ''

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
            
        self.info('Start access in data directory %s'%data_path)
        self.info('Looking for data files in: %s'%data_path)

        dircontents = []
        dircontents = os.listdir(data_path)

        # create a dictionary to keep track of
        # - file name ----- NAME
        # - run number ---- RUN
        # - subrun number - SUBRUN
        # - time-create --- TIMEC
        # - time-modify --- TIMEM
        # : dictionary key: ------ (RUN,SUBRUN)
        # : dictionary content: -- (NAME,TIMEC,TIMEM)

        file_info = {}
        
        for f in dircontents:
                
            filepath = os.path.join(data_path,f)
            
            # check that this is a file
            if (os.path.isfile(filepath) == False):
                continue

            try:
                    
                time_create  = os.path.getctime(filepath)
                time_modify  = os.path.getmtime(filepath)
                    
                # file format:
                # snova_RUN-SUBRUN.bin

                run    = int(f.split('.')[0].split('_')[-1].split('-')[1])
                subrun = int(f.split('.')[0].split('_')[-1].split('-')[2])
                             
                #self.info('Found run %i, %i in dropbox from file %s)'%(run,subrun,f))
                file_info[tuple((run,subrun))] = [f,time_create,time_modify]
                
            except:
                
                # if file-name is .ubdaq then we have a problem
                # were not able to read run-subrun info from file
                if (f.find('.ubdaq')):
                    self.info('Could not read RUN/SUBRUN info for file %s'%f)

            # sort the dictionary
            # we want to ignore the largest run/subrun information
            # this will prevent us from potentially logging info
            # for a file that has not yet been closed
            sorted_file_info = sorted(file_info)
            # get tuple with largest run/subrun info found in files
            max_file_info = sorted_file_info[-1]
            
            # fetch from database the last run/subrun number recorded
            logger = pub_logger.get_logger(self._project)
            reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)
            last_recorded_info = reader.get_last_run_subrun(self._runtable)

            # log which (run,subrun) pair was added last
            #self.info('last recorded (run,subrun) is (%d,%d)'%(int(last_recorded_info[0]),int(last_recorded_info[1])))
            #self.info('No run with (run,subrun) smaller than this will be added to the RunTable')

            logger = pub_logger.get_logger('death_star')
            rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),logger)

            #self.info("max file info: %s"%str(max_file_info))
            #self.info("last recorded info: %s"%str(last_recorded_info))
            
            # loop through dictionary keys and write to DB info
            # for runs/subruns not yet stored
            for info in sorted_file_info:
                
                # this key needs to be larger than the last logged value
                # but less than the last element in the dictionary
                if (info >= max_file_info):
                    continue;

                if (info <= last_recorded_info):
                    continue;

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
                    
                    self.info('filling death star...')

                    # insert into the death start
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

    proj_name = 'register_snova_dropbox_%s'%sys.argv[1]

    test_obj = register_snova_dropbox( proj_name )

    test_obj.process_newruns()



