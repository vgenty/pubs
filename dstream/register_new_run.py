## @namespace dstream.REGISTER_NEW_RUN
#  @ingroup dstream
#  @brief Defines a project register_new_run
#  @author david caratelli

# python include
import time
import os, sys
# pub_dbi package include
from pub_dbi import DBException, pubdb_conn_info
# pub_util package include
from pub_util import pub_logger
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream import ds_api
from pub_dbi.pubdb_conn import pubdb_conn

## @class register_new_run
#  @brief register a new run number in the DB
#  @details
#  check closed files written to disk by DAQ
#  and save run/subrun info for this file
#  to DB.
#  DAQ creates new run/subrun numbers but does
#  not log this info to DB (to reduce dependency)
#  this project bridges the gap and makes sure
#  information for new runs/subruns is promptly
#  stored in the database

class register_new_run(ds_project_base):

    # Define project name as class attribute
    _project = 'register_new_run'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super(register_new_run,self).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._data_dir = [] # list of directories where to find binary data
        self_run_bound = [] # list of RUN numbers defining the upper bound of the run number in a given directory
        self._experts = ''
        self._runtable = ''
        self._suffix = ''

        # Offline PUBS run table insertion needs a separate DB handle
        self._offline_conn_info = pubdb_conn_info(os.environ['OFFLINE_PUB_PSQL_ADMIN_HOST'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_PORT'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_DB'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_USER'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_PASS'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_CONN_NTRY'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_CONN_SLEEP'],
                                                  os.environ['OFFLINE_PUB_PSQL_ADMIN_ROLE'])

        self._offline_cursor=None

        self.get_resource()

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._data_dir  = resource['DATADIR'].split(":")
        self._run_bound = [ int(x) for x in resource['RUNBOUND'].split(":") ]
        self._experts   = resource['EXPERTS']
        self._runtable  = resource['RUNTABLE']
        if 'SUFFIX' in resource:
            self._suffix = resource['SUFFIX']

        if not self._offline_cursor:
            self._offline_cursor = pubdb_conn.cursor(self._offline_conn_info)
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
            return


        # loop through all directories in which data is to be found
        for path_num in xrange(len(self._data_dir)):

            data_path = self._data_dir[path_num]

            self.info('Start access in data directory %s'%data_path)

            run_lim = None
            # if we are not yet at the last directory (for which there should be no run limit)
            if ( path_num < len(self._run_bound) ):
                run_lim = self._run_bound[path_num]
                self.info('Run limit for this directory: %i'%run_lim)

            # get ALL closed data files in DATADIR
            if (os.path.isdir(data_path) == False):
                self.error('DATA DIR %s does not exist'%data_path)
                return

            self.info('Looking for data files in: %s'%data_path)

            dircontents = []
            if self._suffix:
                dircontents = [x for x in os.listdir(data_path) if x.endswith(self._suffix)]
            else:
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
            
                filepath = data_path+'/'+f

                # check that this is a file
                if (os.path.isfile(filepath) == False):
                    continue
            
                try:
                    
                    time_create  = os.path.getctime(filepath)
                    time_modify = os.path.getmtime(filepath)
                
                    # file format:
                    # NoiseRun-YYYY_M_DD_HH_MM_SS-RUN-SUBRUN.ubdaq
                    run    = int(f.replace('.ubdaq','').split('-')[-2])
                    subrun = int(f.replace('.ubdaq','').split('-')[-1])
                    #self.info('found run (%i, %i)'%(run,subrun))
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
            logger = pub_logger.get_logger('register_new_run')
            reader = ds_api.ds_reader(pubdb_conn_info.reader_info(), logger)
            last_recorded_info = reader.get_last_run_subrun(self._runtable)

            # log which (run,subrun) pair was added last
            self.info('last recorded (run,subrun) is (%d,%d)'%(int(last_recorded_info[0]),int(last_recorded_info[1])))
            self.info('No run with (run,subrun) smaller than this will be added to the RunTable')

            # if we made it this far the file info needs to be
            # recorded to the database
            # DANGER *** DANGER *** DANGER *** DANGER
            # we will now invoke the death_start
            # this API will access the RUN table end edit
            # informaton, which is exactly what we need to do
            # however, this is dangerous and you should not
            # copy this code and re-use it somewhere
            # if you do, the Granduca's wrath will be upon you
            # lucikly for you, the Granduca was the first to
            # abolish the death penalty on November 30th 1786
            # http://en.wikipedia.org/wiki/Grand_Duchy_of_Tuscany#Reform
            # However, the imperial army may be less mercyful.
            # DANGER *** DANGER *** DANGER *** DANGER
            logger = pub_logger.get_logger('death_star')
            rundbWriter = ds_api.death_star(pubdb_conn_info.admin_info(),logger)
            
            # loop through dictionary keys and write to DB info
            # for runs/subruns not yet stored
            for info in sorted_file_info:
                
                # this key needs to be larger than the last logged value
                # but less than the last element in the dictionary
                if (info >= max_file_info):
                    continue;
                if (run_lim):
                    if ( info[0] > run_lim):
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
                    rundbWriter.insert_into_death_star(self._runtable,info[0],info[1],file_creation, file_closing)
                    # offline
                    self.debug('filling death star offline...')
                    query = 'SELECT InsertIntoTestRunTable(\'%s\',%d,%d,\'%s\'::TIMESTAMP,\'%s\'::TIMESTAMP)' % (self._runtable,
                                                                                                                 info[0],
                                                                                                                 info[1],
                                                                                                                 file_creation,
                                                                                                                 file_closing) 
                    self.debug(query)
                    self._offline_cursor.execute(query)
                    pubdb_conn.commit(self._offline_conn_info)
                    # Report starting
                    self.info('recording info for new run: run=%d, subrun=%d ...' % (int(run),int(subrun)))

                except:
                    
                    # we did not succeed in adding this (run,subrun)
                    self.info('FAILED to add run=%d, subrun=%d to RunTable'%(int(run),int(subrun)))



# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    test_obj = register_new_run( proj_name )

    test_obj.process_newruns()



