## @namespace dstream_online.get_checksum
#  @ingroup get_checksum
#  @brief Defines a project get_checksum
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
from ds_online_env import *
import samweb_client.utility
import traceback
import glob

class get_checksum( ds_project_base ):

    _project = 'get_checksum'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( get_checksum, self ).__init__( arg )

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
        self._nruns_to_postpone = 0
        self._parallelize = 0
        self._max_proc_time = 30

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']

        if 'PARENT_PROJECT' in resource:
            self._parent_project = resource['PARENT_PROJECT']
        self._experts = resource['EXPERTS']

        try:
            self._nruns_to_postpone = int(resource['NRUNS_POSTPONE'])
            self.info('Will process %d runs to be postponed (status=%d)' % (self._nruns_to_postpone,kSTATUS_POSTPONE))
        except KeyError,ValueError:
            pass

        if 'PARALLELIZE' in resource:
            self._parallelize = int(resource['PARALLELIZE'])
        if 'MAX_PROC_TIME' in resource:
            self._max_proc_time = int(resource['MAX_PROC_TIME'])

    ## @brief calculate the checksum of a file
    def calculate_checksum( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        #self.info('Here, self._nruns=%d ... ' % (self._nruns))


        #
        # Process Postpone first
        #
        ctr_postpone = 0
        parent_list = []
        if self._parent_project: parent_list.append(self._parent_project)
        for parent in parent_list:
            if ctr_postpone >= self._nruns_to_postpone: break
            if parent == self._project: continue
        
            postpone_name_list = [self._project, parent]
            postpone_status_list = [kSTATUS_INIT, kSTATUS_POSTPONE]
            target_runs = self.get_xtable_runs(postpone_name_list,postpone_status_list)
            self.info('Found %d runs to be postponed due to parent %s...' % (len(target_runs),parent))
            for x in target_runs:
                status = ds_status( project = self._project,
                                    run     = int(x[0]),
                                    subrun  = int(x[1]),
                                    seq     = 0,
                                    status  = kSTATUS_POSTPONE )
                self.log_status(status)
                ctr_postpone += 1
                if ctr_postpone > self._nruns_to_postpone: break

        # Fetch runs from DB and process for # runs specified for this instance.
        runlist=[]
        if self._parent_project:
            runlist = self.get_xtable_runs( [self._project, self._parent_project], [1, 0] )
        else:
            runlist = self.get_runs(self._project,1)
        ctr = self._nruns
        in_file_v = []
        runid_v = []
        for x in runlist:

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            #self.info('Calculating the file checksum: run=%d subrun=%d @ %s' % (run,subrun,time.strftime('%Y-%m-%d %H:%M:%S')))

            statusCode = 1

            in_file_name = self._infile_format % ( run, subrun )
            in_file_holder = '%s/%s' % ( self._in_dir, in_file_name )
            filelist = glob.glob(in_file_holder)
            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('List of files found %s' % filelist)

            in_file_v.append(filelist[0])
            runid_v.append((run,subrun))
            # Break from loop if counter became 0
            if not ctr: break
        
        mp = self.process_files(in_file_v)

        for i in xrange(len(in_file_v)):

            (out,err) = mp.communicate(i)
            
            if err or not out:
                self.error('Checksum calculculation failed for %s' % in_file_v[i])
                self.error(err)
                self.log_status( ds_status( project = self._project,
                                            run     = runid_v[i][0],
                                            subrun  = runid_v[i][1],
                                            seq     = 0,
                                            status  = 101,
                                            data    = '' ) )
                continue

            statusCode = 1
            try:
                metadata=None
                exec('metadata = %s' % out)
                self._data = metadata['crc_value']
                statusCode = 0
            except Exception:
                errorMessage = traceback.print_exc()
                subject = 'Failed to obtain the checksum of the file %s' % in_file
                text = """File: %s
Error message:
%s
                """ % ( in_file, errorMessage )

                pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )
                statusCode = 100
                self._data = ''
                
            self.log_status( ds_status( project = self._project,
                                        run     = runid_v[i][0],
                                        subrun  = runid_v[i][1],
                                        seq     = 0,
                                        status  = statusCode,
                                        data    = self._data ) )

    ## @brief process multiple files checksum calculation
    def process_files(self, in_file_v):

        mp = ds_multiprocess(self._project)

        cmd_template = 'python -c "import samweb_client.utility;print samweb_client.utility.fileEnstoreChecksum(\'%s\')"'

        for f in in_file_v:
            self.info('Calculating checksum for: %s @ %s' % (f,time.strftime('%Y-%m-%d %H:%M:%S')))
            cmd = cmd_template % f
            print cmd
            index,active_ctr = mp.execute(cmd)

            if not self._parallelize:
                mp.communicate(index)
            else:
                time_slept = 0
                while active_ctr > self._parallelize:
                    time.sleep(0.2)
                    time_slept += 0.2
                    active_ctr = mp.active_count()

                    if time_slept > self._max_proc_time:
                        self.error('Exceeding time limit %s ... killing %d jobs...' % (self._max_proc_time,active_ctr))
                        mp.kill()
                        break
                    if int(time_slept) and int(time_slept)%3 < 0.3 == 0:
                        self.info('Waiting for %d/%d process to finish...' % (active_ctr,len(in_file_v)))
        time_slept=0
        while mp.active_count():
            time.sleep(0.2)
            time_slept += 0.2
            if time_slept > self._max_proc_time:
                mp.kill()
                break
        return mp
        
    ## @brief check the checksum is in the table
    def check_db( self ):
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs( self._project, 2 ):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Calculating the file checksum: run=%d subrun=%d @ %s' % (run,subrun,time.strftime('%Y-%m-%d %H:%M:%S')))

            statusCode = 2
            in_file_name = self._infile_format % ( run, subrun )
            in_file = '%s/%s' % ( self._in_dir, in_file_name )

            # Get status object
            status = self._api.get_status(ds_status(self._project,
                                                    x[0],x[1],x[2]))

            self._data = status._data
            self._data = str( self._data )

            if self._data:
               statusCode = 0
            else:
                subject = 'Checksum of the file %s not in database' % in_file
                text = """File: %s
Checksum is not in database
                """ % ( in_file )

                pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

                statusCode = 100

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = statusCode,
                                data    = self._data )

            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break


if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = get_checksum( proj_name )

    obj.calculate_checksum()
