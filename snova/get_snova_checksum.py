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

import subprocess

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
        self._in_dir = str("")
        self._infile_format = str("")
        self._parent_project = str("")
        self._experts = str("")
        self._data = str("")
        self._max_proc_time = int(30)
        self._min_run = int(0)

        self._seb= str("")
	self._remote_host = str("")
	self._file_destination = str("")
        self._user = str("")

        self.get_resource()
        
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])

        self._parent_project = resource['PARENT_PROJECT']

	self._experts = resource['EXPERTS']

        self._max_proc_time = int(60)

        self._seb = resource["SEB"]

        self._remote_host = str(resource['REMOTE_HOST'])
        
        self._file_destination = str(resource['FILE_DESTINATION'])

        self._user = str(resource['USER'])
        
        return
        
    ## @brief calculate the checksum of a file
    def calculate_checksum( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()
            
        self.info("Calculating checksum...")

        # Fetch runs from DB and process for # runs specified for this instance.
        runlist = self.get_xtable_runs( [self._project, self._parent_project], [kSTATUS_INIT, kSTATUS_DONE] )

        ctr = self._nruns
        in_file_v = []
        runid_v = []
        
        # slice the run list upto counter
        sliced_runlist = runlist[:ctr]
            
        for x in sliced_runlist:
            # Break from loop if counter became 0
            if ctr <= 0: break

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            # Counter decreases by 1
            ctr -= 1

            # Report starting
            self.info('Calculating the file checksum: run=%d subrun=%d @ %s' % (run,subrun,time.strftime('%Y-%m-%d %H:%M:%S')))
            
            ref_status = self._api.get_status( ds_status( self._parent_project, run, subrun, kSTATUS_DONE ) )
            fname = os.path.join(self._file_destination, self._seb, os.path.basename(ref_status._data))

            self.info("Got ref_status: %s" % str(ref_status._data))
            self.info("Got fname: %s" % str(fname))

            if "ubdaq" not in fname:
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND ) )
                continue
            
            in_file_v.append(fname)
            runid_v.append((run,subrun))
            

        for i in xrange(len(in_file_v)):
            self.info(" @i=%d"%int(i))
            f = in_file_v[i]

            self.info('Calculating checksum for: %s @ %s' % (f,time.strftime('%Y-%m-%d %H:%M:%S')))

            out = query_checksum(self._user,self._remote_host,f)
            
            self.info("Got return %s"%str(out[0]))

            if not out:
                self.error('Checksum calculation failed for %s' % in_file_v[i])
                self.error(out)
                self.log_status( ds_status( project = self._project,
                                            run     = runid_v[i][0],
                                            subrun  = runid_v[i][1],
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_CHECKSUM_CALCULATION_FAILED,
                                            data    = '' ) )
                continue

            statusCode = kSTATUS_INIT
            try:
                exec("checksum = %s " % out[0])
                checksum = checksum[0].split(":")[-1]

                self._data = in_file_v[i]+":"+checksum
                statusCode = kSTATUS_DONE
                self.info("Set CRC: %s on file: %s"%(checksum,in_file_v[i]))

            except Exception:
                errorMessage = traceback.print_exc()
                subject = 'Failed to obtain the checksum of the file %s' % in_file_v[i]
                text = """File: %s
                          Error message:
                          %s
                """ % ( in_file_v[i], errorMessage )

                pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )
                self._data = ''
                
            self.log_status( ds_status( project = self._project,
                                        run     = runid_v[i][0],
                                        subrun  = runid_v[i][1],
                                        seq     = 0,
                                        status  = statusCode,
                                        data    = self._data ) )
        
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

            statusCode = kSTATUS_TO_BE_VALIDATED

            # Get status object
            status = self._api.get_status(ds_status(self._project,
                                                    x[0],x[1],x[2]))

            self._data = status._data
            self._data = str( self._data )

            if self._data:
               statusCode = 0
            else:
                subject = 'Checksum of the run %s and subrun %s not in database' % (str(run),str(subrun))
                text = """Checksum for run %s and subrun %s is not in database""" % ( str(run),str(subrun))

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
    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
    obj.calculate_checksum()
    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
