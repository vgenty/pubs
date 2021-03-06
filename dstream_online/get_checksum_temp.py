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
import samweb_client.utility
import traceback
# script module tools
from scripts import find_run

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
        # self._parent_project = ''
        self._experts = ''
        self._data = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        # self._parent_project = resource['PARENT_PROJECT']
        self._experts = resource['EXPERTS']


    ## @brief calculate the checksum of a file
    def calculate_checksum( self ):

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
        for x in self.get_runs( self._project, 1 ):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))
            # Report starting
            self.info('Calculating the file checksum: run=%d, subrun=%d ...' % (run,subrun))

            statusCode = 1

            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
            if (len(filelist)>1):
                self.error('ERROR: There is more than one file matching that pattern: %s' % in_file_name)
            if (len(filelist)<1):
                errorMessage = "Failed to find file%s"%in_file_holder
                subject = "get_checksum_temp Failed to find file%s"%in_file_holder
                text = """File: %s
Error message:
%s
                """ % ( in_file, errorMessage )
                pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )
                statusCode = 200
            else:
                in_file = filelist[0]
                metadata = {}
                try:
                    metadata['crc'] = samweb_client.utility.fileEnstoreChecksum( in_file )
                    self._data = metadata['crc']['crc_value']
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
            self.info('Calculating the file checksum: run=%d, subrun=%d ...' % (run,subrun))

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
