## @namespace dstream_online.COMPARE_CHECKSUM
#  @ingroup dstream_online
#  @brief Defines a project compare_checksum
#  @author yuntse

# python include
import time, sys, os
# pub_dbi package include
from pub_dbi import DBException
# pub_util package include
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class compare_checksum
#  @brief yuntse should give a brief comment here
#  @details
#  yuntse should give a detailed comment here
class compare_checksum( ds_project_base ):

    # Define project name as class attribute
    _project = 'compare_checksum'

    # Define # of runs to process per request
    _nruns   = 5

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '', nruns = None ):

        # Call base class ctor
        super( compare_checksum, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        if nruns:

            self._nruns   = int(nruns)

        self._ref_project = ''
        self._parent_project = ''
        self._experts = ''
        self._data = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._ref_project = resource['REF_PROJECT']
        self._parent_project = resource['PARENT_PROJECT']
        self._experts = resource['EXPERTS']

    ## @brief access DB and retrieves new runs
    def compare( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns

        for x in self.get_xtable_runs( [self._project, self._ref_project, self._parent_project],                                       [1, 0, 0] ):

            # Counter decreases by 1
            ctr -=1

            # Currently hard-coded seq = 0
            (run, subrun, seq) = (int(x[0]), int(x[1]), 0)

            # Report starting
            now_str  = time.strftime('%Y-%m-%d %H:%M:%S')
            self.info('Comparing checksums: run=%d, subrun=%d @ %s' % ( run, subrun, now_str ))

            statusCode = 1

            # Get status objects
            RefStatus = self._api.get_status( ds_status( self._ref_project, run, subrun, seq ))
            ParentStatus = self._api.get_status( ds_status( self._parent_project, run, subrun, seq ))

            if RefStatus._data == ParentStatus._data:
                statusCode = 0
            else:
                subject = 'Checksum different in run %d, subrun %d between %s and %s' % ( run, subrun, self._ref_project, self._parent_project )

                text = '%s\n' % subject
                text += 'Run %d, subrun %d\n' % ( run, subrun )
                text += '%s checksum: %s\n' % ( self._ref_project, RefStatus._data )
                text += '%s checksum: %s\n' % ( self._parent_project, ParentStatus._data )

                pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )
                statusCode = 1000
                self._data = '%s:%s;%s:%s' % ( self._ref_project, RefStatus._data, self._parent_project, ParentStatus._data )


            # Pretend I'm doing something
            time.sleep(0.5)

            # Report finishing
            now_str  = time.strftime('%Y-%m-%d %H:%M:%S')
            self.info('Finished comparing checksums: run=%d, subrun=%d @ %s' % ( run, subrun, now_str ))

            # Create a status object to be logged to DB (if necessary)
            # Let's say we set the status to be 10
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = seq,
                                status  = statusCode,
                                data    = self._data )
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]
    obj = compare_checksum( proj_name, 5 )

    obj.compare()


