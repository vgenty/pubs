from dstream import ds_status
from dstream import ds_project_base
from ds_online_constants import *
import os
import glob
import datetime
import tarfile
import sys

class compress_daq_logs(ds_project_base):

    # Define project name as class attribute
    _project = 'compress_daq_logs'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( compress_daq_logs, self ).__init__( arg )

        self._project = str( arg )
        self._nruns   = 0
        self._in_dir  = ''
        self._out_dir = ''
        self._outfile_format = ''
        self._infile_foramt  = ''
        self._infile_age     = 0
        self._data    = ''

        if not self.load_params():
            raise Exception()

    ## @brief load project parameters
    def load_params(self):

        self.debug('Loading parameters...')
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return False

        resource = self._api.get_resource(self._project)
        self._nruns = int(resource['NRUNS'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._infile_age = resource['INFILE_AGE']

        return True

    ## @brief access DB and retrieves new runs
    def process_newruns(self):
        ctr = self._nruns
        for x in self.get_runs(self._project, kSTATUS_INIT, False):
            # Break from loop if counter became 0
            if ctr <= 0: break

            ( run, subrun ) = ( int(x[0]), int(x[1]) )
            # self.debug('Processing run %d, subrun %d...' %( run, subrun ) )
            if subrun > 0: continue
            self.debug('Processing run %d, subrun %d...' %( run, subrun ) )

            # Counter decreases by 1
            ctr -= 1

            fFormat = self._infile_format % run
            fFormat = '%s/%s' %( self._in_dir, fFormat )
            # self.debug('Fileformat: %s ' % fFormat )
            flist = glob.glob( fFormat )
            # self.debug('Filelist for run %d, subrun %d: %r' %( run, subrun, flist ) )
            if flist:
                date, hour, minute, second = self.find_run_time( flist[0] )
                self.debug('Run time: %s, %s:%s%s' %( date, hour, minute, second ) )

                date_format = '%m.%d.%Y'
                deltaT = datetime.datetime.today() - datetime.datetime.strptime( date, date_format )
                self.debug('DeltaT: %s' % deltaT )

                if deltaT.days > int(self._infile_age):
                   outFile = self.process_files( run, date, hour, minute, second, flist )

        return

    def process_files( self, run, date, hour, minute, second, flist ):
        outName = self._outfile_format % ( run, date, hour, minute, second )
        outFile = '%s/%s' % ( self._out_dir, outName )
        if os.path.exists( outFile ):
            os.remove( outFile )

        self.info('Compressing DAQ logs from run %d to %s...' %( run, outFile ) )
        tf = tarfile.open( outFile, 'w:bz2' )
        for f in flist:
            if os.path.exists( f ):
               tf.add( f )

        tf.close()
        statusCode = kSTATUS_TO_BE_VALIDATED

        # Create a status object to be logged to DB (if necessary)
        status = ds_status( project = self._project,
                            run     = run,
                            subrun  = 0,
                            seq     = 0,
                            status  = statusCode,
                            data    = self._data )
        # Log status
        self.log_status( status )

        return outFile

    def find_run_time( self, fname ):
        date = fname.split('-')[-2]
        hour = fname.split('-')[-1].split('.')[0]
        minute = fname.split('-')[-1].split('.')[1]
        second = fname.split('-')[-1].split('.')[2]

        return date, hour, minute, second

    ## @brief access DB and validate finished runs
    def validate( self ):
        ctr = self._nruns
        for x in self.get_runs( self._project, kSTATUS_TO_BE_VALIDATED, False ):
            # Break from loop if counter became 0
            if ctr <= 0: break

            ( run, subrun ) = ( int(x[0]), int(x[1]) )
            if subrun > 0: continue

            # Counter decreases by 1
            ctr -= 1

            fFormat = self._infile_format % run
            fFormat = '%s/%s' %( self._in_dir, fFormat )
            self.debug('Fileformat: %s ' % fFormat )
            flist = glob.glob( fFormat )
            if flist:
                date, hour, minute, second = self.find_run_time( flist[0] )
            else:
                outFileMissingInFiles = '%s/*-%d-*.tar.bz2' %(self._out_dir, run)
                if os.path.exists(outFileMissingInFiles):
                    if os.path.getsize(outFileMissingInFiles) > 0:
                        self.info('Found a tarball for run %d without any input logs so marking complete.' % run )
                        statusCode = kSTATUS_DONE
                        status = ds_status( project = self._project,
                                            run     = run,
                                            subrun  = 0,
                                            seq     = 0,
                                            status  = statusCode,
                                            data    = self._data )
                        self.log_status( status )
                    else:
                        self.info('Found a tarball for run %d but there are still log files in %s.' % (run, self._in_dir) )
                        statusCode = kSTATUS_ERROR_UNKNOWN
                        status = ds_status( project = self._project,
                                            run     = run,
                                            subrun  = 0,
                                            seq     = 0,
                                            status  = statusCode,
                                            data    = self._data )
                        self.log_status( status )
                else:
                    self.info('Found no tarball or input files for run %d. Marking it done.' % run )
                    statusCode = kSTATUS_DONE
                    status = ds_status( project = self._project,
                                        run     = run,
                                        subrun  = 0,
                                        seq     = 0,
                                        status  = statusCode,
                                        data    = self._data )
                    self.log_status( status )

                continue

            outName = self._outfile_format % ( run, date, hour, minute, second )
            outFile = '%s/%s' % ( self._out_dir, outName )

            if os.path.exists( outFile ):
                if os.path.getsize( outFile ) == 0:
                    for f in flist:
                        if os.path.getsize( f ) > 0:
                            self.info('DAQ logs from run %d have zero-size output %s, try again...' % ( run, outFile ) )
                            statusCode = kSTATUS_INIT
                            break
                        self.info('Checked DAQ logs from run %d have nothing.' % run )
                        statusCode = kSTATUS_DONE
                else:
                    # Remove the original DAQ logs
                    self.info('Done with compressing DAQ logs from run %d!  Removing...' % run )
                    for f in flist:
                        self.debug('Removing %s' % f )
                        os.remove( f )
                    self.debug('fFormat: %s' % fFormat )
                    check_flist = glob.glob( fFormat )
                    if len( check_flist ) == 0:
                        statusCode = kSTATUS_DONE
                    else:
                        statusCode = kSTATUS_TO_BE_VALIDATED
            else:
                statusCode = kSTATUS_INIT

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = 0,
                                seq     = 0,
                                status  = statusCode,
                                data    = self._data )
            # Log status
            self.log_status( status )
        return

# A unit test section
if __name__ == '__main__':

    obj = compress_daq_logs(sys.argv[1])

    obj.process_newruns()

    obj.validate()
