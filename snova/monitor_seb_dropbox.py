# python include
import time,os
from pub_dbi import DBException
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

class monitor_seb_dropbox(ds_project_base):

    # Define project name as class attribute
    _project = 'monitor_seb_dropbox'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self, arg=''):

        # Call base class ctor
        super(monitor_seb_dropbox,self).__init__( arg )

        self._project = arg
        
        self._snova_file_format = ''

        self._dbox_dir = ''
        
        self._nruns   = None

    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:

            resource = self._api.get_resource(self._project)

            self._nruns = int(resource['NRUNS'])
            self._dbox_dir = str(resource['DATADIR'])
            self._snova_file_format = str(resource['FILEFMT'])
            self._max_dir_size = int(resource['MAXBYTES'])
            
        ctr = self._nruns
        for x in self.get_runs(self._project,1):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            # Report starting
            self.info('processing new run: run=%d, subrun=%d ...' % (run,subrun))

            infile=os.path.join(self._dbox_dir,self._snova_file_format%(run,subrun))

            cmd="mv %s %s"%(infile,self._out_dir)
            self.info(cmd)            
            
            os.system( cmd )

            # Pretend this file is large and it's taking me 5 seconds to transfer, not
            # inconceivable for pnfs transfer time
            
            time.sleep(5)

            # log it in personal db... but hey, it's not validated yet
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = 2)

            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break


    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1

            run    = int(x[0])
            subrun = int(x[1])

            status = 0

            filename=self._snova_file_format%(run,subrun))
            file_ = os.path.join(self._dbox_dir,filename)

            if os.path.isfile( outfile ):

                cmd="rm -rf %s"%infile

                os.system(cmd)
                
                self.info('validated run: run=%d, subrun=%d ...' % (run,subrun))
                self.info(cmd)

            else:

                self.error('error on run: run=%d, subrun=%d ...' % (run,subrun))

                status = 1

            # Pretend I'm doing something!
            time.sleep(0.5)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = status,
                                data    = outfile)
            
            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

# A unit test section
if __name__ == '__main__':

    proj_name = "monitor_seb_dropbox_%s"%sys.argv[1]
    
    cock_obj = monitor_seb_dropbox( proj_name )

    cock_obj.process_newruns()

    cock_obj.validate()



