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
from snova_util import *

class binary_to_dropbox( ds_project_base ):

    _project = 'binary_to_dropbox'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( binary_to_dropbox, self ).__init__( arg )

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
        self._parallelize = int(0)
        self._max_proc_time = int(30)
        self._min_run = int(0)

        self._seb= str("")
	self._remote_host = str("")
	self._file_destination = str("")

        self.get_resource()
        
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])

        self._parent_project = resource['PARENT_PROJECT']

	self._experts = resource['EXPERTS']

        self._seb = resource['SEB']
        self._remote_host = str(resource['REMOTE_HOST'])
        self._binary_location = str(resource['BINARY_LOCATION'])
        self._json_location = str(resource['JSON_LOCATION'])
        self._dropbox_location = str(resource['DROPBOX_LOCATION'])


    def transfer_files( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()
            
        self.info("Attempting file transfer...")

        # Fetch runs from DB and process for # runs specified for this instance.
        runlist=[]
        
        self.info("@ parent         %s " % str(self._project))
        self.info("@ parent project %s " % str(self._parent_project))

        runlist = self.get_xtable_runs( [self._project, self._parent_project], [kSTATUS_INIT, kSTATUS_DONE] )

        ctr = self._nruns
        in_file_v = []
        json_file_v = []
        runid_v = []
        
        # Slice the run list
        sliced_runlist = runlist[:ctr]

        self.info("See %s"%str(sliced_runlist))

        for x in sliced_runlist:
            # Break from loop if counter became 0
            if ctr <= 0: break

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            # Counter decreases by 1
            ctr -= 1

            ref_status = self._api.get_status( ds_status( self._parent_project, run, subrun, kSTATUS_DONE ) )
            json_file = os.path.join(self._json_location, self._seb, os.path.basename(ref_status._data))
            
            fname = os.path.basename(".".join(json_file.split(".")[:-1]))
            
            if "ubdaq" not in fname:
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND ) )
                continue
            
            fname = os.path.join(self._binary_location,self._seb,fname)
            in_file_v.append(fname)
            json_file_v.append(json_file)
            runid_v.append((run,subrun))

        self.info("Processing in_file_v %s"%str(in_file_v))
        self.info("Processing json_file_v %s"%str(json_file_v))

        mp = self.process_files(in_file_v,json_file_v)
        
        for i in xrange(len(in_file_v)):
            (out,err) = mp.communicate(i)
            
            self.info("Got return %s"%str(out))
            if err or out:
                self.error('Binary transfer failed for %s' % in_file_v[i])
                self.error(err)
                self.log_status( ds_status( project = self._project,
                                            run     = runid_v[i][0],
                                            subrun  = runid_v[i][1],
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_CHECKSUM_CALCULATION_FAILED,
                                            data    = '' ) )
                continue

            statusCode = kSTATUS_DONE
            self.log_status( ds_status( project = self._project,
                                        run     = runid_v[i][0],
                                        subrun  = runid_v[i][1],
                                        seq     = 0,
                                        status  = statusCode,
                                        data    = self._data ) )

    ## @brief process multiple files checksum calculation
    def process_files(self, in_file_v, in_json_v):

        mp = ds_multiprocess(self._project)
        cmd_template  = ""
        cmd_template += "scp %s vgenty@" + self._remote_host + ":/nashome/v/vgenty/tmp ; "
        cmd_template += "ssh -T -x vgenty@" + self._remote_host + " "
        cmd_template += "'source /nashome/v/vgenty/setupsam.sh 1>/dev/null 2>/dev/null; "
        cmd_template += "ifdh cp /nashome/v/vgenty/tmp/%s %s;"
        cmd_template += "ifdh cp %s %s; '"

        for in_file,in_json in zip(in_file_v,in_json_v):
            
            in_file_origin = in_file
            in_file_name = os.path.basename(in_file)
            in_file_destination = os.path.join(self._dropbox_location,in_file_name)

            in_file_pre_name = os.path.basename(in_file)
            in_file_pre_name = in_file.split("-")
            in_file_pre_name = in_file_pre_name[:2] + in_file_pre_name[3:]
            in_file_pre_name = "-".join(in_file_pre_name)
            in_file_pre_origin = os.path.join(self._binary_location,self._seb,in_file_pre_name)
            
            in_json_origin = in_json
            in_json_file_name = os.path.basename(in_json_origin)
            in_json_destination = os.path.join(self._dropbox_location,in_json_file_name)

            cmd = cmd_template % (in_json_origin,
                                  in_json_file_name,
                                  in_json_destination,
                                  in_file_pre_origin,
                                  in_file_destination)
            
            self.info(cmd)
            index, active_ctr = mp.execute(cmd)
            mp.communicate(index)

        while mp.active_count():
            time.sleep(0.2)
            time_slept += 0.2
            if time_slept > self._max_proc_time:
                mp.kill()
                break

        return mp
        


if __name__ == '__main__':
    proj_name = sys.argv[1]
    obj = binary_to_dropbox( proj_name )
    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
    obj.transfer_files()
    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
