## @namespace dstream_online.transfer
#  @ingroup dstream_online
#  @brief Defines a project transfer
#  @author echurch,yuntse

# python include
import time, os, sys, subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
# ifdh
import ifdh
import subprocess as sub
import samweb_cli, extractor_dict
import pdb, json

## @class transfer
#  @brief Transferring files
#  @details
#  This process mv's a file to a dropbox directory for SAM to whisk it away...
#  Status codes:
#    2: Copied the file to dropbox

class transfer( ds_project_base ):

    # Define project name as class attribute
    _project = 'transfer'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( transfer, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._nruns = None
        self._out_dir = ''
        self._outfile_format = ''
        self._in_dir = ''
        self._meta_dir = ''
        self._infile_format = ''
        self._parent_project = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._meta_dir = '%s' % (resource['METADIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']

    ## @brief Transfer files to dropbox
    def transfer_file( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # self.info('Here, self._nruns=%d ... ' % (self._nruns) )

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project, self._parent_project],
                                      [1, 0]):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Transferring a file: run=%d, subrun=%d ...' % (run,subrun) )

            status = 1

            # Check input file exists. Otherwise report error
            in_file = '%s/%s' % ( self._in_dir, self._infile_format % ( run, subrun ) )
            in_json = '%s/%s.json' %( self._meta_dir, self._infile_format % ( run, subrun ) )
            out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (run,subrun) )
            out_json = '%s/%s.json' %( self._out_dir, self._outfile_format % (run,subrun) )

            # construct ifdh object
            ih = ifdh.ifdh()


            if os.path.isfile( in_file ) and (os.path.isfile( in_json ) or ("pnnl" in self._project)):
                self.info('Found %s' % (in_file) )
                self.info('Found %s' % (in_json) )
                
                try:
                    if "pnnl" not in self._project:
                        resi = ih.cp(( in_file, out_file ))
                        resj = ih.cp(( in_json, out_json ))

                    # If this project is xfer'ing data to PNNL, we use gsiftp-to-sshftp in pnnl_transfer().
                    else: 
                        status = 102
                        (resi, resj) = self.pnnl_transfer(in_file)

                    if resi == 0 and resj == 0:
                        status = 0
                    else:
                        status = 101
                except:
                    status = 1

            else:
                status = 100

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = 0,
                                status  = status )

            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

    ## @brief Validate the dropbox
    def validate_outfile( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs( self._project, 2 ):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Validating a file in the output directory: run=%d, subrun=%d ...' % (run,subrun))

            status = 2
            out_file = '%s' % ( self._outfile_format % (run,subrun) )
            out_json = '%s.json' %( self._outfile_format % (run,subrun) )

            # construct ifdh object
            ih = ifdh.ifdh()

            try:
                ih.locateFile( out_file )
                ih.locateFile( out_json )
                status = 0
            except:
                status = 1

            # Pretend I'm doing something
            time.sleep(1)

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = int(x[0]),
                                subrun  = int(x[1]),
                                seq     = int(x[2]),
                                status  = status )

            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

    def pnnl_transfer( self, file_arg ):

        # sshftp-to-sshftp not enabled on near1, so must use gsiftp: must thus ship from the Sam'd-up dcache file.
        # uboonepro from near1 has an ssh-key to sshftp to dtn2.pnl.gov as chur558.
        # This requires that uboonepro owns a valid proxy. We should get 2 Gbps throughput with this scenario.
        # Need more logging to message service ... also let's put all this into a function that we call.


        cmd = "voms-proxy-info -all "
        proc = sub.Popen(cmd,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
        (out,err) = proc.communicate()
        goodProxy = False

        for line in out.split('\n'):
            if "timeleft" in line:
                if int(line.split(" : ")[1].replace(":","")) > 0:
                    goodProxy = True
                    break;

        if not goodProxy:
            self.error('uboonepro has no proxy.')
            raise Exception 
   

        in_file = os.path.basename(file_arg)
    # We do a samweb.fileLocate on basename of in_file. This project's parent must be transfer-root-to-dropbox.
        transfer = 0
        samweb = samweb_cli.SAMWebClient(experiment="uboone")
        loc = samweb.locateFile(filenameorid=in_file)
        jsonOb = extractor_dict.getmetadata(file_arg)
        size_in = jsonOb['file_size']
        samcode = 12


        if not ('enstore' in loc[0]["full_path"] and 'pnfs' in loc[0]["full_path"]):
            return (transfer, samcode)


        full_file = loc[0]["full_path"].strip('enstore:/pnfs/uboone') + "/" +  in_file
#        pdb.set_trace()
        pnnl_machine = "dtn2.pnl.gov"
        pnnl_dir = 'pic/projects/microboone/data/'
        ddir = str(jsonOb['runs'][0][0]) # Run number
        cmd_mkdir = "ssh chur558@" + pnnl_machine + " mkdir "  + "/" + pnnl_dir + ddir
        proc = sub.Popen(cmd_mkdir,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
        # block, but plow on w.o. regard to whether I was successful to create ddir. (Cuz this will complain if run is not new.) 
        (out,err) = proc.communicate() 
        
        pnnl_loc = pnnl_machine + "/" + pnnl_dir + ddir + "/" + in_file
        cmd_gsiftp_to_sshftp = "globus-url-copy -vb -p 10 gsiftp://fndca1.fnal.gov:2811" + full_file + " sshftp://chur558@" + pnnl_loc
        self.info('Will launch ' + cmd_gsiftp_to_sshftp)
        # Popen() gymnastics here, with resi capturing the return status.
        proc = sub.Popen(cmd_gsiftp_to_sshftp,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
        wait = 0
        delay = 5


        while  proc.poll() is None:
            wait+=delay
            if wait > delay*20:
                self.error ("pnnl_transfer timed out in awaiting transfer.")
                proc.kill()
                transfer = 11
                break
            self.info('pnnl_transfer process ' + str(proc.pid) + ' active for ' + str(wait) + ' [sec]')
            time.sleep (delay)
    
        size_out = 0
        if not transfer:
            (out,err) = proc.communicate()
            transfer = proc.returncode
                # also grep the out for indication of success at end.
            if not transfer:
                size_out = int(out.split("\n")[4].split("    ")[6].split(" ")[0])
                transfer = 10
                if size_out == size_in:
                    transfer = 0


# end file lives in enstore


        if not transfer:
            try:
                # Then samweb.addFileLocation() to pnnl location, with resj capturing that return status.
                pnnl_loc_withcolon = pnnl_machine + ":/" + pnnl_dir + ddir + "/" + in_file
                samadd = samweb.addFileLocation(filenameorid=in_file,location=pnnl_loc_withcolon)
                samloc  = samweb.locateFile(filenameorid=in_file)
                if len(samloc)>1:
                    samcode = 0
                    self.info('pnnl_transfer() finished moving ' + in_file + ', size ' + str(size_in) + ' [bytes], to PNNL')
                    self.info('Transfer rate was ' + str(out.split("\n")[4].split("    ")[8]))
                    self.info('Transfer and samaddFile are succsessful. Full SAM location for file is now ' + str(samloc))
            except:
                self.error('pnnl_transfer finished with a problem on ' + in_file + ' during addFile or locFile. samadd/samloc is: ' + str(samadd)+"/"+str(samloc))
        else:
            self.error('pnnl_transfer finished with a problem on ' + in_file)
                
        return (transfer, samcode)




# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = transfer( proj_name )

    obj.transfer_file()

    # if "pnnl" not in self._project:
    #    obj.validate_outfile()
