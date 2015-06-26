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
import samweb_cli

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

            if os.path.isfile( in_file ) and os.path.isfile( in_json ):
                self.info('Found %s' % (in_file) )
                self.info('Found %s' % (in_json) )
                
                try:
                    if "pnnl" not in self._project:
                        resi = ih.cp(( in_file, out_file ))
                        resj = ih.cp(( in_json, out_json ))
                    else:
                        status = 102
                        # sshftp-to-sshftp not enabled on near1, so must use gsiftp: must thus ship from the Sam'd-up dcache file.
                        # uboonepro from near1 has an ssh-key to sshftp to dtn2.pnl.gov as chur558.
                        # This requires that uboonepro owns a cert. We should get 2 Gbps throughput with this scenario.

                        # We do a samweb.fileLocate on basename of in_file. This project's parent must be transfer-root-to-dropbox.
                        samweb = samweb_cli.SAMWebClient(experiment="uboone")
                        loc = samweb.locateFile(filenameorid=in_file)
                        # This needs testing!!!
                        base_in_file = os.path.basename(loc...) # loc needs checking for "pnfs"
                        pnnl_loc = "dtn2.pnl.gov" + out_file
                        cmd_gsiftp_to_sshftp = "globus-url-copy -vb -p 10 gsiftp://fndca1.fnal.gov:2811/" + base_in_file + " sshftp://chur558@" + out_file
                        # Popen() gymnastics here, with resi capturing the return status.
                        proc = sub.Popen(cmd,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
                        (out,err) = proc.communicate()
                        resi = proc.returncode
                        # Then samweb.addFileLocation() to pnnl location, with resj capturing that return status.
                        resj = samweb.addFileLocation(filenameorid=in_file,location=pnnl_loc)
                        
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

# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = transfer( proj_name )

    obj.transfer_file()

    # if "pnnl" not in self._project:
    #    obj.validate_outfile()
