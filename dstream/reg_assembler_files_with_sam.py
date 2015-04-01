## @namespace dummy_dstream.dummy_nubin_xfer
#  @ingroup dummy_dstream
#  @brief Defines a project dummy_nubin_xfer
#  @author echurch,yuntse

# python include
import time, os, shutil, sys, subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
import samweb_cli
import json
import traceback


## @class dummy_nubin_xfer
#  @brief A dummy nu bin file xfer project
#  @details
#  This process registers the file with samweb *.ubdaq 
#  and mv's it to a dropbox directory for SAM to whisk it away...
#  Status codes:
#  100: Cannot find the file to be declared
#  101: File already exists at SAM, email the expert
#   12: Declared the file to SAM
#   11: File doesn't exists at SAM, but failed to declare to SAM
#   10: Validated the file at SAM
#   32: Copied the file to dropbox

class reg_assembler_files_with_sam(ds_project_base):

    # Define project name as class attribute
    _project = 'reg_assembler_files_with_sam'

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self):

        # Call base class ctor
        super(reg_assembler_files_with_sam,self).__init__()

        self._nruns = None
        self._out_dir = ''
        self._outfile_format = ''
        self._in_dir = ''
        self._meta_dir = ''
        self._infile_format = ''
        self._parent_project = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        self._meta_dir = '%s' % (resource['METADIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']

    ## @brief declare a file to SAM
    def declare_to_sam(self):

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
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [1,0]):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Declaring a file to SAM: run=%d, subrun=%d ...' % (run,subrun))

            status = 1
            
            # Check input file exists. Otherwise report error
            in_file_base = self._infile_format % ( run, subrun )
            in_file = '%s/%s' % ( self._in_dir, in_file_base )
            in_json = '%s/%s.json' %( self._meta_dir, in_file_base )
            
            if os.path.isfile(in_file) and os.path.isfile(in_json):
                self.info('Found %s' % (in_file))
                self.info('Found %s' % (in_json))
                json_dict = json.load( open( in_json ) )

                # native SAM python call, instead of a system call
                # make sure you've done get-cert
                # Perhaps we want a try block for samweb?
                samweb = samweb_cli.SAMWebClient(experiment="uboone")

                # Check if the file already exists at SAM
                try:
                    samweb.getMetadata(filenameorid=in_file_base)
                    status = 101
                    # Want to email the experts
                except samweb_cli.exceptions.FileNotFound:
                    # metadata already validated in get_assembler_metadata_file.py
                    try:
                        samweb.declareFile(md=json_dict)
                        status = 12
                    except:
                        print "Unexpected error: samweb declareFile problem: "
                        print traceback.print_exc()
                        # print "Give some null properties to this meta data"
                        print "Give this file a status 11"
                        status = 11

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


    ## @brief Check the SAM definition
    def validate_sam( self ):
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
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [12,0]):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Checking the SAM declaration: run=%d, subrun=%d ...' % (run,subrun))

            status = 12

            in_file_base = self._infile_format % ( run, subrun )
            samweb = samweb_cli.SAMWebClient(experiment="uboone")

            # Check if the file already exists at SAM
            try:
                samweb.getMetadata(filenameorid=in_file_base)
                status = 10
            except samweb_cli.exceptions.FileNotFound:
                status = 1

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

    ## For future use
    # def create_sam_def():
            # defname = 'AssemblerRawBinary'
            ## dim = '(defname %s)' % defname
            # dim = 'file_type %s' % 'data'
            # dim = dim + ' and data_tier %s' % 'raw'
##            dim = dim + ' and ub_project.name %s' % project.name
##            dim = dim + ' and ub_project.stage %s' % stage.name
            # dim = dim + ' and ub_project.version %s' % 'v6_00_01'  # generalize
            ## dim = dim + ' and availability: anylocation'
            # samweb.createDefinition(defname=defname, dims=dim)

    ## @brief Transfer files to dropbox
    def transfer_to_dropbox( self ):

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
        for x in self.get_xtable_runs([self._project,self._parent_project],
                                      [10,0]):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Transferring a file to dropbox: run=%d, subrun=%d ...' % (run,subrun))

            status = 10

            # Check input file exists. Otherwise report error
            in_file = '%s/%s' % ( self._in_dir, self._infile_format % ( run, subrun ) )
            in_json = '%s/%s.json' %( self._meta_dir, self._infile_format % ( run, subrun ) )
            # out_dir is the dropbox.
            out_file = '%s/%s' % ( self._out_dir, self._outfile_format % (run,subrun) )
            out_json = '%s/%s.json' %( self._out_dir, self._outfile_format % (run,subrun) )

            if os.path.isfile(in_file) and os.path.isfile(in_json):
                self.info('Found %s' % (in_file))
                self.info('Found %s' % (in_json))

                try:
                    subprocess.call(['rsync', '-e', 'ssh', in_file, 'uboonepro@uboonegpvm06.fnal.gov:%s' % out_file ])
                    subprocess.call(['rsync', '-e', 'ssh', in_json, 'uboonepro@uboonegpvm06.fnal.gov:%s' % out_json ])
                    status = 32
                except:
                    status = 10

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
    def validate_dropbox( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project, 32):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Validating a file in dropbox: run=%d, subrun=%d ...' % (run,subrun))

            status = 32
            in_file = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._outfile_format % (run,subrun))

            res = subprocess.call(['ssh', 'uboonegpvm06', '-x', 'ls', out_file])
            if res:
                # didn't find the file
                status = 10
            else:
                status = 0

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

    test_obj = reg_assembler_files_with_sam()

    test_obj.declare_to_sam()

    test_obj.validate_sam()

    # May insert SAM dataset definition here

    test_obj.transfer_to_dropbox()

    test_obj.validate_dropbox()

