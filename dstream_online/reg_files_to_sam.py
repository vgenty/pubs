## @namespace dstream_online.reg_files_to_sam
#  @ingroup dstream_online
#  @brief Defines a project reg_files_to_sam
#  @author echurch,yuntse

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
from ds_online_env import *
import samweb_cli
import json
import traceback
import glob


## @class dummy_nubin_xfer
#  @brief A dummy nu bin file xfer project
#  @details
#  This process registers the file with samweb *.ubdaq 
#  and mv's it to a dropbox directory for SAM to whisk it away...
#  Status codes:
#  100: Cannot find the file to be declared
#  101: File already exists at SAM, email the expert
#    2: Declared the file to SAM
#  102: File doesn't exists at SAM, but failed to declare to SAM
#    0: Validated the file at SAM

class reg_files_to_sam( ds_project_base ):

    # Define project name as class attribute
    _project = 'reg_files_to_sam'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( reg_files_to_sam, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._nruns = None
        self._in_dir = ''
        #self._meta_dir = ''
        self._infile_format = ''
        self._parent_project = []
        self._project_list = [ self._project, ]
        self._project_requirement = [ kSTATUS_INIT ]
        self._nruns_to_postpone = 0
        
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )
        
        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        #self._meta_dir = '%s' % (resource['METADIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._experts = resource['EXPERTS']

        try:
            self._parent_project = resource['PARENT_PROJECT'].split(':')

        except Exception:
            self.error('Failed to load parent projects...')
            return False

        try:
            self._nruns_to_postpone = int(resource['NRUNS_POSTPONE'])
            self.info('Will process %d runs to be postponed (status=%d)' % (self._nruns_to_postpone,kSTATUS_POSTPONE))
        except KeyError,ValueError:
            pass

        for x in xrange( len(self._parent_project) ):
            self._project_list.append( self._parent_project[x] )
            self._project_requirement.append( 0 )

    ## @brief declare a file to SAM
    def declare_to_sam( self ):
        
        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # self.info('Here, self._nruns=%d ... ' % (self._nruns))
        self._project_requirement[0] = kSTATUS_INIT

        #
        # Process Postpone first
        #
        ctr_postpone = 0
        for parent in self._project_list:
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

        for p in self._project_list:
            self._api.commit('DROP TABLE IF EXISTS temp%s;' % p)
        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs(self._project_list,
                                      self._project_requirement):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Declaring a file to SAM: run=%d, subrun=%d ...' % (run,subrun) )

            status = 1
            
            # Check input file exists. Otherwise report error
            in_file_holder = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            filelist = glob.glob( in_file_holder )
            if (len(filelist)<1):
                self.error('ERROR: Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('ERROR: Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('ERROR: List of files found %s' % filelist)

            in_file = filelist[0]
            in_json = '%s.json' % in_file

            self.info('Declaring ' + in_file + ' to SAM: using ' + in_json  )
            

            if os.path.isfile( in_file ) and os.path.isfile( in_json ):
                self.info('Found %s' % (in_file) )
                self.info('Found %s' % (in_json) )
                json_dict = json.load( open( in_json ) )

                # native SAM python call, instead of a system call
                # make sure you've done get-cert
                # Perhaps we want a try block for samweb?
                samweb = samweb_cli.SAMWebClient(experiment="uboone")

                # Check if the file already exists at SAM
                try:
                    in_file_base=os.path.basename(in_file)
                    samweb.getMetadata(filenameorid=in_file_base)
                    status = 101
                    # Email the experts
                    subject = 'File %s Existing at SAM' % in_file_base
                    text = """
File %s has already exists at SAM!
                    """ % in_file_base

                    pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

                except samweb_cli.exceptions.FileNotFound:
                    # metadata already validated in get_assembler_metadata_file.py
                    try:
                        samweb.declareFile(md=json_dict)
                        status = 2
                    except Exception as e:
#                        print "Unexpected error: samweb declareFile problem: "
                        self.error( "Unexpected error: samweb declareFile problem: ")
                        self.error( "%s" % e)
                        subject = "samweb declareFile problem: %s" % in_file_base
                        text = """
File %s failed to be declared to SAM!
%s
                        """ % ( in_file_base, traceback.print_exc() )

                        pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

                        # print "Give some null properties to this meta data"
                        self.error( "Give this file a status 102")
                        status = 102

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

        # self.info('Here, self._nruns=%d ... ' % (self._nruns) )
        self._project_requirement[0] = 2

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        #for x in [(391,10,0,0)]:
        for x in self.get_xtable_runs(self._project_list,
                                      self._project_requirement):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Checking the SAM declaration: run=%d, subrun=%d ...' % (run,subrun))

            status = 12

            in_file_holder = '%s/%s' % (self._in_dir,self._infile_format % (run,subrun))
            filelist = glob.glob( in_file_holder )
            if (len(filelist)<1):
                self.error('ERROR: Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                status_code=100
                status = ds_status( project = self._project,
                                    run     = run,
                                    subrun  = subrun,
                                    seq     = 0,
                                    status  = status_code )
                self.log_status( status )                
                continue

            if (len(filelist)>1):
                self.error('ERROR: Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.error('ERROR: List of files found %s' % filelist)

            in_file = filelist[0]
            in_file_base = os.path.basename(in_file)
            samweb = samweb_cli.SAMWebClient(experiment="uboone")

            # Check if the file already exists at SAM
            try:
                samweb.getMetadata(filenameorid=in_file_base)
                status = 0
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


# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = reg_files_to_sam( proj_name )

    obj.declare_to_sam()

    obj.validate_sam()

    # May insert SAM dataset definition here

