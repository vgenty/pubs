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
from ds_online_util import *
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

        # self._meta_dir = ''

        self._infile_format = ''
        self._parent_project = []
        self._project_list = [ self._project, ]
        self._project_requirement = [ kSTATUS_INIT ]
        self._min_run = 0

        self._nskip = 0
        self._skip_ref_project = []
        self._skip_ref_status = None
        self._skip_status = None

        self._seb = None
        self._remote_host = None

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )
        
        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._experts = resource['EXPERTS']
        self._seb = resource['SEB']
        self._remote_host = resource['REMOTE_HOST']
        self._json_location = resource['JSON_LOCATION']

        try:
            self._parent_project = resource['PARENT_PROJECT'].split(':')

        except Exception:
            self.error('Failed to load parent projects...')
            return False

        for x in xrange( len(self._parent_project) ):
            self._project_list.append( self._parent_project[x] )
            self._project_requirement.append( 0 )

        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])

    ## @brief declare a file to SAM
    def process_runs(self):
        
        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # self.info('Here, self._nruns=%d ... ' % (self._nruns))
        self._project_requirement[0] = kSTATUS_INIT 

        # fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        runid_v = []
        files_v = []
        json_v = []

        self.info("Asking for ctr: %s"%str(ctr))
        
        self.info("project list %s " % str(self._project_list))
        self.info("project req %s " % str(self._project_requirement))

        for x in self.get_xtable_runs(self._project_list,
                                      self._project_requirement):
            
            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            if ctr < 0 : break

            # Report starting
            self.info('Declaring a file to SAM: run=%d, subrun=%d ...' % (run,subrun) )

            status = kSTATUS_INIT

            # Check input file exists. Otherwise report error
            ref_status = self._api.get_status( ds_status( self._parent_project[0], run, subrun, 0 ) )
            file_ = ref_status._data
            self.info("Asking parent project %s for file %s"%(self._parent_project[0],file_))

            filelist = [file_]

            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND ) )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE ) )
                continue

            in_file = filelist[0]
            infile  = os.path.basename(in_file)
            
            out_file_name = infile.split("-")
            out_file_name.insert(2,self._seb)
            out_file_name = "-".join(out_file_name)

            in_json = '%s/%s/%s.json' % (self._json_location,self._seb,out_file_name)
            
            self.info("Asking for json file %s" % in_json)

            if not os.path.isfile( in_json ):
                self.error('Missing json file: %s' % in_json)
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_INPUT_FILE_NOT_FOUND) )

            files_v.append(out_file_name)
            runid_v.append((run,subrun))
            json_v.append(in_json)

        self.info("self.process_files")

        status_v = self.process_files(files_v,json_v)

        for i in xrange(len(status_v)):
            run,subrun = runid_v[i]
            status = status_v[i]
            in_json = json_v[i]
            data = os.path.basename(in_json)
            # Create a status object to be logged to DB (if necessary)
            self.log_status( ds_status( project = self._project,
                                        run     = run,
                                        subrun  = subrun,
                                        seq     = 0,
                                        status  = status,
                                        data    = data) )
            
    ## @brief given files attempt to register to sam
    def process_files(self, in_file_v,json_v):

        status_v=[kSTATUS_ERROR_UNKNOWN] * len(in_file_v)

        for i in xrange(len(status_v)):

            json_dict = None
            in_file = os.path.basename(in_file_v[i])
            in_json = json_v[i]

            try:
                json_dict = json.load( open( in_json ) )
                self.info("Loading json dict @ %s" % in_json)
            except ValueError:
                self.error('Failed loading json file: %s' % in_json)
                status_v[i] = kSTATUS_ERROR_WRONG_JSON_FORMAT
                continue

            # native SAM python call, instead of a system call
            samweb = samweb_cli.SAMWebClient(experiment="uboone")
                
            # Check if the file already exists at SAM
            try:
                samweb.getMetadata(filenameorid=in_file)
                # Email the experts
                subject = 'File %s Existing at SAM' % in_file
                text = "File %s has already exists at SAM!" % in_file
                pub_smtp( os.environ['PUB_SMTP_ACCT'], 
                          os.environ['PUB_SMTP_SRVR'], 
                          os.environ['PUB_SMTP_PASS'], 
                          self._experts, subject, text )
                status_v[i] = kSTATUS_ERROR_DUPLICATE_SAM_ENTRY 
                continue
            except samweb_cli.exceptions.FileNotFound:
                try:
                    samweb.declareFile(md=json_dict)
                    status_v[i] = kSTATUS_TO_BE_VALIDATED
                except Exception as e:
                    self.error( "Unexpected error: samweb declareFile problem: ")
                    self.error( "%s" % e)
                    subject = "samweb declareFile problem: %s" % in_file
                    text = "File %s failed to be declared to SAM! %s" % ( in_file, traceback.print_exc() )
                    pub_smtp( os.environ['PUB_SMTP_ACCT'], 
                              os.environ['PUB_SMTP_SRVR'], 
                              os.environ['PUB_SMTP_PASS'], 
                              self._experts, subject, text )

                    # print "Give some null properties to this meta data"
                    status_v[i] = kSTATUS_ERROR_CANNOT_MAKE_SAM_ENTRY

        return status_v

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

        for p in self._project_list:
            self._api.commit('DROP TABLE IF EXISTS temp%s' % p)

        for x in self.get_xtable_runs(self._project_list,
                                      self._project_requirement):

            # Counter decreases by 1
            ctr -= 1

            if ctr < 0 : break

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Checking the SAM declaration: run=%d, subrun=%d ...' % (run,subrun))

            status = kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND

            ref_status = self._api.get_status( ds_status( self._parent_project[0], run, subrun, 0 ) )
            file_ = ref_status._data
            filelist = [file_]

            if (len(filelist)<1):
                self.error('Failed to find the file for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND ) )
                continue

            if (len(filelist)>1):
                self.error('Found too many files for (run,subrun) = %s @ %s !!!' % (run,subrun))
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = kSTATUS_ERROR_OUTPUT_FILE_NOT_UNIQUE ) )

            in_file = filelist[0]
            in_file_base  = os.path.basename(in_file)

            out_file_name = in_file_base.split("-")
            out_file_name.insert(2,self._seb)
            out_file_name = "-".join(out_file_name)

            self.info("Asking for %s"%out_file_name)

            samweb = samweb_cli.SAMWebClient(experiment="uboone")

            # Check if the file already exists at SAM
            try:
                samweb.getMetadata(filenameorid=out_file_name)
                status = kSTATUS_DONE
                self.info("It was there...")
            except samweb_cli.exceptions.FileNotFound:
                status = kSTATUS_INIT
                self.info("File not found...")

            data = out_file_name + ".json"
            
            # Create a status object to be logged to DB (if necessary)
            self.log_status( ds_status( project = self._project,
                                        run     = int(x[0]),
                                        subrun  = int(x[1]),
                                        seq     = 0,
                                        status  = status,
                                        data    = data) )


# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = reg_files_to_sam( proj_name )

    obj.info('Start project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    #obj.declare_to_sam()
    obj.process_runs()    

    obj.validate_sam()

    obj.info('End project @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))

    # May insert SAM dataset definition here

