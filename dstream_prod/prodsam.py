# @namespace dstream_prod.prodsam
#  @ingroup dstream_prod
#  @brief Project to declare files to sam and store files in enstore.
#  @author yuntse

# python include
import os,sys,time
import traceback
import StringIO
# pub_dbi package include
from pub_dbi import DBException
# pub_util package include
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
# From larbatch
import project
import project_utilities

## @class prodsam
#  @brief A fake job submission process, only printing out the commands
#  @details
class prodsam(ds_project_base):

    PROD_STATUS = ( kDONE,
                    kINITIATED,
                    kDECLARED,
                    kSTORED) = xrange(4)

    # Define project name as class attribute
    _project = 'prodsam'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super(prodsam,self).__init__(arg)

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg
        self._parent  = ''
        self._parent_status = None
        self._stage = ''

        # Actions associated with single subruns.

        self.PROD_ACTION = { self.kDONE          : None,
                             self.kINITIATED     : self.declare,
                             self.kDECLARED      : self.store,
                             self.kSTORED        : self.check_location }
        self._max_runid   = None
        self._min_runid   = None
        self._store       = 0
        self._storeana    = 0
        self._xml_file    = ''
        self._xml_outdir   = ''
        self._xml_template = False
        self._xml_rep_var  = {}
        self._data = "None"
        self._runid_status = {}
        if not self.loadProjectParams():
            self.info('Failed to load project @ %s' % self.now_str())
            sys.exit(1)

    def getXML(self,run,remake=False):

        if not self._xml_template: return self._xml_file
        
        out_xml_name = '%s/%s_run_%07d.xml' % (self._xml_outdir,self._project,int(run))

        if os.path.isfile(out_xml_name):
            out_ctime = os.path.getctime(out_xml_name)
            in_ctime  = os.path.getctime(self._xml_template)

            if in_ctime > out_ctime:
                self.warning('Re-creating XML file as input is newer (%s)' % out_xml_name)
                remake = True

        if not remake and os.path.isfile(out_xml_name): return out_xml_name

        if not os.path.isfile(self._xml_template):
            raise DSException('Input XML template does not exist: %s' % self._xml_template)

        fout = open(out_xml_name,'w')
        contents = open(self._xml_template,'r').read()
        contents = contents.replace('REP_RUN_NUMBER','%d' % int(run))
        contents = contents.replace('REP_ZEROPAD_RUN_NUMBER','%07d' % int(run))
        for key,value in self._xml_rep_var.iteritems():
            contents = contents.replace(key,value)
        fout.write(contents)
        fout.close()

        return out_xml_name

    def loadProjectParams( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return False

        proj_info = self._api.project_info(self._project)
        
        try:
            if 'PARENT' in proj_info._resource:
                self._parent = proj_info._resource['PARENT']
                self._parent_status = int(proj_info._resource['PARENT_STATUS'])
            if 'XML_TEMPLATE' in proj_info._resource:
                if self._xml_file:
                    raise DSException('Resource has both XMLFILE and XML_TEMPLATE (not allowed!)')
                self._xml_template = proj_info._resource['XML_TEMPLATE']
                self._xml_outdir = proj_info._resource['XML_OUTDIR']
                if not '/' in self._xml_template:
                    self._xml_template = '%s/dstream_prod/xml/%s' % (os.environ['PUB_TOP_DIR'],self._xml_template)
                if not os.path.isfile(self._xml_template):
                    raise DSException('XML template file not found: %s' % self._xml_template)
                if not os.path.isdir(self._xml_outdir):
                    os.makedirs(self._xml_outdir)
            elif not 'XMLFILE' in proj_info._resource:
                raise DSException('XML file not specified in resource!')
            else:
                self._xml_file = proj_info._resource['XMLFILE']
                if not os.path.isfile(self._xml_file):
                    raise DSException('XML file not found: %s' % self._xml_file)

            # Store XML replacement variables if appropriate
            for key,value in proj_info._resource.iteritems():
                if not key.startswith('PUBS_XMLVAR_'): continue
                
                if not self._xml_template:
                    raise DSException('XML template file not set but replacement variable set! (%s)' % key)

                self._xml_rep_var[key]=value

            self._experts = proj_info._resource['EXPERTS']
            self._period = proj_info._period
            self._version = proj_info._ver
            self._max_runid = (int(proj_info._resource['MAX_RUN']),int(proj_info._resource['MAX_SUBRUN']))
            if proj_info._resource.has_key('MIN_RUN') and proj_info._resource.has_key('MIN_SUBRUN'):
                self._min_runid = (int(proj_info._resource['MIN_RUN']),int(proj_info._resource['MIN_SUBRUN']))

            # Stage name in xml file.
            if proj_info._resource.has_key('STAGE_NAME'):
                self._stage = proj_info._resource['STAGE_NAME']

            # Set store flag.
            if proj_info._resource.has_key('STORE'):
                self._store = int(proj_info._resource['STORE'])

            # Set storeana flag.

            if proj_info._resource.has_key('STOREANA'):
                self._storeana = int(proj_info._resource['STOREANA'])

        except Exception as e:
            self.error('Failed to load project parameters...')
            raise e

        self.info('Project loaded @ %s' % self.now_str())
        return True

    def now_str(self):
        return time.strftime('%Y-%m-%d %H:%M:%S')

    # Function to declare files for a single (run, subrun) to sam.

    def declare( self, statusCode, run, subrun ):

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self.getXML(run), '', self._stage, run, [subrun], self._version)
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode

        # Do declaration.
        try:
            real_stdout = sys.stdout
            real_stderr = sys.stderr
            sys.stdout = StringIO.StringIO()
            sys.stderr = StringIO.StringIO()

            # Declare artroot files.

            declare_status = project.docheck_declarations(stobj.logdir, stobj.outdir,
                                                          declare=True, ana=False)

            # Declare analysis root files.

            if declare_status == 0:
                declare_status = project.docheck_declarations(stobj.logdir, stobj.outdir,
                                                              declare=True, ana=True)

            # Create artroot dataset definition.

            if declare_status == 0:
                nopubs_stobj = stobj
                nopubs_stobj.pubs_output = 0
                dim = project_utilities.dimensions(probj, nopubs_stobj, ana=False)
                declare_status = project.docheck_definition(stobj.defname, dim, True)

            # Create analysis dataset definition.

            if declare_status == 0:
                dim = project_utilities.dimensions(probj, stobj, ana=True)
                declare_status = project.docheck_definition(stobj.defname, dim, True)

            strout = sys.stdout.getvalue()
            strerr = sys.stderr.getvalue()
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            if strout:
                self.info(strout)
            if strerr:
                self.warning(strerr)
        except:
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            self.error('Exception raised by project.docheck_definitions:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode

        # Update pubs status.
        if declare_status == 0:
            if not self._store and not self._storeana:
                statusCode = 10
            else:
                statusCode = self.kDECLARED

        self.info("SAM declarations, status: %d" % statusCode)

        return statusCode
    # def declare( self, statusCode, run, subrun ):


    def store( self, statusCode, run, subrun ):

        # Check store flag.

        if not self._store and not self._storeana:
            self.info('Skipping store.')
            return 10

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self.getXML(run), '', self._stage, run, [subrun], self._version)
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode

        # Do store.
        try:
            real_stdout = sys.stdout
            real_stderr = sys.stderr
            sys.stdout = StringIO.StringIO()
            sys.stderr = StringIO.StringIO()

            # Store files.

            store_status = 0
            if self._store:
                self.info('Storing artroot files.')
                dim = project_utilities.dimensions(probj, stobj, ana=False)
                store_status = project.docheck_locations(dim, stobj.outdir, 
                                                         add=False,
                                                         clean=False,
                                                         remove=False,
                                                         upload=True)

            if self._storeana and store_status == 0 and stobj.ana_data_tier != '':
                self.info('Storing analysis root files.')
                dim = project_utilities.dimensions(probj, stobj, ana=True)
                store_status = project.docheck_locations(dim, stobj.outdir, 
                                                         add=False,
                                                         clean=False,
                                                         remove=False,
                                                         upload=True)

            strout = sys.stdout.getvalue()
            strerr = sys.stderr.getvalue()
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            if strout:
                self.info(strout)
            if strerr:
                self.warning(strerr)
        except:
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            self.error('Exception raised by project.docheck_locations:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode

        # Update pubs status.
        if store_status == 0:
           statusCode = self.kSTORED

        self.info("SAM store, status: %d" % statusCode)

        return statusCode
    # def store( self, statusCode, run, subrun ):


    def check_location( self, statusCode, run, subrun ):

        # Check store flag.

        if not self._store and not self._storeana:
            self.info('Skipping check location.')
            return 10

        # Get project and stage object.
        try:
            probj, stobj = project.get_pubs_stage(self.getXML(run), '', self._stage, run, [subrun], self._version)
        except:
            self.error('Exception raised by project.get_pubs_stage:')
            e = sys.exc_info()
            for item in e:
                self.error(item)
            for line in traceback.format_tb(e[2]):
                self.error(line)
            return statusCode

        # Here is where we could check for a sam location.
        # For now we don't do anything, but just hope the delay of an extra step
        # is enought to let files get a sam location.

        loc_status = 0

        # Update pubs status.
        if loc_status == 0:
           statusCode = 10

        self.info("SAM store, status: %d" % statusCode)

        return statusCode
    # def check_location( self, statusCode, run, subrun ):


    ## @brief access DB and retrieves new runs
    def process( self ):

        status_v = list(self.PROD_STATUS)
        status_v.reverse()
        # temporary fix: record the processed run and only process once per process function call
        processed_run=[]
        for istatus in status_v:
            self.debug('Inspecting status %s @ %s' % (istatus,self.now_str()))
                
            target_list = []
            if istatus == self.kINITIATED and self._parent:
                target_list = self.get_xtable_runs([self._project,self._parent],[istatus,self._parent_status])
            else:
                target_list = self.get_runs( self._project, istatus )

            run_subruns = {}
            for x in target_list:

                run    = int(x[0])
                subrun = int(x[1])
                runid = (run,subrun)

                if self._max_runid and runid > self._max_runid:
                    self.debug('Ignoring (run,subrun) = (%d,%d) above set run range max (%d,%d)' % (run,subrun,self._max_runid[0],self._max_runid[1]))
                    continue
                if self._min_runid and runid < self._min_runid:
                    self.debug('Ignoring (run,subrun) = (%d,%d) below set run range min (%d,%d)' % (run,subrun,self._min_runid[0],self._min_runid[1]))
                    continue
                if runid in processed_run: continue
                processed_run.append(runid)

                self.debug('Found run/subrun: %s/%s' % (run,subrun))
                if not run_subruns.has_key(run):
                    self.info('Found run: %s ... inspecting @ %s' % (run,self.now_str()))
                    run_subruns[run] = set()
                if not subrun in run_subruns[run]:
                    run_subruns[run].add(subrun)

            # Loop over runs.
            for run in sorted(run_subruns.keys(), reverse=True):

                # Loop over subruns and do actions.

                for subrun in run_subruns[run]:

                    action = self.PROD_ACTION[istatus]
                    if action != None:

                        # Read data.

                        status = self._api.get_status(ds_status(self._project,
                                                                run, subrun, 0))
                        old_data  = status._data
                        old_state = status._status
                        self._data = status._data

                        self.debug('Starting an action: %s @ %s' % (
                                action.__name__,self.now_str()))
                        statusCode = action( istatus, run, subrun )
                        self.debug('Finished an action: %s @ %s' % (
                                action.__name__,self.now_str()))

                        # Create a status object to be logged to DB (if necessary)
                        if not old_state == statusCode or not self._data == old_data:
                            self.log_status( ds_status( project = self._project,
                                                        run     = run,
                                                        subrun  = subrun,
                                                        seq     = 0,
                                                        status  = statusCode,
                                                        data    = self._data ) )

                        runid = (run, subrun)
                        self._runid_status[runid] = statusCode

        return


# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    test_obj = prodsam(proj_name)
    
    now_str = time.strftime('%Y-%m-%d %H:%M:%S')
    test_obj.info("Project %s start @ %s" % (proj_name,now_str))
    
    test_obj.process()
    
    now_str = time.strftime('%Y-%m-%d %H:%M:%S')
    test_obj.info("Project %s end @ %s" % (proj_name,now_str))

    sys.exit(0)
