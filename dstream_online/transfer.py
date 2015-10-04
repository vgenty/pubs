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
from dstream import ds_multiprocess
from ds_online_constants import *
# ifdh
import ifdh
import subprocess as sub
import samweb_cli, extractor_dict
import pdb, json
import glob
# script module tools
from scripts import find_run

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
        #self._outfile_format = ''
        self._in_dir = ''
        #self._meta_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._parallelize = 0
        self._max_proc_time = 120
        self._min_run = 0
    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        #self._outfile_format = resource['OUTFILE_FORMAT']
        self._in_dir = '%s' % (resource['INDIR'])
        #self._meta_dir = '%s' % (resource['METADIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']

        if 'PARALLELIZE' in resource:
            self._parallelize = int(resource['PARALLELIZE'])
            
        if 'MAX_PROC_TIME' in resource:
            self._max_proc_time = int(resource['MAX_PROC_TIME'])
        
        if 'MIN_RUN' in resource:
            self._min_run = int(resource['MIN_RUN'])
    ## @brief Transfer files to dropbox
    def transfer_file( self ):

        proc_list=[]
        done_list=[]
        run_id=[]
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Start transfer_file @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
                
        # Fetch runs from DB and process for # runs specified for this instance.
        args_v  = []
        runid_v = []
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project, self._parent_project],
                                      [1, 0]):
            if ctr <=0: break

            (run, subrun) = (int(x[0]), int(x[1]))
            if run < self._min_run: break

            # Counter decreases by 1
            ctr -= 1
            
            # Report starting
            self.info('Transferring a file: run=%d, subrun=%d ...' % (run,subrun) )
            
            status = 1
            
            # Check input file exists. Otherwise report error
            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
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
            in_file_base = os.path.basename(in_file)
            out_file = '%s/%s' % ( self._out_dir, in_file_base)
            out_json = '%s/%s.json' % ( self._out_dir, in_file_base)
            
            # construct ifdh object
            #ih = ifdh.ifdh()
            #we're gonna use subprocess to parallelize these transfers and construct an ifdh command by hand

            if not os.path.isfile( in_file ) or not os.path.isfile( in_json ):
                self.error('Did not find the files that you told me to look for (run,subrun) = (%s, %s)' % (x[0],x[1]))
                self.error('Not found: %s' % (in_file) )
                self.error('Or not found: %s' % (in_json) )
                self.log_status( ds_status( project = self._project,
                                            run     = run,
                                            subrun  = subrun,
                                            seq     = 0,
                                            status  = 100 ) )
                continue

            #self.log_status( ds_status( project = self._project,
            #                            run     = run,
            #                            subrun  = subrun,
            #                            seq     = 0,
            #                            status  = 3 ) )

            args_v.append((in_file, in_json, self._out_dir))
            runid_v.append((run,subrun))

        mp = self.process_files(args_v)

        for i in xrange(len(args_v)):

            if mp.poll(i):
                self.info('Failed copy %s @ %s' % (runid_v[i],time.strftime('%Y-%m-%d %H:%M:%S')))
                self.log_status ( ds_status( project = self._project,
                                             run     = runid_v[i][0],
                                             subrun  = runid_v[i][1],
                                             seq     = 0,
                                             status  = 555 ) )

            else:
                self.info('Finished copy %s @ %s' % (runid_v[i],time.strftime('%Y-%m-%d %H:%M:%S')))
                self.log_status( ds_status( project = self._project,
                                            run     = runid_v[i][0],
                                            subrun  = runid_v[i][1],
                                            seq     = 0,
                                            status  = 0 ) )

        self.info('All finished @ %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
                
    def process_files(self,in_filelist_v):

        mp = ds_multiprocess(self._project)

        for i in xrange(len(in_filelist_v)):

            in_file,in_json,out_dir = in_filelist_v[i]
            cmd = ['ifdh','cp','-D',in_file,in_json,out_dir]

            self.info('Transferring %s @ %s' % (in_file,time.strftime('%Y-%m-%d %H:%M:%S')))

            index,active_ctr = mp.execute(cmd)

            if not self._parallelize:
                mp.communicate(index)
            else:
                time_slept = 0
                while active_ctr > self._parallelize:
                    time.sleep(0.2)
                    time_slept += 0.2
                    active_ctr = mp.active_count()

                    if time_slept > self._max_proc_time:
                        self.error('Exceeding time limit %s ... killing %d jobs...' % (self._max_proc_time,active_ctr))
                        mp.kill()
                        break
                    if int(time_slept) and int(time_slept)%3 < 0.3 == 0:
                        self.info('Waiting for %d/%d process to finish...' % (active_ctr,len(in_filelist_v)))
        time_slept=0
        while mp.active_count():
            time.sleep(0.2)
            time_slept += 0.2
            if time_slept > self._max_proc_time:
                mp.kill()
                break
        return mp


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
            filelist = find_run.find_file(self._in_dir,self._infile_format,run,subrun)
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
            if_file_basename = os.path.basename(in_file)
            out_file = '%s/%s' % ( self._out_dir, in_file_base)
            out_json = '%s/%s.json' %( self._out_dir, in_file_base)

            # construct ifdh object
            ih = ifdh.ifdh()

            try:
                ih.locateFile( out_file )
                ih.locateFile( out_json )
                status = 0
            except:
                status = 1

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

    #if "pnnl" not in proj_name:
    #    obj.validate_outfile()
