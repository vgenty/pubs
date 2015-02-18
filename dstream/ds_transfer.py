## @namespace dummy_dstream.ds_transfer
#  @ingroup dummy_dstream
#  @brief Defines a project ds_transfer
#  @author zarko

# python include
import sys
import time, os, shutil, subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status

## @class ds_transfer
#  @brief Project to transfer files
#  @details
#  More complete transfer file script based on Kazu's dummy_nubin_xfer\n
#  Copies file from one path to another using rsync. \n
#  Copy is validated by checking md5sum

class ds_transfer(ds_project_base):

    _project='ds_transfer'

    ## @brief default ctor
    def __init__(self, project_name):
        
        self._project=project_name
        # Call base class ctor
        super(ds_transfer,self).__init__()

        self.info('Running transfer project %s'%self._project)
        if (self._project==''):
            self.error('Missing project name argument')
            return

        self._nruns = None
        self._in_dir = ''
        self._out_dir = ''
        self._name_pattern = ''
        self._bwlimit = 0

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource(self):

        resource = self._api.get_resource(self._project)
        
        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._out_dir = '%s' % (resource['OUTDIR'])
        self._name_pattern = resource['NAME_PATTERN']
        self._bwlimit = int(resource['BANDWIDTH_LIMIT'])

    ## @brief access DB and retrieves new runs and process
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs([self._project,'mainrun'],
                                      [1,0]):
#        for x in self.get_runs(self._project,1):

            (run, subrun) = (int(x[0]), int(x[1]))
            status = 1
            # Counter decreases by 1
            ctr -=1

            # Generate input, output file names
            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._name_pattern % (run,subrun))

            dt=time.strftime("%Y%m%d-%H%M%S")
            cmd=["rsync","-e \"ssh -x\"","-bptgo","--suffix=_%s"%dt,in_file,out_file]
            if self._bwlimit>0:
                cmd+=["--bwlimit=%i"%self._bwlimit]
            p=subprocess.Popen(' '.join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.debug('Executing %s'%' '.join(cmd))
            (out,err)=p.communicate(None)
            if not p.returncode:
                status=2
                self.info('Transfered %s to %s'%(in_file,out_file))                
            else:
                status=100
                self.error("Failed to transfer %s to %s"%(in_file,out_file))
                for line in err.split("\n"):
                    self.error(line)

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

    ## @brief access DB and retrieves processed run for validation
    def validate(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project,2):

            # Counter decreases by 1
            ctr -=1
            (run, subrun) = (int(x[0]), int(x[1]))

            status = 1
            in_file = '%s/%s' % (self._in_dir,self._name_pattern % (run,subrun))
            out_file = '%s/%s' % (self._out_dir,self._name_pattern % (run,subrun))
            # Report starting
            self.info('Checking %s transfer' % (out_file))
            
            fmd5=[]
            for fin in in_file, out_file:                
                if ":" in fin:
                    if not os.system('ssh -x %s "test -f %s"'%(tuple(fin.split(":")))):
                        fmd5.append(os.popen('ssh -x %s "md5sum -b %s"'%(tuple(fin.split(":")))).read())
                    else:
                        self.error('File %s does not exist'%fin)
                        status = 100
                else:
                    if os.path.isfile(fin):
                        fmd5.append(os.popen('md5sum -b %s'%(fin)).read())
                    else:
                        self.error('File %s does not exist'%fin)
                        status = 100

            if status == 1:
                if (fmd5[0].split()[0]==fmd5[1].split()[0]):
                    self.info('Cheksum ok! %s %s'%tuple(fmd5[1].split()))
                    status = 0
                else:
                    self.error('Failed md5sum!')
                    status = 100

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

    ## @brief access DB and retrieves runs for which 1st process failed. Clean up.
    def error_handle(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs(self._project,100):

            # Counter decreases by 1
            ctr -=1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            status = 1
            out_file = '%s/%s' % (self._out_dir,self._name_pattern % (run,subrun))
            self.info('Removing failed transfer %s' %(out_file))

            if ":" in out_file:
                #check that out_file is a file before trying to remove 
                #(hopefully should avoid unintentional rm with bad out_dir/name_pattern combo)
                if not os.system('ssh -x %s "test -f %s"'%(tuple(out_file.split(":")))):
                    os.system('ssh -x %s "rm %s"' % tuple(out_file.split(":")))
            else:
                if os.path.isfile(out_file):
                    os.system('rm %s' % out_file)

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


# A unit test section
if __name__ == '__main__':
    
    if (len(sys.argv)==2):
        test_obj = ds_transfer(sys.argv[1])
        test_obj.process_newruns()        
        test_obj.error_handle()        
        test_obj.validate()
    else:
        test_obj = ds_transfer('')
