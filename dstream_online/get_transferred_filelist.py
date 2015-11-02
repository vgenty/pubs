## @namespace dstream.GET_TRANSFERRED_FILELIST
#  @ingroup dstream
#  @brief Defines a project get_transferred_filelist
#  @author uboonepro

# python include
import time,sys,os
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from dstream_online.scripts.get_last_files_pubs import GetAvailableFileList
## @class get_transferred_filelist
#  @brief uboonepro should give a brief comment here
#  @details
#  uboonepro should give a detailed comment here
class get_transferred_filelist(ds_project_base):

    # Define project name as class attribute
    _project = 'get_transferred_filelist'

    # Define # of runs to process per request
    _nruns   = 0

    # Define output file location
    _outdir  = ''

    ## @brief default ctor can take # runs to process for this instance
    def __init__(self,project):

        self._project = project

        # Call base class ctor
        super(get_transferred_filelist,self).__init__(self._project)

        self._nruns  = 0
        self._outdir = '%s/data/' % os.environ['PUB_TOP_DIR']

    def get_resource(self):

        resource = self._api.get_resource(self._project)

        self._nruns = int(resource['NRUNS'])

        if 'OUTDIR' in resource:
            self._outdir = resource['OUTDIR']
        
    ## @brief access DB and retrieves new runs
    def process_newruns(self):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
	    self.error('Cannot connect to DB! Aborting...')
	    return

        if not self._nruns:
            self.get_resource()

        flist = GetAvailableFileList(self._nruns)

        fout = open('%s/flist_pnfs.txt' % self._outdir,'w')
        for f in flist:
            fout.write('%s\n' % str(f))
        fout.close()


# A unit test section
if __name__ == '__main__':

    test_obj = get_transferred_filelist(sys.argv[1])

    test_obj.process_newruns()



