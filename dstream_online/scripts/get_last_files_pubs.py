import sys
from dstream.ds_api import ds_reader
from dstream_online.ds_online_constants import *
import samweb_cli

def GetAvailableFileList(NRUNS=100):

    MAIN_RUN_TABLE = 'MainRun'
    LIMIT_NRUNS    = NRUNS
    SAM_LIST_FORMAT_STAGED = "data_tier = raw and file_type = data and file_format = binaryraw-uncompressed and run_number = %d and availability: virtual"
    SAM_LIST_FORMAT_STORED = "data_tier = raw and file_type = data and file_format = binaryraw-uncompressed and run_number = %d and availability: physical"
    SQL_CMD_FORMAT = 'SELECT Run, SubRun FROM prod_transfer_binary_evb2dropbox_near1 WHERE Status=%d ORDER BY Run DESC, SubRun DESC LIMIT %d' 
    SQL_CMD_FORMAT = SQL_CMD_FORMAT % (kSTATUS_VALIDATE_DATA,LIMIT_NRUNS)
    FNAME_SUFFIX_FORMAT = '%07d-%05d.ubdaq'

    k=ds_reader()
    samweb = samweb_cli.SAMWebClient(experiment='uboone')
    k.execute(SQL_CMD_FORMAT)

    runid_v = []
    virtual_flist_by_run = {}
    physical_flist_by_run = {}
    result_flist = []
    for run,subrun in k:

        runid = (run,subrun)
        runid_v.append(runid)
        if not run in virtual_flist_by_run:
            virtual_flist_by_run[run]  = list(samweb.listFiles(SAM_LIST_FORMAT_STAGED % run))
            physical_flist_by_run[run] = list(samweb.listFiles(SAM_LIST_FORMAT_STORED % run))

        fname_suffix = FNAME_SUFFIX_FORMAT % runid
        fname = ''
        loc   = ''
        for f in physical_flist_by_run[run]:
            if f.endswith(fname_suffix):
                fname = f
                loc = samweb.locateFile(fname)
                if loc: loc = loc[-1]
                else: loc = ''
                break

        if not fname: 
            for f in virtual_flist_by_run[run]:
                if f.endswith(fname_suffix):
                    fname = f
                    break
            if not fname:
                print '\033[93mERROR\033[00m File not found on SAM for run,subrun: %s' % str(runid)
                raise Exception()

        if not loc: loc = 'virtual'
        result_flist.append((run,subrun,fname,''))

    del k
    return result_flist

if __name__ == '__main__':

    nruns = 100
    if len(sys.argv)>1:
        nruns = int(sys.argv[1])

    for f in GetAvailableFileList(nruns):
        print f
