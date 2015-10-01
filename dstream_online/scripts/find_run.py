import os, sys, glob

# find the path to a file, if it exists
# input:
# filedir -> path to the folder where the files should be
# format  -> format of file name (eg *-%07d-%05d.ubdaq)
# run     -> run number to search for
# subrun  -> subrun number to search for
# output:
# list of filepaths for all files found that match the search criteria
def find_file(filedir,format,run,subrun):

    path_to_file = '%s/%s'%(filedir, format%(run,subrun) )
    
    # use glob to find all files that match this path
    filelist = glob.glob( path_to_file )

    return filelist


# Unit-test script

if __name__ == '__main__' :

    if (len(sys.argv) != 5):
        print
        print 'Correct usage:'
        print 'python find_run.py FILE_DIR FILE_FORMAT RUN SUBRUN'
        print
        sys.exit(0)

    file_dir    = sys.argv[1]
    file_format = sys.argv[2]
    run         = int(sys.argv[3])
    subrun      = int(sys.argv[4])
    
    print find_file(file_dir,file_format,run,subrun)

    sys.exit(0)
