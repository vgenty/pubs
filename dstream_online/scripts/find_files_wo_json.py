import os
import sys

PREFIX = 'NoiseRun'
SUFFIX = 'ubdaq'
ASS_SUFFIX = 'json'
def match_file_name(path,outname):

    # make sure the path to this directory exists
    if (os.path.isdir(path) == False):
        return -1

    ubdaq_files = [ x for x in os.listdir(path) if x.endswith(SUFFIX) and x.startswith(PREFIX) ]
    json_files  = [ x for x in os.listdir(path) if x.endswith(SUFFIX) and x.startswith(PREFIX) ]

    nojson_files = []

    # count number of bad files
    badfiles = 0

    for f in ubdaq_files:

        filepath = path+'/'+f

        # skip this object if not a file
        if (os.path.isfile(filepath) == False):
            continue

        # search for the file-pattern in the file-name
        # pattern is:
        # PREFIX-*-%07i-%05i.SUFFIX
        
        # split file according to '-'
        words = f.split('-')

        try:
            run    = int(words[-2])
            subrunstr = words[-1].split('.')[0]
            subrun = int(subrunstr)
            # reaching this point means it's right format. Now ask if json exists
            json_name = f + '.json'
            if not json_name in json_files:
                nojson_files.append(f)
        except Exception:
            continue

    print 'Found %d/%d w/o json file...' % (len(nojson_files),len(ubdaq_files))

    fout = open(outname,'w+')
    for f in nojson_files:
        while f.find('//') >= 0: f=f.replace('//','/')
        fout.write('%s\n' % f)

    fout.close()

    return 0

        
# A unit test section
if __name__ == '__main__':

    path = sys.argv[1]
    outname = sys.argv[2]
    
    match_file_name(path,outname)
