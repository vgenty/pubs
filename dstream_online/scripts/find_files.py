import os
import sys

PREFIX = 'NoiseRun'
SUFFIX = 'ubdaq'

def match_file_name(path,prefix,suffix):

    # make sure the path to this directory exists
    if (os.path.isdir(path) == False):
        return -1

    fout = open(outname,'w+')

    dircontents = [ x for x in os.listdir(path) if x.endswith(SUFFIX) and x.startswith(PREFIX) ]

    # count number of bad files
    badfiles = 0
    # count number of good files
    goodfiles = 0

    for f in dircontents:

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
            while filepath.find('//') >= 0 : filepath = filepath.replace('//','/')
            fout.write('%07i\t%05i\t%s\n'%(run,subrun,filepath))
            goodfiles += 1
        except:
            print filepath
            badfiles += 1
            
    
    fout.close()

    # report
    print 'Found %d files successfully matched with the pattern...' % goodfiles
    print 'Found %d files w/ right pre/suf-fix but did not match the pattern...' % badfiles

    return 0

        
# A unit test section
if __name__ == '__main__':

    path = sys.argv[1]
    outname = sys.argv[2]
    
    match_file_name(path,outname)
