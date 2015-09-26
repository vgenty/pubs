import os
import sys

PREFIX = 'PhysicsRun'
SUFFIX = 'ubdaq'

def match_file_name(path,outname):

    # make sure the path to this directory exists
    if (os.path.isdir(path) == False):
        return -1

    fout = open(outname,'w+')

    dircontents = os.listdir(path)

    # count number of bad files
    badfiles = 0

    for f in dircontents:

        filepath = path+'/'+f

        # skip this object if not a file
        if (os.path.isfile(filepath) == False):
            continue

        # search for the file-pattern in the file-name
        # pattern is:
        # PREFIX-*-%07i-%05i.SUFFIX
        
        # is the PREFIX at the beginning?
        if (f.find(PREFIX) != 0):
            continue

        # make sure we find the SUFFIX
        if (f.find(SUFFIX) < 0):
            continue

        # make sure the SUFFIX is at the end
        if ( (len(f) - (f.find(SUFFIX) + len(SUFFIX)) ) != 0):
            continue

        # split file according to '-'
        words = f.split('-')

        try:
            run    = int(words[2])
            subrunstr = words[3].split('.')[0]
            subrun = int(subrunstr)
            fout.write('%07i\t%05i\t%s\n'%(run,subrun,filepath))

        except:
            badfiles += 1
            

    fout.close()

    return 0

        
# A unit test section
if __name__ == '__main__':

    path = sys.argv[1]
    outname = sys.argv[2]
    
    match_file_name(path,outname)
