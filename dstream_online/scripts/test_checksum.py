import os, sys

import dstream_online.get_checksum

def test_checksum(infile):

    # take infile which should have one file per line
    # and convert it to list
    file_list = []

    if not os.path.isfile(infile):
        print 'input file not valid...exiting'
        sys.exit(1)

    fin = open(infile,'r')
    for line in fin:
        fname = line.split()[0]
        file_list.append(fname)

    # declare project name
    PROJ_NAME = 'get_binary_checksum_evb'
    
    # instantiate rpoject
    get_checksum_proj = dstream_online.get_checksum.get_checksum(PROJ_NAME)

    # get resources
    get_checksum_proj.get_resource()

    # actually perform checksum calculation
    mp = get_checksum_proj.process_files(file_list)

    # get file where to store checksum info
    fout = open('checksum_results.txt','w+')

    for i in xrange(len(file_list)):

        (out,err) = mp.communicate(i)

        fout.write( '%s\t%s\t%s' % ( file_list[i], out, err) )
        

    fout.close()

    return

# A unit test
if __name__ == '__main__':

    if (len(sys.argv) != 2):
        print
        print 'usage : python test_checksum.py INFILE'
        sys.exit(0)

    infile = sys.argv[1]
    
    test_checksum(infile)
    sys.exit(0)
        

