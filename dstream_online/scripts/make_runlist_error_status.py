import os
import sys

from dstream.ds_api import ds_reader

def write_runlist_error_status(proj_name):

    # open an instance of the DB reader
    reader = ds_reader()    

    # get list of all statuses
    status_list = reader.list_status(proj_name)[proj_name]

    # loop through status values (ignoring those != 0 or 1)
    for status in status_list:
        
        status_number  = int(status[0])
        status_entries = int(status[1])

        if ( (status_number == 0) or (status_number == 1) ):
            continue

        # get the runs that have this status
        runs = reader.get_runs(proj_name,status_number)

        # open file for this status
        fout = open('%s_status_%05i.txt'%(proj_name,status_number),'w+')

        for run in runs:
            fout.write('%07i\t%05i\n'%(run[0],run[1]))

        fout.close()

    return
        


# A unit test section

if __name__ == '__main__':

    if (len(sys.argv) == 1):
        print
        print 'incorrect usage:'
        print 'try: python make_runlist_error_status.py PROJ_NAME'
        print
        sys.exit(0)

    write_runlist_error_status(sys.argv[1])
        
    sys.exit(0)
    
