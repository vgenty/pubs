import os, sys
from dstream.ds_api import ds_reader

# given a list of parent names and status for those parents
# return a list of (run,subrun) for this project
# that match the conditions specified
def get_runs_to_be_processed( parent_names , parent_statuses, new_to_old=True ):

    # parent_names = AAAA:BBBB:CCCC
    # where AAAA,BBBB,CCCC are project names
    # parent_statuses = 0:0:1
    # where 0,0,1 are the status required for each project, respecively
    
    proj_list  = parent_names.split(':')
    proj_value_str = parent_statuses.split(':')
    proj_value_int = []
    for value in proj_value_str:
        proj_value_int.append(int(value))
    
    reader = ds_reader()

    runs = reader.get_xtable_runs(proj_list,proj_value_int,new_to_old)

    return runs

# A unit-test function
if __name__ == '__main__':

    if (len(sys.argv) != 3):
        print
        print 'correct usage:'
        print 'python get_runs_to_be_processed PARENT_LIST PARENT_VALUE'
        print
        sys.exit(0)

    parent_list = sys.argv[1]
    parent_value = sys.argv[2]

    runs = get_runs_to_be_processed(parent_list,parent_value)

    for run in runs:
        print 'run: %07i\tsubrun: %05i'%(run[0],run[1])

    sys.exit(0)
