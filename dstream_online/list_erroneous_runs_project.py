import sys

# USAGE

if len(sys.argv) != 3:
    print "You haven't given enough input arguments."
    print "Usage: python %s project_name status_number" % sys.argv[0]
    print
    print "Note a common usage for this script is: "
    print ">> python %s prod_transfer_binary_near12dropbox_near1 160" % sys.argv[0]
    print
    print "Which will tell you the run/subruns with status 160 in that project."
    print "(160 here is the failure mode when pnfs goes down)"
    print "Then you can figure out which run/subruns you want to set those 160's to 1's and use:"
    print "$PUB_TOP_DIR/dstream_online/correct_file_status.py prod_transfer_binary_near12dropbox_near1 160 1 <run> {subrun}"
    quit()

from dstream.ds_api import ds_reader
from pub_dbi import pubdb_conn_info

# Establish a connection to the database through the DBI
dbi = ds_reader(pubdb_conn_info.reader_info())

try:
    self.dbi.connect()
except:
    print "Unable to connect to database in query thread... womp womp :("

# Get what you want
projname  = str(sys.argv[1])
statusnum = int(sys.argv[2])

dummy = dbi.get_runs(tname=projname,status=statusnum)

print "Complete summary of runs/subruns with status %d:"%statusnum
for x in dummy:
    print "Run %d \tSubrun %d" % ( x[0], x[1] )

print
print "If you want to reset entire runs, these are the runs which have any errors in them at all:"
runs = set([x[0] for x in dummy])
print [x for x in runs]
print

