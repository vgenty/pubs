## @namespace dstream_online.verify_dropbox
#  @ingroup verify_dropbox
#  @brief Defines a project verify_dropbox
#  @author kirby

# python include
import time, os, sys
# pub_dbi package include
from pub_dbi import DBException
# pub_util package include
from pub_util import pub_smtp
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
import samweb_cli
import traceback

pnfs_prefixes = {
   "/pnfs/cdfen": "srm://cdfdca1.fnal.gov:8443/srm/managerv2?SFN=",
   "/pnfs/dzero": "srm://d0ca1.fnal.gov:8443/srm/managerv2?SFN=",
   "/pnfs/": "srm://fndca1.fnal.gov:8443/srm/managerv2?SFN=",
}

BASE = 65521

def get_prefix( path ) :
    list = pnfs_prefixes.keys()
    list.sort(reverse=1)
    for prefix in list:
        if path.startswith(prefix):
            return pnfs_prefixes[prefix]
    raise LookupError("no server known for %s" % path)

def get_pnfs_1_adler32_and_size( path ):
    sum = 0
    first = True
    cmd = "srmls -2 -l %s/pnfs/fnal.gov/usr/%s" % ( get_prefix(path), path[5:])
    #print "running: " , cmd
    pf = os.popen(cmd)
    for line in pf:
        #print "read: " , line
        if first:
            if line[-1] == '/' or line[-2] == '/':
                pf.close()
                raise LookupError('path is a directory: %s' % path)

            size = long(line[2:line.find('/')-1])
            first = False
            continue

        if line.find("Checksum value:") > 0:
            ssum = line[line.find(':') + 2:]
            #print "got string: ", ssum
            sum = long( ssum , base = 16 )
            #print "got val: %lx" % sum
            pf.close()
            return  sum, size

    pf.close()
    raise LookupError("no checksum found for %s" % path)

def convert_0_adler32_to_1_adler32(crc, filesize):
    crc = long(crc)
    filesize = long(filesize)
    size = int(filesize % BASE)
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) &  0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    return (s2 << 16) + s1


class verify_dropbox( ds_project_base ):

    _project = 'verify_dropbox'


    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( verify_dropbox, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg

        self._nruns = None
        self._in_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._experts = ''
        self._data = ''

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._in_dir = '%s' % (resource['INDIR'])
        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']
        self._ref_project = resource['REF_PROJECT']
        self._experts = resource['EXPERTS']


    ## @brief calculate the checksum of a file
    def compare_dropbox_checksum( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_xtable_runs( [self._project, self._parent_project], [1, 0] ):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Calculating the file checksum: run=%d, subrun=%d ...' % (run,subrun))

            statusCode = 1

            in_file_name = self._infile_format % ( run, subrun )
            in_file = '%s/%s' % ( self._in_dir, in_file_name )

            #Note that this has the sequence number hard coded as number 0
            RefStatus = self._api.get_status( ds_status(self._ref_project, run, subrun, 0))
            near1_checksum = RefStatus._data

            try:
                pnfs_adler32_1, pnfs_size = get_pnfs_1_adler32_and_size( in_file )
                near1_adler32_1 = convert_0_adler32_to_1_adler32(near1_checksum, pnfs_size)

                if near1_adler32_1 == pnfs_adler32_1:
                    statusCode = 0
                else:
                    subject = 'Checksum different in run %d, subrun %d between %s and PNFS' % ( run, subrun, self._ref_project )
                    text = '%s\n' % subject
                    text += 'Run %d, subrun %d\n' % ( run, subrun )
                    text += 'Converted %s checksum: %s\n' % ( self._ref_project, near1_adler32_1 )
                    text += 'Converted PNFS checksum: %s\n' % ( pnfs_adler32_1 )
                    
                    pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )
                    statusCode = 1000
                    self._data = '%s:%s;PNFS:%s' % ( self._ref_project, near1_adler32_1, pnfs_adler32_1 )

            except LookupError:

                self.warning("Could not find file in the dropbox %s" % in_file)
                self.warning("Gonna go looking on tape %s" % in_file_name)
                samweb = samweb_cli.SAMWebClient(experiment="uboone")
                meta = {}

                try:
                    meta = samweb.getMetadata(filenameorid=in_file_name)
                    checksum_info = meta['checksum'][0].split(':')
                    if checksum_info[0] == 'enstore':
                        self._data = checksum_info[1]
                        statusCode = 0
                    else:
                        statusCode = 10

                except samweb_cli.exceptions.FileNotFound:
                    subject = 'Failed to locate file %s at SAM' % in_file
                    text = 'File %s is not found at SAM!' % in_file
                    pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )
                    statusCode = 100

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = statusCode,
                                data    = self._data )

            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break

    ## @brief check the checksum is in the table
    def check_db( self ):
        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        self.info('Here, self._nruns=%d ... ' % (self._nruns))

        # Fetch runs from DB and process for # runs specified for this instance.
        ctr = self._nruns
        for x in self.get_runs( self._project, 2 ):

            # Counter decreases by 1
            ctr -= 1

            (run, subrun) = (int(x[0]), int(x[1]))

            # Report starting
            self.info('Calculating the file checksum: run=%d, subrun=%d ...' % (run,subrun))

            statusCode = 2
            in_file_name = self._infile_format % ( run, subrun )
            in_file = '%s/%s' % ( self._in_dir, in_file_name )

            # Get status object
            status = self._api.get_status(ds_status(self._project,
                                                    x[0],x[1],x[2]))

            self._data = status._data
            self._data = str( self._data )

            if self._data:
               statusCode = 0
            else:
                subject = 'Checksum of the file %s not in database' % in_file
                text = """File: %s
Checksum is not in database
                """ % ( in_file )

                pub_smtp( os.environ['PUB_SMTP_ACCT'], os.environ['PUB_SMTP_SRVR'], os.environ['PUB_SMTP_PASS'], self._experts, subject, text )

                statusCode = 100

            # Create a status object to be logged to DB (if necessary)
            status = ds_status( project = self._project,
                                run     = run,
                                subrun  = subrun,
                                seq     = 0,
                                status  = statusCode,
                                data    = self._data )

            # Log status
            self.log_status( status )

            # Break from loop if counter became 0
            if not ctr: break


if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = verify_dropbox( proj_name )

    obj.compare_dropbox_checksum()
