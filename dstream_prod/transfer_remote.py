## @namespace dstream_online.transfer_remote
#  @ingroup dstream_online
#  @brief Defines a project transfer_remote
#  @author echurch,yuntse

# python include
import time, os, sys, subprocess
# pub_dbi package include
from pub_dbi import DBException
# dstream class include
from dstream import DSException
from dstream import ds_project_base
from dstream import ds_status
from pub_util import pub_smtp
import subprocess as sub
from collections import defaultdict
import samweb_cli, extractor_dict
import pdb, json


## @class transfer_remote
#  @brief Transferring files
#  @details
#  This process mv's a file to a dropbox directory for SAM to whisk it away...
#  Status codes:
#    2: Copied the file to dropbox

kSTATUS_NO_KNOWN_SITE = 100
kSTATUS_FAILED_FILE_XFER = 101

class transfer_remote( ds_project_base ):

    # Define project name as class attribute
    _project = 'transfer_remote'

    ## @brief default ctor can take # runs to process for this instance
    def __init__( self, arg = '' ):

        # Call base class ctor
        super( transfer_remote, self ).__init__( arg )

        if not arg:
            self.error('No project name specified!')
            raise Exception

        self._project = arg
        self._nruns = None
        self._out_dir = ''
        self._infile_format = ''
        self._parent_project = ''
        self._stream = ''
        self._samweb = None

    ## @brief method to retrieve the project resource information if not yet done
    def get_resource( self ):

        resource = self._api.get_resource( self._project )

        self._nruns = int(resource['NRUNS'])
        self._ndelays = int(resource['NDELAYS'])
#        self._out_dir = '%s' % (resource['OUTDIR'])
#        self._infile_format = resource['INFILE_FORMAT']
        self._parent_project = resource['PARENT_PROJECT']
        self._stream = resource['STREAM']
        exec('self._sort_new_to_old = bool(%s)' % resource['SORT_NEW_TO_OLD'])


    def transfer_file( self ):

        # Attempt to connect DB. If failure, abort
        if not self.connect():
            self.error('Cannot connect to DB! Aborting...')
            return

        # If resource info is not yet read-in, read in.
        if self._nruns is None:
            self.get_resource()

        pdb.set_trace()
        if not self.voms_proxy_check ():
            self.voms_proxy_get()
            if not self.voms_proxy_check ():
                self.error ('Could not get a proxy')
                raise Exception


        # self.info('Here, self._nruns=%d ... ' % (self._nruns) )
        self._samweb = samweb_cli.SAMWebClient(experiment="uboone")

        # Fetch runs from DB and process for # runs specified for this instance.
        # Remember that we're looking for files with many subruns collapsed. So, most files will not exist.
        l = list()
        ctr = self._nruns


        run = subrun = None
        for x in self.get_xtable_runs([self._project, self._parent_project],
                                      [1, 10] ):  # ,self._sort_new_to_old):
            # Counter decreases by 1
            ctr -= 1
            (run, subrun) = (int(x[0]), int(x[1]))
            l.append([run, subrun])
            if not ctr: break


        rs_dict = defaultdict(list)
        for k,v in l:
            rs_dict[k].append(v)


        for runk in rs_dict.keys():

            # This is a unique run from our above list of runs.
            files = self._samweb.listFiles("run_number=" + str(runk) + " AND data_tier=reconstructed-2d AND ub_project.name=" + self._stream)
            
            for f in files:

                subs = list()
                status = kSTATUS_FAILED_FILE_XFER
                m = self._samweb.getMetadata(filenameorid = f)
                # Make a list subs of subruns merged in this file.
                for s in range(len(m['runs'])):
                    sub = m['runs'][s][1] # subrun from meta data
                    subs.append(sub)

                if len(list(set(subs) & set(rs_dict[runk]))) > 0:
                    # If any of the r,s pairs that we got from get_xtable live in this file, we transfer the file
                    # And below set the status for all those r,s pairs.
                    # And, we note, if we come into this project subsequently, asking for different subruns from 
                    # this same run, which shouldn't happen, it'll fail the get_xtable call, and we won't try again
                    # to transfer that file (as is appropriate).
                    if "pnnl" in self._project:
                        self.info('Will  look for  %s' % f )
                        try:
                            if "pnnl" in self._project:
                                (resi, resj) = self.pnnl_transfer(f)
                            if resi == 0 and resj == 0:
                                status = 0
                            else:
                                status = 1
                        except:
                            pass

                    else:
                        status = kSTATUS_NO_KNOWN_SITE
                        self.error("Big problem: This project not doing a transfer to PNNL.")

                        
                    # Create a status object to be logged to DB (if necessary). Do this for each subrun in this run's file.
                    for s in subs:

                        statuss = ds_status( project = self._project,
                                            run     = runk,
                                            subrun  = s,
                                            seq     = 0,
                                            status  = status,
                                            data = None )

                        # Log status
                        self.log_status( statuss )



    def voms_proxy_check ( self ):

        cmd = "voms-proxy-info -all "
        proc = sub.Popen(cmd,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
        (out,err) = proc.communicate()
        goodProxy = False

        for line in out.split('\n'):
            if "timeleft" in line:
                if int(line.split(" : ")[1].replace(":","")) > 0:
                    goodProxy = True
                    break;

        if not goodProxy:
            self.info('uboonepro has no good proxy.')

        return goodProxy

    def voms_proxy_get ( self ):

        cmd = "voms-proxy-init -rfc -cert /uboone/app/home/uboonepro/ubooneprocert.pem -key /uboone/app/home/uboonepro/ubooneprokey.pem -voms fermilab:/fermilab/uboone/Role=Analysis"
        proc = sub.Popen(cmd,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
        (out,err) = proc.communicate()

        for line in out.split('\n'):
            if "Your proxy " in line:
                self.info('Apparently got uboonepro a good proxy')
                break;

        return
   


    def pnnl_transfer( self, file_arg ):

        # sshftp-to-sshftp not enabled on near1, so must use gsiftp: must thus ship from the Sam'd-up dcache file.
        # uboonepro from uboonegpvm0N has an ssh-key to sshftp to dtn2.pnl.gov as chur558.
        # This requires that uboonepro owns a valid proxy. We might get 2 Gbps throughput with this scenario.

        in_file = file_arg
    # We do a samweb.fileLocate on basename of in_file. 
        transfer = 0

        loc = self._samweb.locateFile(filenameorid=in_file)
        size_in = self._samweb.getMetadata(filenameorid=in_file)['file_size']
        samcode = 12

        # We expect to transfer files out of dCache.
        if not ('enstore' in loc[0]["full_path"] and 'pnfs' in loc[0]["full_path"]):
            self.error('No enstore or pnfs in loc[0]["full_path"]')
            return (transfer, samcode)


        full_file = loc[0]["full_path"].replace('enstore:/pnfs','') + "/" +  in_file

        pnnl_machine = "dtn2.pnl.gov"
        pnnl_dir = 'pic/projects/microboone/data/'
        ddir = str(self._samweb.getMetadata(filenameorid=in_file)['runs'][0][0])
        cmd_mkdir = "ssh chur558@" + pnnl_machine + " mkdir -p "  + "/" + pnnl_dir + self._stream + "/" + ddir 
        proc = sub.Popen(cmd_mkdir,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
        # block, but plow on w.o. regard to whether I was successful to create ddir. (Cuz this will complain harmlessly if run is not new.) 
        (out,err) = proc.communicate() 
        
        pnnl_loc = pnnl_machine + "/" + pnnl_dir + self._stream + "/" + ddir + "/" + in_file
        cmd_gsiftp_to_sshftp = "globus-url-copy -rst -vb -p 10 gsiftp://fndca1.fnal.gov:2811" + full_file + " sshftp://chur558@" + pnnl_loc

        # Popen() gymnastics here
        ntry = 0
        delay = 20
        ntry_max = 1 # more than 1 is not demonstrably helping. In fact, it creates lotsa orphaned ssh's. EC, 8-Aug-2015.
        ndelays = self._ndelays # 20
        while (ntry != ntry_max):
            self.info('Will launch ' + cmd_gsiftp_to_sshftp)
            info_str = "pnnl_transfer trying " + str(ntry+1) + " (of " + str(ntry_max) + ") times."
            self.info (info_str)
            proc = sub.Popen(cmd_gsiftp_to_sshftp,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
            wait = 0
            transfer = 0
            while  proc.poll() is None:
                wait+=delay
                if wait > delay*ndelays:
                    self.error ("pnnl_transfer timed out in awaiting transfer.")
                    proc.kill()
                    transfer = 11
                    break
                self.info('pnnl_transfer process ' + str(proc.pid) + ' active for ' + str(wait) + ' [sec]')
                time.sleep (delay)
            if (transfer != 11):
                break

            # This rm usually fails for reasons I don't understand. If it succeeded the retry would work.
            # Commenting it out, since we're setting ntry_max=1 now anyway, EC 8-Aug-2015.
            # rm the (usually) 0 length file.
            #cmd_rm = "ssh chur558@" + pnnl_machine + " rm -f "  + "/" + pnnl_dir + ddir + "/" + in_file
            #proc = sub.Popen(cmd_rm,shell=True,stderr=sub.PIPE,stdout=sub.PIPE)
            #self.info("pnnl_transfer process: Attempting to remove " + pnnl_dir + ddir + "/" + in_file)
            #time.sleep(5) # give it 5 seconds, then kill it if not done.
            #if proc.poll() is None:
            #    proc.kill()
            #    self.error("pnnl_transfer process: " + cmd_rm + " failed.")

            ntry+=1
            ndelays+=10 # extend time period to force timeout for next effort.




        size_out = 0
        if not transfer:
            (out,err) = proc.communicate()
            transfer = proc.returncode
                # also grep the out for indication of success at end.

            if not transfer:
#                self.info('out is ' + out)
                li = out.split(" ")
                mind = max(ind for ind, val in enumerate(li) if val == 'bytes') - 1
                size_out = int(li[mind])
                transfer = 10

                if size_out == size_in:
                    transfer = 0


# end file lives in enstore

        pdb.set_trace()
        if not transfer:
            try:
                # Then samweb.addFileLocation() to pnnl location, with resj capturing that return status.
                pnnl_loc_withcolon = pnnl_machine + ":/" + pnnl_dir + self._stream + "/" + ddir + "/" + in_file
                samadd = self._samweb.addFileLocation(filenameorid=in_file,location=os.path.dirname(pnnl_loc_withcolon))
                samloc  = self._samweb.locateFile(filenameorid=in_file)
                if len(samloc)>0:
                    samcode = 0
                    self.info('pnnl_transfer() finished moving ' + in_file + ', size ' + str(size_in) + ' [bytes], to PNNL')
                    self.info('Transfer rate was ' + str(out.split("\n")[4].split("    ")[8]))
                    self.info('Transfer and samaddFile are successful. Full SAM location for file is now ' + str(samloc))
            except:
                self.error('pnnl_transfer finished with a problem on ' + in_file + ' during addFile or locFile. samadd/samloc is: ' + str(samadd)+"/"+str(samloc))
        else:
            self.error('pnnl_transfer finished with a problem on ' + in_file)
                
        return (transfer, samcode)




# A unit test section
if __name__ == '__main__':

    proj_name = sys.argv[1]

    obj = transfer_remote( proj_name )

    obj.transfer_file()

    # if "pnnl" not in self._project:
    #    obj.validate_outfile()
