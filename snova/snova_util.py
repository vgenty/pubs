import os, sys
import subprocess
from itertools import izip

#
# persistent file to determine if draining
#

# start draining -- put a 1
def start_drain(drain_file):
    with open(drain_file,'w+') as f: 
        f.write(str(1))
    return

# stop draning -- put a 0
def stop_drain(drain_file):
    with open(drain_file,'w+') as f: 
        f.write(str(0))
    return 

# read the drain
def read_drain(drain_file):
    drain = None
    with open(drain_file,'r') as f: 
        drain = f.read()
    return int(drain)

#
# subprocess execute
#
def exec_system(input_):
    return subprocess.Popen(input_,stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]

#
# ssh command 
#
def exec_ssh(who,where,what):
    SS = ["ssh","-oStrictHostKeyChecking=no","-oGSSAPIAuthentication=yes","-T","-x","%s@%s"%(who,where),what]
    return exec_system(SS)

#
# argmax without numpy (https://goo.gl/N3oR3T)
#
def argmax(arr_):
    return max(izip(arr_, xrange(len(arr_))))[1]


#
# query each seb and calculate the disk usage
#
def query_seb_snova_size(seb_ids,seb_names):
    
        seb_size_v = [0.0]*len(seb_ids)

        for ix,seb in enumerate(seb_names):
            ret_ = exec_ssh("root",seb,"nice -19 ionice -c3 df /datalocal/")
            size_, used_, unused_ = ret_[-1].split(" ")[6:9]
            seb_size_v[ix] = float(used_) / float(size_)
        
        max_idx = argmax(seb_size_v)

        return seb_size_v, max_idx

#
# query for the creation and modified times for these files over ssh
# open SSH connection and call `stat` for all necessary files at once
# to avoid per-file-ssh-query
#
def query_creation_times(data_path,file_info,sebname):

    sshproc = subprocess.Popen(['ssh','-T',sebname], 
                               stdin = subprocess.PIPE, 
                               stdout = subprocess.PIPE, 
                               universal_newlines = True,
                               bufsize = 0)
    
    for f_ in file_info:
        filepath = os.path.join(data_path,file_info[f_][0])
        cmd = "nice -19 ionice -c3 stat -c %%Y-%%Z %s" % filepath
        sshproc.stdin.write("%s\n"%cmd)
        sshproc.stdin.write("echo END\n")

    sshproc.stdin.close()

    values = []

    print values

    # get the return
    for return_ in sshproc.stdout:
        if return_.rstrip('\n')!="END":
            values.append(return_.rstrip('\n'))
    
    # fill the dictionary with creation and modify time
    for ix, run_subrun in enumerate(file_info):
            
        time_create, time_modify = values[ix].split("-")
        
        file_info[run_subrun][1] = time_create
        file_info[run_subrun][2] = time_modify

    return file_info

#
# query checksum on remote serve
#
def query_checksum(who,where,what):
    cmd = str("")
    cmd = "source /grid/fermiapp/products/uboone/setup_uboone.sh 1>/dev/null 2>/dev/null; setup sam_web_client; samweb file-checksum %s;"  % what
    out = exec_ssh(who,where,cmd)
    print out
    return out

#
# insert sebname into frament name
#
def insert_sebname(in_file_name,seb):
    out_file_name = in_file_name.split("-")
    out_file_name.insert(2,seb)
    out_file_name = "-".join(out_file_name)

    return out_file_name

