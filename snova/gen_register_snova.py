#
# generate config file for pubs registration
# register_snova_template.cfg => register_snova.cfg
#

import os,sys
INFILE = "register_snova_template.cfg"
OUTFILE= "register_snova.cfg"
SEB_v = ["%02d" % id_ for id_ in xrange(1,11)]

infile_data = None
with open(INFILE,'r') as f_:
    infile_data = f_.read()

fout = open(OUTFILE,'w+')

SPLIT_STR="############################################################"
infile_data_v = infile_data.split(SPLIT_STR)[1:]

for outfile_data in infile_data_v:
    SS = outfile_data
    SS = SS.split("\n")[1:]
    project = SS[0].split("###############")[1].split(" ")[-1]
    SS = "\n".join(SS[1:])

    fout.write(SPLIT_STR+"\n")
    fout.write("#" + project + "\n")

    if project == "MONITOR":
        fout.write(SS)
        continue

    for seb in SEB_v:
        outfile_data = SS.replace("XX",seb)
        fout.write(outfile_data)


fout.close()

