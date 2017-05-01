import subprocess
from itertools import izip

def exec_system(input_):
    return subprocess.Popen(input_,stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]
 
def exec_ssh(who,where,what):
    SS = ["ssh","-T","-x","%s@%s"%(who,where),what]
    return exec_system(SS)

# thanks daniel
# http://lemire.me/blog/2004/11/25/computing-argmax-fast-in-python/
def argmax(arr_):
    return max(izip(arr_, xrange(len(arr_))))[1]


