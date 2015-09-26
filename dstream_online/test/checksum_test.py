from dstream_online.get_checksum_mp import get_checksum
import sys

files = [x for x in open(sys.argv[1],'r').read().split()]

obj = get_checksum('get_binary_checksum_near1')
obj.get_resource()
obj._parallelize = 10
obj._max_proc_time=60
mp = obj.process_files(files)
