from dstream_online.get_metadata_no-hang import get_metadata
import sys

files = [x for x in open(sys.argv[1],'r').read().split() if x.endswith('.ubdaq')]

obj = get_checksum('get_binary_metadata_evb')
obj.get_resource()
obj._parallelize = 5
obj._max_proc_time=60
mp = obj.process_ubdaq_files(files)
