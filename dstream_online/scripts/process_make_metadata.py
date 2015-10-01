from dstream_online.eric import get_metadata
from dstream_online.ds_online_env import *
import sys,json

PROCESS_ROOT = False

input_list = [(int(x.split()[0]),
               int(x.split()[1]),
               str(x.split()[2])) for x in open(sys.argv[1],'r').read().split('\n') if x.endswith('.ubdaq')]

if not len(input_list):
    input_list = [(int(x.split()[0]),
                   int(x.split()[1]),
                   str(x.split()[2])) for x in open(sys.argv[1],'r').read().split('\n') if x.endswith('.root')]
    PROCESS_ROOT = True
#
# Make json files
#

# json maker
json_maker = get_metadata('get_binary_metadata')
json_maker.get_resource()
json_maker._parallelize = 5
json_maker._max_proc_time=60
json_maker._metadata_type = kUBDAQ_METADATA
if PROCESS_ROOT:
    json_maker._metadata_type = kSWIZZLED_METADATA
# run json maker
files_v = []
runid_v = []
for f in input_list:
    files_v.append(f[2])
    runid_v.append((f[0],f[1]))
res_v = None
if not PROCESS_ROOT:
    res_v = json_maker.process_ubdaq_files(files_v)
else:
    res_v = json_maker.process_swizzled_files(files_v)

json_fail_list=[]
json_success_list=[]
for i in xrange(len(files_v)):
    
    status,jsonData = res_v[i]

    run,subrun = runid_v[i]
    fname = files_v[i]

    if not jsonData:
        json_maker.error('Failure status %s for file %s' % (status,fname))
        json_fail_list.append((run,subrun,fname))

    else:
        fout = open('%s.json' % fname, 'w')
        json.dump(jsonData, fout, sort_keys = True, indent = 4, ensure_ascii=False)
        json_success_list.append((run,subrun,fname))

    if i%10==0:
        json_maker.info('Finished processing %d/%d...' % (i,len(files_v)))

print
print 'All %d Success %d Fail %d' % (len(files_v),len(json_success_list),len(json_fail_list))
print

while 1:
    sys.stdout.write('Proceed? [y/n]:')
    sys.stdout.flush()
    user_input = sys.stdin.readline().rstrip('\n')
    if not user_input in ['y','n']:
        print 'Invalid input %s' % user_input
    else:
        if not user_input == 'y': sys.exit(0)
        break

fout=open('metadata_ready.txt','w')
for item in json_success_list:
    fout.write('%s %s %s\n' % item)
fout.close()

fout=open('metadata_fail.txt','w')
for item in json_fail_list:
    fout.write('%s %s %s\n' % item)
fout.close()

