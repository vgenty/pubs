from dstream_online.eric import get_metadata
import sys

input_list = [(int(x.split()[0]),
               int(x.split()[1]),
               str(x.split()[2])) for x in open(sys.argv[1],'r').read().split() if x.endswith('.ubdaq')]

#
# Make json files
#

# json maker
xfer = get_metadata('transfer_binary_dropbox')
xfer.get_resource()
xfer._parallelize = 5
xfer._max_proc_time=60

# run json maker
files_v = []
runid_v = []
for f in input_list:
    files_v.append(f[2])
    runid_v.append((f[0],f[1]))
mp = json_maker.process_files(files_v)

xfer_fail_list=[]
xfer_success_list=[]

for i in xrange(len(args_v)):
    fname = files_v[i]
    run,subrun = runid_v[i]
    if mp.poll(i):
        obj.error('Failed copy %s @ %s' % (runid_v[i],time.strftime('%Y-%m-%d %H:%M:%S')))
        xfer_fail_list.append((run,subrun,fname))
    else:
        obj.info('Finished copy %s @ %s' % (runid_v[i],time.strftime('%Y-%m-%d %H:%M:%S')))
        xfer_success_list.append((run,subrun,fname))

print
print 'All %d Success %d Fail %d' % (len(files_v),len(xfer_success_list),len(xfer_fail_list))
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

fout=open('storage_ready.txt','w')
for item in xfer_success_list:
    fout.write('%s %s %s\n' % item)
fout.close()

fout=open('storage_fail.txt','w')
for item in xfer_fail_list:
    fout.write('%s %s %s\n' % item)
fout.close()

