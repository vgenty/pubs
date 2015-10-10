from dstream import ds_multiprocess,ds_api,ds_status
import sys,time,os,json

flist_v = [ ( int(x.split()[0]), int(x.split()[1]), x.split()[2] ) for x in open(sys.argv[1],'r').read().split('\n') if len(x.split()) == 3 ]

reader = ds_api.ds_reader()

mp = ds_multiprocess()

for run,subrun,fname in flist_v:
    
    print 'processing',run,subrun,fname

    #fname = '/datalocal/uboonepro/swizzled/' + fname_only

    json_name = fname + '.json'

    if not os.path.isfile(fname):
        print 'Error: data file not found...'
        continue
    if not os.path.isfile(json_name):
        print 'Error: json file not found...'
        continue

    #cmd = ['ifdh','cp','-D',fname,json_name,'/pnfs/uboone/scratch/uboonepro/dropbox/data/uboone/raw']
    cmd = ['ifdh','cp','-D',fname,json_name,'/pnfs/uboone/scratch/uboonepro/dropbox/data/uboone/raw']

    index, active_ctr = mp.execute(cmd)

    time_slept = 0
    while active_ctr >= 5:

        if time_slept and (int(time_slept*10))%50 == 0:
            print '... Waiting parallel processes to be done...'
        
        time.sleep(0.2)
        time_slept += 0.2

        active_ctr = mp.active_count()

        if time_slept > 180:
            print '... Killing...'
            mp.kill()

while mp.active_count():
        
    time.sleep(0.2)
    time_slept += 0.2
    
    if (int(time_slept*10))%50 == 0:
        print '... Waiting parallel processes to be done...'
        
    if time_slept > 210:
        print '... Killing...'
        mp.kill()


bad_transfer_v=[]
good_transfer_v=[]
for i in xrange(len(flist_v)):

    run,subrun,fname = flist_v[i]
    if mp.poll(i):
        print 'Transfer failed for',fname
        bad_transfer_v.append((run,subrun,fname))
    else:
        good_transfer_v.append((run,subrun,fname))

fout = open('%s_bad_transfer.txt' % sys.argv[1],'w')
for f in bad_transfer_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()

fout = open('%s_good_transfer.txt' % sys.argv[1],'w')
for f in good_transfer_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()
