from dstream.ds_multiprocess import ds_multiprocess
import os,sys,time

files=[x for x in open(sys.argv[1],'r').read().split('\n') if x.endswith('.ubdaq')]
dest='/pnfs/uboone/scratch/uboonepro/pubs_testbed_dropbox/'

PARALLEL=7
MAX_PROC_TIME=180

k=ds_multiprocess('transfer_test')

for f in files:

    cmd = ('ifdh cp -D %s %s' % (f,dest)).split()
    #cmd = 'sleep 2'

    index,active_count = k.execute(cmd)
    
    time_slept = 0
    while active_count >= PARALLEL:

        time_slept += 0.2
        time.sleep(0.2)

        if time_slept and (int(time_slept*10))%10 == 0:
            k.info('Waiting %g sec. for %d parallel transfer to be done (%d/%d done so far)...' % ( time_slept,
                                                                                                    active_count,
                                                                                                    index+1-active_count,
                                                                                                    len(files ) )
                   )

        if time_slept > MAX_PROC_TIME:
            k.info('Killing %d parallel as it exceeds allowed time...' % active_count)
            k.kill()
            break
        active_count = k.active_count()
        
while active_count >= PARALLEL:

    time_slept += 0.2
    time.sleep(0.2)
    
    if time.sleep and (int(time.sleep*10))%10 == 0:
        k.info('Waiting %g sec. for %d parallel transfer to be done (%d/%d done so far)...' % ( time_slept,
                                                                                                active_count,
                                                                                                len(files)-active_count,
                                                                                                len(files ) )
               )
        
    active_count = k.active_count()

    if time_slept > MAX_PROC_TIME:
        k.info('Killing %d parallel as it exceeds allowed time...' % active_count)
        k.kill()
        break
    active_count = k.active_count()

k.info('Finished...')


