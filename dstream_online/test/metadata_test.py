from dstream_online.eric import get_metadata
import sys

files = [x for x in open(sys.argv[1],'r').read().split() if x.endswith('.ubdaq')]

GENERATE_JSON = False

obj = get_metadata('get_binary_metadata')
obj.get_resource()
obj._parallelize = 5
obj._max_proc_time=60
res_v = obj.process_ubdaq_files(files)

if GENERATE_JSON:
    for i in xrange(len(files)):

        status,jsonData = res_v[i]
        fname = files[i]

        if not jsonData:
            obj.error('Failure status %s for file %s' % (status,fname))
        else:
            fout = open('%s.json' % fname, 'w')
            json.dump(jsonData, fout, sort_keys = True, indent = 4, ensure_ascii=False)
            
        if i%10==0:
            obj.info('Finished processing %d/%d...' % (i,len(files)))

