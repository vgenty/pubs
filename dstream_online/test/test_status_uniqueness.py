import dstream_online.ds_online_constants
import dstream_online.ds_online_util

for k,val in dstream_online.ds_online_constants.__dict__.iteritems():
    if not k.startswith('kSTATUS'):
        continue
    
    print dstream_online.ds_online_util.status_name(val),val
    

