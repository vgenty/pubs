import commands
from pub_util import pub_watch
from ds_messenger import daemon_messenger as d_msg

def dummy_logger():
    result = {}
    dt = pub_watch.time('dummy_logger')
    if not dt:
        pub_watch.start('dummy_logger')
        return result

    if dt < 40.:
        return result
        
    lines = [x for x in commands.getoutput('df').split('\n') if len(x.split())==5]
    for l in lines:
        words=l.split(None)
        result[words[-1]] = float(words[1])/float(words[0])

    d_msg.email('proc_daemon','hello world','executed dummy_logger! dt=%g' % dt)
    pub_watch.start('dummy_logger')

    return result

def dummy_handler():
    return (True,'')    
