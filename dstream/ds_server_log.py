import commands

def ubdaq_logger_smc():
    lines = [x for x in commands.getoutput('df').split('\n') if len(x.split())==5]
    result = {}
    for l in lines:
        words=l.split(None)
        result[words[-1]] = float(words[1])/float(words[0])
    return result

def ubdaq_handler_smc():
    return (True,'')
