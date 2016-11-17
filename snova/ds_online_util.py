import ds_online_constants
from ds_online_constants import *
from dstream import DSException

class ds_online_util_vars:
    kSTATUS_UNIQUENESS_CHECKED=False
    kSTATUS_VALUE_TO_NAME={}
    def __init__(self): pass

def check_unique_status():
    if ds_online_util_vars.kSTATUS_UNIQUENESS_CHECKED: return
    unique_value={}
    for key,value in ds_online_constants.__dict__.iteritems():
        if not key.startswith('kSTATUS'):
            #raise DSException('Found an invalid status code name: %s' % key)
            continue

        try:
            value = int(value)
        except TypeError:
            raise DSException('Non-integer status code: %s => %s' % (key,value))

        if value in unique_value:
            raise DSException('Status code value %d is assigned to two keys: %s and %s' % (value,key,unique_value[value]))

        unique_value[value]=key

    ds_online_util_vars.kSTATUS_UNIQUENESS_CHECKED = True
    ds_online_util_vars.kSTATUS_VALUE_TO_NAME=unique_value

def status_name(val):
    try:
        val = int(val)
    except Exception:
        raise DSException("Status code must be an integer (%s not int)" % val)
    
    check_unique_status()
    if not val in ds_online_util_vars.kSTATUS_VALUE_TO_NAME:
        raise DSException("Status code %d not defined in online status list!" % val)

    return ds_online_util_vars.kSTATUS_VALUE_TO_NAME[val]

def is_valid_metadata_type(t):
    try:
        ret = False
        exec('ret = int(%s) >=0 and int(%s) < kMAXTYPE_METADATA' % (t,t))
        return ret
    except Exception:
        return False

check_unique_status()


