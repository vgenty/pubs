
#
# Some fixed status values for online processing
# 
kSTATUS_INIT     = 1
kSTATUS_POSTPONE = 1000

#
# Metadata types
#
kUBDAQ_METADATA, kSWIZZLED_METADATA, kMAXTYPE_METADATA = xrange(3)
def is_valid_metadata_type(t):
    try:
        ret = False
        exec('ret = int(%s) >=0 and int(%s) < kMAXTYPE_METADATA' % (t,t))
        return ret
    except Exception:
        return False
