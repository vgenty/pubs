## @namespace dstream.ds_exception
#  @ingroup dstream
#  @brief Defines exception classes in dstream package

## @class DSException
#  @brief Simple exception class for dstream classes
class DSException(Exception):

    ## default ctor
    def __init__(self,v=''):
        self.v = v

    ## print output string
    def __str__(self):
        return repr(self.v)


        

