
#
# Some fixed status values for online processing
# 
kSTATUS_DONE            = 0
kSTATUS_INIT            = 1
kSTATUS_TO_BE_VALIDATED = 2
kSTATUS_POSTPONE        = 1000
kSTATUS_TO_BE_SWIZZLED  = 1001

# Generic error status, discouraged to use
kSTATUS_ERROR_UNKNOWN = 100

# Error regarding the intepretation of a reference data
kSTATUS_ERROR_REFERENCE_PROJECT_DATA = 109

#
# Failure related to json file creation ... 120s
#
kSTATUS_ERROR_CANNOT_MAKE_BIN_METADATA   = 120
kSTATUS_ERROR_CANNOT_UPLOAD_BIN_METADATA = 121

#
# Failure related for sam registration of metadata ... 110s
#
kSTATUS_ERROR_DUPLICATE_SAM_ENTRY   = 110
kSTATUS_ERROR_WRONG_JSON_FORMAT     = 111
kSTATUS_ERROR_CANNOT_MAKE_SAM_ENTRY = 112

#
# Failure for checksum related items ... 130s
#
kSTATUS_ERROR_CHECKSUM_MISMATCH  = 130
kSTATUS_ERROR_CHECKSUM_NOT_FOUND = 131

#
# Failure related to file removal .. 140s
#
kSTATUS_ERROR_CANNOT_REMOVE_FILE = 140

#
# Failure related to input/output file not found or not unique ... 400s
#
kSTATUS_ERROR_INPUT_FILE_NOT_FOUND   = 404
kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE  = 405
kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND  = 406
kSTATUS_ERROR_OUTPUT_FILE_NOT_UNIQUE = 407

#
# Metadata types
#
kUBDAQ_METADATA, kSWIZZLED_METADATA, kMAXTYPE_METADATA = xrange(3)
