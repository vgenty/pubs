
#
# Some fixed status values for online processing
# 
kSTATUS_DONE            = 0
kSTATUS_INIT            = 1
kSTATUS_TO_BE_VALIDATED = 2
kSTATUS_POSTPONE        = 1000

# Generic error status, discouraged to use
kSTATUS_ERROR_UNKNOWN = 100

# Error regarding the intepretation of a reference data
kSTATUS_ERROR_REFERENCE_PROJECT_DATA = 109

#
# Failure related for sam registration of metadata ... 110s
#
kSTATUS_ERROR_DUPLICATE_SAM_ENTRY   = 110
kSTATUS_ERROR_WRONG_JSON_FORMAT     = 111
kSTATUS_ERROR_CANNOT_MAKE_SAM_ENTRY = 112

#
# Failure related to json file creation ... 120s
#
kSTATUS_ERROR_CANNOT_MAKE_BIN_METADATA   = 120
kSTATUS_ERROR_CANNOT_UPLOAD_BIN_METADATA = 121

#
# Failure for checksum related items ... 130s
#
kSTATUS_ERROR_CHECKSUM_MISMATCH  = 130
kSTATUS_ERROR_CHECKSUM_NOT_FOUND = 131
kSTATUS_ERROR_CHECKSUM_CALCULATION_FAILED = 132

#
# Failure related to file removal .. 140s
#
kSTATUS_ERROR_CANNOT_REMOVE_FILE = 140

#
# Failure in swizzling ... 150s
#
kSTATUS_ERROR_CANNOT_SWIZZLE = 150
#
# Failure for file tansfer protocole ... 160s
#
kSTATUS_ERROR_TRANSFER_FAILED = 160

#
# Failure related to input/output file not found or not unique ... 400s
#
kSTATUS_ERROR_INPUT_FILE_NOT_FOUND     = 404
kSTATUS_ERROR_INPUT_FILE_NOT_UNIQUE    = 405
kSTATUS_ERROR_OUTPUT_FILE_NOT_FOUND    = 406
kSTATUS_ERROR_OUTPUT_FILE_NOT_UNIQUE   = 407
kSTATUS_ERROR_INPUT_FILE_NOT_FORMATED  = 408
kSTATUS_ERROR_OUTPUT_FILE_NOT_FORMATED = 409
#
# Successful status for Binary Transfer on evb (transfer_binary_dropbox_evb)
#
kSTATUS_TRANSFER_BINARY_NEAR1 = 1001 # -> if Dropbox is down, then move files to near1
kSTATUS_VALIDATE_DATA         = 1002 # -> files moved to dropbox, then validate success of this operation

#
# Successful status for Binary Transfer Validation on evb (compare_binary_checksum_evb)
#
kSTATUS_SWIZZLE_DATA = 1003 # -> transfer successful, choose to swizzle this file online
kSTATUS_REMOVE_DATA  = 1004 # -> transfer successful, choose to remove the binary file
kSTATUS_SKIP_SWIZZLE = 1005 # -> if we decide to remove binary file w/o swizzling, this is assigned to all ROOT related projects
kSTATUS_BYPASS_BINARY_TRANSFER_NEAR1 = 1006

#
# Metadata types
#
kUBDAQ_METADATA, kSWIZZLED_METADATA, kMAXTYPE_METADATA = xrange(3)
