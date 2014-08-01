#
# Python configuration
#

# If PUB_TOP_DIR not set, try to guess
if [[ -z $PUB_TOP_DIR ]]; then
    # Find the location of this script:
    me="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    # Find the directory one above.
    export PUB_TOP_DIR="$( cd "$( dirname "$me" )" && pwd )"
fi
# Set PYTHONPATH
export PYTHONPATH=$PUB_TOP_DIR:$PYTHONPATH


#
# Project configuration
# 

# Default logger level
export PUB_LOGGER_LEVEL=kLOGGER_DEBUG

# Default message drain
export PUB_LOGGER_DRAIN=kLOGGER_COUT
export PUB_LOGGER_FILE_LOCATION=$PUB_TOP_DIR/log

# SQL reader account config
export PUB_PSQL_READER_HOST=localhost
export PUB_PSQL_READER_USER=$USER
export PUB_PSQL_READER_DB=procdb
export PUB_PSQL_READER_PASS=""

# SQL writer account config
export PUB_PSQL_WRITER_HOST=localhost
export PUB_PSQL_WRITER_USER=$USER
export PUB_PSQL_WRITER_DB=procdb
export PUB_PSQL_WRITER_PASS=""



