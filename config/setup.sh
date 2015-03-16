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
# BIN executable directory
export PUB_BIN_DIR=$PUB_TOP_DIR
export PATH=$PUB_BIN_DIR:$PATH


#
# Project configuration
# 

# Default logger level
export PUB_LOGGER_LEVEL=kLOGGER_DEBUG

# Default message drain
export PUB_LOGGER_DRAIN=kLOGGER_COUT
#export PUB_LOGGER_DRAIN=kLOGGER_FILE
export PUB_LOGGER_FILE_LOCATION=$PUB_TOP_DIR/log

export PUB_PSQL_READER_USER=postgres
export PUB_PSQL_WRITER_USER=postgres
source /home/$USER/development/uboonedaq/projects/cpp2py/config/setup_cpp2py.sh
setup sam_web_client
setup ifdhc
# setup ubutil v01_08_01 -qe6:prof

