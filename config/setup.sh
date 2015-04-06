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

export PUB_PSQL_READER_USER=echurch
export PUB_PSQL_WRITER_USER=echurch
export PUB_PSQL_READER_PASS="echurchargon!"
export PUB_PSQL_WRITER_PASS="echurchargon!"
source /home/$USER/development/uboonedaq/projects/cpp2py/config/setup_cpp2py.sh
echo "Now we setup larsoft. This takes 15 seconds, or so ...."
#source /home/echurch/setuplar.sh
setup sam_web_client
setup ifdhc

# setup ubutil v01_08_01 -qe6:prof

