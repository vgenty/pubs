#!/usr/bin/env bash
#
# PSQL configuration
#
export PGOPTIONS="-c client_min_messages=WARNING";

#
# Server-specific configuration
#
case `uname -n` in
    (*uboonegpvm*)
	echo Setting up for uboonegpvm...
	source /grid/fermiapp/products/uboone/setup_uboone.sh
	setup psycopg2 v2_5_4
	setup uboonecode v04_03_01 -q e7:prof
	;;
    (*ubdaq-prod*)
	echo Setting up for ubdaq-prod machines...
	source /uboone/larsoft/setup
	setup git
        #setup sam_web_client
        #setup ifdhc
	setup psycopg2 v2_5_4
	setup postgres v9_2_4 #-f slf6.x86_64.prof
        #setup uboonecode v03_04_00 -q e6:prof
        #source /home/$USER/development/uboonedaq/projects/cpp2py/config/setup_cpp2py.sh
	;;
    (*)
	echo No special setup done for the server `uname -n`
	;;
esac

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
# Set PATH
export PATH=$PUB_TOP_DIR/bin:$PATH
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

# setup ubutil v01_08_01 -qe6:prof

