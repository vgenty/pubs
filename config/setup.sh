#!/usr/bin/env bash

# If PUB_TOP_DIR not set, try to guess
if [[ -z $PUB_TOP_DIR ]]; then
    # Find the location of this script:
    me="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    # Find the directory one above.
    export PUB_TOP_DIR="$( cd "$( dirname "$me" )" && pwd )"
fi

#
# PSQL configuration
#
export PGOPTIONS="-c client_min_messages=WARNING";

#
# Python configuration
#
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

#
# Daemon configuration
#
export PUB_DAEMON_LOG_MODULE=""
export PUB_DAEMON_HANDLER_MODULE=""

#
# Server-specific configuration
#
case `uname -n` in
    (*uboonegpvm*)
	echo Setting up for uboonegpvm...
	if [ -z $HOME/.sqlaccess/prod_access.sh ]; then
	    echo 'Configuration @ gpvm requires \$HOME/.sqlaccess/prod_access.sh!'
	    echo 'Exiting...'
	    echo 
	    return;
	fi
	source $HOME/.sqlaccess/prod_access.sh
	source /grid/fermiapp/products/uboone/setup_uboone.sh
	setup psycopg2 v2_5_4
	setup uboonecode v04_08_01 -q e7:prof
	setup larbatch v01_09_00 
	export PUB_LOGGER_FILE_LOCATION=$PUB_TOP_DIR/log/`uname -n`
	mkdir -p $PUB_LOGGER_FILE_LOCATION;
	export PUB_DAEMON_LOG_MODULE=dstream_prod.gpvm_logger
	;;
    (*ubdaq-prod*)
	echo Setting up for ubdaq-prod machines...
#The SSL_CERT_DIR variable is being set on the advice of Robert Illingworth and a potential
#mismatch between the version of python SSL authentication and sam_web_client. If the project
#reg_binary_to_sam gives error 102 it is likely SSL problems in samweb.declareFile and may
#require an update to this setting.
#The SSL_CERT_DIR variable is being set on the advice of Robert Illingworth and a potential
#mismatch between the version of python SSL authentication and sam_web_client. If the project
#reg_binary_to_sam gives error 102 it is likely SSL problems in samweb.declareFile and may
#require an update to this setting.                                                                                          export SSL_CERT_DIR=/etc/grid-security/certificates
	export SSL_CERT_DIR=/etc/grid-security/certificates
        source /uboone_offline/setup
        export PUB_LOGGER_FILE_LOCATION=$PUB_TOP_DIR/log/`uname -n`/$USER
        mkdir -p $PUB_LOGGER_FILE_LOCATION;

	case `uname -n` in
	    (ubdaq-prod-smc*)
		setup postgresql v9_3_6 -q p279
		export PUB_DAEMON_LOG_MODULE=dstream_online.ubdaq_logger_smc
		export PUB_DAEMON_HANDLER_MODULE=dstream_online.ubdaq_handler_smc
		;;
	    (ubdaq-prod-evb*)
		setup postgresql v9_3_6 -q p279
	        setup sam_web_client
		export PUB_DAEMON_LOG_MODULE=dstream_online.evb_logger
		export PUB_DAEMON_HANDLER_MODULE=dstream_online.evb_handler
		;;
	    (ubdaq-prod-near1*)
	        #
                # This is not guaranteed to work (Kazu June-02-2015)
                #
	        setup sam_web_client
		setup ifdhc v1_8_2 -q e7:p279:prof
		setup uboonedaq_datatypes v6_10_03 -q e7:debug
		setup uboonecode v04_08_00 -q debug:e7
                export PUB_DAEMON_LOG_MODULE=dstream_online.near1_logger
                export PUB_DAEMON_HANDLER_MODULE=dstream_online.near1_handler
		;;
	esac
	;;
    (*)
	export PUB_DAEMON_LOG_MODULE=ds_server_dummy.dummy_logger
	export PUB_DAEMON_HANDLER_MODULE=ds_server_dummy.dummy_handler
	echo No special setup done for the server `uname -n`
	;;
esac



