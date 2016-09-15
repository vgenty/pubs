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

case `whoami` in
    (uboonepro)
        echo Setting up PUBS for uboonepro account...
	;;
    (*)
        echo This should only be used for uboonepro account!!!
	echo Exiting with extreme prejudice.
	return 1
	;;
esac
	
case `uname -n` in
    (*ubdaq-prod*)
	echo Setting up PUBS for ubdaq-prod machines...
#The SSL_CERT_DIR variable is being set on the advice of Robert Illingworth and a potential
#mismatch between the version of python SSL authentication and sam_web_client. If the project
#reg_binary_to_sam gives error 102 it is likely SSL problems in samweb.declareFile and may
#require an update to this setting.
	export SSL_CERT_DIR=/etc/grid-security/certificates;
	export X509_USER_PROXY=/home/uboonepro/uboonepro_production_near1_proxy_file;
	source /uboone_offline/setup;
	source /home/uboonepro/.sql_access/uboonepro_prod_conf.sh;
	export PUB_LOGGER_FILE_LOCATION=$PUB_TOP_DIR/log/`uname -n`
	mkdir -p $PUB_LOGGER_FILE_LOCATION;
	case `uname -n` in
            (ubdaq-prod-smc*)
		setup pyqtgraph;
		setup postgresql v9_3_6 -q p279;
	        setup uboonedaq_datatypes v6_19_00c -q e7:prof;
                export PUB_DAEMON_LOG_MODULE=dstream_online.ubdaq_logger_smc;
                export PUB_DAEMON_HANDLER_MODULE=dstream_online.ubdaq_handler_smc;
                ;;
            (ubdaq-prod-evb*)
	        setup uboonecode v04_26_00 -q prof:e7;
	        setup uboonedaq_datatypes v6_19_00c -q e7:prof;
	        setup sam_web_client;
		setup postgresql v9_3_6 -q p279;
		setup pyqtgraph;
                export PUB_DAEMON_LOG_MODULE=dstream_online.evb_logger;
                export PUB_DAEMON_HANDLER_MODULE=dstream_online.evb_handler;
		export KRB5CCNAME=FILE:/tmp/krb5cc_uboonepro_evb;
                ;;
            (ubdaq-prod-near1*)
                #
	            # This is not guaranteed to work (Kazu June-02-2015)
                #
		#setup uboonedaq_datatypes v6_13_06 -q e7:debug;
		#we should setup the uboonedaq_datatypes by hand on near1, but instead
		#let it be setup as a dependency of the uboonecode version
		setup uboonecode v04_26_00 -q prof:e7;
	        setup uboonedaq_datatypes v6_19_00c -q e7:prof;
	        setup sam_web_client;
		setup ifdhc v1_8_2 -q e7:p279:prof;
		setup pyqtgraph;
                export PUB_DAEMON_LOG_MODULE=dstream_online.near1_logger;
                export PUB_DAEMON_HANDLER_MODULE=dstream_online.near1_handler;

		# for password log file
		export FW_SEARCH_PATH=/home/uboonepro/.sql_access/:$FW_SEARCH_PATH
		;;
	esac
	;;

    (*)
        echo This script should not be used except by uboonepro account on the ubdaq-prod machines!
	echo Seriously, stop it.
	;;
esac



