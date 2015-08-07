#!/bin/sh

# Suppress the annoying "$1: unbound variable" error when no option
# was given
if [ -z $1 ] ; then
	echo "Usage: $0 [start|stop|restart] "
	exit 1
fi

daemon_script=$PUB_TOP_DIR/dstream/daemon.py
proc=$(ps aux | grep "dstream/daemon.py" | grep "python" | awk '{print $2}');

case $1 in
    (start)
    if [[ -z $proc ]]; then
	echo starting daemon;
	export PUB_LOGGER_LEVEL=kLOGGER_INFO
	export PUB_LOGGER_DRAIN=kLOGGER_FILE
	nohup $PUB_TOP_DIR/dstream/daemon.py > $PUB_TOP_DIR/log/`uname -n`/daemon.sh.log &
    else
	echo daemon already running;
    fi
	;;
    (stop)
    if [[ -z $proc ]]; then
	echo daemon is not running...;
    else
	echo stopping daemon;
	kill $proc;
    fi
    ;;
    (restart)
    if [[ $proc ]]; then
	echo restarting daemon;
	kill $proc;
    else
	echo starting daemon;
    fi
    export PUB_LOGGER_LEVEL=kLOGGER_INFO
    export PUB_LOGGER_DRAIN=kLOGGER_FILE
    nohup $PUB_TOP_DIR/dstream/daemon.py > $PUB_TOP_DIR/log/`uname -n`/daemon.sh.log &
    ;;
esac

