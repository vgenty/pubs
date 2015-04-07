#!/bin/sh

# Suppress the annoying "$1: unbound variable" error when no option
# was given
if [ -z $1 ] ; then
	echo "Usage: $0 [start|stop|restart] "
	exit 1
fi

daemon_script=$PUB_TOP_DIR/dstream/daemon.py

case $1 in
    (start)
	echo starting;
	export PUB_LOGGER_LEVEL=kLOGGER_INFO
	export PUB_LOGGER_DRAIN=kLOGGER_FILE
	nohup $PUB_TOP_DIR/dstream/daemon.py > /dev/null &
	;;
    (stop)
	echo stopping;
	kill $(ps aux | grep 'dstream/daemon.py' | awk '{print $2}')
	;;
    (restart)
	echo restarting;
	export PUB_LOGGER_LEVEL=kLOGGER_INFO
	export PUB_LOGGER_DRAIN=kLOGGER_FILE
	kill $(ps aux | grep 'dstream/daemon.py' | awk '{print $2}')	
	nohup $PUB_TOP_DIR/dstream/daemon.py > /dev/null &
	;;
esac

