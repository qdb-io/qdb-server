#!/bin/bash

### BEGIN INIT INFO
# Provides:	         qdb
# Required-Start:    $local_fs $remote_fs $network $syslog $named
# Required-Stop:     $local_fs $remote_fs $network $syslog $named
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Starts qdb
# Description:       Starts qdb server
### END INIT INFO

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

set -e

. /lib/lsb/init-functions

start() {
		log_daemon_msg "Starting qdb"
        su - qdb /var/lib/qdb/start.sh
		log_end_msg $?
}

stop() {
		log_daemon_msg "Stopping qdb"
        su - qdb /var/lib/qdb/stop.sh
		log_end_msg $?
}

case "$1" in
	start)
		start
		;;

	stop)
		stop
		;;

	restart)
		log_daemon_msg "Restarting qdb"
		stop
		sleep 5
		start
		log_end_msg 0
		;;

	status)
		status_of_proc -p /var/lib/qdb/qdb.pid java qdb && exit 0 || exit $?
		status=$?

		if [ $status -eq 0 ]; then
			log_success_msg "qdb is running"
		else
			log_failure_msg "qdb is not running"
		fi
		exit $status
		;;

	*)
		echo "Usage: qdb {start|stop|restart|status}" >&2
		exit 1
		;;
esac

exit 0
