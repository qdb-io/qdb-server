#!/bin/bash
# Start QDB server running in the background

QDB_LOG=qdb.log
QDB_PIDFILE=qdb.pid
QDB_SERVER_OPTS="-server"

[ -f /etc/default/qdb ] && . /etc/default/qdb

set -e
nohup ./bin/qdb-server >> $QDB_LOG 2>&1 &

PID=$!

echo $PID > $QDB_PIDFILE

echo "QDB PID ${PID}, tail -f $QDB_LOG to check for successful startup"
