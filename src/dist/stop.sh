#!/bin/bash
# Stops the QDB server

QDB_PIDFILE=qdb.pid

[ -f /etc/default/qdb ] && . /etc/default/qdb

if [ ! -f $QDB_PIDFILE ] ; then
    echo "$QDB_PIDFILE not found"
    exit 1
fi

PID=`cat $QDB_PIDFILE`
echo "Killing ${PID}"
kill $PID
