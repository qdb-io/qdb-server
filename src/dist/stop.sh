#!/bin/bash
# Stops the QDB server

PIDFILE=qdb.pid
if [ ! -f $PIDFILE ] ; then
    echo "$PIDFILE not found"
    exit 1
fi

PID=`cat $PIDFILE`
echo "Killing ${PID}"
kill $PID
