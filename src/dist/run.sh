#!/bin/bash
# Start QDB server running in the foreground

QDB_SERVER_OPTS="-server"

[ -f /etc/default/qdb ] && . /etc/default/qdb

set -e

CP=""
for f in lib/*.jar; do
    [ -f "$f" ] && CP="$CP$f:"
done
CP="${CP}qdb-server.jar"

java -cp $CP $QDB_SERVER_OPTS io.qdb.server.Main
