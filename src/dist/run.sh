#!/bin/bash
# Start QDB server running in the foreground

QDB_SERVER_OPTS="-server"

[ -f /etc/default/qdb ] && . /etc/default/qdb

set -e

java -cp lib/*.jar $QDB_SERVER_OPTS io.qdb.server.Main
