#!/bin/bash
# Start QDB server running in the background

set -e
nohup ./bin/qdb-server >> qdb.log 2>&1 &

PID=$!

echo $PID > qdb.pid

echo "QDB PID ${PID}, tail -f qdb.log to check for successful startup"
