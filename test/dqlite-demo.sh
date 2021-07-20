#!/bin/sh -eu
#
# Test the dqlite-demo application.

BASEDIR=$(dirname "$0")
. "$BASEDIR"/dqlite-demo-util.sh

trap tear_down EXIT
trap sig_handler HUP INT TERM

set_up

echo "=> Start test"

echo "=> Put key to node 1"
if [ "$(curl -s -X PUT -d my-key http://127.0.0.1:8001/my-value)" != "done" ]; then
    echo "Error: put key to node 1"
fi

echo "=> Get key from node 1"
if [ "$(curl -s http://127.0.0.1:8001/my-value)" != "my-key" ]; then
    echo "Error: get key from node 1"
fi

echo "=> Kill node 1"
kill_node 1

echo "=> Get key from node 2"
if [ "$(curl -s http://127.0.0.1:8002/my-value)" != "my-key" ]; then
    echo "Error: get key from node 2"
fi

echo "=> Test successful"
