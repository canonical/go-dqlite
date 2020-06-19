#!/bin/sh -eu
#
# Test the dqlite-demo application.

GO=${GO:-go}
VERBOSE=${VERBOSE:-0}

$GO build -tags libsqlite3 ./cmd/dqlite-demo/

DIR=$(mktemp -d)

start_node() {
    n="${1}"
    pidfile="${DIR}/pid.${n}"
    join="${2}"
    verbose=""

    if [ $VERBOSE -eq 1 ]; then
        verbose="--verbose"
    fi

    ./dqlite-demo --dir $DIR --api=127.0.0.1:800${n} --db=127.0.0.1:900${n} $join $verbose &
    echo "${!}" > "${pidfile}"

    i=0
    while ! nc -z 127.0.0.1 800${n} 2>/dev/null; do
        i=$(expr $i + 1)
        sleep 0.2
        if [ $i -eq 25 ]; then
            echo "Error: node ${n} not yet up after 5 seconds"
            exit 1
        fi
    done
}

kill_node() {
    n=$1
    pidfile="${DIR}/pid.${n}"

    if ! [ -e $pidfile ]; then
        return
    fi

    pid=$(cat ${pidfile})

    kill -TERM $pid
    wait $pid

    rm ${pidfile}
}

set_up_node() {
    n=$1
    join=""
    if [ $n -ne 1 ]; then
        join=--join=127.0.0.1:9001
    fi

    echo "=> Set up dqlite-demo node $n"

    start_node "${n}" "${join}"
}

tear_down_node() {
    n=$1

    echo "=> Tear down dqlite-demo node $n"

    kill_node $n
}

set_up() {
    echo "=> Set up dqlite-demo cluster"
    set_up_node 1
    set_up_node 2
    set_up_node 3
}

tear_down() {
    err=$?
    trap '' HUP INT TERM

    echo "=> Tear down dqlite-demo cluster"
    tear_down_node 3
    tear_down_node 2
    tear_down_node 1

    rm -rf $DIR

    exit $err
}

sig_handler() {
    trap '' EXIT
    tear_down
}

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
