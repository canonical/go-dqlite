#!/bin/sh -eu
#
# Test dynamic roles management.

GO=${GO:-go}
VERBOSE=${VERBOSE:-0}

DIR=$(mktemp -d)
BINARY=$DIR/main
CLUSTER=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005,127.0.0.1:9006

$GO build -tags libsqlite3 ./cmd/dqlite/

set_up_binary() {
    cat > $DIR/main.go <<EOF
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "time"
    "path/filepath"
    "strconv"
    "github.com/canonical/go-dqlite/client"
    "github.com/canonical/go-dqlite/app"
    "golang.org/x/sys/unix"
)

func main() {
     dir := filepath.Join("$DIR", os.Args[1])
     index, _ := strconv.Atoi(os.Args[1])
     verbose := $VERBOSE
     logFunc := func(l client.LogLevel, format string, a ...interface{}) {
         if verbose != 1 {
             return
         }
         fmt.Printf(fmt.Sprintf("%d: %s: %s\n", index, l.String(), format), a...)
     }
     join := []string{}
     if index > 1 {
         join = append(join, "127.0.0.1:9001")
     }
     addr := fmt.Sprintf("127.0.0.1:900%d", index)
     if err := os.MkdirAll(dir, 0755); err != nil {
         panic(err)
     }
     app, err := app.New(
         dir,
         app.WithAddress(addr),
         app.WithCluster(join),
         app.WithLogFunc(logFunc),
         app.WithRolesAdjustmentFrequency(5 * time.Second),
     )
     if err != nil {
         panic(err)
     }
     if err := app.Ready(context.Background()); err != nil {
         panic(err)
     }
     ch := make(chan os.Signal)
     signal.Notify(ch, unix.SIGPWR)
     signal.Notify(ch, unix.SIGINT)
     signal.Notify(ch, unix.SIGQUIT)
     signal.Notify(ch, unix.SIGTERM)
     <-ch
     ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
     defer cancel()
     app.Handover(ctx)
     app.Close()
}
EOF
    $GO build -o $BINARY -tags libsqlite3 $DIR/main.go
}

start_node() {
    n="${1}"
    pidfile="${DIR}/pid.${n}"

    $BINARY $n &
    echo "${!}" > "${pidfile}"
}

kill_node() {
    n=$1
    signal=$2
    pidfile="${DIR}/pid.${n}"

    if ! [ -e $pidfile ]; then
        return
    fi

    pid=$(cat ${pidfile})

    kill -${signal} $pid
    wait $pid || true

    rm ${pidfile}
}

# Wait for the cluster to have 3 voters, 2 stand-bys and 1 spare
wait_stable() {
  i=0
  while true; do
    i=$(expr $i + 1)
    voters=$(./dqlite -s $CLUSTER test .cluster | grep voter | wc -l)
    standbys=$(./dqlite -s $CLUSTER test .cluster | grep stand-by | wc -l)
    spares=$(./dqlite -s $CLUSTER test .cluster | grep spare | wc -l)
    if [ $voters -eq 3 ] && [ $standbys -eq 2 ] &&  [ $spares -eq 1 ] ; then
        break
    fi
    if [ $i -eq 40 ]; then
      echo "Error: node roles not yet stable after 10 seconds"
      ./dqlite -s $CLUSTER test .cluster
      exit 1
    fi
    sleep 0.25
  done
}

# Wait for the given node to have the given role
wait_role() {
    index=$1
    role=$2
    i=0
    while true; do
        i=$(expr $i + 1)
        current=$(./dqlite -s $CLUSTER test .cluster | grep "900${index}" | cut -f 3 -d "|")
        if [ "$current" = "$role" ]; then
            break
        fi
        if [ $i -eq 40 ]; then
            echo "Error: stand-by node $index has role $current instead of $role"
            ./dqlite -s $CLUSTER test .cluster
            exit 1
        fi
        sleep 0.25
    done
}

set_up_node() {
    n=$1
    echo "=> Set up test node $n"
    start_node "${n}"
}

set_up() {
    echo "=> Set up test cluster"
    set_up_binary
    set_up_node 1
    set_up_node 2
    set_up_node 3
    set_up_node 4
    set_up_node 5
    set_up_node 6
}

tear_down_node() {
    n=$1
    echo "=> Tear down test node $n"
    kill_node $n TERM
}

tear_down() {
    err=$?
    trap '' HUP INT TERM

    echo "=> Tear down test cluster"

    tear_down_node 6
    tear_down_node 5
    tear_down_node 4
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

echo "=> Wait for roles to get stable"
wait_stable

echo "=> Stop a stand-by gracefully"

index=$(./dqlite -s $CLUSTER test .cluster | grep stand-by | tail -1 | cut -b 31)
kill_node $index TERM

wait_role $index spare
wait_stable
start_node $index

echo "=> Stop a stand-by ungracefully"

index=$(./dqlite -s $CLUSTER test .cluster | grep stand-by | tail -1 | cut -b 31)
kill_node $index KILL

wait_role $index spare
wait_stable

#start_node $index

echo "=> Test successful"
