#!/bin/sh -eu
#
# Test the dqlite cluster recovery.

BASEDIR=$(dirname "$0")
. "$BASEDIR"/dqlite-demo-util.sh

$GO build -tags libsqlite3 ./cmd/dqlite/

trap tear_down EXIT
trap sig_handler HUP INT TERM

set_up

echo "=> Start test"

echo "=> Put key to node 1"
if [ "$(curl -s -X PUT -d my-value http://127.0.0.1:8001/my-key)" != "done" ]; then
    echo "Error: put key to node 1"
fi

echo "=> Get key from node 1"
if [ "$(curl -s http://127.0.0.1:8001/my-key)" != "my-value" ]; then
    echo "Error: get key from node 1"
fi

echo "=> Stopping the cluster"
tear_down_node 3
tear_down_node 2
tear_down_node 1

echo "=> Running recovery on node 1"
node1_dir=$DIR/127.0.0.1:9001
node2_dir=$DIR/127.0.0.1:9002
node1_id=$(grep ID "$node1_dir"/info.yaml | cut -d" " -f2)
node2_id=$(grep ID "$node2_dir"/info.yaml | cut -d" " -f2)
target_yaml=${DIR}/cluster.yaml
cat <<EOF > "$target_yaml"
- Address: 127.0.0.1:9001
  ID: ${node1_id}
  Role: 0
- Address: 127.0.0.1:9002
  ID: ${node2_id}
  Role: 1
EOF

if ! ./dqlite -s 127.0.0.1:9001 test ".reconfigure ${node1_dir} ${target_yaml}"; then
    echo "Error: Reconfigure failed"
    exit 1
fi

echo "=> Starting nodes 1 & 2"
start_node 1 ""
start_node 2 ""

echo "=> Confirming new config"
if [ "$(./dqlite -s 127.0.0.1:9001 test .leader)" != 127.0.0.1:9001 ]; then
    echo "Error: Expected node 1 to be leader"
    exit 1
fi

if [ "$(./dqlite -s 127.0.0.1:9001 test .cluster | wc -l)" != 2 ]; then
    echo "Error: Expected 2 servers in the cluster"
    exit 1
fi

if ! ./dqlite -s 127.0.0.1:9001 test .cluster | grep -q "127.0.0.1:9001|voter"; then
    echo "Error: server 1 not voter"
    exit 1
fi

if ! ./dqlite -s 127.0.0.1:9001 test .cluster | grep -q "127.0.0.1:9002|stand-by"; then
    echo "Error: server 2 not stand-by"
    exit 1
fi

echo "=> Get original key from node 1"
if [ "$(curl -s http://127.0.0.1:8001/my-key)" != "my-value" ]; then
    echo "Error: get key from node 1"
    exit 1
fi

echo "=> Put new key to node 1"
if [ "$(curl -s -X PUT -d my-value-new http://127.0.0.1:8001/my-key-new)" != "done" ]; then
    echo "Error: put new key to node 1"
    exit 1
fi

echo "=> Get new key from node 1"
if [ "$(curl -s http://127.0.0.1:8001/my-key-new)" != "my-value-new" ]; then
    echo "Error: get new key from node 1"
    exit 1
fi

echo "=> Test successful"
