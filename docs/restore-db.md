Note this document is not complete and work in progress.

# A. INFO

**Always backup your database folders before performing any of the steps
described below and make sure no dqlite nodes are running!**

## A.1 cluster.yaml

File containing the node configuration of your installation.

### Contents
```
- Address: 127.0.0.1:9001
  ID: 1
  Role: 0
- Address: 127.0.0.1:9002
  ID: 2
  Role: 1
- Address: 127.0.0.1:9003
  ID: 3
  Role: 0
```

*Address* : The `host:port` that will be used in database replication, we will refer to this as `DbAddress`.  
*ID* : Raft id of the node  
*Role* :  
- 0: Voter, takes part in quorum, replicates DB.  
- 1: Standby, doesn't take part in quorum, replicates DB.  
- 2: Backup, doesn't take part in quorum, doesn't replicate DB.  

## A.2 info.yaml

File containing the node specific information.

### Contents
```
Address: 127.0.0.1:9001
ID: 1
Role: 0
```


## A.3 Finding the node with the most up-to-date data.

1. For every known node, make a new directory, `NodeDirectory` and copy
   the data directory of the node into it.
2. Make a `cluster.yaml` conforming to the structure layed out above, with the
   desired node configuration, save it at `TargetClusterYamlPath`.
   e.g. you can perform:
```
cat <<EOF > "cluster.yaml"
- Address: 127.0.0.1:9001
  ID: 1
  Role: 0
- Address: 127.0.0.1:9002
  ID: 2
  Role: 1
- Address: 127.0.0.1:9003
  ID: 3
  Role: 0
EOF
```
3. For every node, run `dqlite -s <DbAddress> <DbName> ".reconfigure <NodeDirectory>
   <TargetClusterYamlPath>"`
   The `DbAddress`, `DbName` aren't really important, just use something
   syntactically correct, we are more interested in the side effects of this
   command on the `NodeDirectory`. The command should return `OK`.
4. Look in the `NodeDirectory` of every node, there should be at least 1 new segment file
   e.g. `0000000057688811-0000000057688811` with the start index (the number
   before `-`) equal to the end index (the number after `-`), this will
   be the most recently created segment file. Remember this index.
5. The node with the highest index from the previous step has the most up-to-date data.
   If there is an ex aequo, pick one.

note: A new command that doesn't rely on the side effects of the `.reconfigure`
command will be added in the future.

# B. Restoring Data

## B.1 Loading existing data and existing network/node configuration in `dqlite-demo`

*Use this when you have access to the machines where the database lives and want
to start the database with the unaltered data of every node.*


0. Stop all database nodes & backup all the database folders.
1. Make a base directory for your data e.g. `data`, we will refer to this as
   the `DataDirectory`.
2. For every node in `cluster.yaml`, create a directory with name equal to
   `DbAddress` under the `DataDirectory`, unique to the node,  this `host:port`
   will be needed later on for the `--db` argument when you start the `dqlite-demo`
   application, e.g. for node 1 you now have a directory `data/127.0.0.1:9001`.
   We will refer to this as the `NodeDirectory`.
3. For every node in `cluster.yaml`, copy all the data for that node to
   its `NodeDirectory`.
4. For every node in `cluster.yaml`, make sure there exists an `info.yaml`
   in `NodeDirectory` that contains the information as found in `cluster.yaml`.
5. For every node in `cluster.yaml`, run:
   `dqlite-demo --dir <DataDirectory> --api <ApiAddress> --db <DbAddress>`,
   where `ApiAddress` is a `host:port`,
   e.g. `dqlite-demo --dir data --api 127.0.0.1:8001 --db 127.0.0.1:9001`.
   Remark that it is important that `--dir` is a path to the newly created
   `DataDirectory`, otherwise the demo will create a new directory without the
   existing data.
6. You should have an operational cluster, access it through e.g. the `dqlite`
   cli tool.


## B.2 Restore existing data and new network/node configuration in `dqlite-demo`.

*Use this when you don't have access to the machines where the database lives and want
to start the database with data from a specific node or when you have access to
the machines but the cluster has to be reconfigured or repaired.*

0. Stop all database nodes & backup all the database folders.
1. Create a `cluster.yaml` containing your desired node configuration.
We will refer to this file by `TargetClusterYaml` and to its location by
`TargetClusterYamlPath`.
2. Follow steps 1 and 2 of part `B.1`, where `cluster.yaml` should be interpreted
   as `TargetClusterYaml`.
3. Find the node with the most up-to-date data following the steps in `A.3`, but
   use the directories and `cluster.yaml` created in the previous steps.
4. For every non up-to-date node, remove the data files and metadata files from the `NodeDirectory`.
5. For every non up-to-date node, copy the data files of the node with
   the most up-to-date data to the `NodeDirectory`, don't copy the metadata1 &
   metadata2 files over.
6. For every node, copy `TargetClusterYaml` to `NodeDirectory`, overwriting
   `cluster.yaml` that's already there.
7. For every node, make sure there is an `info.yaml` in `NodeDirectory` that is in line with
   `cluster.yaml` and correct for that node.
8. For every node, run:
   `dqlite-demo --dir <DataDirectory> --api <ApiAddress> --db <DbAddress>`.
9. You should have an operational cluster, access it through e.g. the `dqlite`
   cli tool.

## Terminology

- ApiAddress: `host:port` where the `dqlite-demo` REST api is available.
- DataDirectory: Base directory under which the NodeDirectories are saved.
- data file: segment file, snapshot file or snapshot.meta file.
- DbAddress: `host:port` used for database replication.
- DbName: name of the sqlite database.
- metadata file: file named `metadata1` or `metadata2`.
- NodeDirectory: Directory where node specific data is saved, for `dqlite-demo`
  it should be named `DbAddress` and exist under `DataDirectory`.
- segment file: file named like `0000000057685378-0000000057685875`,
  meaning `startindex-endindex`, these contain raft log entries.
- snapshot file: file named like `snapshot-2818-57687002-3645852168`,
  meaning `snapshot-term-index-timestamp`.
- snapshot.meta file: file named like `snapshot-2818-57687002-3645852168.meta`,
  contains metadata about the matching snapshot file.
- TargetClusterYaml: `cluster.yaml` file containing the desired cluster configuration.
- TargetClusterYamlPath: location of `TargetClusterYaml`.

# C. Startup Errors

## C.1 raft_start(): io: closed segment 0000xxxx-0000xxxx is past last snapshot-x-xxxx-xxxxxx

### C.1.1 Method with data loss

This situation can happen when you only have 1 node for example.

1. Backup your data folder and stop the database.
2. Remove the offending segment and try to start again.
3. Repeat step 2 if another segment is preventing you from starting.

### C.1.2 Method preventing data loss

1. Backup your data folders and stop the database.
2. TODO [Variation of the restoring data process]
