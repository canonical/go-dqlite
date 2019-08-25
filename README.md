go-dqlite [![Build Status](https://travis-ci.org/canonical/go-dqlite.png)](https://travis-ci.org/canonical/go-dqlite) [![Coverage Status](https://coveralls.io/repos/github/canonical/go-dqlite/badge.svg?branch=master)](https://coveralls.io/github/canonical/go-dqlite?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/canonical/go-dqlite)](https://goreportcard.com/report/github.com/canonical/go-dqlite) [![GoDoc](https://godoc.org/github.com/canonical/go-dqlite?status.svg)](https://godoc.org/github.com/canonical/go-dqlite)
======

This repository provides the `go-dqlite` Go package, containing bindings for the
[dqlite](https://github.com/canonical/canonical/dqlite) C library and a pure-Go
client for the dqlite wire [protocol](https://github.com/canonical/dqlite/blob/master/doc/protocol.md).

How does it compare to rqlite?
------------------------------

The main differences from [rqlite](https://github.com/rqlite/rqlite) are:

* Full support for transactions
* No need for statements to be deterministic (e.g. you can use ```time()```)
* Frame-based replication instead of statement-based replication, this
  means in dqlite there's more data flowing between nodes, so expect
  lower performance. Should not really matter for most use cases.

Status
------

This is **beta** software for now, but we'll get to rc/release soon.

Demo
----

To see dqlite in action, make sure you have the following dependencies
installed:

* Go (tested on 1.8)
* gcc
* any dependency/header that SQLite needs to build from source
* Python 3

Then run:

```
go get -d github.com/canonical/go-dqlite
cd $GOPATH/src/github.com/canonical/go-dqlite
make dependencies
./run-demo
```

This should spawn three dqlite-based nodes, each of one running the
code in the [demo Go source](testdata/demo.go).

Each node inserts data in a test table and then dies abruptly after a
random timeout. Leftover transactions and failover to other nodes
should be handled gracefully.

While the demo is running, to get more details about what's going on
behind the scenes you can also open another terminal and run a command
like:

```
watch ls -l /tmp/dqlite-demo-*/ /tmp/dqlite-demo-*/snapshots/
```

and see how the data directories of the three nodes evolve in terms
SQLite databases (```test.db```), write-ahead log files (```test.db-wal```),
raft logs store (```raft.db```), and raft snapshots.


Documentation
-------------

The documentation for this package can be found on [Godoc](http://godoc.org/github.com/canonical/go-dqlite).

FAQ
---

**Q**: How does dqlite behave during conflict situations? Does Raft
select a winning WAL write and any others in flight are aborted?

**A**: There can't be a conflict situation. Raft's model is that
only the leader can append new log entries, which translated to dqlite
means that only the leader can write new WAL frames. So this means
that any attempt to perform a write transaction on a non-leader node
will fail with a sqlite3x.ErrNotLeader error (and in this case clients
are supposed to retry against whoever is the new leader).

**Q**: When not enough nodes are available, are writes hung until
consensus?

**A**: Yes, however there's a (configurable) timeout. This is a
consequence of Raft sitting in the CP spectrum of the CAP theorem: in
case of a network partition it chooses consistency and sacrifices
availability.
