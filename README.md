go-dqlite [![CI tests](https://github.com/canonical/go-dqlite/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/canonical/go-dqlite/actions/workflows/build-and-test.yml) [![Coverage Status](https://coveralls.io/repos/github/canonical/go-dqlite/badge.svg?branch=master)](https://coveralls.io/github/canonical/go-dqlite?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/canonical/go-dqlite)](https://goreportcard.com/report/github.com/canonical/go-dqlite) [![GoDoc](https://godoc.org/github.com/canonical/go-dqlite?status.svg)](https://godoc.org/github.com/canonical/go-dqlite)
======

This repository provides the `go-dqlite` Go package, containing bindings for the
[dqlite](https://github.com/canonical/dqlite) C library and a pure-Go
client for the dqlite wire [protocol](https://github.com/canonical/dqlite/blob/master/doc/protocol.md).

Usage
-----

The best way to understand how to use the ```go-dqlite``` package is probably by
looking at the source code of the [demo
program](https://github.com/canonical/go-dqlite/blob/master/cmd/dqlite-demo/dqlite-demo.go) and
use it as example.

In general your application will use code such as:


```go
dir := "/path/to/data/directory"
address := "1.2.3.4:666" // Unique node address
cluster := []string{...} // Optional list of existing nodes, when starting a new node
app, err := app.New(dir, app.WithAddress(address), app.WithCluster(cluster))
if err != nil {
        // ...
}

db, err := app.Open(context.Background(), "my-database")
if err != nil {
        // ...
}

// db is a *sql.DB object
if _, err := db.Exec("CREATE TABLE my_table (n INT)"); err != nil
        // ...
}
```

Build
-----

In order to use the go-dqlite package in your application, you'll need to have
the [dqlite](https://github.com/canonical/dqlite) C library installed on your
system, along with its dependencies. You'll also need to put ``CGO_LDFLAGS_ALLOW="-Wl,-z,now"``
in the environment of any Go build commands (see [here](https://github.com/golang/go/wiki/InvalidFlag)
for the explanation).

By default, go-dqlite's `client` module supports storing a cache of the
cluster's state in a SQLite database, locally on each cluster member. (This is
not to be confused with any SQLite databases that are managed by dqlite.) In
order to do this, it imports https://github.com/mattn/go-sqlite3, and so you
can use the `libsqlite3` build tag to control whether go-sqlite3 links to a
system libsqlite3 or builds its own. You can also disable support for SQLite
node stores entirely with the `nosqlite3` build tag (unique to go-dqlite). If
you pass this tag, your application will not link *directly* to libsqlite3 (but
it will still link it *indirectly* via libdqlite, unless you've dropped the
sqlite3.c amalgamation into the dqlite build).

Documentation
-------------

The documentation for this package can be found on [pkg.go.dev](https://pkg.go.dev/github.com/canonical/go-dqlite).

Demo
----

To see dqlite in action, either install the Debian package from the PPA:

```bash
sudo add-apt-repository -y ppa:dqlite/dev
sudo apt install dqlite-tools libdqlite-dev
```

or build the dqlite C library and its dependencies from source, as described
[here](https://github.com/canonical/dqlite#build), and then run:

```
go install -tags libsqlite3 ./cmd/dqlite-demo
```

from the top-level directory of this repository.

This builds a demo dqlite application, which exposes a simple key/value store
over an HTTP API.

Once the `dqlite-demo` binary is installed (normally under `~/go/bin` or
`/usr/bin/`), start three nodes of the demo application:

```bash
dqlite-demo --api 127.0.0.1:8001 --db 127.0.0.1:9001 &
dqlite-demo --api 127.0.0.1:8002 --db 127.0.0.1:9002 --join 127.0.0.1:9001 &
dqlite-demo --api 127.0.0.1:8003 --db 127.0.0.1:9003 --join 127.0.0.1:9001 &
```

The `--api` flag tells the demo program where to expose its HTTP API.

The `--db` flag tells the demo program to use the given address for internal
database replication.

The `--join` flag is optional and should be used only for additional nodes after
the first one. It informs them about the existing cluster, so they can
automatically join it.

Now we can start using the cluster. Let's insert a key pair:

```bash
curl -X PUT -d my-value http://127.0.0.1:8001/my-key
```

and then retrieve it from the database:

```bash
curl http://127.0.0.1:8001/my-key
```

Currently the first node is the leader. If we stop it and then try to query the
key again curl will fail, but we can simply change the endpoint to another node
and things will work since an automatic failover has taken place:

```bash
kill -TERM %1; curl http://127.0.0.1:8002/my-key
```

Shell
------

A basic SQLite-like dqlite shell is available in the `dqlite-tools` package or
can be built with:
```
go install -tags libsqlite3 ./cmd/dqlite
```
```
Usage:
  dqlite -s <servers> <database> [command] [flags]
```

Example usage in the case of the `dqlite-demo` example listed above:
```
dqlite -s 127.0.0.1:9001 demo

dqlite> SELECT * FROM model;
my-key|my-value
```

The shell supports normal SQL queries plus the special `.cluster` and `.leader`
commands to inspect the cluster members and the current leader.
