go-dqlite [![CI tests][cibadge]][ciyml] [![Coverage Status][coverallsbadge]][coveralls] [![Go Report Card][reportcardbadge]][reportcard] [![GoDoc][godocbadge]][godoc]
======

This repository provides the go-dqlite Go package, containing bindings for
the [dqlite][dqlite] C library and a pure-Go client for the dqlite wire
[protocol][protocol].

Usage
-----

The current major version of the package is `v3`:

```
$ go get github.com/canonical/go-dqlite/v3
```

The [`v2`][v2] major version is used for an LTS series that gets fixes but no new
features. Major version 1 is no longer receiving updates of any kind and should
not be used.

The best way to understand how to use the package is probably by looking at the
source code of the [demo program][demo] and using it as an example.

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
the [dqlite][dqlite] C library installed on your system, along with its
dependencies.

go-dqlite can use the [go-sqlite3][go-sqlite3] package to store some
information about the cluster locally on each node in a SQLite database file.
Pass `-tags libsqlite3` when building go-dqlite to request that go-sqlite3 link
to your system's libsqlite3, instead of building its own. Alternatively, pass
`-tags nosqlite3` to disable the go-sqlite3 dependency; go-dqlite can store the
same information in YAML files instead.

Documentation
-------------

The documentation for this package can be found on [pkg.go.dev][godoc].

Demo
----

To see dqlite in action, either install the Debian package from the PPA:

```bash
$ sudo add-apt-repository -y ppa:dqlite/dev
$ sudo apt install dqlite-tools-v3 libdqlite-dev
```

or build the dqlite C library and its dependencies from source, as described
[here][dqlitebuild], and then run:

```
$ go install -tags libsqlite3 ./cmd/dqlite-demo
```

from the top-level directory of this repository.

This builds a demo dqlite application, which exposes a simple key/value store
over an HTTP API.

Once the `dqlite-demo` binary is installed (normally under `~/go/bin` or
`/usr/bin/`), start three nodes of the demo application:

```bash
$ dqlite-demo --api 127.0.0.1:8001 --db 127.0.0.1:9001 &
$ dqlite-demo --api 127.0.0.1:8002 --db 127.0.0.1:9002 --join 127.0.0.1:9001 &
$ dqlite-demo --api 127.0.0.1:8003 --db 127.0.0.1:9003 --join 127.0.0.1:9001 &
```

The `--api` flag tells the demo program where to expose its HTTP API.

The `--db` flag tells the demo program to use the given address for internal
database replication.

The `--join` flag is optional and should be used only for additional nodes after
the first one. It informs them about the existing cluster, so they can
automatically join it.

Now we can start using the cluster. Let's insert a key pair:

```bash
$ curl -X PUT -d my-value http://127.0.0.1:8001/my-key
```

and then retrieve it from the database:

```bash
$ curl http://127.0.0.1:8001/my-key
```

Currently the first node is the leader. If we stop it and then try to query the
key again curl will fail, but we can simply change the endpoint to another node
and things will work since an automatic failover has taken place:

```bash
$ kill -TERM %1; curl http://127.0.0.1:8002/my-key
```

Shell
-----

A basic SQLite-like dqlite shell is available in the `dqlite-tools-v3` package
or can be built with:

```
$ go install -tags libsqlite3 ./cmd/dqlite
```

The general usage is:

```
dqlite -s <servers> <database> [command] [flags]
```

Example usage in the case of the `dqlite-demo` example listed above:

```
$ dqlite -s 127.0.0.1:9001 demo
dqlite> SELECT * FROM model;
my-key|my-value
```

The shell supports normal SQL queries plus the special `.cluster` and `.leader`
commands to inspect the cluster members and the current leader.

[cibadge]: https://github.com/canonical/go-dqlite/actions/workflows/build-and-test.yml/badge.svg
[ciyml]: https://github.com/canonical/go-dqlite/actions/workflows/build-and-test.yml
[coveralls]: https://coveralls.io/github/canonical/go-dqlite?branch=v3
[coverallsbadge]: https://coveralls.io/repos/github/canonical/go-dqlite/badge.svg?branch=v3
[demo]: https://github.com/canonical/go-dqlite/blob/v3/cmd/dqlite-demo/dqlite-demo.go
[dqlite]: https://github.com/canonical/dqlite
[dqlitebuild]: https://github.com/canonical/dqlite#build
[go-sqlite3]: https://github.com/mattn/go-sqlite3
[godoc]: https://godoc.org/github.com/canonical/go-dqlite/v3
[godocbadge]: https://godoc.org/github.com/canonical/go-dqlite/v3?status.svg
[protocol]: https://dqlite.io/docs/reference/wire-protocol
[reportcard]: https://goreportcard.com/report/github.com/canonical/go-dqlite/v3
[reportcardbadge]: https://goreportcard.com/badge/github.com/canonical/go-dqlite/v3
[v2]: https://github.com/canonical/go-dqlite/tree/v2
