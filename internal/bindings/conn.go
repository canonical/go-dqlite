package bindings

/*
#include <stdlib.h>
#include <sqlite3.h>
*/
import "C"
import (
	"database/sql/driver"
	"io"
	"unsafe"

	"github.com/pkg/errors"
)

// Open modes.
const (
	OpenReadWrite = C.SQLITE_OPEN_READWRITE
	OpenReadOnly  = C.SQLITE_OPEN_READONLY
	OpenCreate    = C.SQLITE_OPEN_CREATE
)

// Conn is a Go wrapper around a SQLite database handle.
type Conn C.sqlite3

// OpenFollower is a wrapper around Open that opens the connection in follower
// replication mode, and sets any additional dqlite-related options.
func OpenFollower(name string, vfs string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	db, err := open(name, flags, vfs)
	if err != nil {
		return nil, err
	}

	rc := C.sqlite3_wal_replication_follower(db, walReplicationSchema)
	if rc != C.SQLITE_OK {
		err := codeToError(rc)
		return nil, errors.Wrap(err, "failed to set follower mode")
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// OpenLeader is a wrapper around open that opens connection in leader
// replication mode, and sets any additional dqlite-related options.
func OpenLeader(name string, vfs string, replication string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	db, err := open(name, flags, vfs)
	if err != nil {
		return nil, err
	}

	creplication := C.CString(replication)
	defer C.free(unsafe.Pointer(creplication))

	rc := C.sqlite3_wal_replication_leader(db, walReplicationSchema, creplication, unsafe.Pointer(db))
	if rc != C.SQLITE_OK {
		err := codeToError(rc)
		return nil, errors.Wrap(err, "failed to set leader mode")
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// Open a SQLite connection.
func Open(name string, vfs string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	// Open the database.
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cvfs := C.CString(vfs)
	defer C.free(unsafe.Pointer(cvfs))

	var db *C.sqlite3

	rc := C.sqlite3_open_v2(cname, &db, C.int(flags), cvfs)
	if rc != C.SQLITE_OK {
		err := lastError(db)
		C.sqlite3_close_v2(db)
		return nil, err
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// Open a SQLite connection, setting anything that is common between leader and
// follower connections.
func open(name string, flags int, vfs string) (db *C.sqlite3, err error) {
	// Open the database.
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cvfs := C.CString(vfs)
	defer C.free(unsafe.Pointer(cvfs))

	rc := C.sqlite3_open_v2(cname, &db, C.int(flags), cvfs)
	if rc != C.SQLITE_OK {
		err := lastError(db)
		C.sqlite3_close_v2(db)
		return nil, err
	}

	return db, nil
}

// Close the connection.
func (c *Conn) Close() error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	rc := C.sqlite3_close(db)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}

	return nil
}

// Filename of the underlying database file.
func (c *Conn) Filename() string {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	return C.GoString(C.sqlite3_db_filename(db, walReplicationSchema))
}

// Exec executes a query.
func (c *Conn) Exec(query string) error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))

	rc := C.sqlite3_exec(db, sql, nil, nil, nil)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}

	return nil
}

// Query the database.
func (c *Conn) Query(query string) (*Rows, error) {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	var stmt *C.sqlite3_stmt
	var tail *C.char

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))

	rc := C.sqlite3_prepare(db, sql, -1, &stmt, &tail)
	if rc != C.SQLITE_OK {
		return nil, lastError(db)
	}

	rows := &Rows{db: db, stmt: stmt}

	return rows, nil
}

// Rows represents a result set.
type Rows struct {
	db   *C.sqlite3
	stmt *C.sqlite3_stmt
}

// Next fetches the next row of a result set.
func (r *Rows) Next(values []driver.Value) error {
	rc := C.sqlite3_step(r.stmt)
	if rc == C.SQLITE_DONE {
		return io.EOF
	}
	if rc != C.SQLITE_ROW {
		return lastError(r.db)
	}

	for i := range values {
		values[i] = int64(C.sqlite3_column_int64(r.stmt, C.int(i)))
	}

	return nil
}

// Close finalizes the underlying statement.
func (r *Rows) Close() error {
	rc := C.sqlite3_finalize(r.stmt)
	if rc != C.SQLITE_OK {
		return lastError(r.db)
	}

	return nil
}
