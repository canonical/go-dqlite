// +build !nosqlite3

package bindings

import (
	"github.com/canonical/go-dqlite/internal/protocol"
)

/*
#cgo linux LDFLAGS: -lsqlite3

#include <sqlite3.h>

static int sqlite3ConfigSingleThread()
{
	return sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
}

static int sqlite3ConfigMultiThread()
{
	return sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
}
*/
import "C"

func ConfigSingleThread() error {
	if rc := C.sqlite3ConfigSingleThread(); rc != 0 {
		return protocol.Error{Message: C.GoString(C.sqlite3_errstr(rc)), Code: int(rc)}
	}
	return nil
}

func ConfigMultiThread() error {
	if rc := C.sqlite3ConfigMultiThread(); rc != 0 {
		return protocol.Error{Message: C.GoString(C.sqlite3_errstr(rc)), Code: int(rc)}
	}
	return nil
}
