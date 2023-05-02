package bindings

/*
#cgo linux LDFLAGS: -ldqlite
*/
import "C"

// required dqlite version
var dqliteMajorVersion int = 1
var dqliteMinorVersion int = 14
