package bindings

/*
#cgo linux LDFLAGS: -ldqlite
#include <dqlite.h>
*/
import "C"

// required dqlite version
const DqliteMinSupportedVersion = 1_17_00

var DqliteVersion = int(C.dqlite_version_number())

func versionComponents(version int) (major, minor, revision int) {
	revision = version % 100
	version /= 100

	minor = version % 100
	version /= 100

	major = version

	return major, minor, revision
}
