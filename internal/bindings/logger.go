package bindings

/*
#include <dqlite.h>
*/
import "C"

// Logging levels.
const (
	LogDebug = C.DQLITE_DEBUG
	LogInfo  = C.DQLITE_INFO
	LogWarn  = C.DQLITE_WARN
	LogError = C.DQLITE_ERROR
)
