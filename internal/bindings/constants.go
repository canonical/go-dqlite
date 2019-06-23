package bindings

/*
#include <sqlite3.h>
#include <dqlite.h>
*/
import "C"

// ProtocolVersion is the latest dqlite server protocol version.
const ProtocolVersion = uint64(C.DQLITE_PROTOCOL_VERSION)

// SQLite datatype codes
const (
	Integer = C.SQLITE_INTEGER
	Float   = C.SQLITE_FLOAT
	Text    = C.SQLITE_TEXT
	Blob    = C.SQLITE_BLOB
	Null    = C.SQLITE_NULL
)

// Special data types for time values.
const (
	UnixTime = C.DQLITE_UNIXTIME
	ISO8601  = C.DQLITE_ISO8601
	Boolean  = C.DQLITE_BOOLEAN
)

// Logging levels.
const (
	LogDebug = C.DQLITE_DEBUG
	LogInfo  = C.DQLITE_INFO
	LogWarn  = C.DQLITE_WARN
	LogError = C.DQLITE_ERROR
)

// Request types.
const (
	RequestLeader    = C.DQLITE_REQUEST_LEADER
	RequestClient    = C.DQLITE_REQUEST_CLIENT
	RequestHeartbeat = C.DQLITE_REQUEST_HEARTBEAT
	RequestOpen      = C.DQLITE_REQUEST_OPEN
	RequestPrepare   = C.DQLITE_REQUEST_PREPARE
	RequestExec      = C.DQLITE_REQUEST_EXEC
	RequestQuery     = C.DQLITE_REQUEST_QUERY
	RequestFinalize  = C.DQLITE_REQUEST_FINALIZE
	RequestExecSQL   = C.DQLITE_REQUEST_EXEC_SQL
	RequestQuerySQL  = C.DQLITE_REQUEST_QUERY_SQL
	RequestInterrupt = C.DQLITE_REQUEST_INTERRUPT
	RequestJoin      = C.DQLITE_REQUEST_JOIN
	RequestPromote   = C.DQLITE_REQUEST_PROMOTE
	RequestRemove    = C.DQLITE_REQUEST_REMOVE
)

// Response types.
const (
	ResponseFailure = C.DQLITE_RESPONSE_FAILURE
	ResponseServer  = C.DQLITE_RESPONSE_SERVER
	ResponseWelcome = C.DQLITE_RESPONSE_WELCOME
	ResponseServers = C.DQLITE_RESPONSE_SERVERS
	ResponseDb      = C.DQLITE_RESPONSE_DB
	ResponseStmt    = C.DQLITE_RESPONSE_STMT
	ResponseResult  = C.DQLITE_RESPONSE_RESULT
	ResponseRows    = C.DQLITE_RESPONSE_ROWS
	ResponseEmpty   = C.DQLITE_RESPONSE_EMPTY
)
