package bindings

/*
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>

#include <dqlite.h>
#include <raft.h>
#include <sqlite3.h>

#define EMIT_BUF_LEN 1024

// Duplicate a file descriptor and prevent it from being cloned into child processes.
static int dupCloexec(int oldfd) {
	int newfd = -1;

	newfd = dup(oldfd);
	if (newfd < 0) {
		return -1;
	}

	if (fcntl(newfd, F_SETFD, FD_CLOEXEC) < 0) {
		return -1;
	}

	return newfd;
}

// Allocate an array of n dqlite_server structs.
static int allocServers(int n, struct dqlite_server **servers) {
        *servers = malloc(n * sizeof **servers);
        if (servers == NULL) {
                return -1;
        }
        return 0;
}

// Set the attributes of the i'th server in the given array.
static void setServer(struct dqlite_server *servers, int i, unsigned id, const char *address) {
        servers[i].id = id;
        servers[i].address = address;
}

// Get the attributes of the i'th server in the given array.
static void getServer(struct dqlite_server *servers, int i, unsigned *id, const char **address) {
        *id = servers[i].id;
        *address = servers[i].address;
}


typedef struct dqlite_server dqlite_server;

// C to Go trampoline for custom connect function.
int connectWithDial(uintptr_t handle, unsigned id, char *address, int *fd);

// Wrapper to call the Go trampoline.
static int connectTrampoline(void *data, unsigned id, const char *address, int *fd) {
        uintptr_t handle = (uintptr_t)(data);
        return connectWithDial(handle, id, (char*)address, fd);
}

// Configure a custom connect function.
static int configConnectFunc(dqlite_task *t, uintptr_t handle) {
        return dqlite_task_set_connect_func(t, connectTrampoline, (void*)handle);
}

// C to Go trampoline for custom logging function.
void logWithLogger(uintptr_t handle, int level, char *message);

// Wrapper to call the Go trampoline.
static void logTrampoline(void *data, int level, const char *fmt, va_list args) {
        uintptr_t handle = (uintptr_t)(data);
	char buf[EMIT_BUF_LEN];

	vsnprintf(buf, EMIT_BUF_LEN, fmt, args);
        // FIXME: this seems to cause the stack to grow too much
        // see https://github.com/therecipe/qt/issues/399#issuecomment-315191666
        // logWithLogger(handle, level, buf);
}

// Configure a custom log function.
static void configLogger(dqlite_task *d, uintptr_t handle) {
        dqlite_config(d, DQLITE_CONFIG_LOGGER, logTrampoline, (void*)handle);
}

// C to Go trampoline for custom watch function.
void triggerWatch(uintptr_t handle, int old_state, int new_state);

// Wrapper to call the Go trampoline.
static void watchHandler(void *data, int old_state, int new_state) {
        uintptr_t efd = (uintptr_t)(data);
        uint64_t value = new_state;
        int rv = write(efd, &value, sizeof value);
        assert(rv == sizeof value);
}

// Configure a custom watch function.
static void configWatcher(dqlite_task *d, uintptr_t efd) {
        dqlite_config(d, DQLITE_CONFIG_WATCHER, watchHandler, (void*)efd);
}


static int initializeSQLite()
{
	int rc;

	// Configure SQLite for single-thread mode. This is a global config.
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	if (rc != SQLITE_OK) {
		assert(rc == SQLITE_MISUSE);
		return DQLITE_MISUSE;
	}
	return 0;
}

*/
import "C"
import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
	"unsafe"

	endian "github.com/gxed/GoEndian"
	"golang.org/x/sys/unix"
)

// ServerInfo is the Go equivalent of dqlite_server.
type ServerInfo struct {
	ID      uint64
	Address string
}

// Server is a Go wrapper arround dqlite_server.
type Server C.dqlite_task

// DialFunc is a function that can be used to establish a network connection.
type DialFunc func(context.Context, string) (net.Conn, error)

// LogFunc is a function emitting a single log message.
type LogFunc func(int, string)

// WatchFunc is a function notifiying about state changes.
type WatchFunc func(int, int)

// Init initializes dqlite global state.
func Init() error {
	// FIXME: ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254
	C.signal(C.SIGPIPE, C.SIG_IGN)
	// Don't enable single thread mode when running tests. TODO: find a
	// better way to expose this functionality.
	if os.Getenv("GO_DQLITE_MULTITHREAD") == "1" {
		return nil
	}
	if rc := C.initializeSQLite(); rc != 0 {
		return fmt.Errorf("%d", rc)
	}
	return nil
}

// NewServer creates a new Server instance.
func NewServer(id uint, address string, dir string) (*Server, error) {
	var server *C.dqlite_task
	cid := C.unsigned(id)

	caddress := C.CString(address)
	defer C.free(unsafe.Pointer(caddress))

	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))

	if rc := C.dqlite_task_create(cid, caddress, cdir, &server); rc != 0 {
		return nil, fmt.Errorf("failed to create task object")
	}

	return (*Server)(unsafe.Pointer(server)), nil
}

func (s *Server) SetDialFunc(dial DialFunc) error {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	connectLock.Lock()
	defer connectLock.Unlock()
	connectIndex++
	connectRegistry[connectIndex] = dial
	if rc := C.configConnectFunc(server, connectIndex); rc != 0 {
		return fmt.Errorf("failed to set connect func")
	}
	return nil
}

func (s *Server) SetBindAddress(address string) error {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	caddress := C.CString(address)
	defer C.free(unsafe.Pointer(caddress))
	if rc := C.dqlite_task_set_bind_address(server, caddress); rc != 0 {
		return fmt.Errorf("failed to set bind address")
	}
	return nil
}

func (s *Server) Start() error {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	if rc := C.dqlite_task_start(server); rc != 0 {
		return fmt.Errorf("failed to start task")
	}
	return nil
}

func (s *Server) Stop() error {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	if rc := C.dqlite_task_stop(server); rc != 0 {
		return fmt.Errorf("task stopped with error code %d", rc)
	}
	return nil
}

// Close the server releasing all used resources.
func (s *Server) Close() {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	C.dqlite_task_destroy(server)
}

// SetLogFunc configure a custom log function.
func (s *Server) SetLogFunc(log LogFunc) {
	logLock.Lock()
	defer logLock.Unlock()
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	logIndex++
	// TODO: unregister when destroying the server.
	logRegistry[logIndex] = log
	C.configLogger(server, logIndex)
}

// SetWatchFunc configures a watch function to be notified about state changes.
func (s *Server) SetWatchFunc(watch WatchFunc) {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	efd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		panic(err)
	}
	// TODO: close eventfd an termnate the goroutine upon server shutdown
	go func() {
		buf := make([]byte, 8)
		for {
			n, err := unix.Read(efd, buf)
			if err != nil {
				panic(err)
			}
			if n != 8 {
				panic("short read")
			}
			newState := endian.Endian.Uint64(buf)
			watch(-1, int(newState))
		}
	}()
	C.configWatcher(server, C.uintptr_t(efd))
}

// Dump a database file.
func (s *Server) Dump(filename string) ([]byte, error) {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	cfilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cfilename))
	var buf unsafe.Pointer
	var bufLen C.size_t
	rv := C.dqlite_dump(server, cfilename, &buf, &bufLen)
	if rv != 0 {
		return nil, fmt.Errorf("dump failed with %d", rv)
	}
	data := C.GoBytes(buf, C.int(bufLen))
	C.sqlite3_free(buf)
	return data, nil
}

// Cluster returns information about all servers in the cluster.
func (s *Server) Cluster() ([]ServerInfo, error) {
	server := (*C.dqlite_task)(unsafe.Pointer(s))
	var servers *C.dqlite_server
	var n C.unsigned
	rv := C.dqlite_cluster(server, &servers, &n)
	if rv != 0 {
		return nil, fmt.Errorf("cluster failed with %d", rv)
	}
	defer C.sqlite3_free(unsafe.Pointer(servers))
	infos := make([]ServerInfo, int(n))
	for i := range infos {
		var id C.unsigned
		var address *C.char
		C.getServer(servers, C.int(i), &id, &address)
		infos[i].ID = uint64(id)
		infos[i].Address = C.GoString(address)
	}
	return infos, nil
}

// Extract the underlying socket from a connection.
func connToSocket(conn net.Conn) (C.int, error) {
	file, err := conn.(fileConn).File()
	if err != nil {
		return C.int(-1), err
	}

	fd1 := C.int(file.Fd())

	// Duplicate the file descriptor, in order to prevent Go's finalizer to
	// close it.
	fd2 := C.dupCloexec(fd1)
	if fd2 < 0 {
		return C.int(-1), fmt.Errorf("failed to dup socket fd")
	}

	conn.Close()

	return fd2, nil
}

// Interface that net.Conn must implement in order to extract the underlying
// file descriptor.
type fileConn interface {
	File() (*os.File, error)
}

//export connectWithDial
func connectWithDial(handle C.uintptr_t, id C.unsigned, address *C.char, fd *C.int) C.int {
	connectLock.Lock()
	defer connectLock.Unlock()
	dial := connectRegistry[handle]
	// TODO: make timeout customizable.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info := ServerInfo{ID: uint64(id), Address: C.GoString(address)}
	conn, err := dial(ctx, info.Address)
	if err != nil {
		return C.RAFT_NOCONNECTION
	}
	socket, err := connToSocket(conn)
	if err != nil {
		return C.RAFT_NOCONNECTION
	}
	*fd = socket
	return C.int(0)
}

//export logWithLogger
func logWithLogger(handle C.uintptr_t, level C.int, message *C.char) {
	log := logRegistry[handle]
	msg := C.GoString(message)
	log(int(level), msg)
}

func convertState(state C.int) int {
	switch state {
	case C.DQLITE_UNAVAILABLE:
		return Unavailable
	case C.DQLITE_FOLLOWER:
		return Follower
	case C.DQLITE_CANDIDATE:
		return Candidate
	case C.DQLITE_LEADER:
		return Leader
	default:
		panic("unknown state")
	}
}

// Use handles to avoid passing Go pointers to C.
var connectRegistry = make(map[C.uintptr_t]DialFunc)
var connectIndex C.uintptr_t = 100
var connectLock = sync.Mutex{}

var logRegistry = make(map[C.uintptr_t]LogFunc)
var logIndex C.uintptr_t = 100
var logLock = sync.Mutex{}

// ErrServerStopped is returned by Server.Handle() is the server was stopped.
var ErrServerStopped = fmt.Errorf("server was stopped")

// To compare bool values.
var cfalse C.bool
