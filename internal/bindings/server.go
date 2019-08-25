package bindings

/*
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <stdint.h>
#include <signal.h>

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
)

// Server is a Go wrapper arround dqlite_server.
type Server C.dqlite_task

// DialFunc is a function that can be used to establish a network connection.
type DialFunc func(context.Context, string) (net.Conn, error)

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
	conn, err := dial(ctx, C.GoString(address))
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

// Use handles to avoid passing Go pointers to C.
var connectRegistry = make(map[C.uintptr_t]DialFunc)
var connectIndex C.uintptr_t = 100
var connectLock = sync.Mutex{}

// ErrServerStopped is returned by Server.Handle() is the server was stopped.
var ErrServerStopped = fmt.Errorf("server was stopped")

// To compare bool values.
var cfalse C.bool
