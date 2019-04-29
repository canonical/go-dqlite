package bindings

/*
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include <dqlite.h>
#include <raft.h>
#include <sqlite3.h>

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
int connectWithDial(uintptr_t handle, dqlite_server *server, int *fd);

// Wrapper to call the Go trampoline.
static int connectTrampoline(void *data, const dqlite_server *server, int *fd) {
        uintptr_t handle = (uintptr_t)(data);
        return connectWithDial(handle, (dqlite_server *)server, fd);
}

// Configure a custom connect function.
static void configConnect(dqlite *d, uintptr_t handle) {
        dqlite_config(d, DQLITE_CONFIG_CONNECT, connectTrampoline, (void*)handle);
}
*/
import "C"
import (
	"context"
	"fmt"
	"net"
	"os"
	"time"
	"unsafe"
)

// ServerInfo is the Go equivalent of dqlite_server.
type ServerInfo struct {
	ID      uint64
	Address string
}

// Server is a Go wrapper arround dqlite_server.
type Server C.dqlite

// DialFunc is a function that can be used to establish a network connection.
type DialFunc func(context.Context, string) (net.Conn, error)

// Init initializes dqlite global state.
func Init() error {
	rc := C.dqlite_initialize()
	if rc != 0 {
		return fmt.Errorf("%d", rc)
	}
	return nil
}

// NewServer creates a new Server instance.
func NewServer(id uint, address string, dir string) (*Server, error) {
	cid := C.unsigned(id)

	caddress := C.CString(address)
	defer C.free(unsafe.Pointer(caddress))

	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))

	var server *C.dqlite
	rc := C.dqlite_create(cid, caddress, cdir, &server)
	if rc != 0 {
		return nil, fmt.Errorf("failed to create server object")
	}

	return (*Server)(unsafe.Pointer(server)), nil
}

// Bootstrap the a server, setting its initial raft configuration.
func (s *Server) Bootstrap(servers []ServerInfo) error {
	var cservers *C.dqlite_server
	n := len(servers)
	server := (*C.dqlite)(unsafe.Pointer(s))
	rv := C.allocServers(C.int(n), &cservers)
	if rv != 0 {
		return fmt.Errorf("out of memory")
	}
	for i := 0; i < n; i++ {
		cid := C.unsigned(servers[i].ID)
		caddress := C.CString(servers[i].Address)
		defer C.free(unsafe.Pointer(caddress))
		C.setServer(cservers, C.int(i), cid, caddress)
	}
	rv = C.dqlite_bootstrap(server, C.unsigned(n), cservers)
	if rv != 0 {
		if rv == C.DQLITE_CANTBOOTSTRAP {
			return ErrServerCantBootstrap
		}
		return fmt.Errorf("bootstrap failed with %d", rv)
	}
	return nil
}

// Close the server releasing all used resources.
func (s *Server) Close() {
	server := (*C.dqlite)(unsafe.Pointer(s))
	C.dqlite_destroy(server)
}

// SetLogger sets the server logger.
// func (s *Server) SetLogger(logger *Logger) {
// 	server := (*C.dqlite)(unsafe.Pointer(s))

// 	rc := C.dqlite_server_config(server, C.DQLITE_CONFIG_LOGGER, unsafe.Pointer(logger))
// 	if rc != 0 {
// 		// Setting the logger should never fail.
// 		panic("failed to set logger")
// 	}
// }

// SetDialFunc configure a custom dial function.
func (s *Server) SetDialFunc(dial DialFunc) {
	server := (*C.dqlite)(unsafe.Pointer(s))
	connectIndex++
	// TODO: unregister when destroying the server.
	connectRegistry[connectIndex] = dial
	C.configConnect(server, connectIndex)
}

// Run the server.
func (s *Server) Run() error {
	server := (*C.dqlite)(unsafe.Pointer(s))
	rc := C.dqlite_run(server)
	if rc != 0 {
		return fmt.Errorf("run failed with %d", rc)
	}
	return nil
}

// Ready waits for the server to be ready to handle connections.
func (s *Server) Ready() bool {
	server := (*C.dqlite)(unsafe.Pointer(s))
	return C.dqlite_ready(server) != cfalse
}

// Leader returns information about the current leader, if any.
func (s *Server) Leader() *ServerInfo {
	server := (*C.dqlite)(unsafe.Pointer(s))
	var info C.dqlite_server
	if C.dqlite_leader(server, &info) != cfalse {
		return &ServerInfo{
			ID:      uint64(info.id),
			Address: C.GoString(info.address),
		}
	}
	return nil
}

// Cluster returns information about all servers in the cluster.
func (s *Server) Cluster() ([]ServerInfo, error) {
	server := (*C.dqlite)(unsafe.Pointer(s))
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

// Handle a new connection.
func (s *Server) Handle(conn net.Conn) error {
	server := (*C.dqlite)(unsafe.Pointer(s))

	fd, err := connToSocket(conn)
	if err != nil {
		return err
	}

	rc := C.dqlite_handle(server, fd)
	if rc != 0 {
		C.close(fd)
		if rc == C.DQLITE_STOPPED {
			return ErrServerStopped
		}
		return fmt.Errorf("hadle failed with %d", rc)
	}

	return nil
}

// Interface that net.Conn must implement in order to extract the underlying
// file descriptor.
type fileConn interface {
	File() (*os.File, error)
}

// Stop the server.
func (s *Server) Stop() error {
	server := (*C.dqlite)(unsafe.Pointer(s))
	rc := C.dqlite_stop(server)
	if rc != 0 {
		return fmt.Errorf("stop failed with %d", rc)
	}
	return nil
}

//export connectWithDial
func connectWithDial(handle C.uintptr_t, server *C.dqlite_server, fd *C.int) C.int {
	dial := connectRegistry[handle]
	// TODO: make timeout customizable.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := uint64(server.id)
	info := ServerInfo{ID: id, Address: C.GoString(server.address)}
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

// Use handles to avoid passing Go pointers to C.
var connectRegistry = make(map[C.uintptr_t]DialFunc)
var connectIndex C.uintptr_t = 100

// ErrServerStopped is returned by Server.Handle() is the server was stopped.
var ErrServerStopped = fmt.Errorf("server was stopped")

// ErrServerCantBootstrap is returned by Server.Bootstrap() if the server has
// already a raft configuration.
var ErrServerCantBootstrap = fmt.Errorf("server already bootstrapped")

// To compare bool values.
var cfalse C.bool
