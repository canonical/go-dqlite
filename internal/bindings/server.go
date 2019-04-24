package bindings

/*
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include <dqlite.h>
#include <sqlite3.h>

// Duplicate a file descriptor and prevent it from being cloned into child processes.
int dupCloexec(int oldfd) {
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
int allocServers(int n, struct dqlite_server **servers) {
        *servers = malloc(n * sizeof **servers);
        if (servers == NULL) {
                return -1;
        }
        return 0;
}

// Set the attributes of the i'th server in the given array.
void setServer(struct dqlite_server *servers, int i, unsigned id, const char *address) {
        servers[i].id = id;
        servers[i].address = address;
}
*/
import "C"
import (
	"fmt"
	"net"
	"os"
	"unsafe"
)

// ServerInfo is the Go equivalent of dqlite_server.
type ServerInfo struct {
	ID      uint64
	Address string
}

// Server is a Go wrapper arround dqlite_server.
type Server C.dqlite

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
	var cfalse C.bool
	return C.dqlite_ready(server) != cfalse
}

// Handle a new connection.
func (s *Server) Handle(conn net.Conn) error {
	server := (*C.dqlite)(unsafe.Pointer(s))

	file, err := conn.(fileConn).File()
	if err != nil {
		return err
	}
	defer file.Close()

	fd1 := C.int(file.Fd())

	// Duplicate the file descriptor, in order to prevent Go's finalizer to
	// close it.
	fd2 := C.dupCloexec(fd1)
	if fd2 < 0 {
		return fmt.Errorf("failed to dup socket fd")
	}

	conn.Close()

	rc := C.dqlite_handle(server, fd2)
	if rc != 0 {
		C.close(fd2)
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

// ErrServerStopped is returned by Server.Handle() is the server was stopped.
var ErrServerStopped = fmt.Errorf("server was stopped")
