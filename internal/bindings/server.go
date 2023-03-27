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

#define EMIT_BUF_LEN 1024

typedef unsigned long long nanoseconds_t;
typedef unsigned long long failure_domain_t;

// Duplicate a file descriptor and prevent it from being cloned into child processes.
static int dupCloexec(int oldfd) {
	int newfd = -1;

	newfd = dup(oldfd);
	if (newfd < 0) {
		return -1;
	}

	if (fcntl(newfd, F_SETFD, FD_CLOEXEC) < 0) {
		close(newfd);
		return -1;
	}

	return newfd;
}

// C to Go trampoline for custom connect function.
int connectWithDial(uintptr_t handle, char *address, int *fd);

// Wrapper to call the Go trampoline.
static int connectTrampoline(void *data, const char *address, int *fd) {
        uintptr_t handle = (uintptr_t)(data);
        return connectWithDial(handle, (char*)address, fd);
}

// Configure a custom connect function.
static int configConnectFunc(dqlite_node *t, uintptr_t handle) {
        return dqlite_node_set_connect_func(t, connectTrampoline, (void*)handle);
}

static dqlite_node_info_ext *makeInfos(int n) {
	return calloc(n, sizeof(dqlite_node_info_ext));
}

static void setInfo(dqlite_node_info_ext *infos, unsigned i, dqlite_node_id id,
		    const char *address, int role) {
	dqlite_node_info_ext *info = &infos[i];
	info->size = sizeof(dqlite_node_info_ext);
	info->id = id;
	info->address = (uint64_t)(uintptr_t)address;
	info->dqlite_role = role;
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

	"github.com/canonical/go-dqlite/internal/protocol"
)

type Node struct {
	node   *C.dqlite_node
	ctx    context.Context
	cancel context.CancelFunc
}

type SnapshotParams struct {
	Threshold uint64
	Trailing  uint64
}

// Initializes state.
func init() {
	// FIXME: ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254
	C.signal(C.SIGPIPE, C.SIG_IGN)
}

// NewNode creates a new Node instance.
func NewNode(ctx context.Context, id uint64, address string, dir string) (*Node, error) {
	var server *C.dqlite_node
	cid := C.dqlite_node_id(id)

	caddress := C.CString(address)
	defer C.free(unsafe.Pointer(caddress))

	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))

	if rc := C.dqlite_node_create(cid, caddress, cdir, &server); rc != 0 {
		errmsg := C.GoString(C.dqlite_node_errmsg(server))
		C.dqlite_node_destroy(server)
		return nil, fmt.Errorf("%s", errmsg)
	}

	node := &Node{node: (*C.dqlite_node)(unsafe.Pointer(server))}
	node.ctx, node.cancel = context.WithCancel(ctx)

	return node, nil
}

func (s *Node) SetDialFunc(dial protocol.DialFunc) error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	connectLock.Lock()
	defer connectLock.Unlock()
	connectIndex++
	connectRegistry[connectIndex] = dial
	contextRegistry[connectIndex] = s.ctx
	if rc := C.configConnectFunc(server, connectIndex); rc != 0 {
		return fmt.Errorf("failed to set connect func")
	}
	return nil
}

func (s *Node) SetBindAddress(address string) error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	caddress := C.CString(address)
	defer C.free(unsafe.Pointer(caddress))
	if rc := C.dqlite_node_set_bind_address(server, caddress); rc != 0 {
		return fmt.Errorf("failed to set bind address %q: %d", address, rc)
	}
	return nil
}

func (s *Node) SetNetworkLatency(nanoseconds uint64) error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	cnanoseconds := C.nanoseconds_t(nanoseconds)
	if rc := C.dqlite_node_set_network_latency(server, cnanoseconds); rc != 0 {
		return fmt.Errorf("failed to set network latency")
	}
	return nil
}

func (s *Node) SetSnapshotParams(params SnapshotParams) error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	cthreshold := C.unsigned(params.Threshold)
	ctrailing := C.unsigned(params.Trailing)
	if rc := C.dqlite_node_set_snapshot_params(server, cthreshold, ctrailing); rc != 0 {
		return fmt.Errorf("failed to set snapshot params")
	}
	return nil
}

func (s *Node) SetFailureDomain(code uint64) error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	ccode := C.failure_domain_t(code)
	if rc := C.dqlite_node_set_failure_domain(server, ccode); rc != 0 {
		return fmt.Errorf("set failure domain: %d", rc)
	}
	return nil
}

func (s *Node) EnableDiskMode() error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	if rc := C.dqlite_node_enable_disk_mode(server); rc != 0 {
		return fmt.Errorf("failed to set disk mode")
	}
	return nil
}

func (s *Node) GetBindAddress() string {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	return C.GoString(C.dqlite_node_get_bind_address(server))
}

func (s *Node) Start() error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	if rc := C.dqlite_node_start(server); rc != 0 {
		errmsg := C.GoString(C.dqlite_node_errmsg(server))
		return fmt.Errorf("%s", errmsg)
	}
	return nil
}

func (s *Node) Stop() error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	if rc := C.dqlite_node_stop(server); rc != 0 {
		return fmt.Errorf("task stopped with error code %d", rc)
	}
	return nil
}

// Close the server releasing all used resources.
func (s *Node) Close() {
	defer s.cancel()
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	C.dqlite_node_destroy(server)
}

// Remark that Recover doesn't take the node role into account
func (s *Node) Recover(cluster []protocol.NodeInfo) error {
	for i, _ := range cluster {
		cluster[i].Role = protocol.Voter
	}
	return s.RecoverExt(cluster)
}

// RecoverExt has a similar purpose as `Recover` but takes the node role into account
func (s *Node) RecoverExt(cluster []protocol.NodeInfo) error {
	server := (*C.dqlite_node)(unsafe.Pointer(s.node))
	n := C.int(len(cluster))
	infos := C.makeInfos(n)
	defer C.free(unsafe.Pointer(infos))
	for i, info := range cluster {
		cid := C.dqlite_node_id(info.ID)
		caddress := C.CString(info.Address)
		crole := C.int(info.Role)
		defer C.free(unsafe.Pointer(caddress))
		C.setInfo(infos, C.unsigned(i), cid, caddress, crole)
	}
	if rc := C.dqlite_node_recover_ext(server, infos, n); rc != 0 {
		return fmt.Errorf("recover failed with error code %d", rc)
	}
	return nil
}

// GenerateID generates a unique ID for a server.
func GenerateID(address string) uint64 {
	caddress := C.CString(address)
	defer C.free(unsafe.Pointer(caddress))
	id := C.dqlite_generate_node_id(caddress)
	return uint64(id)
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
func connectWithDial(handle C.uintptr_t, address *C.char, fd *C.int) C.int {
	connectLock.Lock()
	defer connectLock.Unlock()

	dial := connectRegistry[handle]
	ctx := contextRegistry[handle]

	// TODO: make timeout customizable.
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := dial(dialCtx, C.GoString(address))
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
var contextRegistry = make(map[C.uintptr_t]context.Context)
var connectRegistry = make(map[C.uintptr_t]protocol.DialFunc)
var connectIndex C.uintptr_t = 100
var connectLock = sync.Mutex{}

// ErrNodeStopped is returned by Node.Handle() is the server was stopped.
var ErrNodeStopped = fmt.Errorf("server was stopped")

// To compare bool values.
var cfalse C.bool
