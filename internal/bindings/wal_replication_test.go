package bindings_test

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWalReplication(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	methods := &directWalReplication{}

	replication, err := bindings.NewWalReplication("test", methods)
	require.NoError(t, err)

	assert.Equal(t, "test", replication.Name())

	err = replication.Close()
	require.NoError(t, err)
}

func TestNewWalReplication_AlreadyRegistered(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	methods := &directWalReplication{}

	replication1, err := bindings.NewWalReplication("test", methods)
	require.NoError(t, err)

	replication2, err := bindings.NewWalReplication("test", methods)

	assert.Nil(t, replication2)
	assert.EqualError(t, err, "WAL replication name already registered")

	err = replication1.Close()
	require.NoError(t, err)
}

func TestConn_WalReplicationLeader_Trampolines(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnVolatileWal(t)
	defer cleanup()

	methods := &countWalReplication{}

	replication, err := bindings.NewWalReplication("test", methods)
	require.NoError(t, err)

	defer replication.Close()

	err = conn.WalReplicationLeader("test")
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	assert.Equal(t, 1, methods.begin)
	assert.Equal(t, 1, methods.frames)
	assert.Equal(t, 1, methods.end)
}

func TestConn_WalReplicationFollower_Frames(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs1, cleanup := newVfsWithName(t, "test1")
	defer cleanup()

	vfs2, cleanup := newVfsWithName(t, "test2")
	defer cleanup()

	leader, cleanup := newConnWithVfs(t, "test.db", vfs1.Name())
	defer cleanup()

	follower, cleanup := newConnWithVfs(t, "test.db", vfs2.Name())
	defer cleanup()

	err := leader.Exec("PRAGMA synchronous=OFF; PRAGMA journal_mode=wal")
	require.NoError(t, err)

	err = follower.Exec("PRAGMA synchronous=OFF; PRAGMA journal_mode=wal")
	require.NoError(t, err)

	methods := &directWalReplication{
		follower: follower,
	}

	replication, err := bindings.NewWalReplication("test", methods)
	require.NoError(t, err)

	defer replication.Close()

	err = leader.WalReplicationLeader("test")
	require.NoError(t, err)

	err = follower.WalReplicationFollower()
	require.NoError(t, err)

	err = leader.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)
}

func TestConn_WalReplicationFollower_Undo(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs1, cleanup := newVfsWithName(t, "test1")
	defer cleanup()

	vfs2, cleanup := newVfsWithName(t, "test2")
	defer cleanup()

	leader, cleanup := newConnWithVfs(t, "test.db", vfs1.Name())
	defer cleanup()

	follower, cleanup := newConnWithVfs(t, "test.db", vfs2.Name())
	defer cleanup()

	err := leader.Exec("PRAGMA page_size=512; PRAGMA synchronous=OFF; PRAGMA journal_mode=wal")
	require.NoError(t, err)

	// Lower the cache size.
	err = leader.Exec("PRAGMA cache_size = 1")
	require.NoError(t, err)

	err = follower.Exec("PRAGMA page_size=512; PRAGMA synchronous=OFF; PRAGMA journal_mode=wal")
	require.NoError(t, err)

	methods := &directWalReplication{
		follower: follower,
	}

	replication, err := bindings.NewWalReplication("test", methods)
	require.NoError(t, err)

	defer replication.Close()

	err = leader.WalReplicationLeader("test")
	require.NoError(t, err)

	err = follower.WalReplicationFollower()
	require.NoError(t, err)

	err = leader.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	// Insert a lot of rows to trigger a page cache flush to the WAL.
	err = leader.Exec("BEGIN")
	require.NoError(t, err)

	for i := 0; i < 256; i++ {
		err = leader.Exec("INSERT INTO foo(n) VALUES(1)")
		require.NoError(t, err)
	}

	err = leader.Exec("ROLLBACK")
	require.NoError(t, err)
}

// WalReplication implementation that just keeps track of the count of the
// method calls.
type countWalReplication struct {
	begin  int
	abort  int
	frames int
	undo   int
	end    int
}

func (r *countWalReplication) Begin(*bindings.Conn) int {
	r.begin++
	return 0
}

func (r *countWalReplication) Abort(*bindings.Conn) int {
	r.abort++
	return 0
}

func (r *countWalReplication) Frames(*bindings.Conn, bindings.WalReplicationFrameList) int {
	r.frames++
	return 0
}

func (r *countWalReplication) Undo(*bindings.Conn) int {
	r.undo++
	return 0
}

func (r *countWalReplication) End(*bindings.Conn) int {
	r.end++
	return 0
}

// WalReplication implementation that replicates WAL commands directly to the
// given follower.
type directWalReplication struct {
	follower *bindings.Conn
	writing  bool
}

func (r *directWalReplication) Begin(conn *bindings.Conn) int {
	return 0
}

func (r *directWalReplication) Abort(conn *bindings.Conn) int {
	return 0
}

func (r *directWalReplication) Frames(conn *bindings.Conn, list bindings.WalReplicationFrameList) int {
	begin := false
	if !r.writing {
		begin = true
		r.writing = true
	}

	pageSize := list.PageSize()
	length := list.Len()

	info := bindings.WalReplicationFrameInfo{}
	info.IsBegin(begin)
	info.PageSize(pageSize)
	info.Len(length)
	info.Truncate(list.Truncate())
	info.IsCommit(list.IsCommit())

	numbers := make([]bindings.PageNumber, length)
	pages := make([]byte, length*pageSize)
	for i := range numbers {
		data, pgno, _ := list.Frame(i)
		numbers[i] = pgno
		header := reflect.SliceHeader{Data: uintptr(data), Len: pageSize, Cap: pageSize}
		var slice []byte
		slice = reflect.NewAt(reflect.TypeOf(slice), unsafe.Pointer(&header)).Elem().Interface().([]byte)
		copy(pages[i*pageSize:(i+1)*pageSize], slice)
	}
	info.Pages(numbers, unsafe.Pointer(&pages[0]))

	if err := r.follower.WalReplicationFrames(info); err != nil {
		panic(fmt.Sprintf("frames failed: %v", err))
	}

	if list.IsCommit() {
		r.writing = false
	}
	return 0
}

func (r *directWalReplication) Undo(conn *bindings.Conn) int {
	if r.writing {
		if err := r.follower.WalReplicationUndo(); err != nil {
			panic(fmt.Sprintf("undo failed: %v", err))
		}
	}
	return 0
}

func (r *directWalReplication) End(conn *bindings.Conn) int {
	return 0
}
