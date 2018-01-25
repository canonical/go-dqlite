package transaction_test

import (
	"fmt"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
)

func TestRegistry_AddLeader(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, "1", nil)

	if txn.ID() == "" {
		t.Error("no ID assigned to transaction")
	}
	if txn.Conn() != conn {
		t.Error("transaction associated with wrong connection")
	}
	if !txn.IsLeader() {
		t.Error("transaction reported wrong replication mode")
	}
}

func TestRegistry_AddLeaderPanicsIfPassedSameLeaderConnectionTwice(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, "1", nil)

	want := fmt.Sprintf("a transaction for this connection is already registered with ID %s", txn.ID())
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.AddLeader(conn, "2", nil)
}

func TestRegistry_AddFollower(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, "abcd")

	if txn.ID() != "abcd" {
		t.Errorf("expected transaction ID abcd, got %s", txn.ID())
	}
	if txn.Conn() != conn {
		t.Error("transaction associated with wrong connection")
	}
	if txn.IsLeader() {
		t.Error("transaction reported wrong replication mode")
	}
}

func TestRegistry_GetByID(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, "0", nil)
	if registry.GetByID(txn.ID()) != txn {
		t.Error("transactions instances don't match")
	}
}

func TestRegistry_GetByIDNotFound(t *testing.T) {
	registry := newRegistry()
	if registry.GetByID("abcd") != nil {
		t.Error("expected no transaction instance for non-existing ID")
	}
}

func TestRegistry_GetByConn(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, "0", nil)
	if registry.GetByConn(conn) != txn {
		t.Error("transactions instances don't match")
	}
}

func TestRegistry_GetByConnFound(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	if registry.GetByConn(conn) != nil {
		t.Error("expected no transaction instance for non-registered conn")
	}
}

func TestRegistry_Remove(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, "0", nil)

	registry.Remove(txn.ID())
	if registry.GetByID(txn.ID()) != nil {
		t.Error("expected no transaction instance for unregistered ID")
	}
}

func TestRegistry_RemovePanicsIfPassedNonRegisteredID(t *testing.T) {
	registry := transaction.NewRegistry()
	const want = "attempt to remove unregistered transaction abcd"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.Remove("abcd")
}

// Create a new registry with the replication mode check disabled.
func newRegistry() *transaction.Registry {
	registry := transaction.NewRegistry()
	registry.SkipCheckReplicationMode(true)
	return registry
}