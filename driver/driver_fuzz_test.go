//go:build go1.18
// +build go1.18

package driver_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite/v3"
	"github.com/canonical/go-dqlite/v3/client"
	dqlitedriver "github.com/canonical/go-dqlite/v3/driver"
	"github.com/stretchr/testify/require"
)

const maxFuzzTrivia = 1024

func FuzzConnExec(f *testing.F) {
	addSQLFuzzCorpus(f)
	conn := newFuzzConn(f)
	execer := conn.(driver.ExecerContext)
	mustFuzzExec(f, execer, "CREATE TABLE fuzz_exec (value TEXT)", nil)

	f.Fuzz(func(t *testing.T, prefix, between, suffix, value []byte) {
		limitFuzzInput(t, prefix, between, suffix)
		query := fuzzTrivia(prefix) +
			"UPDATE fuzz_exec SET value = ? WHERE 0;" + fuzzTrivia(between) +
			"UPDATE fuzz_exec SET value = ? WHERE 0;" + fuzzTrivia(suffix)
		args := []driver.NamedValue{
			{Ordinal: 1, Value: value},
			{Ordinal: 2, Value: value},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := execer.ExecContext(ctx, query, args)
		require.NoError(t, err)
		rows, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})
}

func FuzzConnQuery(f *testing.F) {
	addSQLFuzzCorpus(f)
	conn := newFuzzConn(f)
	queryer := conn.(driver.QueryerContext)

	f.Fuzz(func(t *testing.T, prefix, between, suffix, value []byte) {
		limitFuzzInput(t, prefix, between, suffix)
		// between is placed inside a quoted result to exercise arbitrary bytes
		// without allowing fuzz input to become executable SQL.
		marker := fmt.Sprintf("%x", between)
		query := fuzzTrivia(prefix) + "SELECT ?, '" + marker + ";--/*';" + fuzzTrivia(suffix)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := queryer.QueryContext(ctx, query, []driver.NamedValue{{Ordinal: 1, Value: value}})
		require.NoError(t, err)
		defer rows.Close()

		values := make([]driver.Value, 2)
		require.NoError(t, rows.Next(values))
		require.Equal(t, value, values[0])
		require.Equal(t, marker+";--/*", values[1])
		require.ErrorIs(t, rows.Next(values), io.EOF)
	})
}

func FuzzConnPrepare(f *testing.F) {
	addSQLFuzzCorpus(f)
	conn := newFuzzConn(f)
	execer := conn.(driver.ExecerContext)
	mustFuzzExec(f, execer, "CREATE TABLE fuzz_prepare (value TEXT)", nil)

	f.Fuzz(func(t *testing.T, prefix, between, suffix, value []byte) {
		limitFuzzInput(t, prefix, between, suffix)
		query := fuzzTrivia(prefix) +
			"UPDATE fuzz_prepare SET value = ? WHERE 0;" + fuzzTrivia(between) +
			"UPDATE fuzz_prepare SET value = ? WHERE 0;" + fuzzTrivia(suffix)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stmt, err := conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
		require.NoError(t, err)
		defer stmt.Close()
		require.Equal(t, 2, stmt.NumInput())

		_, err = stmt.(driver.StmtExecContext).ExecContext(ctx, []driver.NamedValue{
			{Ordinal: 1, Value: value},
			{Ordinal: 2, Value: value},
		})
		require.NoError(t, err)
	})
}

func addSQLFuzzCorpus(f *testing.F) {
	f.Helper()
	f.Add([]byte{}, []byte{}, []byte{}, []byte("value"))
	f.Add([]byte{0, 1, 2}, []byte{3, 4}, []byte{5, 6, 7}, []byte("semi;colon"))
	f.Add([]byte{7, 7, 3}, []byte{4, 2, 3}, []byte{1, 0}, []byte("-- /* */"))
	f.Add([]byte{2, 2, 2}, []byte{5, 5, 5}, []byte{3}, []byte{0, 0xff, 0xfe})
}

func fuzzTrivia(input []byte) string {
	var query strings.Builder
	query.Grow(len(input) * 8)
	for _, b := range input {
		switch b % 8 {
		case 0:
			query.WriteByte(' ')
		case 1:
			query.WriteString("\t\r\n\f")
		case 2:
			query.WriteByte(';')
		case 3:
			query.WriteString("-- ; /* line */\n")
		case 4:
			query.WriteString("/* ; -- block */")
		case 5:
			query.WriteString("/**/")
		case 6:
			query.WriteString("-- carriage return\r\n")
		case 7:
			query.WriteString(";\n/* adjacent */-- comment\n")
		}
	}
	return query.String()
}

func limitFuzzInput(t *testing.T, inputs ...[]byte) {
	t.Helper()
	for _, input := range inputs {
		if len(input) > maxFuzzTrivia {
			t.Skip()
		}
	}
}

func newFuzzConn(f *testing.F) driver.Conn {
	f.Helper()
	address := fmt.Sprintf("@go-dqlite-%d-%s", os.Getpid(), strings.ToLower(f.Name()))
	node, err := dqlite.New(uint64(1), address, f.TempDir(), dqlite.WithBindAddress(address))
	require.NoError(f, err)
	require.NoError(f, node.Start())
	f.Cleanup(func() { require.NoError(f, node.Close()) })

	store := client.NewInmemNodeStore()
	require.NoError(f, store.Set(context.Background(), []client.NodeInfo{{ID: 1, Address: address}}))
	drv, err := dqlitedriver.New(store)
	require.NoError(f, err)
	conn, err := drv.Open("fuzz.db")
	require.NoError(f, err)
	f.Cleanup(func() { require.NoError(f, conn.Close()) })
	return conn
}

func mustFuzzExec(f *testing.F, execer driver.ExecerContext, query string, args []driver.NamedValue) {
	f.Helper()
	_, err := execer.ExecContext(context.Background(), query, args)
	require.NoError(f, err)
}
