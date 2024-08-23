package protocol_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/canonical/go-dqlite/logging"
)

// func TestProtocol_Heartbeat(t *testing.T) {
// 	c, cleanup := NewProtocol(t)
// 	defer cleanup()

// 	request, response := newMessagePair(512, 512)

// 	protocol.EncodeHeartbeat(&request, uint64(time.Now().Unix()))

// 	makeCall(t, c, &request, &response)

// 	servers, err := protocol.DecodeNodes(&response)
// 	require.NoError(t, err)

// 	assert.Len(t, servers, 2)
// 	assert.Equal(t, client.Nodes{
// 		{ID: uint64(1), Address: "1.2.3.4:666"},
// 		{ID: uint64(2), Address: "5.6.7.8:666"}},
// 		servers)
// }

// Test sending a request that needs to be written into the dynamic buffer.
func TestProtocol_RequestWithDynamicBuffer(t *testing.T) {
	p, cleanup := newProtocol(t)
	defer cleanup()

	request, response := newMessagePair(64, 64)

	protocol.EncodeOpen(&request, "test.db", 0, "test-0")

	makeCall(t, p, &request, &response)

	id, err := protocol.DecodeDb(&response)
	require.NoError(t, err)

	sql := `
CREATE TABLE foo (n INT);
CREATE TABLE bar (n INT);
CREATE TABLE egg (n INT);
CREATE TABLE baz (n INT);
`
	protocol.EncodeExecSQLV0(&request, uint64(id), sql, nil)

	makeCall(t, p, &request, &response)
}

func TestProtocol_Prepare(t *testing.T) {
	c, cleanup := newProtocol(t)
	defer cleanup()

	request, response := newMessagePair(64, 64)

	protocol.EncodeOpen(&request, "test.db", 0, "test-0")

	makeCall(t, c, &request, &response)

	db, err := protocol.DecodeDb(&response)
	require.NoError(t, err)

	protocol.EncodePrepare(&request, uint64(db), "CREATE TABLE test (n INT)")

	makeCall(t, c, &request, &response)

	_, stmt, params, err := protocol.DecodeStmt(&response)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), stmt)
	assert.Equal(t, uint64(0), params)
}

type testProtocolContext struct {
	summary   string
	f         func(protocol *protocol.Protocol, ctx context.Context, request, response *protocol.Message) error
	hangRead  bool
	hangWrite bool
	cancel    time.Duration
	timeout   time.Duration
	err       string
}

var testsProtocolContext = []testProtocolContext{{
	summary:  "Call timeout read",
	f:        (*protocol.Protocol).Call,
	hangRead: true,
	timeout:  time.Millisecond * 200,
	err:      `call leader \(budget .*ms\): receive: header: read pipe: i/o timeout`,
}, {
	summary:   "Call timeout write",
	f:         (*protocol.Protocol).Call,
	hangWrite: true,
	timeout:   time.Millisecond * 200,
	err:       `call leader \(budget .*ms\): send: header: write pipe: i/o timeout`,
}, {
	summary:  "Call cancel read",
	f:        (*protocol.Protocol).Call,
	hangRead: true,
	cancel:   time.Millisecond * 200,
	err:      `call leader \(canceled\): receive: header: read pipe: i/o timeout`,
}, {
	summary:   "Call cancel write",
	f:         (*protocol.Protocol).Call,
	hangWrite: true,
	cancel:    time.Millisecond * 200,
	err:       `call leader \(canceled\): send: header: write pipe: i/o timeout`,
}, {
	summary:  "Interrupt timeout read",
	f:        (*protocol.Protocol).Interrupt,
	hangRead: true,
	timeout:  time.Millisecond * 200,
	err:      `interrupt request \(budget .*ms\): receive: header: read pipe: i/o timeout`,
}, {
	summary:   "Interrupt timeout write",
	f:         (*protocol.Protocol).Interrupt,
	hangWrite: true,
	timeout:   time.Millisecond * 200,
	err:       `interrupt request \(budget .*ms\): send: header: write pipe: i/o timeout`,
}, {
	summary:  "Interrupt cancel read",
	f:        (*protocol.Protocol).Interrupt,
	hangRead: true,
	cancel:   time.Millisecond * 200,
	err:      `interrupt request \(canceled\): receive: header: read pipe: i/o timeout`,
}, {
	summary:   "Interrupt cancel write",
	f:         (*protocol.Protocol).Interrupt,
	hangWrite: true,
	cancel:    time.Millisecond * 200,
	err:       `interrupt request \(canceled\): send: header: write pipe: i/o timeout`,
}}

func runTestProtocolContext(t *testing.T, test testProtocolContext) {
	// Setup client and sever.
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()
	// Kill goroutines when test is finished.
	goroutineCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// If set, the sever will write continuously to the connection.
	if !test.hangRead {
		go func() {
			for {
				select {
				case <-goroutineCtx.Done():
					return
				default:
					server.Write([]byte{1, 2, 3})
				}
			}
		}()
	}
	// If set, the sever will read continuously from the connection.
	if !test.hangWrite {
		go func() {
			b := make([]byte, 10)
			for {
				select {
				case <-goroutineCtx.Done():
					return
				default:
					server.Read(b)
				}
			}
		}()
	}

	p := protocol.NewProtocol(0, client)
	assert.NotNil(t, p)

	request := &protocol.Message{}
	request.Init(8)
	request.PutBlob([]byte{1, 2, 3, 4, 5})
	response := &protocol.Message{}
	response.Init(8) // TODO: turn this into a proper constructor.

	// Setup the context based on the test parameters.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if test.timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, test.timeout)
		defer cancel()
	}
	if test.cancel != 0 {
		go func() {
			time.Sleep(test.cancel)
			cancel()
		}()
	}

	err := test.f(p, ctx, request, response)
	if test.err != "" {
		assert.Regexp(t, test.err, err.Error())
		return
	}
	assert.NoError(t, err)
}

func TestProtocol_Context(t *testing.T) {
	for _, test := range testsProtocolContext {
		runTestProtocolContext(t, test)
	}
}

/*
func TestProtocol_Exec(t *testing.T) {
	client, cleanup := NewProtocol(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := client.Open(ctx, "test.db", "volatile")
	require.NoError(t, err)

	stmt, err := client.Prepare(ctx, db.ID, "CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	require.NoError(t, err)
}

func TestProtocol_Query(t *testing.T) {
	client, cleanup := NewProtocol(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := client.Open(ctx, "test.db", "volatile")
	require.NoError(t, err)

	start := time.Now()

	stmt, err := client.Prepare(ctx, db.ID, "CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	stmt, err = client.Prepare(ctx, db.ID, "INSERT INTO test VALUES(1)")
	require.NoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	stmt, err = client.Prepare(ctx, db.ID, "SELECT n FROM test")
	require.NoError(t, err)

	_, err = client.Query(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	fmt.Printf("time %s\n", time.Since(start))
}
*/

func newProtocol(t *testing.T) (*protocol.Protocol, func()) {
	t.Helper()

	address, serverCleanup := newNode(t, 0)

	store := newStore(t, []string{address})
	config := protocol.Config{
		AttemptTimeout: 100 * time.Millisecond,
		BackoffFactor:  time.Millisecond,
	}
	connector := protocol.NewConnector(0, store, config, logging.Test(t))

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)

	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		serverCleanup()
	}

	return client, cleanup
}

// Perform a client call.
func makeCall(t *testing.T, p *protocol.Protocol, request, response *protocol.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := p.Call(ctx, request, response)
	require.NoError(t, err)
}

// Return a new message pair to be used as request and response.
func newMessagePair(size1, size2 int) (protocol.Message, protocol.Message) {
	message1 := protocol.Message{}
	message1.Init(size1)

	message2 := protocol.Message{}
	message2.Init(size2)

	return message1, message2
}
