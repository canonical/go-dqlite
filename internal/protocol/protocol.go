package protocol

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Protocol sends and receive the dqlite message on the wire.
type Protocol struct {
	version uint64     // Protocol version
	conn    net.Conn   // Underlying network connection.
	mu      sync.Mutex // Serialize requests
	netErr  error      // A network error occurred
	addr    string
	lt      *LeaderTracker
}

// Call invokes a dqlite RPC, sending a request message and receiving a
// response message.
func (p *Protocol) Call(ctx context.Context, request, response *Message) (err error) {
	// We need to take a lock since the dqlite server currently does not
	// support concurrent requests.
	p.mu.Lock()
	defer p.mu.Unlock()

	if err = p.netErr; err != nil {
		return
	}

	defer func() {
		if err == nil {
			return
		}
		p.Bad()
		switch errors.Cause(err).(type) {
		case *net.OpError:
			p.netErr = err
		}
	}()

	var budget time.Duration

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		budget = time.Until(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	desc := requestDesc(request.mtype)

	if err = p.send(request); err != nil {
		return errors.Wrapf(err, "call %s (budget %s): send", desc, budget)
	}

	if err = p.recv(response); err != nil {
		return errors.Wrapf(err, "call %s (budget %s): receive", desc, budget)
	}

	return
}

// More is used when a request maps to multiple responses.
func (p *Protocol) More(ctx context.Context, response *Message) (err error) {
	if err = p.recv(response); err != nil {
		p.Bad()
	}
	return
}

// Interrupt sends an interrupt request and awaits for the server's empty
// response.
func (p *Protocol) Interrupt(ctx context.Context, request *Message, response *Message) (err error) {
	// We need to take a lock since the dqlite server currently does not
	// support concurrent requests.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(deadline)
		defer p.conn.SetDeadline(time.Time{})
	}

	EncodeInterrupt(request, 0)

	defer func() {
		if err != nil {
			p.Bad()
		}
	}()

	if err = p.send(request); err != nil {
		return errors.Wrap(err, "failed to send interrupt request")
	}

	for {
		if err = p.recv(response); err != nil {
			return errors.Wrap(err, "failed to receive response")
		}

		mtype, _ := response.getHeader()

		if mtype == ResponseEmpty {
			break
		}
	}

	return nil
}

// Bad prevents a protocol from being reused when it is released.
//
// There is no need to call Bad after a method of Protocol returns an error.
// Only call Bad when the protocol is deemed unsuitable for reuse for some
// higher-level reason.
func (p *Protocol) Bad() {
	p.lt = nil
}

// Close releases a protocol.
//
// If the protocol was associated with a LeaderTracker, it will be made
// available for reuse by that tracker. Otherwise, the underlying connection
// will be closed.
func (p *Protocol) Close() error {
	if tr := p.lt; tr == nil || !tr.DonateSharedProtocol(p) {
		return p.conn.Close()
	}
	return nil
}

func (p *Protocol) send(req *Message) error {
	if err := p.sendHeader(req); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := p.sendBody(req); err != nil {
		return errors.Wrap(err, "body")
	}

	return nil
}

func (p *Protocol) sendHeader(req *Message) error {
	n, err := p.conn.Write(req.header[:])
	if err != nil {
		return err
	}

	if n != messageHeaderSize {
		return io.ErrShortWrite
	}

	return nil
}

func (p *Protocol) sendBody(req *Message) error {
	buf := req.body.Bytes[:req.body.Offset]
	n, err := p.conn.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func (p *Protocol) recv(res *Message) error {
	res.reset()

	if err := p.recvHeader(res); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := p.recvBody(res); err != nil {
		return errors.Wrap(err, "body")
	}

	return nil
}

func (p *Protocol) recvHeader(res *Message) error {
	if err := p.recvPeek(res.header); err != nil {
		return err
	}

	res.words = binary.LittleEndian.Uint32(res.header[0:])
	res.mtype = res.header[4]
	res.schema = res.header[5]
	res.extra = binary.LittleEndian.Uint16(res.header[6:])

	return nil
}

func (p *Protocol) recvBody(res *Message) error {
	n := int(res.words) * messageWordSize

	for n > len(res.body.Bytes) {
		// Grow message buffer.
		bytes := make([]byte, len(res.body.Bytes)*2)
		res.body.Bytes = bytes
	}

	buf := res.body.Bytes[:n]

	if err := p.recvPeek(buf); err != nil {
		return err
	}

	return nil
}

// Read until buf is full.
func (p *Protocol) recvPeek(buf []byte) error {
	for offset := 0; offset < len(buf); {
		n, err := p.recvFill(buf[offset:])
		if err != nil {
			return err
		}
		offset += n
	}

	return nil
}

// Try to fill buf, but perform at most one read.
func (p *Protocol) recvFill(buf []byte) (int, error) {
	// Read new data: try a limited number of times.
	//
	// This technique is copied from bufio.Reader.
	for i := messageMaxConsecutiveEmptyReads; i > 0; i-- {
		n, err := p.conn.Read(buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if err != nil {
			return -1, err
		}
		if n > 0 {
			return n, nil
		}
	}
	return -1, io.ErrNoProgress
}

// DecodeNodeCompat handles also pre-1.0 legacy server messages.
func DecodeNodeCompat(protocol *Protocol, response *Message) (uint64, string, error) {
	if protocol.version == VersionLegacy {
		address, err := DecodeNodeLegacy(response)
		if err != nil {
			return 0, "", err
		}
		return 0, address, nil

	}
	return DecodeNode(response)
}
