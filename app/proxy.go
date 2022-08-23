package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Copies data between a remote TCP network connection (possibly with TLS) and
// a local unix socket.
//
// The function will return if one of the following events occurs:
//
// - the other end of the remote network socket closes the connection
// - the other end of the local unix socket closes the connection
// - the context is cancelled
// - an error occurs when writing or reading data
//
// In case of errors, details are returned.
func proxy(ctx context.Context, remote net.Conn, local net.Conn, config *tls.Config) error {
	tcp, err := extractTCPConn(remote)
	if err != nil {
		return err
	}

	if err := setKeepalive(tcp); err != nil {
		return err
	}

	if config != nil {
		if config.ClientCAs != nil {
			remote = tls.Server(remote, config)
		} else {
			remote = tls.Client(remote, config)
		}
	}

	remoteToLocal := make(chan error, 0)
	localToRemote := make(chan error, 0)

	// Start copying data back and forth until either the client or the
	// server get closed or hit an error.
	go func() {
		_, err := io.Copy(local, remote)
		remoteToLocal <- err
	}()

	go func() {
		_, err := io.Copy(remote, local)
		localToRemote <- err
	}()

	errs := make([]error, 2)

	select {
	case <-ctx.Done():
		// Force closing, ignore errors.
		remote.Close()
		local.Close()
		<-remoteToLocal
		<-localToRemote
	case err := <-remoteToLocal:
		if err != nil {
			errs[0] = fmt.Errorf("remote -> local: %v", err)
		}
		local.(*net.UnixConn).CloseRead()
		if err := <-localToRemote; err != nil {
			errs[1] = fmt.Errorf("local -> remote: %v", err)
		}
		remote.Close()
		local.Close()
	case err := <-localToRemote:
		if err != nil {
			errs[0] = fmt.Errorf("local -> remote: %v", err)
		}
		tcp.CloseRead()
		if err := <-remoteToLocal; err != nil {
			errs[1] = fmt.Errorf("remote -> local: %v", err)
		}
		remote.Close()
		local.Close()

	}

	if errs[0] != nil || errs[1] != nil {
		return proxyError{first: errs[0], second: errs[1]}
	}

	return nil
}

// extractTCPConn tries to extract the underlying net.TCPConn, potentially from a tls.Conn.
func extractTCPConn(conn net.Conn) (*net.TCPConn, error) {
	tcp, ok := conn.(*net.TCPConn)
	if ok {
		return tcp, nil
	}

	// Go doesn't currently expose the underlying TCP connection of a TLS connection, but we need it in order
	// to set timeout properties on the connection. We use some reflect/unsafe magic to extract the private
	// remote.conn field, which is indeed the underlying TCP connection.
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return nil, fmt.Errorf("connection is not a tls.Conn")
	}

	field := reflect.ValueOf(tlsConn).Elem().FieldByName("conn")
	field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	c := field.Interface()

	tcpConn, ok := c.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("connection is not a net.TCPConn")
	}

	return tcpConn, nil
}

// Set TCP_USER_TIMEOUT and TCP keepalive with 3 seconds idle time, 3 seconds
// retry interval with at most 3 retries.
//
// See https://thenotexpert.com/golang-tcp-keepalive/.
func setKeepalive(conn *net.TCPConn) error {
	err := conn.SetKeepAlive(true)
	if err != nil {
		return err
	}

	err = conn.SetKeepAlivePeriod(time.Second * 3)
	if err != nil {
		return err
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	raw.Control(
		func(ptr uintptr) {
			fd := int(ptr)
			// Number of probes.
			err = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, _TCP_KEEPCNT, 3)
			if err != nil {
				return
			}
			// Wait time after an unsuccessful probe.
			err = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, _TCP_KEEPINTVL, 3)
			if err != nil {
				return
			}

			// Set TCP_USER_TIMEOUT option to limit the maximum amount of time in ms that transmitted data may remain
			// unacknowledged before TCP will forcefully close the corresponding connection and return ETIMEDOUT to the
			// application. This combined with the TCP keepalive options on the socket will ensure that should the
			// remote side of the connection disappear abruptly that dqlite will detect this and close the socket quickly.
			// Decreasing the user timeouts allows applications to "fail fast" if so desired. Otherwise it may take
			// up to 20 minutes with the current system defaults in a normal WAN environment if there are packets in
			// the send queue that will prevent the keepalive timer from working as the retransmission timers kick in.
			// See https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=dca43c75e7e545694a9dd6288553f55c53e2a3a3
			err = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(30*time.Microsecond))
			if err != nil {
				return
			}
		})
	return err
}

// Returns a pair of connected unix sockets.
func socketpair() (net.Conn, net.Conn, error) {
	fds, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}

	c1, err := fdToFileConn(fds[0])
	if err != nil {
		return nil, nil, err
	}

	c2, err := fdToFileConn(fds[1])
	if err != nil {
		c1.Close()
		return nil, nil, err
	}

	return c1, c2, err
}

func fdToFileConn(fd int) (net.Conn, error) {
	f := os.NewFile(uintptr(fd), "")
	defer f.Close()
	return net.FileConn(f)
}

type proxyError struct {
	first  error
	second error
}

func (e proxyError) Error() string {
	msg := ""
	if e.first != nil {
		msg += "first: " + e.first.Error()
	}
	if e.second != nil {
		if e.first != nil {
			msg += " "
		}
		msg += "second: " + e.second.Error()
	}
	return msg
}
