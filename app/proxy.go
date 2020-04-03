package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"
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
	tcp := remote.(*net.TCPConn)

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
		local.Close()

	}

	if errs[0] != nil || errs[1] != nil {
		return proxyError{first: errs[0], second: errs[1]}
	}

	return nil
}

// Set TCP keepalive with 30 seconds idle time, 3 seconds retry interval with
// at most 3 retries.
//
// See https://thenotexpert.com/golang-tcp-keepalive/.
func setKeepalive(conn *net.TCPConn) error {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Second * 30)

	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	raw.Control(
		func(ptr uintptr) {
			fd := int(ptr)
			// Number of probes.
			err = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)
			if err != nil {
				return
			}
			// Wait time after an unsuccessful probe.
			err = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3)
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
