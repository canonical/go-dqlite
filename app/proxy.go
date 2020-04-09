package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

// Copies data between a client network connection and a local unix server
// socket.
//
// The function will return if one of the following events occurs:
//
// - the remote end of the client network socket closes the connection
// - the remote end of the server unix socket closes the connection
// - the context is cancelled
// - an error occurs when writing or reading data
//
// In case of errors, details are returned.
func proxy(ctx context.Context, client net.Conn, server net.Conn) error {
	if err := setKeepalive(client.(*net.TCPConn)); err != nil {
		return err
	}

	serverToClient := make(chan error, 0)
	clientToServer := make(chan error, 0)

	// Start copying data back and forth until either the client or the
	// server get closed or hit an error.
	go func() {
		_, err := io.Copy(server, client)
		clientToServer <- err
	}()

	go func() {
		_, err := io.Copy(client, server)
		serverToClient <- err
	}()

	errs := make([]error, 2)

	select {
	case <-ctx.Done():
		// Force closing, ignore errors.
		client.Close()
		server.Close()
		<-clientToServer
		<-serverToClient
	case err := <-clientToServer:
		if err != nil {
			errs[0] = fmt.Errorf("client -> server: %w", err)
		}
		server.(*net.UnixConn).CloseRead()
		if err := <-serverToClient; err != nil {
			errs[1] = fmt.Errorf("server -> client: %w", err)
		}
		client.Close()
		server.Close()
	case err := <-serverToClient:
		if err != nil {
			errs[0] = fmt.Errorf("client -> server: %w", err)
		}
		client.(*net.TCPConn).CloseRead()
		if err := <-clientToServer; err != nil {
			errs[1] = fmt.Errorf("client -> server: %w", err)
		}
		server.Close()

	}

	if errs[0] != nil || errs[1] != nil {
		return proxyError{first: errs[0], second: errs[0]}
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
