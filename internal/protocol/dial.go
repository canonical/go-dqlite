package protocol

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
)

// Dial function handling plain TCP and Unix socket endpoints.
func Dial(ctx context.Context, address string) (net.Conn, error) {
	family := "tcp"
	if strings.HasPrefix(address, "@") {
		family = "unix"
	}
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, family, address)
}

// TLSCipherSuites are the cipher suites by the go-dqlite TLS helpers.
var TLSCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
}
