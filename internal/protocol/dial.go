package protocol

import (
	"context"
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
