// +build darwin

package app

// from netinet/tcp.h (OS X 10.9.4)
const (
	_TCP_KEEPINTVL = 0x101 /* interval between keepalives */
	_TCP_KEEPCNT   = 0x102 /* number of keepalives before close */
)
