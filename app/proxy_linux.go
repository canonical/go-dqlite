// +build linux

package app

import (
	"syscall"
)

const (
	_TCP_KEEPINTVL = syscall.TCP_KEEPINTVL /* interval between keepalives */
	_TCP_KEEPCNT   = syscall.TCP_KEEPCNT   /* number of keepalives before close */
)
