package client

import (
	"github.com/canonical/go-dqlite/internal/protocol"
)

// Node roles
const (
	StandBy = protocol.StandBy
	Voter   = protocol.Voter
	Spare   = protocol.Spare
)
