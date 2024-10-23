package client

import (
	"github.com/canonical/go-dqlite/v3/internal/protocol"
)

// Node roles
const (
	Voter   = protocol.Voter
	StandBy = protocol.StandBy
	Spare   = protocol.Spare
)
