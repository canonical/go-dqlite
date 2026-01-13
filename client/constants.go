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

// RoleMask identifies which roles a node is allowed to hold.
type RoleMask uint8

const (
	RoleMaskVoter RoleMask = 1 << iota
	RoleMaskStandBy
	RoleMaskSpare

	RoleMaskAll = RoleMaskVoter | RoleMaskStandBy | RoleMaskSpare
)

// RoleMaskFor returns the mask bit for the given role.
func RoleMaskFor(role NodeRole) RoleMask {
	switch role {
	case Voter:
		return RoleMaskVoter
	case StandBy:
		return RoleMaskStandBy
	case Spare:
		return RoleMaskSpare
	default:
		return 0
	}
}
