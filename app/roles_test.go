package app

import (
	"testing"

	"github.com/canonical/go-dqlite/v3/client"
)

func roleMask(mask client.RoleMask) *client.RoleMask {
	return &mask
}

func TestAdjustDemotesDisallowedVoter(t *testing.T) {
	node1 := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.Voter}
	node4 := client.NodeInfo{ID: 4, Address: "4", Role: client.Spare}

	state := map[client.NodeInfo]*client.NodeMetadata{
		node1: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskStandBy | client.RoleMaskSpare)},
		node2: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node3: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node4: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 3, StandBys: 1},
		State:  state,
	}

	role, candidates := changes.Adjust(2)
	if role != client.StandBy {
		t.Fatalf("expected standby demotion, got %v", role)
	}
	if len(candidates) != 1 || candidates[0].ID != node1.ID {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}
}

func TestAdjustDemotesDisallowedStandby(t *testing.T) {
	node1 := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.StandBy}

	state := map[client.NodeInfo]*client.NodeMetadata{
		node1: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node3: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskSpare)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 2, StandBys: 1},
		State:  state,
	}

	role, candidates := changes.Adjust(1)
	if role != client.Spare {
		t.Fatalf("expected spare demotion, got %v", role)
	}
	if len(candidates) != 1 || candidates[0].ID != node3.ID {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}
}

func TestAdjustFiltersPromotionByAllowedRoles(t *testing.T) {
	node1 := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.Spare}

	state := map[client.NodeInfo]*client.NodeMetadata{
		node1: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node3: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskSpare)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 3, StandBys: 0},
		State:  state,
	}

	role, candidates := changes.Adjust(1)
	if role != -1 || len(candidates) != 0 {
		t.Fatalf("expected no promotion candidates, got role=%v candidates=%#v", role, candidates)
	}
}

func TestAdjustSkipsLeaderEligibility(t *testing.T) {
	leader := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.Voter}

	state := map[client.NodeInfo]*client.NodeMetadata{
		leader: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskStandBy | client.RoleMaskSpare)},
		node2:  {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node3:  {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 3, StandBys: 0},
		State:  state,
	}

	role, candidates := changes.Adjust(leader.ID)
	if role != -1 || len(candidates) != 0 {
		t.Fatalf("expected no change for leader with disallowed voter role, got role=%v candidates=%#v", role, candidates)
	}
}

func TestAdjustDemotesVoterToSpareWhenClusterTooSmall(t *testing.T) {
	leader := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}

	state := map[client.NodeInfo]*client.NodeMetadata{
		leader: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2:  {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskVoter | client.RoleMaskStandBy)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 1, StandBys: 0},
		State:  state,
	}

	role, candidates := changes.Adjust(leader.ID)
	if role != client.Spare {
		t.Fatalf("expected spare demotion, got %v", role)
	}
	if len(candidates) != 1 || candidates[0].ID != node2.ID {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}
}

func TestAdjustSkipsStandbyDemotionWhenSpareDisallowed(t *testing.T) {
	leader := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.StandBy}

	state := map[client.NodeInfo]*client.NodeMetadata{
		leader: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2:  {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskStandBy)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 1, StandBys: 0},
		State:  state,
	}

	role, candidates := changes.Adjust(leader.ID)
	if role != -1 || len(candidates) != 0 {
		t.Fatalf("expected no demotion candidates, got role=%v candidates=%#v", role, candidates)
	}
}

func TestAssumeFallsBackToStandbyWhenVoterDisallowed(t *testing.T) {
	node1 := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.Spare}

	state := map[client.NodeInfo]*client.NodeMetadata{
		node1: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node3: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskStandBy | client.RoleMaskSpare)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 3, StandBys: 1},
		State:  state,
	}

	role := changes.Assume(node3.ID)
	if role != client.StandBy {
		t.Fatalf("expected standby fallback, got %v", role)
	}
}

func TestAssumeReturnsNoChangeWhenAllPromotionsDisallowed(t *testing.T) {
	node1 := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.Voter}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.Spare}

	state := map[client.NodeInfo]*client.NodeMetadata{
		node1: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node3: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskSpare)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 3, StandBys: 1},
		State:  state,
	}

	role := changes.Assume(node3.ID)
	if role != -1 {
		t.Fatalf("expected no role change, got %v", role)
	}
}

func TestHandoverFiltersCandidatesByAllowedRoles(t *testing.T) {
	node1 := client.NodeInfo{ID: 1, Address: "1", Role: client.Voter}
	node2 := client.NodeInfo{ID: 2, Address: "2", Role: client.StandBy}
	node3 := client.NodeInfo{ID: 3, Address: "3", Role: client.Spare}

	state := map[client.NodeInfo]*client.NodeMetadata{
		node1: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskAll)},
		node2: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskVoter | client.RoleMaskStandBy)},
		node3: {FailureDomain: 0, AllowedRoles: roleMask(client.RoleMaskSpare)},
	}

	changes := RolesChanges{
		Config: RolesConfig{Voters: 3, StandBys: 1},
		State:  state,
	}

	role, candidates := changes.Handover(node1.ID)
	if role != client.Voter {
		t.Fatalf("expected voter handover, got %v", role)
	}
	if len(candidates) != 1 || candidates[0].ID != node2.ID {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}
}
