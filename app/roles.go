package app

import (
	"sort"

	"github.com/canonical/go-dqlite/client"
)

const minVoters = 3

// RolesConfig can be used to tweak the algorithm implemented by RolesChanges.
type RolesConfig struct {
	Voters   int // Target number of voters, 3 by default.
	StandBys int // Target number of stand-bys, 3 by default.
}

// RolesChanges implements an algorithm to take decisions about which node
// should have which role in a cluster.
//
// You normally don't need to use this data structure since it's already
// transparently wired into the high-level App object. However this is exposed
// for users who don't want to use the high-level App object but still want to
// implement the same roles management algorithm.
type RolesChanges struct {
	// Algorithm configuration.
	Config RolesConfig

	// Current state of the cluster. Each node in the cluster must be
	// present as a key in the map, and its value should be its associated
	// failure domain and weight metadata or nil if the node is currently
	// offline.
	State map[client.NodeInfo]*client.NodeMetadata
}

// Assume decides if a node should assume a different role than the one it
// currently has. It should normally be run at node startup, where the
// algorithm might decide that the node should assume the Voter or Stand-By
// role in case there's a shortage of them.
//
// Return -1 in case no role change is needed.
func (c *RolesChanges) Assume(id uint64) client.NodeRole {
	// If the cluster is still too small, do nothing.
	if c.size() < minVoters {
		return -1
	}

	node := c.get(id)

	// If we are not in the cluster, it means we were removed, just do nothing.
	if node == nil {
		return -1
	}

	// If we already have the Voter or StandBy role, there's nothing to do.
	if node.Role == client.Voter || node.Role == client.StandBy {
		return -1
	}

	onlineVoters := c.list(client.Voter, true)
	onlineStandbys := c.list(client.StandBy, true)

	// If we have already the desired number of online voters and
	// stand-bys, there's nothing to do.
	if len(onlineVoters) >= c.Config.Voters && len(onlineStandbys) >= c.Config.StandBys {
		return -1
	}

	// Figure if we need to become stand-by or voter.
	role := client.StandBy
	if len(onlineVoters) < c.Config.Voters {
		role = client.Voter
	}

	return role
}

// Handover decides if a node should transfer its current role to another
// node. This is typically run when the node is shutting down and is hence going to be offline soon.
//
// Return the role that should be handed over and list of candidates that
// should receive it, in order of preference.
func (c *RolesChanges) Handover(id uint64) (client.NodeRole, []client.NodeInfo) {
	node := c.get(id)

	// If we are not in the cluster, it means we were removed, just do nothing.
	if node == nil {
		return -1, nil
	}

	// If we aren't a voter or a stand-by, there's nothing to do.
	if node.Role != client.Voter && node.Role != client.StandBy {
		return -1, nil
	}

	// Make a list of all online nodes with the same role and get their
	// failure domains.
	peers := c.list(node.Role, true)
	for i := range peers {
		if peers[i].ID == node.ID {
			peers = append(peers[:i], peers[i+1:]...)
			break
		}
	}
	domains := c.failureDomains(peers)

	// Online spare nodes are always candidates.
	candidates := c.list(client.Spare, true)

	// Stand-by nodes are candidates if we need to transfer voting
	// rights, and they are preferred over spares.
	if node.Role == client.Voter {
		candidates = append(c.list(client.StandBy, true), candidates...)
	}

	if len(candidates) == 0 {
		// No online node available to be promoted.
		return -1, nil
	}

	c.sortCandidates(candidates, domains)

	return node.Role, candidates
}

// Adjust decides if there should be changes in the current roles.
//
// Return the role that should be assigned and a list of candidates that should
// assume it, in order of preference.
func (c *RolesChanges) Adjust(leader uint64) (client.NodeRole, []client.NodeInfo) {
	if c.size() == 1 {
		return -1, nil
	}

	// If the cluster is too small, make sure we have just one voter (us).
	if c.size() < minVoters {
		for node := range c.State {
			if node.ID == leader || node.Role != client.Voter {
				continue
			}
			return client.Spare, []client.NodeInfo{node}
		}
		return -1, nil
	}

	onlineVoters := c.list(client.Voter, true)
	onlineStandbys := c.list(client.StandBy, true)
	offlineVoters := c.list(client.Voter, false)
	offlineStandbys := c.list(client.StandBy, false)

	// If we have exactly the desired number of voters and stand-bys, and they are all
	// online, we're good.
	if len(offlineVoters) == 0 && len(onlineVoters) == c.Config.Voters && len(offlineStandbys) == 0 && len(onlineStandbys) == c.Config.StandBys {
		return -1, nil
	}

	// If we have less online voters than desired, let's try to promote
	// some other node.
	if n := len(onlineVoters); n < c.Config.Voters {
		candidates := c.list(client.StandBy, true)
		candidates = append(candidates, c.list(client.Spare, true)...)

		if len(candidates) == 0 {
			return -1, nil
		}

		domains := c.failureDomains(onlineVoters)
		c.sortCandidates(candidates, domains)

		return client.Voter, candidates
	}

	// If we have more online voters than desired, let's demote one of
	// them.
	if n := len(onlineVoters); n > c.Config.Voters {
		nodes := []client.NodeInfo{}
		for _, node := range onlineVoters {
			// Don't demote the leader.
			if node.ID == leader {
				continue
			}
			nodes = append(nodes, node)
		}

		return client.Spare, nodes
	}

	// If we have offline voters, let's demote one of them.
	if n := len(offlineVoters); n > 0 {
		return client.Spare, offlineVoters
	}

	// If we have less online stand-bys than desired, let's try to promote
	// some other node.
	if n := len(onlineStandbys); n < c.Config.StandBys {
		candidates := c.list(client.Spare, true)

		if len(candidates) == 0 {
			return -1, nil
		}

		domains := c.failureDomains(onlineStandbys)
		c.sortCandidates(candidates, domains)

		return client.StandBy, candidates
	}

	// If we have more online stand-bys than desired, let's demote one of
	// them.
	if n := len(onlineStandbys); n > c.Config.StandBys {
		nodes := []client.NodeInfo{}
		for _, node := range onlineStandbys {
			// Don't demote the leader.
			if node.ID == leader {
				continue
			}
			nodes = append(nodes, node)
		}

		return client.Spare, nodes
	}

	// If we have offline stand-bys, let's demote one of them.
	if n := len(offlineStandbys); n > 0 {
		return client.Spare, offlineStandbys
	}

	return -1, nil
}

// Return the number of nodes il the cluster.
func (c *RolesChanges) size() int {
	return len(c.State)
}

// Return information about the node with the given ID, or nil if no node
// matches.
func (c *RolesChanges) get(id uint64) *client.NodeInfo {
	for node := range c.State {
		if node.ID == id {
			return &node
		}
	}
	return nil
}

// Return the online or offline nodes with the given role.
func (c *RolesChanges) list(role client.NodeRole, online bool) []client.NodeInfo {
	nodes := []client.NodeInfo{}
	for node, metadata := range c.State {
		if node.Role == role && metadata != nil == online {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Return the number of online or offline nodes with the given role.
func (c *RolesChanges) count(role client.NodeRole, online bool) int {
	return len(c.list(role, online))
}

// Return a map of the failure domains associated with the
// given nodes.
func (c *RolesChanges) failureDomains(nodes []client.NodeInfo) map[uint64]bool {
	domains := map[uint64]bool{}
	for _, node := range nodes {
		metadata := c.State[node]
		if metadata == nil {
			continue
		}
		domains[metadata.FailureDomain] = true
	}
	return domains
}

// Sort the given candidates according to their failure domain and
// weight. Candidates belonging to a failure domain different from the given
// domains take precedence.
func (c *RolesChanges) sortCandidates(candidates []client.NodeInfo, domains map[uint64]bool) {
	less := func(i, j int) bool {
		metadata1 := c.metadata(candidates[i])
		metadata2 := c.metadata(candidates[j])

		// If i's failure domain is not in the given list, but j's is,
		// then i takes precedence.
		if !domains[metadata1.FailureDomain] && domains[metadata2.FailureDomain] {
			return true
		}

		// If j's failure domain is not in the given list, but i's is,
		// then j takes precedence.
		if !domains[metadata2.FailureDomain] && domains[metadata1.FailureDomain] {
			return false
		}

		return metadata1.Weight < metadata2.Weight
	}

	sort.Slice(candidates, less)
}

// Return the metadata of the given node, if any.
func (c *RolesChanges) metadata(node client.NodeInfo) *client.NodeMetadata {
	return c.State[node]
}
