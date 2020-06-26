package app

import (
	"sort"

	"github.com/canonical/go-dqlite/client"
)

// Map each node to its metadata. If the node is offline, map to nil.
type clusterState map[client.NodeInfo]*client.NodeMetadata

// Size returns the number of nodes il the cluster.
func (c clusterState) Size() int {
	return len(c)
}

// Count returns the number of online or offline nodes with the given role.
func (c clusterState) Count(role client.NodeRole, online bool) int {
	return len(c.List(role, online))
}

// List returns the online or offline nodes with the given role.
func (c clusterState) List(role client.NodeRole, online bool) []client.NodeInfo {
	nodes := []client.NodeInfo{}
	for node, metadata := range c {
		if node.Role == role && metadata != nil == online {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Get returns information about the node with the given ID, or nil if no node
// matches.
func (c clusterState) Get(id uint64) *client.NodeInfo {
	for node := range c {
		if node.ID == id {
			return &node
		}
	}
	return nil
}

// Metadata returns the metadata of the given node, if any.
func (c clusterState) Metadata(node client.NodeInfo) *client.NodeMetadata {
	return c[node]
}

// FailureDomains returns a map of the failure domains associated with the
// given nodes.
func (c clusterState) FailureDomains(nodes []client.NodeInfo) map[uint64]bool {
	domains := map[uint64]bool{}
	for _, node := range nodes {
		metadata := c[node]
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
func (c clusterState) SortCandidates(candidates []client.NodeInfo, domains map[uint64]bool) {
	less := func(i, j int) bool {
		metadata1 := c.Metadata(candidates[i])
		metadata2 := c.Metadata(candidates[j])

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

const minVoters = 3

type rolesAdjustmentAlgorithm struct {
	voters   int // Target number of voters, 3 by default.
	standbys int // Target number of stand-bys, 3 by default.
}

// Decide if a node should change its own role at startup.
func (a *rolesAdjustmentAlgorithm) Startup(id uint64, cluster clusterState) client.NodeRole {
	// If the cluster is still too small, do nothing.
	if cluster.Size() < minVoters {
		return -1
	}

	node := cluster.Get(id)

	// If we are not in the cluster, it means we were removed, just do nothing.
	if node == nil {
		return -1
	}

	// If we already have the Voter or StandBy role, there's nothing to do.
	if node.Role == client.Voter || node.Role == client.StandBy {
		return -1
	}

	onlineVoters := cluster.List(client.Voter, true)
	onlineStandbys := cluster.List(client.StandBy, true)

	// If we have already the desired number of online voters and
	// stand-bys, there's nothing to do.
	if len(onlineVoters) >= a.voters && len(onlineStandbys) >= a.standbys {
		return -1
	}

	// Figure if we need to become stand-by or voter.
	role := client.StandBy
	if len(onlineVoters) < a.voters {
		role = client.Voter
	}

	return role
}

// Decide if a node should try to handover its current role to another
// node. Return the role that should be handed over and list of candidates that
// should receive it, in order of preference.
func (a *rolesAdjustmentAlgorithm) Handover(id uint64, cluster clusterState) (client.NodeRole, []client.NodeInfo) {
	node := cluster.Get(id)
	// If we are not in the cluster, it means we were removed, just do nothing.
	if node == nil {
		return -1, nil
	}

	// If we aren't a voter or a stand-by, there's nothing to do.
	if node.Role != client.Voter && node.Role != client.StandBy {
		return -1, nil
	}

	// Online spare nodes are always candidates.
	candidates := cluster.List(client.Spare, true)

	// Stand-by nodes are candidates if we need to transfer voting
	// rights, and they are preferred over spares.
	if node.Role == client.Voter {
		candidates = append(cluster.List(client.StandBy, true), candidates...)
	}

	if len(candidates) == 0 {
		// No online node available to be promoted.
		return -1, nil
	}

	return node.Role, candidates
}

// Decide if there should be changes in the current roles. Return the role that
// should be assigned and a list of candidates that should assume it, in order
// of preference.
func (a *rolesAdjustmentAlgorithm) Adjust(leader uint64, cluster clusterState) (client.NodeRole, []client.NodeInfo) {
	if cluster.Size() == 1 {
		return -1, nil
	}

	// If the cluster is too small, make sure we have just one voter (us).
	if cluster.Size() < minVoters {
		for node := range cluster {
			if node.ID == leader || node.Role != client.Voter {
				continue
			}
			return client.Spare, []client.NodeInfo{node}
		}
		return -1, nil
	}

	onlineVoters := cluster.List(client.Voter, true)
	onlineStandbys := cluster.List(client.StandBy, true)
	offlineVoters := cluster.List(client.Voter, false)
	offlineStandbys := cluster.List(client.StandBy, false)

	// If we have exactly the desired number of voters and stand-bys, and they are all
	// online, we're good.
	if len(offlineVoters) == 0 && len(onlineVoters) == a.voters && len(offlineStandbys) == 0 && len(onlineStandbys) == a.standbys {
		return -1, nil
	}

	// If we have less online voters than desired, let's try to promote
	// some other node.
	if n := len(onlineVoters); n < a.voters {
		candidates := cluster.List(client.StandBy, true)
		candidates = append(candidates, cluster.List(client.Spare, true)...)

		if len(candidates) == 0 {
			return -1, nil
		}

		domains := cluster.FailureDomains(onlineVoters)
		cluster.SortCandidates(candidates, domains)

		return client.Voter, candidates
	}

	// If we have more online voters than desired, let's demote one of
	// them.
	if n := len(onlineVoters); n > a.voters {
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
	if n := len(onlineStandbys); n < a.standbys {
		candidates := cluster.List(client.Spare, true)

		if len(candidates) == 0 {
			return -1, nil
		}

		domains := cluster.FailureDomains(onlineStandbys)
		cluster.SortCandidates(candidates, domains)

		return client.StandBy, candidates
	}

	// If we have more online stand-bys than desired, let's demote one of
	// them.
	if n := len(onlineStandbys); n > a.standbys {
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
