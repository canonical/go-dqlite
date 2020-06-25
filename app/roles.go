package app

import (
	"github.com/canonical/go-dqlite/client"
)

// Map each node to its metadata. If the node is offline, map to nil.
type clusterState map[client.NodeInfo]*client.NodeMetadata

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

const minVoters = 3
