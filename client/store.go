package client

import (
	"context"
	"io/ioutil"
	"os"
	"sync"

	"github.com/google/renameio"
	"gopkg.in/yaml.v2"

	"github.com/canonical/go-dqlite/internal/protocol"
)

// NodeStore is used by a dqlite client to get an initial list of candidate
// dqlite nodes that it can dial in order to find a leader dqlite node to use.
type NodeStore = protocol.NodeStore

// NodeRole identifies the role of a node.
type NodeRole = protocol.NodeRole

// NodeInfo holds information about a single server.
type NodeInfo = protocol.NodeInfo

// InmemNodeStore keeps the list of target dqlite nodes in memory.
type InmemNodeStore = protocol.InmemNodeStore

// NewInmemNodeStore creates NodeStore which stores its data in-memory.
var NewInmemNodeStore = protocol.NewInmemNodeStore

// Persists a list addresses of dqlite nodes in a YAML file.
type YamlNodeStore struct {
	path    string
	servers []NodeInfo
	mu      sync.RWMutex
}

// NewYamlNodeStore creates a new YamlNodeStore backed by the given YAML file.
func NewYamlNodeStore(path string) (*YamlNodeStore, error) {
	servers := []NodeInfo{}

	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}

		if err := yaml.Unmarshal(data, &servers); err != nil {
			return nil, err
		}
	}

	store := &YamlNodeStore{
		path:    path,
		servers: servers,
	}

	return store, nil
}

// Get the current servers.
func (s *YamlNodeStore) Get(ctx context.Context) ([]NodeInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ret := make([]NodeInfo, len(s.servers))
	copy(ret, s.servers)
	return ret, nil
}

// Set the servers addresses.
func (s *YamlNodeStore) Set(ctx context.Context, servers []NodeInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := yaml.Marshal(servers)
	if err != nil {
		return err
	}

	if err := renameio.WriteFile(s.path, data, 0600); err != nil {
		return err
	}

	s.servers = servers

	return nil
}
