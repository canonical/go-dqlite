// +build nosqlite3

package client

import (
	"strings"

	"github.com/pkg/errors"
)

// DefaultNodeStore creates a new NodeStore using the given filename.
//
// The filename must end with ".yaml".
func DefaultNodeStore(filename string) (NodeStore, error) {
	if strings.HasSuffix(filename, ".yaml") {
		return NewYamlNodeStore(filename)
	}

	return nil, errors.New("built without support for DatabaseNodeStore")
}
