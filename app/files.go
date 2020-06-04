package app

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ghodss/yaml"
)

const (
	// Store the node ID and address.
	infoFile = "info.yaml"

	// The node store file.
	storeFile = "cluster.yaml"

	// This is a "flag" file to signal when a brand new node needs to join
	// the cluster. In case the node doesn't successfully make it to join
	// the cluster first time it's started, it will re-try the next time.
	joinFile = "join"
)

// Return true if the given file exists in the given directory.
func fileExists(dir, file string) (bool, error) {
	path := filepath.Join(dir, file)

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("check if %s exists: %w", file, err)
		}
		return false, nil
	}

	return true, nil
}

// Write a file in the given directory.
func fileWrite(dir, file string, data []byte) error {
	path := filepath.Join(dir, file)

	if err := ioutil.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write %s: %w", file, err)
	}

	return nil
}

// Marshal the given object as YAML into the given file.
func fileMarshal(dir, file string, object interface{}) error {
	data, err := yaml.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshall %s: %w", file, err)
	}
	if err := fileWrite(dir, file, data); err != nil {
		return err
	}
	return nil
}

// Unmarshal the given YAML file into the given object.
func fileUnmarshal(dir, file string, object interface{}) error {
	path := filepath.Join(dir, file)

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", file, err)
	}
	if err := yaml.Unmarshal(data, object); err != nil {
		return fmt.Errorf("unmarshall %s: %w", file, err)
	}

	return nil
}

// Remove a file in the given directory.
func fileRemove(dir, file string) error {
	return os.Remove(filepath.Join(dir, file))
}
