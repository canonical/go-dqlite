package app

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

// RecoveryKernel is a bundle of data that supports recovery of an App-managed dqlite cluster.
//
// When using App, the procedure to recover a cluster is as follows:
//
// 0. Make sure all nodes are stopped.
// 1. Call ReadLastEntryInfo on each node to determine which has the most up-to-date log. This is the "template node".
// 2. On the template node, create an empty recovery kernel.
// 3. On the template node, call PrepareRecovery to get a RecoveryKernel.
// 4. Transfer the RecoveryKernel to each remaining node. This can be done, for example, by gob-encoding it.
// 5. On each remaining node, call Propagate.
// 6. Restart the cluster.
type RecoveryKernel struct {
	// Opaque contains data that is used during recovery but should not otherwise be inspected.
	Opaque []byte
	// Cluster is the list of nodes that should be in the cluster after recovery. It should not be mutated directly.
	Cluster []dqlite.NodeInfo
}

// The files we recover are:
//
// - metadata1 and metadata2
// - closed segments like 0000000000000001-0000000000000002
// - open segments like open-3
// - snapshots like snapshot-1-75776-57638163 and their accompanying .meta files
var belongsInKernelPattern = regexp.MustCompile(`\A(metadata[1-2]|[0-9]{16}-[0-9]{16}|open-[0-9]+|snapshot-[0-9]+-[0-9]+-[0-9]+(.meta)?)\z`)

func PrepareRecovery(dir string, address string, cluster []dqlite.NodeInfo) (*RecoveryKernel, error) {
	var me *dqlite.NodeInfo
	for i := range cluster {
		node := cluster[i]
		if node.ID == 0 {
			return nil, fmt.Errorf("node ID may not be zero")
		} else if node.Address == address {
			me = &node
		}
	}
	if me == nil {
		return nil, fmt.Errorf("address %q not in provided cluster list %v", address, cluster)
	}

	if err := dqlite.ReconfigureMembershipExt(dir, cluster); err != nil {
		return nil, errors.Wrapf(err, "reconfigure membership")
	}
	store, err := client.NewYamlNodeStore(filepath.Join(dir, storeFile))
	if err != nil {
		return nil, errors.Wrapf(err, "create node store")
	}
	if err := store.Set(context.Background(), cluster); err != nil {
		return nil, errors.Wrapf(err, "write node store")
	}
	if err := fileMarshal(dir, infoFile, me); err != nil {
		return nil, errors.Wrapf(err, "write info file")
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "read %q", dir)
	}
	var tarball bytes.Buffer
	tw := tar.NewWriter(&tarball)
	defer tw.Close()
	for _, file := range files {
		if file.IsDir() || !belongsInKernelPattern.MatchString(file.Name()) {
			continue
		}
		path := filepath.Join(dir, file.Name())
		hdr, err := tar.FileInfoHeader(file, "")
		if err != nil {
			return nil, errors.Wrapf(err, "file info header for %q", path)
		}
		if err = tw.WriteHeader(hdr); err != nil {
			return nil, errors.Wrapf(err, "write header for %q", path)
		}
		src, err := os.Open(path)
		if err != nil {
			return nil, errors.Wrapf(err, "open %q", path)
		}
		if _, err := io.Copy(tw, src); err != nil {
			return nil, errors.Wrapf(err, "read %q", path)
		}
	}
	if err = tw.Flush(); err != nil {
		return nil, errors.Wrapf(err, "finish tarball for %q", dir)
	}
	return &RecoveryKernel{tarball.Bytes(), cluster}, nil
}

func (kern *RecoveryKernel) Propagate(dir string, address string) error {
	var me *dqlite.NodeInfo
	for _, node := range kern.Cluster {
		if node.Address == address {
			me = &node
			break
		}
	}
	if me == nil {
		return fmt.Errorf("address %q not in provided cluster list %v", address, kern.Cluster)
	}

	info, err := os.Stat(dir)
	if err != nil {
		return errors.Wrapf(err, "stat %q", dir)
	}
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrapf(err, "remove %q", dir)
	}
	if err := os.Mkdir(dir, info.Mode()); err != nil {
		return errors.Wrapf(err, "create %q", dir)
	}

	tr := tar.NewReader(bytes.NewBuffer(kern.Opaque))
	var hdr *tar.Header
	for hdr, err = tr.Next(); err == nil; hdr, err = tr.Next() {
		path := filepath.Join(dir, hdr.Name)
		dest, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, os.FileMode(hdr.Mode))
		if err != nil {
			return errors.Wrapf(err, "create %q", path)
		}
		if _, err := io.Copy(dest, tr); err != nil {
			return errors.Wrapf(err, "write %q", path)
		}
	}
	if err != io.EOF {
		return errors.Wrapf(err, "read tarball for %q", dir)
	}

	store, err := client.NewYamlNodeStore(filepath.Join(dir, storeFile))
	if err != nil {
		return errors.Wrapf(err, "create node store")
	}
	if err := store.Set(context.Background(), kern.Cluster); err != nil {
		return errors.Wrapf(err, "write node store")
	}
	if err := fileMarshal(dir, infoFile, me); err != nil {
		return errors.Wrapf(err, "write info file")
	}

	return nil
}
