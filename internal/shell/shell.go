package shell

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
)

// Shell can be used to implement interactive prompts for inspecting a dqlite
// database.
type Shell struct {
	store  client.NodeStore
	dial   client.DialFunc
	db     *sql.DB
	format string
}

// New creates a new Shell connected to the given database.
func New(database string, store client.NodeStore, options ...Option) (*Shell, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}

	switch o.Format {
	case formatTabular:
	case formatJson:
	default:
		return nil, fmt.Errorf("unknown format %s", o.Format)
	}

	driver, err := driver.New(store, driver.WithDialFunc(o.Dial))
	if err != nil {
		return nil, err
	}
	sql.Register(o.DriverName, driver)

	db, err := sql.Open(o.DriverName, database)
	if err != nil {
		return nil, err
	}

	shell := &Shell{
		store:  store,
		dial:   o.Dial,
		db:     db,
		format: o.Format,
	}

	return shell, nil
}

// Process a single input line.
func (s *Shell) Process(ctx context.Context, line string) (string, error) {
	switch line {
	case ".cluster":
		return s.processCluster(ctx, line)
	case ".leader":
		return s.processLeader(ctx, line)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimLeft(line, " ")), ".remove") {
		return s.processRemove(ctx, line)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimLeft(line, " ")), ".describe") {
		return s.processDescribe(ctx, line)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimLeft(line, " ")), ".weight") {
		return s.processWeight(ctx, line)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimLeft(line, " ")), ".dump") {
		return s.processDump(ctx, line)
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimLeft(line, " ")), ".reconfigure") {
		return s.processReconfigure(ctx, line)
	}
	if strings.HasPrefix(strings.ToUpper(strings.TrimLeft(line, " ")), "SELECT") {
		return s.processSelect(ctx, line)
	} else {
		return "", s.processExec(ctx, line)
	}
}

func (s *Shell) processCluster(ctx context.Context, line string) (string, error) {
	cli, err := client.FindLeader(ctx, s.store, client.WithDialFunc(s.dial))
	if err != nil {
		return "", err
	}
	cluster, err := cli.Cluster(ctx)
	if err != nil {
		return "", err
	}
	result := ""
	switch s.format {
	case formatTabular:
		for i, server := range cluster {
			if i > 0 {
				result += "\n"
			}
			result += fmt.Sprintf("%x|%s|%s", server.ID, server.Address, server.Role)
		}
	case formatJson:
		data, err := json.Marshal(cluster)
		if err != nil {
			return "", err
		}
		var indented bytes.Buffer
		json.Indent(&indented, data, "", "\t")
		result = string(indented.Bytes())
	}

	return result, nil
}

func (s *Shell) processLeader(ctx context.Context, line string) (string, error) {
	cli, err := client.FindLeader(ctx, s.store, client.WithDialFunc(s.dial))
	if err != nil {
		return "", err
	}
	leader, err := cli.Leader(ctx)
	if err != nil {
		return "", err
	}
	if leader == nil {
		return "", nil
	}
	return leader.Address, nil
}

func (s *Shell) processRemove(ctx context.Context, line string) (string, error) {
	parts := strings.Split(line, " ")
	if len(parts) != 2 {
		return "", fmt.Errorf("bad command format, should be: .remove <address>")
	}
	address := parts[1]
	cli, err := client.FindLeader(ctx, s.store, client.WithDialFunc(s.dial))
	if err != nil {
		return "", err
	}
	cluster, err := cli.Cluster(ctx)
	if err != nil {
		return "", err
	}
	for _, node := range cluster {
		if node.Address != address {
			continue
		}
		if err := cli.Remove(ctx, node.ID); err != nil {
			return "", fmt.Errorf("remove node %q: %w", address, err)
		}
		return "", nil
	}

	return "", fmt.Errorf("no node has address %q", address)
}

func (s *Shell) processDescribe(ctx context.Context, line string) (string, error) {
	parts := strings.Split(line, " ")
	if len(parts) != 2 {
		return "", fmt.Errorf("bad command format, should be: .describe <address>")
	}
	address := parts[1]
	cli, err := client.New(ctx, address, client.WithDialFunc(s.dial))
	if err != nil {
		return "", err
	}
	metadata, err := cli.Describe(ctx)
	if err != nil {
		return "", err
	}

	result := ""
	switch s.format {
	case formatTabular:
		result += fmt.Sprintf("%s|%d|%d", address, metadata.FailureDomain, metadata.Weight)
	case formatJson:
		data, err := json.Marshal(metadata)
		if err != nil {
			return "", err
		}
		var indented bytes.Buffer
		json.Indent(&indented, data, "", "\t")
		result = string(indented.Bytes())
	}

	return result, nil
}

func (s *Shell) processDump(ctx context.Context, line string) (string, error) {
	parts := strings.Split(line, " ")
	if len(parts) < 2 || len(parts) > 3 {
		return "NOK", fmt.Errorf("bad command format, should be: .dump <address> [<database>]")
	}
	address := parts[1]
	cli, err := client.New(ctx, address, client.WithDialFunc(s.dial))
	if err != nil {
		return "NOK", fmt.Errorf("dial failed")
	}

	database := "db.bin"
	if len(parts) == 3 {
		database = parts[2]
	}
	files, err := cli.Dump(ctx, database)
	if err != nil {
		return "NOK", fmt.Errorf("dump failed")
	}

	dir, err := os.Getwd()
	if err != nil {
		return "NOK", fmt.Errorf("os.Getwd() failed")
	}

	for _, file := range files {
		path := filepath.Join(dir, file.Name)
		err := ioutil.WriteFile(path, file.Data, 0600)
		if err != nil {
			return "NOK", fmt.Errorf("WriteFile failed on path %s", path)
		}
	}

	return "OK", nil
}

func (s *Shell) processReconfigure(ctx context.Context, line string) (string, error) {
	parts := strings.Split(line, " ")
	if len(parts) != 3 {
		return "NOK", fmt.Errorf("bad command format, should be: .reconfigure <dir> <clusteryaml>\n" +
			"Args:\n" +
			"\tdir - Directory of node with up to date data\n" +
			"\tclusteryaml - Path to a .yaml file containing the desired cluster configuration\n\n" +
			"Help:\n" +
			"\tUse this command when trying to preserve the data from your cluster while changing the\n" +
			"\tconfiguration of the cluster because e.g. your cluster is broken due to unreachablee nodes.\n" +
			"\t0. BACKUP ALL YOUR NODE DATA DIRECTORIES BEFORE PROCEEDING!\n" +
			"\t1. Stop all dqlite nodes.\n" +
			"\t2. Identify the dir of the node with the most up to date raft term and log, this will be the <dir> argument.\n" +
			"\t3. Create a .yaml file with the same format as cluster.yaml (or use/adapt an existing cluster.yaml) with the\n " +
			"\t   desired cluster configuration. This will be the <clusteryaml> argument.\n" +
			"\t   Don't forget to make sure the ID's in the file line up with the ID's in the info.yaml files.\n" +
			"\t4. Run the .reconfigure <dir> <clusteryaml> command, it should return \"OK\".\n" +
			"\t5. Copy the snapshot-xxx-xxx-xxx, snapshot-xxx-xxx-xxx.meta, segment files (00000xxxxx-000000xxxxx), desired cluster.yaml\n" +
			"\t   from <dir> over to the directories of the other nodes identified in <clusteryaml>, deleting any leftover snapshot-xxx-xxx-xxx, snapshot-xxx-xxx-xxx.meta,\n" +
			"\t   segment (00000xxxxx-000000xxxxx, open-xxx) and metadata{1,2} files that it contains.\n" +
			"\t   Make sure an info.yaml is also present that is in line with cluster.yaml.\n" +
			"\t6. Start all the dqlite nodes.\n" +
			"\t7. If, for some reason, this fails or gives undesired results, try again with data from another node (you should still have this from step 0).\n")
	}
	dir := parts[1]
	clusteryamlpath := parts[2]

	store, err := client.NewYamlNodeStore(clusteryamlpath)
	if err != nil {
		return "NOK", fmt.Errorf("failed to create YamlNodeStore from file at %s :%v", clusteryamlpath, err)
	}

	servers, err := store.Get(ctx)
	if err != nil {
		return "NOK", fmt.Errorf("failed to retrieve NodeInfo list :%v", err)
	}

	err = dqlite.ReconfigureMembershipExt(dir, servers)
	if err != nil {
		return "NOK", fmt.Errorf("failed to reconfigure membership :%v", err)
	}

	return "OK", nil
}

func (s *Shell) processWeight(ctx context.Context, line string) (string, error) {
	parts := strings.Split(line, " ")
	if len(parts) != 3 {
		return "", fmt.Errorf("bad command format, should be: .weight <address> <n>")
	}
	address := parts[1]
	weight, err := strconv.Atoi(parts[2])
	if err != nil || weight < 0 {
		return "", fmt.Errorf("bad weight %q", parts[2])
	}

	cli, err := client.New(ctx, address, client.WithDialFunc(s.dial))
	if err != nil {
		return "", err
	}
	if err := cli.Weight(ctx, uint64(weight)); err != nil {
		return "", err
	}

	return "", nil
}

func (s *Shell) processSelect(ctx context.Context, line string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}

	rows, err := tx.Query(line)
	if err != nil {
		err = fmt.Errorf("query: %w", err)
		if rbErr := tx.Rollback(); rbErr != nil {
			return "", fmt.Errorf("unable to rollback: %v", err)
		}
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		err = fmt.Errorf("columns: %w", err)
		if rbErr := tx.Rollback(); rbErr != nil {
			return "", fmt.Errorf("unable to rollback: %v", err)
		}
		return "", err
	}
	n := len(columns)

	var sb strings.Builder
	for rows.Next() {
		row := make([]interface{}, n)
		rowPointers := make([]interface{}, n)
		for i := range row {
			rowPointers[i] = &row[i]
		}

		if err := rows.Scan(rowPointers...); err != nil {
			err = fmt.Errorf("scan: %w", err)
			if rbErr := tx.Rollback(); rbErr != nil {
				return "", fmt.Errorf("unable to rollback: %v", err)
			}
			return "", err
		}

		for i, column := range row {
			if i == 0 {
				fmt.Fprintf(&sb, "%v", column)
			} else {
				fmt.Fprintf(&sb, "|%v", column)
			}
		}
		sb.WriteByte('\n')
	}

	if err := rows.Err(); err != nil {
		err = fmt.Errorf("rows: %w", err)
		if rbErr := tx.Rollback(); rbErr != nil {
			return "", fmt.Errorf("unable to rollback: %v", err)
		}
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}

	return strings.TrimRight(sb.String(), "\n"), nil
}

func (s *Shell) processExec(ctx context.Context, line string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(line); err != nil {
		err = fmt.Errorf("exec: %w", err)
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("unable to rollback: %v", err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
