package shell

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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

func (s *Shell) processSelect(ctx context.Context, line string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}

	rows, err := tx.Query(line)
	if err != nil {
		return "", fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("columns: %w", err)
	}
	n := len(columns)

	result := ""
	for rows.Next() {
		row := make([]interface{}, n)
		rowPointers := make([]interface{}, n)
		for i := range row {
			rowPointers[i] = &row[i]
		}

		if err := rows.Scan(rowPointers...); err != nil {
			return "", fmt.Errorf("scan: %w", err)
		}

		for i, column := range row {
			s := fmt.Sprintf("%v", column)
			if i == 0 {
				result += s
			} else {
				result += "|" + s
			}

		}
		result += "\n"
	}
	result = strings.TrimRight(result, "\n")

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}

	return result, nil
}

func (s *Shell) processExec(ctx context.Context, line string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(line); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
