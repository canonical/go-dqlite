package shell

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
)

// Shell can be used to implement interactive prompts for inspecting a dqlite
// database.
type Shell struct {
	store client.NodeStore
	dial  client.DialFunc
	db    *sql.DB
}

// New creates a new Shell connected to the given database.
func New(database string, store client.NodeStore, options ...Option) (*Shell, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
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
		store: store,
		dial:  o.Dial,
		db:    db,
	}

	return shell, nil
}

// Process a single input line.
func (s *Shell) Process(ctx context.Context, line string) (string, error) {
	if strings.HasPrefix(line, "SELECT") {
		return s.processSelect(ctx, line)
	} else {
		return "", s.processExec(ctx, line)
	}
}

func (s *Shell) processSelect(ctx context.Context, line string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}

	rows, err := tx.Query(line)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
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
			return "", err
		}

		for i, column := range row {
			s := fmt.Sprintf("%v", column)
			if i == 0 {
				result = s
			} else {
				result += "|" + s
			}

		}
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", err
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
