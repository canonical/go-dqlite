// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import "testing"

func TestStmtCache_TryGet(t *testing.T) {
	cache := NewStmtCache(10)

	// Create mock statements
	beginStmt := &Stmt{}
	createStmt := &Stmt{}

	// Populate cache
	cache.Put("BEGIN", beginStmt)
	cache.Put("CREATE TABLE asd(i)", createStmt)

	tests := []struct {
		name     string
		query    string
		wantStmt *Stmt
		wantLen  int
	}{
		{
			name:     "whole string match with spaces",
			query:    "  BEGIN  ",
			wantStmt: beginStmt,
			wantLen:  9,
		},
		{
			name:     "no match with comment",
			query:    "BEGIN -- a comment",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "match with semicolon",
			query:    "BEGIN;",
			wantStmt: beginStmt,
			wantLen:  6,
		},
		{
			name:     "match with spaces and semicolon",
			query:    "BEGIN    ;",
			wantStmt: beginStmt,
			wantLen:  10,
		},
		{
			name:     "no match with extra text",
			query:    "BEGIN TRANSACTION",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "match with semicolon and following statement",
			query:    " BEGIN ; CREATE TABLE asd(i)",
			wantStmt: beginStmt,
			wantLen:  8,
		},
		{
			name:     "create table with semicolon",
			query:    "CREATE TABLE asd(i);",
			wantStmt: createStmt,
			wantLen:  20,
		},
		{
			name:     "no match with different spacing",
			query:    "CREATE TABLE asd( I);",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "no match with extra keyword",
			query:    "CREATE TABLE asd(i)      STRICT",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "match with semicolon and comment",
			query:    "CREATE TABLE asd(i)   ; -- a comment\n ERROR SYNTAX",
			wantStmt: createStmt,
			wantLen:  23,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStmt, gotLen := cache.TryGet(tt.query)
			if gotStmt != tt.wantStmt {
				t.Errorf("TryGet() gotStmt = %v, want %v", gotStmt, tt.wantStmt)
			}
			if gotLen != tt.wantLen {
				t.Errorf("TryGet() gotLen = %v, want %v", gotLen, tt.wantLen)
			}
		})
	}
}

func TestStmtCache_LRU(t *testing.T) {
	cache := NewStmtCache(2)

	stmt1 := &Stmt{}
	stmt2 := &Stmt{}
	stmt3 := &Stmt{}

	cache.Put("QUERY1", stmt1)
	cache.Put("QUERY2", stmt2)

	// Cache should have QUERY1 and QUERY2
	if s, l := cache.TryGet("QUERY1"); s != stmt1 || l != 6 {
		t.Errorf("Expected QUERY1 to be in cache")
	}

	// Add QUERY3, should evict QUERY2 (LRU, since QUERY1 was just accessed)
	cache.Put("QUERY3", stmt3)

	// QUERY1 should still be in cache (recently accessed)
	if s, l := cache.TryGet("QUERY1"); s != stmt1 || l != 6 {
		t.Errorf("Expected QUERY1 to still be in cache")
	}

	// QUERY2 should be evicted
	if s, l := cache.TryGet("QUERY2"); s != nil || l != 0 {
		t.Errorf("Expected QUERY2 to be evicted from cache")
	}

	// QUERY3 should be in cache
	if s, l := cache.TryGet("QUERY3"); s != stmt3 || l != 6 {
		t.Errorf("Expected QUERY3 to be in cache")
	}
}

func TestStmtCache_EdgeCases(t *testing.T) {
	cache := NewStmtCache(20)

	// Prepare various strange SQLite statements
	stmts := make(map[string]*Stmt)
	queries := []string{
		"SELECT 1",
		"SELECT * FROM sqlite_master",
		"PRAGMA foreign_keys",
		"PRAGMA foreign_keys=ON",
		"WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte",
		"INSERT INTO t VALUES(1,2,3)",
		"UPDATE t SET x=1 WHERE y=2",
		"DELETE FROM t WHERE x=1",
		"DROP TABLE IF EXISTS t",
		"ALTER TABLE t ADD COLUMN x INTEGER",
		"CREATE INDEX idx ON t(x,y,z)",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx ON t(x)",
		"CREATE TEMPORARY TABLE temp(x)",
		"CREATE VIEW v AS SELECT 1",
		"ATTACH DATABASE ':memory:' AS aux",
		"DETACH DATABASE aux",
		"VACUUM",
		"ANALYZE",
		"REINDEX",
		"EXPLAIN QUERY PLAN SELECT * FROM t",
	}

	for _, q := range queries {
		stmt := &Stmt{}
		stmts[q] = stmt
		cache.Put(q, stmt)
	}

	tests := []struct {
		name     string
		query    string
		wantStmt *Stmt
		wantLen  int
	}{
		// Empty and whitespace queries
		{
			name:     "empty query",
			query:    "",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "only spaces",
			query:    "   ",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "only tabs and newlines",
			query:    "\t\n\r",
			wantStmt: nil,
			wantLen:  0,
		},

		// Semicolon variations
		{
			name:     "multiple semicolons",
			query:    "SELECT 1;;",
			wantStmt: stmts["SELECT 1"],
			wantLen:  9,
		},
		{
			name:     "semicolon with mixed whitespace",
			query:    "SELECT 1 \t\n ;",
			wantStmt: stmts["SELECT 1"],
			wantLen:  13,
		},
		{
			name:     "semicolon after newline",
			query:    "SELECT 1\n;",
			wantStmt: stmts["SELECT 1"],
			wantLen:  10,
		},

		// Query with additional content after semicolon
		{
			name:     "query with garbage after semicolon",
			query:    "SELECT 1; this is not valid sql",
			wantStmt: stmts["SELECT 1"],
			wantLen:  9,
		},
		{
			name:     "query with another complete statement",
			query:    "VACUUM; SELECT 1",
			wantStmt: stmts["VACUUM"],
			wantLen:  7,
		},

		// PRAGMA variations
		{
			name:     "pragma with assignment no match",
			query:    "PRAGMA foreign_keys = ON",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "pragma exact match",
			query:    "PRAGMA foreign_keys",
			wantStmt: stmts["PRAGMA foreign_keys"],
			wantLen:  19,
		},
		{
			name:     "pragma with semicolon",
			query:    "PRAGMA foreign_keys;",
			wantStmt: stmts["PRAGMA foreign_keys"],
			wantLen:  20,
		},
		{
			name:     "pragma with equals exact match",
			query:    "PRAGMA foreign_keys=ON",
			wantStmt: stmts["PRAGMA foreign_keys=ON"],
			wantLen:  22,
		},

		// Complex queries with lots of whitespace
		{
			name:     "with cte exact",
			query:    "WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte",
			wantStmt: stmts["WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte"],
			wantLen:  93,
		},
		{
			name:     "with cte with leading space",
			query:    "  WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte  ",
			wantStmt: stmts["WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte"],
			wantLen:  97,
		},
		{
			name:     "with cte with semicolon",
			query:    "WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte;",
			wantStmt: stmts["WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<5) SELECT * FROM cte"],
			wantLen:  94,
		},

		// Partial matches that should fail
		{
			name:     "partial select no semicolon",
			query:    "SELECT 1 FROM t",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "insert with extra values",
			query:    "INSERT INTO t VALUES(1,2,3,4)",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "update with different condition",
			query:    "UPDATE t SET x=1 WHERE y=3",
			wantStmt: nil,
			wantLen:  0,
		},

		// Special characters and escaping
		{
			name:     "attach with quoted path",
			query:    "ATTACH DATABASE ':memory:' AS aux",
			wantStmt: stmts["ATTACH DATABASE ':memory:' AS aux"],
			wantLen:  33,
		},
		{
			name:     "attach with trailing space and semicolon",
			query:    "ATTACH DATABASE ':memory:' AS aux  ;",
			wantStmt: stmts["ATTACH DATABASE ':memory:' AS aux"],
			wantLen:  36,
		},

		// Case sensitivity (SQLite keywords are case-insensitive, but we do exact match)
		{
			name:     "vacuum lowercase no match",
			query:    "vacuum",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "VACUUM uppercase match",
			query:    "VACUUM",
			wantStmt: stmts["VACUUM"],
			wantLen:  6,
		},

		// Very long whitespace sequences
		{
			name:     "select with many spaces before semicolon",
			query:    "SELECT 1                    ;",
			wantStmt: stmts["SELECT 1"],
			wantLen:  29,
		},

		// Statements with embedded semicolons (in strings) - should not match
		// Note: Our cache won't have these, so they should return no match
		{
			name:     "query with semicolon in comment",
			query:    "SELECT 1 -- comment with ; semicolon",
			wantStmt: nil,
			wantLen:  0,
		},

		// Multiple statement scenarios
		{
			name:     "two cached statements in sequence",
			query:    "VACUUM; ANALYZE",
			wantStmt: stmts["VACUUM"],
			wantLen:  7,
		},
		{
			name:     "analyze followed by vacuum",
			query:    "ANALYZE; VACUUM",
			wantStmt: stmts["ANALYZE"],
			wantLen:  8,
		},

		// Explain variations
		{
			name:     "explain exact match",
			query:    "EXPLAIN QUERY PLAN SELECT * FROM t",
			wantStmt: stmts["EXPLAIN QUERY PLAN SELECT * FROM t"],
			wantLen:  34,
		},
		{
			name:     "explain no match with different query",
			query:    "EXPLAIN QUERY PLAN SELECT 1",
			wantStmt: nil,
			wantLen:  0,
		},

		// CREATE variations with IF NOT EXISTS
		{
			name:     "create unique index exact",
			query:    "CREATE UNIQUE INDEX IF NOT EXISTS idx ON t(x)",
			wantStmt: stmts["CREATE UNIQUE INDEX IF NOT EXISTS idx ON t(x)"],
			wantLen:  45,
		},
		{
			name:     "create index no match",
			query:    "CREATE INDEX IF NOT EXISTS idx ON t(x)",
			wantStmt: nil,
			wantLen:  0,
		},

		// Trailing content variations
		{
			name:     "reindex with newlines and semicolon",
			query:    "REINDEX\n\n\n;",
			wantStmt: stmts["REINDEX"],
			wantLen:  11,
		},
		{
			name:     "reindex no match with extra",
			query:    "REINDEX t",
			wantStmt: nil,
			wantLen:  0,
		},

		// More bizarre edge cases
		{
			name:     "tab characters before semicolon",
			query:    "SELECT 1\t\t\t;",
			wantStmt: stmts["SELECT 1"],
			wantLen:  12,
		},
		{
			name:     "mixed tabs and spaces",
			query:    "VACUUM \t \t ;",
			wantStmt: stmts["VACUUM"],
			wantLen:  12,
		},
		{
			name:     "carriage return before semicolon",
			query:    "ANALYZE\r;",
			wantStmt: stmts["ANALYZE"],
			wantLen:  9,
		},
		{
			name:     "all whitespace types",
			query:    "REINDEX \t\n\r ;",
			wantStmt: stmts["REINDEX"],
			wantLen:  13,
		},
		{
			name:     "query with only leading tabs",
			query:    "\t\t\tSELECT 1",
			wantStmt: stmts["SELECT 1"],
			wantLen:  11,
		},
		{
			name:     "query with leading newlines",
			query:    "\n\n\nVACUUM",
			wantStmt: stmts["VACUUM"],
			wantLen:  9,
		},
		{
			name:     "longest prefix with multiple matches should choose longest",
			query:    "PRAGMA foreign_keys=ON; SELECT 1",
			wantStmt: stmts["PRAGMA foreign_keys=ON"],
			wantLen:  23,
		},
		{
			name:     "empty string after semicolon",
			query:    "DELETE FROM t WHERE x=1;",
			wantStmt: stmts["DELETE FROM t WHERE x=1"],
			wantLen:  24,
		},
		{
			name:     "unicode spaces should not be treated as whitespace",
			query:    "SELECT 1\u00A0;",
			wantStmt: nil,
			wantLen:  0,
		},
		{
			name:     "very long whitespace sequence",
			query:    "VACUUM                                                                                                    ;",
			wantStmt: stmts["VACUUM"],
			wantLen:  107,
		},
		{
			name:     "semicolon immediately after cached query",
			query:    "INSERT INTO t VALUES(1,2,3);",
			wantStmt: stmts["INSERT INTO t VALUES(1,2,3)"],
			wantLen:  28,
		},
		{
			name:     "multiple statements both cached",
			query:    "VACUUM; REINDEX",
			wantStmt: stmts["VACUUM"],
			wantLen:  7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStmt, gotLen := cache.TryGet(tt.query)
			if gotStmt != tt.wantStmt {
				t.Errorf("TryGet() gotStmt = %v, want %v", gotStmt, tt.wantStmt)
			}
			if gotLen != tt.wantLen {
				t.Errorf("TryGet() gotLen = %v, want %v", gotLen, tt.wantLen)
			}
		})
	}
}

func TestStmtCache_LongestPrefix(t *testing.T) {
	// Test that longest prefix is selected when multiple matches exist
	cache := NewStmtCache(10)

	stmt1 := &Stmt{}
	stmt2 := &Stmt{}
	stmt3 := &Stmt{}

	cache.Put("SELECT", stmt1)
	cache.Put("SELECT *", stmt2)
	cache.Put("SELECT * FROM t", stmt3)

	// Should match the longest prefix
	stmt, length := cache.TryGet("SELECT * FROM t")
	if stmt != stmt3 {
		t.Errorf("Expected longest match SELECT * FROM t, got different stmt")
	}
	if length != 15 {
		t.Errorf("Expected length 15, got %d", length)
	}

	// Should match "SELECT *" not "SELECT"
	stmt, length = cache.TryGet("SELECT * FROM x")
	if stmt != nil {
		t.Errorf("Expected no match for SELECT * FROM x, got stmt")
	}
	if length != 0 {
		t.Errorf("Expected length 0, got %d", length)
	}

	// Should match "SELECT *"
	stmt, length = cache.TryGet("SELECT *;")
	if stmt != stmt2 {
		t.Errorf("Expected match SELECT *, got different stmt")
	}
	if length != 9 {
		t.Errorf("Expected length 9, got %d", length)
	}

	// Should match "SELECT"
	stmt, length = cache.TryGet("SELECT;")
	if stmt != stmt1 {
		t.Errorf("Expected match SELECT, got different stmt")
	}
	if length != 7 {
		t.Errorf("Expected length 7, got %d", length)
	}
}
