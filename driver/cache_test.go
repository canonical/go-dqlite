package driver

import "testing"

func TestTrimSQLSeparatorsCorpus(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "empty", query: ""},
		{name: "SQLite whitespace", query: " \t\n\r\f"},
		{name: "vertical tab is not SQLite whitespace", query: "\vSELECT 1", want: "\vSELECT 1"},
		{name: "semicolons", query: "; ;;;\n"},
		{name: "line comment", query: "-- comment ; /*\nSELECT 1", want: "SELECT 1"},
		{name: "CR does not end line comment", query: "-- comment\rSELECT 1"},
		{name: "block comment", query: "/* ; -- */ SELECT 1", want: "SELECT 1"},
		{name: "minimal block comment", query: "/**/SELECT 1", want: "SELECT 1"},
		{name: "stars in block comment", query: "/*****/SELECT 1", want: "SELECT 1"},
		{name: "adjacent comments", query: "/**/-- x\n/**/;SELECT 1", want: "SELECT 1"},
		{name: "non-nested block comments", query: "/* outer /* inner */ SELECT 1", want: "SELECT 1"},
		{name: "unterminated block comment", query: "/* comment"},
		{name: "unterminated line comment", query: "-- comment"},
		{name: "single dash is SQL", query: "-SELECT 1", want: "-SELECT 1"},
		{name: "single slash is SQL", query: "/SELECT 1", want: "/SELECT 1"},
		{name: "hash is not a SQLite comment", query: "# comment", want: "# comment"},
		{name: "string content is untouched", query: "SELECT '; -- /*'", want: "SELECT '; -- /*'"},
		{name: "blob literal is untouched", query: "SELECT x'2f2a3b'", want: "SELECT x'2f2a3b'"},
		{name: "quoted identifier is untouched", query: `SELECT "a;b"`, want: `SELECT "a;b"`},
		{name: "backtick identifier is untouched", query: "SELECT `a;b`", want: "SELECT `a;b`"},
		{name: "bracket identifier is untouched", query: "SELECT [a;b]", want: "SELECT [a;b]"},
		{name: "trigger body is untouched", query: ";/* lead */CREATE TRIGGER t AFTER INSERT ON x BEGIN UPDATE x SET n=1; INSERT INTO y VALUES (';'); END;", want: "CREATE TRIGGER t AFTER INSERT ON x BEGIN UPDATE x SET n=1; INSERT INTO y VALUES (';'); END;"},
		{name: "Unicode space is not SQLite whitespace", query: "\u00a0SELECT 1", want: "\u00a0SELECT 1"},
		{name: "UTF-8 BOM is not stripped", query: "\ufeffSELECT 1", want: "\ufeffSELECT 1"},
		{name: "NUL is not stripped", query: "\x00SELECT 1", want: "\x00SELECT 1"},

		// These cases generalize patterns used by Juju's schema patches and
		// database maintenance queries.
		{
			name:  "Juju raw string DDL batch",
			query: "\nCREATE TABLE schema (version INTEGER);\n\nCREATE UNIQUE INDEX idx_schema_version ON schema (version);\n",
			want:  "CREATE TABLE schema (version INTEGER);\n\nCREATE UNIQUE INDEX idx_schema_version ON schema (version);\n",
		},
		{
			name: "Juju mixed patch preamble",
			query: "/*\n * Copyright Canonical Ltd.\n * Licensed under the AGPLv3.\n */\n\n" +
				"-- Patch: add a column.\n--\n-- Existing rows are retained; new rows populate it.\n" +
				"ALTER TABLE resource ADD COLUMN revision TEXT;",
			want: "ALTER TABLE resource ADD COLUMN revision TEXT;",
		},
		{
			name: "Juju diagnostic prose block",
			query: "/*\nA migration once failed with: patch 155; no such column.\n" +
				"```sql\nSELECT * FROM prose_not_sql; -- still documentation\n```\n*/\n" +
				"DROP VIEW obsolete_view;",
			want: "DROP VIEW obsolete_view;",
		},
		{
			name: "Juju section comments between statements",
			query: ";\n\n-- ==================================================\n" +
				"-- Step 8: recreate triggers.\n-- ==================================================\n\n" +
				"-- noqa: disable=all\nCREATE TRIGGER log_insert AFTER INSERT ON item BEGIN SELECT 1; END;",
			want: "CREATE TRIGGER log_insert AFTER INSERT ON item BEGIN SELECT 1; END;",
		},
		{
			name:  "Juju comment-only patch remainder",
			query: ";\n\n-- trigger generation is disabled; nothing follows\n/* TODO: restore it on merge. */\n",
		},
		{
			name:  "Juju batched quoted drops",
			query: ";\nDROP INDEX IF EXISTS \"index;with--punctuation\";\nDROP TRIGGER IF EXISTS \"trigger/*name*/\";",
			want:  "DROP INDEX IF EXISTS \"index;with--punctuation\";\nDROP TRIGGER IF EXISTS \"trigger/*name*/\";",
		},
		{
			name:  "Juju inline block comments are untouched",
			query: "SELECT CASE WHEN scope_id = 2 /* local-cloud */ AND type_id = 0 /* ipv4 */ THEN 1 END;",
			want:  "SELECT CASE WHEN scope_id = 2 /* local-cloud */ AND type_id = 0 /* ipv4 */ THEN 1 END;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := trimSQLSeparators(test.query); got != test.want {
				t.Fatalf("trimSQLSeparators(%q) = %q, want %q", test.query, got, test.want)
			}
		})
	}
}

func TestStmtCacheExactAndCompoundMatches(t *testing.T) {
	cache := newStmtCache(10)
	selectOne := &stmtRef{stmt: &Stmt{}}
	begin := &stmtRef{stmt: &Stmt{}}

	if _, err := cache.put("SELECT 1", false, selectOne); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.put("BEGIN;", true, begin); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		query string
		want  *stmtRef
		bytes int
	}{
		{name: "exact", query: "SELECT 1", want: selectOne, bytes: len("SELECT 1")},
		{name: "unsafe token prefix", query: "SELECT 10", want: nil},
		{name: "unterminated cached statement is not a compound prefix", query: "SELECT 1; SELECT 2", want: nil},
		{name: "terminated compound prefix", query: "BEGIN; INSERT INTO t VALUES (1)", want: begin, bytes: len("BEGIN;")},
		{name: "text is not normalized", query: " BEGIN;", want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, n := cache.get(test.query)
			if got != test.want || n != test.bytes {
				t.Fatalf("get(%q) = (%p, %d), want (%p, %d)", test.query, got, n, test.want, test.bytes)
			}
		})
	}
}

func TestStmtCacheDoesNotInferBoundaryFromSemicolonByte(t *testing.T) {
	cache := newStmtCache(10)
	comment := &stmtRef{stmt: &Stmt{}}
	if _, err := cache.put("SELECT 1 -- ;", false, comment); err != nil {
		t.Fatal(err)
	}
	if got, n := cache.get("SELECT 1 -- ;\nDROP TABLE t"); got != nil || n != 0 {
		t.Fatalf("comment semicolon was treated as a statement boundary: (%p, %d)", got, n)
	}
}
