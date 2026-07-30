package driver

import "testing"

func TestTrimSQLSeparators(t *testing.T) {
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
		{name: "adjacent comments", query: "/**/-- x\n/**/;SELECT 1", want: "SELECT 1"},
		{name: "unterminated block comment", query: "/* comment"},
		{name: "string content is untouched", query: "SELECT '; -- /*'", want: "SELECT '; -- /*'"},
		{name: "quoted identifier is untouched", query: `SELECT "a;b"`, want: `SELECT "a;b"`},
		{name: "Unicode space is not SQLite whitespace", query: "\u00a0SELECT 1", want: "\u00a0SELECT 1"},
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
