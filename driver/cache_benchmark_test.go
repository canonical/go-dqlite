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

import (
	"fmt"
	"testing"
)

// BenchmarkStmtCache_Put measures the performance of adding statements to the cache
func BenchmarkStmtCache_Put(b *testing.B) {
	benchmarks := []struct {
		name     string
		capacity int
		queries  int
	}{
		{"Small_10", 10, 10},
		{"Medium_100", 100, 100},
		{"Large_1000", 1000, 1000},
		{"Overflow_10_100", 10, 100}, // Tests LRU eviction
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			queries := make([]string, bm.queries)
			stmts := make([]*Stmt, bm.queries)
			for i := 0; i < bm.queries; i++ {
				queries[i] = fmt.Sprintf("SELECT * FROM table_%d WHERE id = ?", i)
				stmts[i] = &Stmt{}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache := NewStmtCache(bm.capacity)
				for j := 0; j < bm.queries; j++ {
					cache.Put(queries[j], stmts[j])
				}
			}
		})
	}
}

// BenchmarkStmtCache_TryGet_Hit measures cache hit performance
func BenchmarkStmtCache_TryGet_Hit(b *testing.B) {
	benchmarks := []struct {
		name     string
		capacity int
		queries  int
	}{
		{"Small_10", 10, 10},
		{"Medium_100", 100, 100},
		{"Large_1000", 1000, 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache := NewStmtCache(bm.capacity)
			queries := make([]string, bm.queries)
			for i := 0; i < bm.queries; i++ {
				queries[i] = fmt.Sprintf("SELECT * FROM table_%d WHERE id = ?", i)
				cache.Put(queries[i], &Stmt{})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := queries[i%bm.queries]
				cache.TryGet(query)
			}
		})
	}
}

// BenchmarkStmtCache_TryGet_Miss measures cache miss performance
func BenchmarkStmtCache_TryGet_Miss(b *testing.B) {
	benchmarks := []struct {
		name     string
		capacity int
		cached   int
	}{
		{"Small_10", 10, 10},
		{"Medium_100", 100, 100},
		{"Large_1000", 1000, 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache := NewStmtCache(bm.capacity)
			for i := 0; i < bm.cached; i++ {
				query := fmt.Sprintf("SELECT * FROM table_%d WHERE id = ?", i)
				cache.Put(query, &Stmt{})
			}

			// Query that won't match
			missQuery := "INSERT INTO other_table VALUES (?, ?, ?)"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.TryGet(missQuery)
			}
		})
	}
}

// BenchmarkStmtCache_TryGet_PrefixMatch measures prefix matching performance
func BenchmarkStmtCache_TryGet_PrefixMatch(b *testing.B) {
	cache := NewStmtCache(100)

	// Populate with common SQL statements
	statements := []string{
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SELECT * FROM users WHERE id = ?",
		"INSERT INTO users VALUES (?, ?, ?)",
		"UPDATE users SET name = ? WHERE id = ?",
		"DELETE FROM users WHERE id = ?",
		"CREATE TABLE test (id INTEGER PRIMARY KEY)",
		"DROP TABLE IF EXISTS test",
		"PRAGMA foreign_keys",
	}

	for _, stmt := range statements {
		cache.Put(stmt, &Stmt{})
	}

	testQueries := []string{
		"BEGIN;",
		"BEGIN; SELECT 1",
		"  BEGIN  ",
		"COMMIT   ;",
		"SELECT * FROM users WHERE id = ?;",
		"INSERT INTO users VALUES (?, ?, ?); SELECT last_insert_rowid()",
		"PRAGMA foreign_keys;",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := testQueries[i%len(testQueries)]
		cache.TryGet(query)
	}
}

// BenchmarkStmtCache_TryGet_LongestPrefix measures performance when multiple prefixes match
func BenchmarkStmtCache_TryGet_LongestPrefix(b *testing.B) {
	cache := NewStmtCache(50)

	// Create overlapping prefixes
	cache.Put("SELECT", &Stmt{})
	cache.Put("SELECT *", &Stmt{})
	cache.Put("SELECT * FROM", &Stmt{})
	cache.Put("SELECT * FROM users", &Stmt{})
	cache.Put("SELECT * FROM users WHERE", &Stmt{})
	cache.Put("SELECT * FROM users WHERE id", &Stmt{})
	cache.Put("SELECT * FROM users WHERE id = ?", &Stmt{})

	query := "SELECT * FROM users WHERE id = ?;"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.TryGet(query)
	}
}

// BenchmarkStmtCache_Mixed measures realistic mixed workload
func BenchmarkStmtCache_Mixed(b *testing.B) {
	cache := NewStmtCache(50)

	queries := []string{
		"BEGIN",
		"SELECT * FROM users WHERE id = ?",
		"INSERT INTO users VALUES (?, ?, ?)",
		"UPDATE users SET name = ? WHERE id = ?",
		"DELETE FROM users WHERE id = ?",
		"COMMIT",
	}

	stmts := make([]*Stmt, len(queries))
	for i := range stmts {
		stmts[i] = &Stmt{}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(queries)

		// 70% hits, 30% puts (simulating statement preparation)
		if i%10 < 7 {
			cache.TryGet(queries[idx])
		} else {
			cache.Put(queries[idx], stmts[idx])
		}
	}
}

// BenchmarkStmtCache_LRU measures LRU eviction overhead
func BenchmarkStmtCache_LRU(b *testing.B) {
	cache := NewStmtCache(10)

	// Pre-fill cache
	for i := 0; i < 10; i++ {
		query := fmt.Sprintf("SELECT * FROM table_%d", i)
		cache.Put(query, &Stmt{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This will cause evictions
		query := fmt.Sprintf("SELECT * FROM table_%d", i+10)
		cache.Put(query, &Stmt{})
	}
}

// BenchmarkStmtCache_Whitespace measures impact of whitespace variations
func BenchmarkStmtCache_Whitespace(b *testing.B) {
	cache := NewStmtCache(10)
	cache.Put("SELECT 1", &Stmt{})

	queries := []string{
		"SELECT 1",
		"SELECT 1;",
		"  SELECT 1  ",
		"\t\tSELECT 1\t\t",
		"\n\nSELECT 1\n\n",
		"SELECT 1    ;",
		" SELECT 1 ; -- comment",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		cache.TryGet(query)
	}
}

// BenchmarkStmtCache_RealWorld simulates a realistic database workload
func BenchmarkStmtCache_RealWorld(b *testing.B) {
	// Simulate a web application with common query patterns
	cache := NewStmtCache(100)

	// Common queries that would be in the cache
	commonQueries := []string{
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SELECT * FROM users WHERE id = ?",
		"SELECT * FROM users WHERE email = ?",
		"INSERT INTO users (name, email, created_at) VALUES (?, ?, ?)",
		"UPDATE users SET last_login = ? WHERE id = ?",
		"SELECT * FROM sessions WHERE token = ?",
		"DELETE FROM sessions WHERE expires_at < ?",
		"SELECT COUNT(*) FROM users WHERE active = 1",
	}

	for _, q := range commonQueries {
		cache.Put(q, &Stmt{})
	}

	// Workload: 80% reads, 15% writes, 5% transactions
	workload := make([]string, 100)
	for i := 0; i < 80; i++ {
		workload[i] = commonQueries[3] + ";" // SELECT by id
	}
	for i := 80; i < 85; i++ {
		workload[i] = commonQueries[4] + "; " // SELECT by email
	}
	for i := 85; i < 90; i++ {
		workload[i] = commonQueries[5] + "; SELECT last_insert_rowid()" // INSERT
	}
	for i := 90; i < 95; i++ {
		workload[i] = commonQueries[6] + ";" // UPDATE
	}
	for i := 95; i < 100; i++ {
		workload[i] = "BEGIN; " + commonQueries[3] // Transaction
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := workload[i%len(workload)]
		cache.TryGet(query)
	}
}
