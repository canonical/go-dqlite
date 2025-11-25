package driver

import (
	"strings"

	"github.com/tidwall/btree"
)

// cacheEntry represents a single entry in the LRU cache
type cacheEntry struct {
	query string
	stmt  *Stmt
	prev  *cacheEntry
	next  *cacheEntry
}

// entryLess compares two cacheEntry pointers by their query strings
func entryLess(a, b *cacheEntry) bool {
	return a.query < b.query
}

// StmtCache is an LRU cache for prepared statements
type StmtCache struct {
	capacity int
	btree    *btree.BTreeG[*cacheEntry] // Ordered tree for efficient prefix matching
	head     *cacheEntry                // most recently used
	tail     *cacheEntry                // least recently used
}

// NewStmtCache creates a new statement cache with the given capacity
func NewStmtCache(capacity int) *StmtCache {
	return &StmtCache{
		capacity: capacity,
		btree:    btree.NewBTreeGOptions(entryLess, btree.Options{NoLocks: true}),
	}
}

// Put adds a statement to the cache with the given query
func (c *StmtCache) Put(query string, stmt *Stmt) {
	// Normalize the query (trim spaces)
	normalized := strings.TrimRight(strings.TrimLeft(query, " \t\n\r"), " \t\n\r;")

	// Check if already exists in btree
	pivot := &cacheEntry{query: normalized}
	existing, exists := c.btree.Get(pivot)

	if exists {
		c.moveToFront(existing)
		return
	}

	stmt.refcount += 1

	// Create new entry
	entry := &cacheEntry{
		query: normalized,
		stmt:  stmt,
	}

	c.btree.Set(entry)
	c.addToFront(entry)

	// Evict LRU if over capacity
	if c.btree.Len() > c.capacity {
		c.evictLRU()
	}
}

// TryGet attempts to match the query with cached statements using longest prefix matching.
// Returns the matched Stmt and the length of the matched portion, or nil and 0 if no match.
func (c *StmtCache) TryGet(query string) (*Stmt, int) {
	var match *cacheEntry
	var matchLen int

	// Trim leading spaces for prefix matching
	trimmedQuery := strings.TrimLeft(query, " \t\n\r")

	// Use btree's Ascend to iterate in sorted order starting from empty string
	// This allows us to efficiently check all potential prefixes
	c.btree.Descend(&cacheEntry{query: trimmedQuery}, func(item *cacheEntry) bool {
		if len(trimmedQuery) < len(item.query) {
			return true
		}

		if trimmedQuery[0:len(item.query)] != item.query {
			return false // Stop iteration
		}

		if len := c.isMatch(trimmedQuery, item.query); len > 0 {
			match = item
			matchLen = len
		}

		return true
	})

	if match != nil {
		c.moveToFront(match)
		match.stmt.refcount += 1
		return match.stmt, matchLen + len(query) - len(trimmedQuery)
	}

	return nil, 0
}

// isMatch checks if cachedQuery matches as a prefix of query.
// Returns the match length (including trailing whitespace and semicolon if present), or 0 if no match.
func (c *StmtCache) isMatch(trimmedQuery, cachedQuery string) int {
	// Position after the cached query in the original query
	pos := len(cachedQuery)

	// Skip trailing whitespace
	for pos < len(trimmedQuery) && isSpace(trimmedQuery[pos]) {
		pos++
	}

	// Check if we're at end of string or at a semicolon
	if pos >= len(trimmedQuery) {
		// End of string - valid match
		return pos
	}

	if trimmedQuery[pos] == ';' {
		// Found semicolon - valid match
		return pos + 1
	}

	// No valid terminator found
	return 0
}

// isSpace checks if a character is whitespace
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// moveToFront moves an entry to the front of the LRU list
func (c *StmtCache) moveToFront(entry *cacheEntry) {
	if entry == c.head {
		return
	}

	c.removeFromList(entry)
	c.addToFront(entry)
}

// addToFront adds an entry to the front of the LRU list
func (c *StmtCache) addToFront(entry *cacheEntry) {
	entry.next = c.head
	entry.prev = nil

	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

// removeFromList removes an entry from the LRU list
func (c *StmtCache) removeFromList(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
}

// evictLRU removes the least recently used entry from the cache
func (c *StmtCache) evictLRU() {
	if c.tail == nil {
		return
	}

	lru := c.tail
	c.removeFromList(lru)
	c.btree.Delete(lru)
	lru.stmt.Close()
}

func (c *StmtCache) Close() {
	c.btree.Scan(func(item *cacheEntry) bool {
		item.stmt.Close()
		return true
	})
	c.btree.Clear()
}
