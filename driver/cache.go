package driver

import "container/list"

// stmtRef owns a server-side prepared statement. The cache and each caller
// hold independent references to it, so evicting an in-use statement does not
// finalize it underneath the caller.
type stmtRef struct {
	stmt   *Stmt
	refs   int
	cached bool
}

func (r *stmtRef) acquire() *stmtLease {
	r.refs++
	return &stmtLease{ref: r}
}

func (r *stmtRef) release() error {
	if r.refs <= 0 {
		panic("dqlite: prepared statement reference count below zero")
	}
	r.refs--
	if r.refs == 0 && !r.cached {
		return r.stmt.finalize()
	}
	return nil
}

func (r *stmtRef) uncache() error {
	if !r.cached {
		return nil
	}
	r.cached = false
	if r.refs == 0 {
		return r.stmt.finalize()
	}
	return nil
}

type cacheEntry struct {
	query      string
	ref        *stmtRef
	prefixSafe bool
}

// stmtCache is a per-connection LRU cache. It deliberately uses exact SQL
// text as its key: rewriting or normalizing SQL without SQLite's parser can
// change statement boundaries and semantics.
type stmtCache struct {
	capacity int
	entries  map[string]*list.Element
	lru      *list.List
}

func newStmtCache(capacity int) *stmtCache {
	return &stmtCache{
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
		lru:      list.New(),
	}
}

// get returns the longest cached statement that is a safe prefix of query.
// Prefix reuse is only safe when SQLite previously returned this key as the
// first part of a query with a non-empty tail. Looking at the final byte is not
// sufficient because a semicolon there might be inside a SQL comment.
func (c *stmtCache) get(query string) (*stmtRef, int) {
	var match *list.Element
	matchLen := 0
	for _, elem := range c.entries {
		entry := elem.Value.(*cacheEntry)
		if len(entry.query) <= matchLen || len(entry.query) > len(query) {
			continue
		}
		if query[:len(entry.query)] != entry.query {
			continue
		}
		consumed := len(entry.query)
		if consumed != len(query) {
			if len(trimSQLSeparators(query[consumed:])) == 0 {
				consumed = len(query)
			} else if !entry.prefixSafe {
				continue
			}
		}
		match = elem
		matchLen = consumed
	}
	if match == nil {
		return nil, 0
	}
	c.lru.MoveToFront(match)
	return match.Value.(*cacheEntry).ref, matchLen
}

// put transfers cache ownership of ref to c. The query must be the exact byte
// range consumed by SQLite, including a terminating semicolon when present.
func (c *stmtCache) put(query string, prefixSafe bool, ref *stmtRef) (*stmtRef, error) {
	if c.capacity <= 0 {
		return ref, nil
	}
	if elem, ok := c.entries[query]; ok {
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.prefixSafe = entry.prefixSafe || prefixSafe
		if err := ref.stmt.finalize(); err != nil {
			return nil, err
		}
		return entry.ref, nil
	}

	// Do not retain a potentially much larger compound-query backing string.
	key := string(append([]byte(nil), query...))
	ref.cached = true
	elem := c.lru.PushFront(&cacheEntry{query: key, ref: ref, prefixSafe: prefixSafe})
	c.entries[key] = elem

	if c.lru.Len() <= c.capacity {
		return ref, nil
	}

	oldest := c.lru.Back()
	entry := oldest.Value.(*cacheEntry)
	delete(c.entries, entry.query)
	c.lru.Remove(oldest)
	return ref, entry.ref.uncache()
}

func (c *stmtCache) close() error {
	var firstErr error
	for elem := c.lru.Front(); elem != nil; elem = elem.Next() {
		if err := elem.Value.(*cacheEntry).ref.uncache(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.entries = make(map[string]*list.Element, c.capacity)
	c.lru.Init()
	return firstErr
}
