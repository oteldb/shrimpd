package shrimpd

import "sync"

// MemTable is the in-memory write buffer. Drained atomically by flush,
// queried (without draining) by the query path.
type MemTable struct {
	mu      sync.RWMutex
	entries []Entry
}

func (m *MemTable) Write(entries []Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
}

// Len returns the number of buffered entries.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// Snapshot atomically copies and clears the table. Used by flush.
func (m *MemTable) Snapshot() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := make([]Entry, len(m.entries))
	copy(snap, m.entries)
	m.entries = m.entries[:0]
	return snap
}

// All returns a copy of the current entries without clearing. Used by query.
func (m *MemTable) All() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]Entry, len(m.entries))
	copy(cp, m.entries)
	return cp
}
