// Package memkv is an in-memory [replication.KV] for tests and single-node runs.
//
// It reproduces the parts of etcd's semantics replication depends on — lexicographic key order,
// per-key modification revisions, all-or-nothing transactions, and session-scoped ephemeral keys
// — in a map behind a mutex. Several stores can share one keyspace via [Shared], which is how a
// test drives a multi-replica cluster in one process without etcd or Docker.
package memkv

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/shrimpd/replication"
)

// Space is a keyspace shared by any number of [Store] sessions. It plays the role of the etcd
// cluster: sessions come and go, the data does not.
type Space struct {
	mu   sync.Mutex
	rev  int64
	data map[string]entry
}

type entry struct {
	data []byte
	rev  int64
	// session owns the key when it is ephemeral; closing that session removes the key.
	session *Store
}

// NewSpace returns an empty keyspace.
func NewSpace() *Space { return &Space{data: make(map[string]entry)} }

// Store is one session against a [Space] — the [replication.KV] a single replica holds.
type Store struct {
	space *Space

	mu     sync.Mutex
	done   chan struct{}
	closed bool
}

var _ replication.KV = (*Store)(nil)

// New returns a store with its own private keyspace.
func New() *Store { return NewSpace().Session() }

// Session opens a new session against the space.
func (s *Space) Session() *Store {
	return &Store{space: s, done: make(chan struct{})}
}

// Get implements [replication.KV].
func (st *Store) Get(_ context.Context, key string) (replication.Value, error) {
	st.space.mu.Lock()
	defer st.space.mu.Unlock()

	e, ok := st.space.data[key]
	if !ok {
		return replication.Value{}, errors.Wrapf(replication.ErrNotFound, "get %q", key)
	}

	return replication.Value{Key: key, Data: slices.Clone(e.data), Rev: e.rev}, nil
}

// List implements [replication.KV].
func (st *Store) List(ctx context.Context, prefix string) ([]replication.Value, error) {
	return st.ListFrom(ctx, prefix, "", 0)
}

// ListFrom implements [replication.KV].
func (st *Store) ListFrom(_ context.Context, prefix, start string, limit int) ([]replication.Value, error) {
	st.space.mu.Lock()
	defer st.space.mu.Unlock()

	keys := make([]string, 0, len(st.space.data))

	for k := range st.space.data {
		if strings.HasPrefix(k, prefix) && k >= start {
			keys = append(keys, k)
		}
	}

	slices.Sort(keys)

	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	out := make([]replication.Value, 0, len(keys))

	for _, k := range keys {
		e := st.space.data[k]
		out = append(out, replication.Value{Key: k, Data: slices.Clone(e.data), Rev: e.rev})
	}

	return out, nil
}

// checkDisjoint rejects a transaction whose operations conflict, matching etcd's rule exactly:
// two writes to one key conflict, and a write inside a delete's range conflicts, but deletes
// never conflict with each other.
//
// etcd refuses a conflicting transaction ("duplicate key given in txn request"), and a fake that
// quietly accepts one lets a bug pass every in-process test and fail only against a real cluster.
// Being *stricter* than etcd is a bug too — it would reject transactions production allows — so
// the rule is mirrored, not approximated. [kvtest] holds both implementations to it.
func checkDisjoint(ops []replication.Op) error {
	for i := range ops {
		for j := i + 1; j < len(ops); j++ {
			if opsOverlap(ops[i], ops[j]) {
				return errors.Errorf(
					"memkv: duplicate key in txn: %q and %q overlap", ops[i].Key, ops[j].Key)
			}
		}
	}

	return nil
}

func opsOverlap(a, b replication.Op) bool {
	aDel, bDel := isDelete(a), isDelete(b)

	switch {
	case aDel && bDel:
		// Deletes are idempotent and commute, so any number of them may cover the same keys.
		return false
	case aDel:
		return deleteCovers(a, b.Key)
	case bDel:
		return deleteCovers(b, a.Key)
	default:
		// Two writes to one key: the outcome would depend on order, so it is rejected.
		return a.Key == b.Key
	}
}

func isDelete(op replication.Op) bool {
	return op.Kind == replication.OpDelete || op.Kind == replication.OpDeletePrefix
}

// deleteCovers reports whether a delete operation's range includes key.
func deleteCovers(del replication.Op, key string) bool {
	if del.Kind == replication.OpDeletePrefix {
		return strings.HasPrefix(key, del.Key)
	}

	return del.Key == key
}

// Txn implements [replication.KV].
func (st *Store) Txn(_ context.Context, t replication.Txn) (bool, error) {
	if err := checkDisjoint(t.Then); err != nil {
		return false, err
	}

	st.space.mu.Lock()
	defer st.space.mu.Unlock()

	for _, c := range t.If {
		if st.space.data[c.Key].rev != c.Rev {
			return false, nil
		}
	}

	st.space.rev++
	rev := st.space.rev

	for _, op := range t.Then {
		switch op.Kind {
		case replication.OpPut:
			st.space.data[op.Key] = entry{data: slices.Clone(op.Data), rev: rev}
		case replication.OpDelete:
			delete(st.space.data, op.Key)
		case replication.OpDeletePrefix:
			for k := range st.space.data {
				if strings.HasPrefix(k, op.Key) {
					delete(st.space.data, k)
				}
			}
		}
	}

	return true, nil
}

// Ephemeral implements [replication.KV].
func (st *Store) Ephemeral(_ context.Context, key string, data []byte) error {
	st.mu.Lock()
	closed := st.closed
	st.mu.Unlock()

	if closed {
		return errors.New("memkv: session closed")
	}

	st.space.mu.Lock()
	defer st.space.mu.Unlock()

	st.space.rev++
	st.space.data[key] = entry{data: slices.Clone(data), rev: st.space.rev, session: st}

	return nil
}

// Done implements [replication.KV].
func (st *Store) Done() <-chan struct{} { return st.done }

// Close implements [replication.KV]: it ends the session, removing every ephemeral key it owns,
// exactly as an expiring etcd lease would.
func (st *Store) Close() error {
	st.mu.Lock()

	if st.closed {
		st.mu.Unlock()

		return nil
	}

	st.closed = true
	close(st.done)
	st.mu.Unlock()

	st.space.mu.Lock()
	defer st.space.mu.Unlock()

	for k, e := range st.space.data {
		if e.session == st {
			delete(st.space.data, k)
		}
	}

	return nil
}
