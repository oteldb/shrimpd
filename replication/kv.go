package replication

import (
	"context"

	"github.com/go-faster/errors"
)

// ErrNotFound is returned (wrapped) by [KV.Get] when a key is absent.
var ErrNotFound = errors.New("replication: key not found")

// Value is one key/value pair with the revision at which it was last written. The revision is
// what makes optimistic concurrency possible: read a value, decide, then write conditioned on
// nobody else having written since.
type Value struct {
	Key  string
	Data []byte
	// Rev is the store's modification revision for Key. Zero means the key does not exist,
	// which is also how [Cond] expresses "must be absent".
	Rev int64
}

// OpKind is the kind of mutation in a [Txn].
type OpKind uint8

// Mutation kinds.
const (
	// OpPut writes Value.Data at Value.Key, creating or overwriting.
	OpPut OpKind = iota
	// OpDelete removes Value.Key. Deleting an absent key is not an error.
	OpDelete
	// OpDeletePrefix removes every key under Value.Key.
	OpDeletePrefix
)

// Op is one mutation applied by a [Txn].
type Op struct {
	Kind OpKind
	Key  string
	Data []byte
}

// Put returns an [Op] writing data at key.
func Put(key string, data []byte) Op { return Op{Kind: OpPut, Key: key, Data: data} }

// Delete returns an [Op] removing key.
func Delete(key string) Op { return Op{Kind: OpDelete, Key: key} }

// DeletePrefix returns an [Op] removing every key under prefix.
func DeletePrefix(prefix string) Op { return Op{Kind: OpDeletePrefix, Key: prefix} }

// Cond is a precondition on a [Txn]: key's current revision must equal Rev. Rev zero means the
// key must not exist, so a create is `Cond{Key: k, Rev: 0}` plus a [Put].
type Cond struct {
	Key string
	Rev int64
}

// Txn is an all-or-nothing batch: if every [Cond] holds, every [Op] is applied; otherwise
// nothing is. Committing a part is one Txn — the log record and the replica's part entry either
// both appear or neither does, so a crash can never leave the log advertising a part no replica
// claims to hold.
type Txn struct {
	If   []Cond
	Then []Op
}

// KV is the metadata store replication coordinates through: an ordered, revisioned key/value
// space with atomic conditional transactions and session-scoped ephemeral keys.
//
// It is deliberately smaller than etcd's client: everything here maps onto a single etcd Txn,
// Range, or lease-bound Put, and onto a plain map for tests. All methods are safe for concurrent
// use.
type KV interface {
	// Get returns the value at key, or an error satisfying errors.Is(err, [ErrNotFound]).
	Get(ctx context.Context, key string) (Value, error)
	// List returns every value whose key has the given prefix, sorted ascending by key.
	List(ctx context.Context, prefix string) ([]Value, error)
	// ListFrom is List restricted to keys at or after start and capped at limit values (limit
	// zero ⇒ unlimited). It is how the log is read incrementally: a replica ranges from its
	// pointer instead of re-reading the retained window every poll.
	ListFrom(ctx context.Context, prefix, start string, limit int) ([]Value, error)
	// Txn applies t atomically, reporting whether the preconditions held.
	Txn(ctx context.Context, t Txn) (bool, error)
	// Ephemeral writes a key bound to this client's session: it disappears when the session
	// ends, whether cleanly or by the process dying. It is how a replica advertises liveness.
	Ephemeral(ctx context.Context, key string, data []byte) error
	// Done is closed when the session is lost. Everything the replica believed about its
	// ephemeral state is void past that point, so replication restarts from registration.
	Done() <-chan struct{}
	// Close releases the session and any resources.
	Close() error
}

// getString reads key and returns its data as a string, or def when the key is absent.
func getString(ctx context.Context, kv KV, key, def string) (val string, rev int64, err error) {
	v, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return def, 0, nil
		}

		return "", 0, errors.Wrapf(err, "get %q", key)
	}

	return string(v.Data), v.Rev, nil
}
