// Package kvtest is the conformance suite every [replication.KV] implementation must pass.
//
// It exists because the in-memory store and etcd disagreeing is not a theoretical risk: a
// transaction that etcd rejects but a permissive fake accepts produces code that passes every
// fast test and fails only against a real cluster. Running one suite against both keeps them
// honest, and any behavior replication depends on belongs here rather than in a single
// implementation's own tests.
package kvtest

import (
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
)

// Factory opens a session against a keyspace that is empty and isolated per call to [Run]. Calling
// the returned function again must yield an independent session on the *same* keyspace — that is
// what lets the suite check that ephemeral keys are visible to peers and vanish with their owner.
//
// The suite closes every session it opens; the factory owns any other cleanup.
type Factory func(t *testing.T) (session func(t *testing.T) replication.KV)

// Run executes the conformance suite against the implementation the factory produces.
func Run(t *testing.T, newSpace Factory) {
	t.Helper()

	for name, fn := range map[string]func(*testing.T, Factory){
		"GetMissing":              testGetMissing,
		"PutGetRoundTrip":         testPutGetRoundTrip,
		"CreateIfAbsent":          testCreateIfAbsent,
		"CompareAndSwap":          testCompareAndSwap,
		"FailedTxnAppliesNothing": testFailedTxnAppliesNothing,
		"ListSorted":              testListSorted,
		"ListFrom":                testListFrom,
		"DeleteAndDeletePrefix":   testDeleteAndDeletePrefix,
		"RejectsOverlappingOps":   testRejectsOverlappingOps,
		"EphemeralVisibleToPeers": testEphemeralVisibleToPeers,
		"EphemeralExpiresOnClose": testEphemeralExpiresOnClose,
		"DoneClosesOnClose":       testDoneClosesOnClose,
	} {
		t.Run(name, func(t *testing.T) {
			fn(t, newSpace)
		})
	}
}

// open returns one session from a fresh keyspace, closed when the test ends.
func open(t *testing.T, newSpace Factory) replication.KV {
	t.Helper()

	return session(t, newSpace(t))
}

func session(t *testing.T, s func(t *testing.T) replication.KV) replication.KV {
	t.Helper()

	kv := s(t)
	t.Cleanup(func() { _ = kv.Close() })

	return kv
}

func put(t *testing.T, kv replication.KV, key, val string) {
	t.Helper()

	ok, err := kv.Txn(t.Context(), replication.Txn{
		Then: []replication.Op{replication.Put(key, []byte(val))},
	})
	require.NoError(t, err)
	require.True(t, ok)
}

func testGetMissing(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	_, err := kv.Get(t.Context(), "/absent")
	require.ErrorIs(t, err, replication.ErrNotFound)
}

func testPutGetRoundTrip(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	put(t, kv, "/a", "one")

	v, err := kv.Get(t.Context(), "/a")
	require.NoError(t, err)
	require.Equal(t, "one", string(v.Data))
	require.NotZero(t, v.Rev, "an existing key must report a non-zero revision")

	// Overwriting must move the revision forward: the compare-and-swap loops depend on it.
	put(t, kv, "/a", "two")

	updated, err := kv.Get(t.Context(), "/a")
	require.NoError(t, err)
	require.Equal(t, "two", string(updated.Data))
	require.Greater(t, updated.Rev, v.Rev)
}

func testCreateIfAbsent(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	// Revision zero as a precondition means "must not exist".
	create := replication.Txn{
		If:   []replication.Cond{{Key: "/new", Rev: 0}},
		Then: []replication.Op{replication.Put("/new", []byte("first"))},
	}

	ok, err := kv.Txn(t.Context(), create)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = kv.Txn(t.Context(), create)
	require.NoError(t, err)
	require.False(t, ok, "creating an existing key must not succeed")

	v, err := kv.Get(t.Context(), "/new")
	require.NoError(t, err)
	require.Equal(t, "first", string(v.Data), "the losing create must not overwrite")
}

func testCompareAndSwap(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	put(t, kv, "/counter", "1")

	v, err := kv.Get(t.Context(), "/counter")
	require.NoError(t, err)

	ok, err := kv.Txn(t.Context(), replication.Txn{
		If:   []replication.Cond{{Key: "/counter", Rev: v.Rev}},
		Then: []replication.Op{replication.Put("/counter", []byte("2"))},
	})
	require.NoError(t, err)
	require.True(t, ok)

	// The same stale revision must now fail — this is what stops two replicas minting the same
	// log sequence or block number.
	ok, err = kv.Txn(t.Context(), replication.Txn{
		If:   []replication.Cond{{Key: "/counter", Rev: v.Rev}},
		Then: []replication.Op{replication.Put("/counter", []byte("3"))},
	})
	require.NoError(t, err)
	require.False(t, ok)

	after, err := kv.Get(t.Context(), "/counter")
	require.NoError(t, err)
	require.Equal(t, "2", string(after.Data))
}

func testFailedTxnAppliesNothing(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	put(t, kv, "/guard", "held")

	ok, err := kv.Txn(t.Context(), replication.Txn{
		// Wrong precondition: the key exists, so a zero revision cannot match.
		If: []replication.Cond{{Key: "/guard", Rev: 0}},
		Then: []replication.Op{
			replication.Put("/effect/one", []byte("x")),
			replication.Put("/effect/two", []byte("y")),
		},
	})
	require.NoError(t, err)
	require.False(t, ok)

	for _, key := range []string{"/effect/one", "/effect/two"} {
		_, err := kv.Get(t.Context(), key)
		require.ErrorIs(t, err, replication.ErrNotFound, "key %s must not exist", key)
	}
}

func testListSorted(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	for _, key := range []string{"/p/c", "/p/a", "/p/b", "/other/z"} {
		put(t, kv, key, key)
	}

	values, err := kv.List(t.Context(), "/p/")
	require.NoError(t, err)

	got := make([]string, 0, len(values))
	for _, v := range values {
		got = append(got, v.Key)
	}

	require.Equal(t, []string{"/p/a", "/p/b", "/p/c"}, got,
		"List must return exactly the prefix, ascending")
}

func testListFrom(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	// Fixed-width keys, as the log uses: lexicographic order is numeric order.
	for _, key := range []string{"/log/0001", "/log/0002", "/log/0003", "/log/0004"} {
		put(t, kv, key, key)
	}

	put(t, kv, "/log_seq", "4") // adjacent prefix that must not leak into the range

	values, err := kv.ListFrom(t.Context(), "/log/", "/log/0002", 0)
	require.NoError(t, err)

	got := make([]string, 0, len(values))
	for _, v := range values {
		got = append(got, v.Key)
	}

	require.Equal(t, []string{"/log/0002", "/log/0003", "/log/0004"}, got,
		"ListFrom is inclusive of start and confined to the prefix")

	limited, err := kv.ListFrom(t.Context(), "/log/", "/log/0002", 2)
	require.NoError(t, err)
	require.Len(t, limited, 2, "limit must cap the result")
	require.Equal(t, "/log/0002", limited[0].Key)

	// Ranging past the end is empty, not an error: a caught-up replica polls this every tick.
	none, err := kv.ListFrom(t.Context(), "/log/", "/log/9999", 0)
	require.NoError(t, err)
	require.Empty(t, none)
}

func testDeleteAndDeletePrefix(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	for _, key := range []string{"/d/a", "/d/b", "/keep"} {
		put(t, kv, key, key)
	}

	ok, err := kv.Txn(t.Context(), replication.Txn{
		Then: []replication.Op{replication.Delete("/d/a")},
	})
	require.NoError(t, err)
	require.True(t, ok)

	_, err = kv.Get(t.Context(), "/d/a")
	require.ErrorIs(t, err, replication.ErrNotFound)

	// Deleting an absent key is not an error — dequeuing is retried after a crash.
	ok, err = kv.Txn(t.Context(), replication.Txn{
		Then: []replication.Op{replication.Delete("/d/a")},
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = kv.Txn(t.Context(), replication.Txn{
		Then: []replication.Op{replication.DeletePrefix("/d/")},
	})
	require.NoError(t, err)
	require.True(t, ok)

	remaining, err := kv.List(t.Context(), "/d/")
	require.NoError(t, err)
	require.Empty(t, remaining)

	_, err = kv.Get(t.Context(), "/keep")
	require.NoError(t, err, "a prefix delete must not reach outside its prefix")
}

func testRejectsOverlappingOps(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	for name, ops := range map[string][]replication.Op{
		"same key twice": {
			replication.Put("/x", []byte("a")),
			replication.Put("/x", []byte("b")),
		},
		"put and delete the same key": {
			replication.Put("/x", []byte("a")),
			replication.Delete("/x"),
		},
		"write inside a deleted prefix": {
			replication.DeletePrefix("/q/"),
			replication.Put("/q/1", []byte("a")),
		},
		"write inside a deleted prefix, reversed": {
			replication.Put("/q/1", []byte("a")),
			replication.DeletePrefix("/q/"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := kv.Txn(t.Context(), replication.Txn{Then: ops})
			require.Error(t, err,
				"a transaction whose result depends on operation order must be rejected, not silently applied")
		})
	}

	// Deletes commute and are idempotent, so overlapping ones are legal — a store that rejected
	// them would be stricter than etcd and would refuse transactions production allows.
	t.Run("overlapping deletes are allowed", func(t *testing.T) {
		ok, err := kv.Txn(t.Context(), replication.Txn{Then: []replication.Op{
			replication.DeletePrefix("/q/"),
			replication.DeletePrefix("/q/inner/"),
			replication.Delete("/q/leaf"),
		}})
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("disjoint operations are allowed", func(t *testing.T) {
		ok, err := kv.Txn(t.Context(), replication.Txn{Then: []replication.Op{
			replication.Put("/q/1", []byte("a")),
			replication.Put("/q/2", []byte("b")),
			replication.DeletePrefix("/r/"),
		}})
		require.NoError(t, err)
		require.True(t, ok)
	})
}

func testEphemeralVisibleToPeers(t *testing.T, newSpace Factory) {
	space := newSpace(t)
	owner := session(t, space)
	peer := session(t, space)

	require.NoError(t, owner.Ephemeral(t.Context(), "/replicas/a/active", []byte("a:8080")))

	v, err := peer.Get(t.Context(), "/replicas/a/active")
	require.NoError(t, err, "a peer must see another session's liveness key")
	require.Equal(t, "a:8080", string(v.Data))

	values, err := peer.List(t.Context(), "/replicas/")
	require.NoError(t, err)
	require.Len(t, values, 1, "ephemeral keys must appear in listings like any other")
}

func testEphemeralExpiresOnClose(t *testing.T, newSpace Factory) {
	space := newSpace(t)
	owner := space(t)
	peer := session(t, space)

	require.NoError(t, owner.Ephemeral(t.Context(), "/replicas/a/active", []byte("a:8080")))
	put(t, owner, "/replicas/a/host", "a:8080")

	require.NoError(t, owner.Close())

	require.Eventually(t, func() bool {
		_, err := peer.Get(t.Context(), "/replicas/a/active")

		return errors.Is(err, replication.ErrNotFound)
	}, 30*timeUnit, timeUnit, "an ephemeral key must not outlive its session")

	_, err := peer.Get(t.Context(), "/replicas/a/host")
	require.NoError(t, err, "closing a session must not remove its ordinary keys")
}

func testDoneClosesOnClose(t *testing.T, newSpace Factory) {
	kv := open(t, newSpace)

	select {
	case <-kv.Done():
		require.Fail(t, "Done must not be closed while the session is live")
	default:
	}

	require.NoError(t, kv.Close())

	select {
	case <-kv.Done():
	case <-time.After(30 * timeUnit):
		require.Fail(t, "Done must close when the session ends")
	}
}

// timeUnit is the poll granularity for the two assertions that must wait: an etcd lease expires
// on the server's clock, so those cannot be made instantaneous.
const timeUnit = 200 * time.Millisecond
