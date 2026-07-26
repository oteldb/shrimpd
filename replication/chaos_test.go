package replication_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
)

// Fault-injection tests. Replication's contract is that it converges *eventually*, not that
// nothing ever fails — so the interesting assertions are about what happens after a fetch fails,
// a source dies, a process restarts mid-queue, or the log is trimmed under a replica's feet.
//
// Every failure here is injected into the real code path: the production queue, executor,
// conflict rules and clone logic all run, with only the store and the metadata layer made
// unreliable.

// TestConvergesDespiteIntermittentFetchFailures fails a random fraction of transfers and requires
// the cluster to converge anyway.
func TestConvergesDespiteIntermittentFetchFailures(t *testing.T) {
	t.Parallel()

	for _, failEvery := range []int{2, 3, 5} {
		t.Run(fmt.Sprintf("failEvery=%d", failEvery), func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewSource(int64(failEvery))) //nolint:gosec // deterministic

			// The executor runs entries concurrently, so the generator needs its own lock.
			var rngMu sync.Mutex

			c := newCluster(t)
			a := c.add("a")
			b := c.add("b")

			// Every n-th fetch on b fails, as an unreachable peer or a truncated transfer would.
			b.store.set(func(s *fakeStore) {
				s.beforeFetch = func() error {
					rngMu.Lock()
					defer rngMu.Unlock()

					if rng.Intn(failEvery) == 0 {
						return errors.New("injected transfer failure")
					}

					return nil
				}
			})

			var want []string

			for range 8 {
				want = append(want, replication.Key(a.flush(t, "p")))
			}

			// Backoff is cleared between rounds: the point is that retries eventually succeed,
			// not how long the production backoff waits.
			for range 40 {
				c.step()
				replication.ClearBackoffForTest(b.repl)

				if len(b.repl.Inspect().Queue) == 0 {
					break
				}
			}

			require.Empty(t, b.repl.Inspect().Queue, "queue must drain despite failures")
			require.Equal(t, want, b.store.keys())

			fetched, _, _ := b.store.counters()
			require.Greater(t, fetched, 0)
		})
	}
}

// TestSurvivesSourceReplicaDying kills the replica that wrote a part after one peer has it, and
// requires a third replica to still obtain it.
func TestSurvivesSourceReplicaDying(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	writer := c.add("writer")
	witness := c.add("witness")

	var want []string
	for range 3 {
		want = append(want, replication.Key(writer.flush(t, "writer")))
	}

	c.converge()
	require.Equal(t, want, witness.store.keys())

	// The writer disappears entirely — session gone, parts gone.
	require.NoError(t, writer.kv.Close())
	writer.store.set(func(s *fakeStore) { s.blocks = map[string]testBlock{} })
	delete(c.nodes, "writer")

	joiner := c.add("joiner")
	c.converge()

	require.Equal(t, want, joiner.store.keys(),
		"a replica must be able to source parts from any peer that holds them")
}

// TestRestartMidQueueResumes stops a replica with work outstanding and brings it back against the
// same store, as a process restart would.
func TestRestartMidQueueResumes(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	b := c.add("b")

	var want []string
	for range 4 {
		want = append(want, replication.Key(a.flush(t, "a")))
	}

	// b copies the records but cannot execute them.
	b.store.set(func(s *fakeStore) { s.failFetch = true })
	require.NoError(t, replication.PullForTest(t.Context(), b.repl))
	replication.ExecuteForTest(t.Context(), b.repl)

	require.Len(t, b.repl.Inspect().Queue, 4)

	// Restart: a new Replication over the same store and keyspace.
	b.store.set(func(s *fakeStore) { s.failFetch = false })

	restarted, err := replication.New(replication.Config[testBlock]{
		KV:      c.space.Session(),
		Store:   b.store,
		Prefix:  testPrefix,
		Replica: "b",
		Addr:    b.addr,
	})
	require.NoError(t, err)
	require.NoError(t, replication.StartForTest(t.Context(), restarted))

	require.Len(t, restarted.Inspect().Queue, 4, "outstanding work must survive a restart")

	c.nodes["b"].repl = restarted
	c.converge()

	require.Equal(t, want, b.store.keys())
}

// TestApplyIsIdempotent re-executes a record a replica has already applied, standing in for a
// crash between doing the work and recording that it was done.
func TestApplyIsIdempotent(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	b := c.add("b")

	first := a.flush(t, "a")
	second := a.flush(t, "a")
	c.converge()

	merged := a.merge(t, first, second)
	c.converge()

	before := b.store.keys()
	_, _, droppedBefore := b.store.counters()

	// Replay both records straight into the executor, as a queue reloaded after a crash would.
	for range 3 {
		require.NoError(t, replication.ApplyForTest(t.Context(), b.repl, replication.Record[testBlock]{
			Op: replication.OpCreate, Block: first, Replica: "a",
		}))
		require.NoError(t, replication.ApplyForTest(t.Context(), b.repl, replication.Record[testBlock]{
			Op: replication.OpMerge, Block: merged, Sources: []testBlock{first, second}, Replica: "a",
		}))
	}

	require.Equal(t, before, b.store.keys(), "re-applying a record must not change the part set")

	_, _, droppedAfter := b.store.counters()
	require.Equal(t, droppedBefore, droppedAfter, "re-applying must not re-drop")
}

// TestLogGapForcesClone trims the log past a stalled replica's pointer and requires it to notice
// and rebuild, rather than silently skipping the records it can no longer read.
func TestLogGapForcesClone(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	b := c.add("b")

	var want []string
	for range 3 {
		want = append(want, replication.Key(a.flush(t, "a")))
	}

	// a catches up; b has not moved at all.
	require.NoError(t, replication.PullForTest(t.Context(), a.repl))
	require.Zero(t, b.repl.Pointer())

	// Erase the log out from under b: everything it still owes is unreadable.
	require.NoError(t, replication.TrimLogForTest(t.Context(), a.repl))

	c.converge()

	require.Equal(t, want, b.store.keys(),
		"a replica that lost the log must rebuild from a peer instead of skipping records")
	require.Empty(t, b.repl.Inspect().Queue)
}

// TestFirstReplicaOfNewClusterStarts pins the ordinary single-node case: an empty prefix has no
// log, so nothing needs cloning and the replica is healthy from the start.
func TestFirstReplicaOfNewClusterStarts(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	only := c.add("only")

	require.False(t, only.repl.AwaitingClone())

	want := replication.Key(only.flush(t, "only"))
	c.converge()
	require.Equal(t, []string{want}, only.store.keys())
}

// TestSoleReplicaThatLostItsDataFailsLoudly covers the case where waiting cannot help: the
// replica must rebuild, and it is the only one ever registered, so no source exists or ever will.
//
// Retrying forever would hide an unrecoverable cluster behind a healthy-looking process; coming up
// regardless would be worse still, since the replica would advertise itself as a source holding
// nothing.
func TestSoleReplicaThatLostItsDataFailsLoudly(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	only := c.add("only")
	only.flush(t, "only")

	// The single replica discovers its local state is untrustworthy — a trimmed log, say. There
	// is no other replica registered, so no source exists or ever will.
	require.NoError(t, replication.MarkLostForTest(t.Context(), only.repl))

	err := replication.RecoverForTest(t.Context(), only.repl)
	require.Error(t, err)
	require.NotErrorIs(t, err, replication.ErrCloneUnavailable,
		"an unrecoverable cluster must not look like a transient outage that will resolve itself")
	require.Contains(t, err.Error(), "unrecoverable")
}

// TestNewReplicaWaitsForAnOfflinePeer covers the case where waiting is exactly right: peers are
// registered but none is up yet, so the replica starts, holds off, and rebuilds when one returns.
func TestNewReplicaWaitsForAnOfflinePeer(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	want := replication.Key(a.flush(t, "a"))

	// a is registered but its session is gone — a node that is down, not decommissioned.
	require.NoError(t, a.kv.Close())
	delete(c.nodes, "a")

	joinerStore := newFakeStore("joiner", c.peers)
	c.peers.add("joiner:8080", joinerStore)

	joiner, err := replication.New(replication.Config[testBlock]{
		KV:      c.space.Session(),
		Store:   joinerStore,
		Prefix:  testPrefix,
		Replica: "joiner",
		Addr:    "joiner:8080",
	})
	require.NoError(t, err)

	require.NoError(t, replication.StartForTest(t.Context(), joiner),
		"a peer being temporarily down must not prevent startup")
	require.True(t, joiner.AwaitingClone(), "the replica must know it is not yet usable")

	// While waiting it must keep reporting the outage rather than quietly going live.
	require.ErrorIs(t, replication.RetryCloneForTest(t.Context(), joiner),
		replication.ErrCloneUnavailable)
	require.True(t, joiner.AwaitingClone())

	// a comes back with its disk intact — a restart, not a replacement.
	revivedKV := c.space.Session()
	t.Cleanup(func() { _ = revivedKV.Close() })

	revived, err := replication.New(replication.Config[testBlock]{
		KV:      revivedKV,
		Store:   a.store,
		Prefix:  testPrefix,
		Replica: "a",
		Addr:    a.addr,
	})
	require.NoError(t, err)
	require.NoError(t, replication.StartForTest(t.Context(), revived))
	require.Equal(t, []string{want}, a.store.keys())

	c.nodes["a"] = &node{name: "a", addr: a.addr, kv: revivedKV, store: a.store, repl: revived}

	// The next attempt now finds a live, healthy source.
	require.NoError(t, replication.RetryCloneForTest(t.Context(), joiner))
	require.False(t, joiner.AwaitingClone())

	c.nodes["joiner"] = &node{name: "joiner", addr: "joiner:8080", store: joinerStore, repl: joiner}

	c.converge()
	require.Equal(t, []string{want}, joinerStore.keys())
}

// TestExistingReplicaStartsWithoutPeers checks the other side of that rule: a replica that was
// already healthy must come back up alone, since a whole-cluster restart has no live peer to
// clone from and none is needed.
func TestExistingReplicaStartsWithoutPeers(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	want := replication.Key(a.flush(t, "a"))

	require.NoError(t, a.kv.Close())

	restarted, err := replication.New(replication.Config[testBlock]{
		KV:      c.space.Session(),
		Store:   a.store,
		Prefix:  testPrefix,
		Replica: "a",
		Addr:    a.addr,
	})
	require.NoError(t, err)
	require.NoError(t, replication.StartForTest(t.Context(), restarted),
		"a replica with sound local state must not need a peer to start")

	require.Equal(t, []string{want}, a.store.keys())
}

// TestMetadataFailureLeavesQueueIntact makes the metadata store fail mid-pass and requires the
// replica to retry rather than lose the work.
func TestMetadataFailureLeavesQueueIntact(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	b := c.add("b")

	want := replication.Key(a.flush(t, "a"))

	// Fetching works, but recording the outcome does not.
	b.store.set(func(s *fakeStore) { s.failFetch = true })
	require.NoError(t, replication.PullForTest(t.Context(), b.repl))
	replication.ExecuteForTest(t.Context(), b.repl)
	require.Len(t, b.repl.Inspect().Queue, 1)

	b.store.set(func(s *fakeStore) { s.failFetch = false })
	replication.ClearBackoffForTest(b.repl)
	c.converge()

	require.Equal(t, []string{want}, b.store.keys())
	require.Empty(t, b.repl.Inspect().Queue)
}
