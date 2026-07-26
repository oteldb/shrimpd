package replication_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
	"github.com/oteldb/shrimpd/replication/memkv"
)

func TestCreateReplicatesToPeers(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	a.flush(t, "p")
	b.flush(t, "p")

	c.converge()
	c.requireConverged("p/1_1_0", "p/2_2_0")

	// Each replica fetched exactly the block the other wrote — no redundant transfers.
	fetchedA, _, _ := a.store.counters()
	fetchedB, _, _ := b.store.counters()
	require.Equal(t, 1, fetchedA)
	require.Equal(t, 1, fetchedB)
}

func TestBlockNumbersAreGlobalPerPartition(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	// Interleaved allocation across replicas must never repeat a number in a partition, and
	// must be independent between partitions.
	require.Equal(t, uint64(1), mustAllocate(t, a, "p"))
	require.Equal(t, uint64(2), mustAllocate(t, b, "p"))
	require.Equal(t, uint64(3), mustAllocate(t, a, "p"))
	require.Equal(t, uint64(1), mustAllocate(t, b, "q"))
}

func TestMergeIsReproducedLocally(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	first := a.flush(t, "p")
	second := a.flush(t, "p")

	c.converge()
	c.requireConverged("p/1_1_0", "p/2_2_0")

	a.merge(t, first, second)
	c.converge()
	c.requireConverged("p/1_2_1")

	// b held both sources, so it merged rather than transferring the result.
	fetchedB, mergedB, droppedB := b.store.counters()
	require.Equal(t, 1, mergedB, "peer should reproduce the merge locally")
	require.Equal(t, 2, fetchedB, "peer should not fetch the merge result")
	require.Equal(t, 2, droppedB, "peer should drop both superseded sources")
}

func TestMergeFallsBackToFetchWhenDeclined(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	first := a.flush(t, "p")
	second := a.flush(t, "p")
	c.converge()

	b.store.set(func(s *fakeStore) { s.refuseMerge = true })

	a.merge(t, first, second)
	c.converge()
	c.requireConverged("p/1_2_1")

	fetchedB, mergedB, _ := b.store.counters()
	require.Equal(t, 0, mergedB)
	require.Equal(t, 3, fetchedB, "declined merge must fall back to fetching the result")
}

func TestMissingSourcesForceFetch(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")

	first := a.flush(t, "p")
	second := a.flush(t, "p")
	merged := a.merge(t, first, second)

	// A replica joining after the merge never saw the sources, so it can only fetch.
	b := c.add("b")
	c.converge()

	require.Equal(t, []string{replication.Key(merged)}, b.store.keys())

	fetchedB, mergedB, _ := b.store.counters()
	require.Equal(t, 1, fetchedB)
	require.Equal(t, 0, mergedB)
}

func TestNewReplicaClonesFromPeer(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")

	first := a.flush(t, "p")
	second := a.flush(t, "p")
	a.merge(t, first, second)
	a.flush(t, "p")
	a.flush(t, "q")

	// A replica added to a cluster with an existing log starts lost and clones. It must fetch
	// the merge *result* and never its already-merged sources.
	b := c.add("b")
	c.converge()

	c.requireConverged("p/1_2_1", "p/3_3_0", "q/1_1_0")

	fetchedB, _, _ := b.store.counters()
	require.Equal(t, 3, fetchedB, "clone must fetch only the most-covering blocks")
}

func TestLostReplicaRebuildsFromScratch(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	a.flush(t, "p")
	a.flush(t, "p")
	c.converge()
	c.requireConverged("p/1_1_0", "p/2_2_0")

	// Simulate losing b's disk: wipe its blocks and flag it lost.
	b.store.set(func(s *fakeStore) { s.blocks = map[string]testBlock{} })
	require.NoError(t, replication.MarkLostForTest(t.Context(), b.repl))
	require.NoError(t, replication.RecoverForTest(t.Context(), b.repl))
	require.NoError(t, replication.ReloadQueueForTest(t.Context(), b.repl))

	c.converge()
	c.requireConverged("p/1_1_0", "p/2_2_0")
}

func TestFetchFallsBackToAnotherReplica(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	a.flush(t, "p")
	c.converge()

	// Now a writes a block and immediately becomes unreachable. b already has it, so a third
	// replica must still be able to obtain it.
	second := a.flush(t, "p")
	c.converge()
	require.Contains(t, b.store.keys(), replication.Key(second))

	require.NoError(t, a.kv.Close())
	delete(c.nodes, "a")

	d := c.add("d")
	c.converge()

	require.Equal(t, []string{"p/1_1_0", "p/2_2_0"}, d.store.keys())
}

func TestFailedFetchRetriesWithBackoff(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	b.store.set(func(s *fakeStore) { s.failFetch = true })
	a.flush(t, "p")

	require.NoError(t, replication.PullForTest(t.Context(), b.repl))
	replication.ExecuteForTest(t.Context(), b.repl)

	state := b.repl.Inspect()
	require.Len(t, state.Queue, 1)
	require.Equal(t, 1, state.Queue[0].Attempts)
	require.NotEmpty(t, state.Queue[0].LastError)
	require.Empty(t, b.store.keys())

	// A second immediate pass must not retry: the entry is inside its backoff window.
	replication.ExecuteForTest(t.Context(), b.repl)
	require.Equal(t, 1, b.repl.Inspect().Queue[0].Attempts)

	// Once fetching works again and the backoff expires, the entry drains.
	b.store.set(func(s *fakeStore) { s.failFetch = false })
	replication.ClearBackoffForTest(b.repl)
	c.converge()

	c.requireConverged("p/1_1_0")
	require.Empty(t, b.repl.Inspect().Queue)
}

func TestDropRemovesBlockEverywhere(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	b := c.add("b")

	block := a.flush(t, "p")
	a.flush(t, "p")
	c.converge()
	c.requireConverged("p/1_1_0", "p/2_2_0")

	require.NoError(t, a.store.Drop(t.Context(), block))
	require.NoError(t, a.repl.CommitDrop(t.Context(), block))

	c.converge()
	c.requireConverged("p/2_2_0")

	require.NotContains(t, b.store.keys(), "p/1_1_0")
}

func TestQueueSurvivesRestart(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	b.store.set(func(s *fakeStore) { s.failFetch = true })
	a.flush(t, "p")

	require.NoError(t, replication.PullForTest(t.Context(), b.repl))
	replication.ExecuteForTest(t.Context(), b.repl)
	require.Len(t, b.repl.Inspect().Queue, 1)

	// Restart b against the same keyspace and store: the queued work must come back, and the
	// pointer must not have to re-read the log.
	restarted, err := replication.New(replication.Config[testBlock]{
		KV:      c.space.Session(),
		Store:   b.store,
		Prefix:  testPrefix,
		Replica: "b",
		Addr:    b.addr,
	})
	require.NoError(t, err)
	require.NoError(t, restarted.Start(t.Context()))

	state := restarted.Inspect()
	require.Len(t, state.Queue, 1)
	require.Equal(t, "p/1_1_0", state.Queue[0].Block)
	require.Equal(t, uint64(1), state.Pointer)

	b.store.set(func(s *fakeStore) { s.failFetch = false })
	replication.ExecuteForTest(t.Context(), restarted)
	require.Equal(t, []string{"p/1_1_0"}, b.store.keys())
}

func TestOverlappingEntriesDoNotRunConcurrently(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	first := a.flush(t, "p")
	second := a.flush(t, "p")
	third := a.flush(t, "p")
	a.merge(t, first, second)

	require.NoError(t, replication.PullForTest(t.Context(), b.repl))

	// Four records are queued; the merge overlaps blocks 1 and 2, so only the two
	// non-overlapping entries plus one of the overlapping ones may be claimed together.
	claimed := replication.ClaimForTest(b.repl, 10)
	blocks := make([]string, 0, len(claimed))

	for _, e := range claimed {
		blocks = append(blocks, e.Block)
	}

	require.Contains(t, blocks, replication.Key(third))
	require.NotSubset(t, blocks, []string{"p/1_1_0", "p/1_2_1"},
		"an entry may not run alongside one whose range it overlaps")

	c.converge()
	c.requireConverged("p/1_2_1", "p/3_3_0")
}

func TestLagReportsUnconsumedRecords(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a, b := c.add("a"), c.add("b")

	records, _, err := b.repl.Lag(t.Context())
	require.NoError(t, err)
	require.Zero(t, records)

	a.flush(t, "p")
	a.flush(t, "p")

	records, _, err = b.repl.Lag(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(2), records)

	c.converge()

	records, _, err = b.repl.Lag(t.Context())
	require.NoError(t, err)
	require.Zero(t, records)
}

func TestCleanupLogKeepsRetentionWindow(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()
	peers := newPeerRegistry()
	store := newFakeStore("a", peers)
	peers.add("a:8080", store)

	kv := space.Session()
	t.Cleanup(func() { _ = kv.Close() })

	repl, err := replication.New(replication.Config[testBlock]{
		KV:           kv,
		Store:        store,
		Prefix:       testPrefix,
		Replica:      "a",
		Addr:         "a:8080",
		LogRetention: 3,
	})
	require.NoError(t, err)
	require.NoError(t, repl.Start(t.Context()))

	for range 10 {
		num, err := repl.AllocateBlock(t.Context(), "p")
		require.NoError(t, err)

		b := newBlock("p", num, num, 0)
		store.put(b)
		require.NoError(t, repl.Commit(t.Context(), b))
	}

	require.NoError(t, replication.PullForTest(t.Context(), repl))
	require.NoError(t, repl.CleanupLog(t.Context()))

	remaining, err := replication.LogRecordsForTest(t.Context(), repl)
	require.NoError(t, err)
	require.Equal(t, 3, remaining, "retention window must be kept even when every replica is caught up")
}

func TestRunLoopReplicates(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")

	b, err := replication.New(replication.Config[testBlock]{
		KV:           c.space.Session(),
		Store:        newFakeStore("b", c.peers),
		Prefix:       testPrefix,
		Replica:      "b",
		Addr:         "b:8080",
		PollInterval: 5 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))

	go func() { _ = b.Run(t.Context()) }()

	a.flush(t, "p")

	waitFor(t, 5*time.Second, func() bool {
		return len(b.Inspect().Queue) == 0 && b.Pointer() == 1
	})
}

func mustAllocate(t *testing.T, n *node, partition string) uint64 {
	t.Helper()

	num, err := n.repl.AllocateBlock(t.Context(), partition)
	require.NoError(t, err)

	return num
}
