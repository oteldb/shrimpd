package replication_test

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
)

// The properties every replicated set must satisfy, whatever sequence of events produced it.
// These are the guarantees ReplicatedMergeTree makes, stated over this implementation's model:
//
//  1. **Convergence** — once the queues drain, every replica holds an identical part set.
//  2. **No redundancy** — no replica holds a part another of its parts already supersedes;
//     otherwise a query would count the same rows twice.
//  3. **Completeness** — every block number ever written is covered by exactly one held part,
//     unless it was explicitly dropped.
//
// They are asserted after randomized workloads rather than after a fixed script, because the
// interesting failures come from orderings nobody thought to write down.

// requireInvariants asserts properties 2 and 3 on one replica's part set.
func requireInvariants(t *testing.T, name string, blocks []testBlock, written, dropped map[string]map[uint64]bool) {
	t.Helper()

	// No held part may supersede another held part.
	for i, a := range blocks {
		for j, b := range blocks {
			if i == j {
				continue
			}

			require.False(t, replication.Contains(a.Range(), b.Range()) && a.Part == b.Part,
				"replica %s holds %s and the %s it supersedes", name, replication.Key(a), replication.Key(b))
		}
	}

	// Every written, undropped block number is covered exactly once.
	for partition, numbers := range written {
		for num := range numbers {
			if dropped[partition][num] {
				continue
			}

			covering := 0

			for _, b := range blocks {
				r := b.Range()
				if b.Part == partition && r.First <= num && num <= r.Last {
					covering++
				}
			}

			require.Equal(t, 1, covering,
				"replica %s covers block %s/%d %d times, want exactly 1", name, partition, num, covering)
		}
	}
}

// requireIdenticalSets asserts property 1 across the cluster.
func (c *cluster) requireIdenticalSets() {
	c.t.Helper()

	var (
		reference []string
		refName   string
	)

	for _, name := range sortedNames(c.nodes) {
		keys := c.nodes[name].store.keys()

		if reference == nil {
			reference, refName = keys, name

			continue
		}

		require.Equal(c.t, reference, keys,
			"replica %s and %s disagree on the part set", refName, name)
	}
}

func joinLines(lines []string) string { return strings.Join(lines, "\n  ") }

func sortedNames[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for name := range m {
		out = append(out, name)
	}

	slices.Sort(out)

	return out
}

// TestRandomWorkloadConverges drives a randomized sequence of flushes, merges and drops across
// several replicas and asserts the three invariants after the cluster settles.
//
// Each seed is a different interleaving; a failure prints the seed so it can be replayed.
func TestRandomWorkloadConverges(t *testing.T) {
	t.Parallel()

	for seed := int64(1); seed <= 50; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runRandomWorkload(t, seed)
		})
	}
}

func runRandomWorkload(t *testing.T, seed int64) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test input, not security

	c := newCluster(t)

	// A property-test failure is only actionable with the sequence that produced it.
	var events []string

	record := func(format string, args ...any) {
		events = append(events, fmt.Sprintf(format, args...))
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("workload (seed %d):\n  %s", seed, joinLines(events))
		}
	})

	names := []string{"a", "b", "c"}
	for _, name := range names {
		c.add(name)
	}

	// written/dropped track the ground truth the invariants are checked against.
	written := map[string]map[uint64]bool{}
	dropped := map[string]map[uint64]bool{}

	const steps = 40

	for range steps {
		writer := c.nodes[names[rng.Intn(len(names))]]

		switch rng.Intn(10) {
		case 0, 1: // merge everything this replica holds in its own partition
			own := writer.ownParts()
			if len(own) < 2 {
				continue
			}

			merged := writer.merge(t, own...)
			record("%s merged %v into %s", writer.name, keysOf(own), replication.Key(merged))

		case 2: // drop one of this replica's own parts
			own := writer.ownParts()
			if len(own) == 0 {
				continue
			}

			victim := own[rng.Intn(len(own))]

			require.NoError(t, writer.store.Drop(t.Context(), victim))
			require.NoError(t, writer.repl.CommitDrop(t.Context(), victim))
			record("%s dropped %s", writer.name, replication.Key(victim))

			if dropped[victim.Part] == nil {
				dropped[victim.Part] = map[uint64]bool{}
			}

			for n := victim.R.First; n <= victim.R.Last; n++ {
				dropped[victim.Part][n] = true
			}

		default: // write a new part
			b := writer.flush(t, writer.name)
			record("%s wrote %s", writer.name, replication.Key(b))

			if written[b.Part] == nil {
				written[b.Part] = map[uint64]bool{}
			}

			written[b.Part][b.R.First] = true
		}

		// Let replication make progress between steps, so later steps act on a partly
		// replicated cluster rather than a quiescent one.
		if rng.Intn(3) == 0 {
			c.step()
			record("-- replication step --")
		}
	}

	c.converge()
	c.requireIdenticalSets()

	for _, name := range sortedNames(c.nodes) {
		blocks, err := c.nodes[name].store.Local(t.Context())
		require.NoError(t, err)
		requireInvariants(t, name, blocks, written, dropped)
	}
}

// TestConvergesRegardlessOfExecutionOrder runs the same log against replicas that execute their
// queues in different orders, and requires them to end up identical.
//
// Order-independence is the property that makes the queue safe to run concurrently: replication
// only serializes entries whose block ranges overlap, so everything else may be applied in any
// order at all.
func TestConvergesRegardlessOfExecutionOrder(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	writer := c.add("w")
	inOrder := c.add("fast")
	shuffled := c.add("slow")

	first := writer.flush(t, "w")
	second := writer.flush(t, "w")
	third := writer.flush(t, "w")
	writer.merge(t, first, second)
	writer.flush(t, "w")

	// One replica drains as records arrive; the other pulls everything first and only then
	// executes, so it sees the merge alongside its own sources.
	require.NoError(t, replication.PullForTest(t.Context(), inOrder.repl))
	replication.ExecuteForTest(t.Context(), inOrder.repl)

	require.NoError(t, replication.PullForTest(t.Context(), shuffled.repl))

	_ = third

	c.converge()
	c.requireIdenticalSets()

	require.Equal(t, []string{"w/1_2_1", "w/3_3_0", "w/4_4_0"}, inOrder.store.keys())
}

// TestReplicaHoldingNewerDataSkipsRedundantWork checks the shortcut that keeps a rejoining
// replica cheap: a record whose block is already covered by something on disk costs no transfer.
func TestReplicaHoldingNewerDataSkipsRedundantWork(t *testing.T) {
	t.Parallel()

	c := newCluster(t)
	a := c.add("a")
	b := c.add("b")

	first := a.flush(t, "p")
	second := a.flush(t, "p")
	c.converge()

	fetchedBefore, _, _ := b.store.counters()

	merged := a.merge(t, first, second)
	c.converge()

	fetchedAfter, mergedAfter, _ := b.store.counters()

	// b held both sources, so it reproduced the merge rather than transferring the result.
	require.Equal(t, fetchedBefore, fetchedAfter, "reproducing a merge must cost no transfer")
	require.Equal(t, 1, mergedAfter)
	require.Equal(t, []string{replication.Key(merged)}, b.store.keys())
}
