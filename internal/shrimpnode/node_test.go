package shrimpnode_test

import (
	"context"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
	slog "github.com/oteldb/storage/signal/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/shrimpd/internal/shrimpnode"
	"github.com/oteldb/shrimpd/replication/memkv"
)

// newNode opens a node and runs it. The session is returned so a test can end it and simulate
// the node dropping out of the cluster.
func newNode(t *testing.T, space *memkv.Space, id string) (*shrimpnode.Node, *memkv.Store) {
	t.Helper()

	kv := space.Session()
	t.Cleanup(func() { _ = kv.Close() })

	n, err := shrimpnode.New(t.Context(), shrimpnode.Options{
		Dir:           t.TempDir(),
		ID:            id,
		Addr:          id + ":8080",
		KV:            kv,
		FlushInterval: 10 * time.Millisecond,
		MergeInterval: time.Hour,
		Logger:        zaptest.NewLogger(t),
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = n.Run(runCtx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done

		_ = n.Close(context.WithoutCancel(t.Context()))
	})

	return n, kv
}

// waitReady blocks until the node has joined the cluster.
//
// Run joins asynchronously, and a node that has not started yet is not awaiting a clone either —
// so readiness, not the absence of a clone wait, is what a caller must gate on before treating the
// node as a member of the cluster.
func waitReady(t *testing.T, n *shrimpnode.Node) {
	t.Helper()

	require.Eventually(t, n.Ready, 20*time.Second, 5*time.Millisecond, "node never joined")
}

// waitLogged blocks until the node's replication has a log record to show for its parts.
//
// [Node.Flush] is not the only thing that announces: the maintenance loop flushes on its own
// timer, so a Flush that returns nil may mean the loop got there first and is still committing.
// A test that goes on to bring up a node which must find a non-empty log has to wait for the
// record itself, not for the call.
func waitLogged(t *testing.T, n *shrimpnode.Node) {
	t.Helper()

	require.Eventually(t, func() bool {
		state, err := n.Inspect(t.Context())
		require.NoError(t, err)

		return state.Replication.Pointer > 0
	}, 20*time.Second, 5*time.Millisecond, "nothing was ever announced to the log")
}

func ingest(t *testing.T, n *shrimpnode.Node, ts int64, body string) {
	t.Helper()

	var logs slog.Logs

	sl := logs.AddResource().AddScope()
	sl.Scope = signal.Scope{Name: []byte("test")}

	rec := sl.AddRecord()
	rec.Timestamp = ts
	rec.Body = []byte(body)

	_, _, err := n.Ingest(t.Context(), logs)
	require.NoError(t, err)
}

// TestRunJoinsWithoutASeparateStartStep pins the single entry point: Run is enough on its own,
// and the node reports itself ready once it has joined.
func TestRunJoinsWithoutASeparateStartStep(t *testing.T) {
	t.Parallel()

	n, _ := newNode(t, memkv.NewSpace(), "solo")
	waitReady(t, n)

	ingest(t, n, 1000, "hello")
	require.NoError(t, n.Flush(t.Context()))

	entries, err := n.Query(t.Context(), 0, 1<<62, "", 0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestMaintenanceWaitsUntilTheNodeHasJoined is the reason readiness is exposed at all.
//
// A node whose replication has not yet established its place in the cluster must not flush and
// announce parts. Its record engine names parts from a node-local counter, so a node that came up
// with an empty disk would restart that counter at zero and announce a part under a name its peers
// already hold for entirely different rows — two replicas disagreeing on what one part key means.
func TestMaintenanceWaitsUntilTheNodeHasJoined(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()

	// A cluster that already has a log, so a newcomer must rebuild before it can participate.
	seed, seedKV := newNode(t, space, "seed")
	waitReady(t, seed)

	ingest(t, seed, 1000, "from-seed")
	require.NoError(t, seed.Flush(t.Context()))
	waitLogged(t, seed)

	// The only peer goes offline: the newcomer will have nothing to clone from.
	require.NoError(t, seedKV.Close())

	joiner, _ := newNode(t, space, "joiner")

	require.Eventually(t, func() bool {
		state, err := joiner.Inspect(t.Context())
		require.NoError(t, err)

		return state.AwaitingClone
	}, 20*time.Second, 5*time.Millisecond, "joiner should be waiting to rebuild")

	// Records keep arriving and the flush interval is short enough to have fired many times over
	// the wait below — none of them may become an announced part.
	ingest(t, joiner, 2000, "from-joiner")

	require.Never(t, func() bool {
		state, err := joiner.Inspect(t.Context())
		require.NoError(t, err)

		return len(partsOf(state, "joiner")) > 0
	}, 300*time.Millisecond, 10*time.Millisecond,
		"a node that has not joined must not announce parts")

	// The record is not lost, only unannounced: it is still durable and queryable locally.
	entries, err := joiner.Query(t.Context(), 0, 1<<62, "", 0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "from-joiner", entries[0].Data)
}

func partsOf(state shrimpnode.State, partition string) []string {
	var out []string

	for _, p := range state.Parts {
		if p.Partition() == partition {
			out = append(out, p.Prefix())
		}
	}

	return out
}
