package shrimpengine_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
	slog "github.com/oteldb/storage/signal/log"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/internal/shrimpengine"
	"github.com/oteldb/shrimpd/replication"
	"github.com/oteldb/shrimpd/replication/memkv"
)

// node is one shrimpd node for the test: an engine, its part-serving HTTP endpoints, and its
// replication.
type node struct {
	name   string
	addr   string
	engine *shrimpengine.Engine
	repl   *replication.Replication[shrimpengine.Part]
}

// newNode brings up a node against a shared metadata keyspace, with its parts endpoints served on
// a real loopback listener — so replication exercises the actual HTTP transport.
func newNode(t *testing.T, space *memkv.Space, name string) *node {
	t.Helper()

	engine, err := shrimpengine.Open(t.Context(), shrimpengine.Options{
		Dir:  t.TempDir(),
		Node: name,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.WithoutCancel(t.Context())) })

	mux := http.NewServeMux()
	mux.Handle("GET "+shrimpengine.ListPath, shrimpengine.ListHandler(engine.Backend()))
	mux.Handle("GET "+shrimpengine.ObjectPath, shrimpengine.ObjectHandler(engine.Backend()))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://")

	kv := space.Session()
	t.Cleanup(func() { _ = kv.Close() })

	repl, err := replication.New(replication.Config[shrimpengine.Part]{
		KV:           kv,
		Store:        shrimpengine.NewStore(engine, shrimpengine.NewClient(nil), nil),
		Prefix:       "/shrimpd/logs",
		Replica:      name,
		Addr:         addr,
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, repl.Start(t.Context()))

	go func() { _ = repl.Run(t.Context()) }()

	return &node{name: name, addr: addr, engine: engine, repl: repl}
}

// ingest writes one log record per body at consecutive timestamps starting at base.
func (n *node) ingest(t *testing.T, base int64, bodies ...string) {
	t.Helper()

	var logs slog.Logs

	rl := logs.AddResource()
	rl.Resource = signal.Resource{
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
		),
	}

	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("test")}

	for i, body := range bodies {
		r := sl.AddRecord()
		r.Timestamp = base + int64(i)
		r.Body = []byte(body)
		r.SeverityNumber = 9
	}

	accepted, rejected, err := n.engine.Ingest(t.Context(), logs)
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Equal(t, len(bodies), accepted)
}

// bodies returns every record the node can see, sorted, across all partitions.
func (n *node) bodies(t *testing.T) []string {
	t.Helper()

	entries, err := n.engine.Query(t.Context(), 0, 1<<62, nil, 0)
	require.NoError(t, err)

	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Data)
	}

	slices.Sort(out)

	return out
}

// waitForBodies polls until the node sees exactly want, or fails.
func (n *node) waitForBodies(t *testing.T, want []string) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	var got []string

	for time.Now().Before(deadline) {
		if got = n.bodies(t); slices.Equal(got, want) {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	require.Equal(t, want, got, "node %s never converged", n.name)
}

func TestFlushedPartReplicatesToPeer(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()
	a := newNode(t, space, "a")
	b := newNode(t, space, "b")

	a.ingest(t, 1000, "alpha", "bravo")

	part, ok, err := a.engine.Flush(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "a", part.Partition())
	require.Equal(t, replication.Range{First: 0, Last: 0, Level: 0}, part.Range())

	require.NoError(t, a.repl.Commit(t.Context(), part))

	// b must obtain the part over HTTP and serve its records.
	b.waitForBodies(t, []string{"alpha", "bravo"})
}

func TestBothNodesReplicateEachOther(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()
	a := newNode(t, space, "a")
	b := newNode(t, space, "b")

	a.ingest(t, 1000, "from-a")
	b.ingest(t, 2000, "from-b")

	for _, n := range []*node{a, b} {
		part, ok, err := n.engine.Flush(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, n.repl.Commit(t.Context(), part))
	}

	want := []string{"from-a", "from-b"}
	a.waitForBodies(t, want)
	b.waitForBodies(t, want)
}

func TestMergedPartSupersedesSourcesOnPeer(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()
	a := newNode(t, space, "a")
	b := newNode(t, space, "b")

	for i, body := range []string{"one", "two", "three"} {
		a.ingest(t, int64(1000+i*100), body)

		part, ok, err := a.engine.Flush(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, a.repl.Commit(t.Context(), part))
	}

	want := []string{"one", "three", "two"}
	b.waitForBodies(t, want)

	merged, sources, ok, err := a.engine.Merge(t.Context(), 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, sources, 3)
	require.Equal(t, uint64(1), merged.Range().Level)
	require.True(t, replication.Contains(merged.Range(), sources[0].Range()),
		"merged part must supersede its sources")

	require.NoError(t, a.repl.CommitMerge(t.Context(), sources, merged))

	// b fetches the merged part and drops the three sources; the data is unchanged.
	b.waitForBodies(t, want)

	require.Eventually(t, func() bool {
		local, err := shrimpengine.NewStore(b.engine, nil, nil).Local(t.Context())
		require.NoError(t, err)

		count := 0

		for _, p := range local {
			if p.Partition() == "a" {
				count++
			}
		}

		return count == 1
	}, 20*time.Second, 20*time.Millisecond, "peer should keep only the merged part")
}

func TestLateJoinerClonesEverything(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()
	a := newNode(t, space, "a")

	a.ingest(t, 1000, "early")

	part, ok, err := a.engine.Flush(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, a.repl.Commit(t.Context(), part))

	// c joins a cluster that already has a log: it must clone rather than replay.
	c := newNode(t, space, "c")
	c.waitForBodies(t, []string{"early"})
}

func TestQueryRespectsWindow(t *testing.T) {
	t.Parallel()

	space := memkv.NewSpace()
	a := newNode(t, space, "a")

	a.ingest(t, 1000, "in-window")
	a.ingest(t, 9000, "out-of-window")

	entries, err := a.engine.Query(t.Context(), 500, 2000, nil, 0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "in-window", entries[0].Data)
}

func TestRecoversHeadFromWALAcrossRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	engine, err := shrimpengine.Open(t.Context(), shrimpengine.Options{Dir: dir, Node: "a"})
	require.NoError(t, err)

	var logs slog.Logs

	rl := logs.AddResource()
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("test")}
	r := sl.AddRecord()
	r.Timestamp = 1000
	r.Body = []byte("unflushed")

	_, _, err = engine.Ingest(t.Context(), logs)
	require.NoError(t, err)
	require.NoError(t, engine.SyncWAL())
	require.NoError(t, engine.Close(t.Context()))

	// Reopen without ever flushing: the record must come back from the write-ahead log.
	reopened, err := shrimpengine.Open(t.Context(), shrimpengine.Options{Dir: dir, Node: "a"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close(context.WithoutCancel(t.Context())) })

	entries, err := reopened.Query(t.Context(), 0, 1<<62, nil, 0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "unflushed", entries[0].Data)
}
