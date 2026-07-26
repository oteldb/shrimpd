package e2e

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/shrimpd/internal/shrimpapi"
	"github.com/oteldb/shrimpd/internal/shrimpnode"
	"github.com/oteldb/shrimpd/replication/etcdkv"
)

// node is one shrimpd process, run in-process against the shared etcd container.
type node struct {
	id   string
	addr string
	base string
	node *shrimpnode.Node
}

// startNode brings up a node with its own data directory and HTTP server, and runs it until the
// test ends. dir is reused across restarts when a test needs to prove recovery.
func startNode(ctx context.Context, t testing.TB, etcdEndpoint, id, dir string) *node {
	t.Helper()

	must := require.New(t)
	lg := zaptest.NewLogger(t)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)

	waitEtcd(ctx, t, cli)

	kv, err := etcdkv.New(ctx, cli, 0)
	must.NoError(err)

	addr := freeLocalAddr(t)

	n, err := shrimpnode.New(ctx, shrimpnode.Options{
		Dir:  dir,
		ID:   id,
		Addr: addr,
		KV:   kv,
		// Short intervals: an e2e test should observe convergence, not wait out production
		// timers.
		FlushInterval: 200 * time.Millisecond,
		MergeInterval: time.Second,
		Logger:        lg,
	})
	must.NoError(err)

	srv, err := shrimpapi.NewServer(addr, n, lg)
	must.NoError(err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, 2)

	go func() { _ = n.Run(runCtx); done <- struct{}{} }()
	go func() { _ = srv.Run(runCtx); done <- struct{}{} }()

	t.Cleanup(func() {
		cancel()

		for range 2 {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
			}
		}

		_ = n.Close(context.WithoutCancel(ctx))
		_ = kv.Close()
		_ = cli.Close()
	})

	base := "http://" + addr
	waitHTTP(ctx, t, base+"/state")

	return &node{id: id, addr: addr, base: base, node: n}
}

// ingest posts entries in shrimpd's native batch shape.
func (n *node) ingest(ctx context.Context, t testing.TB, base int64, bodies ...string) {
	t.Helper()

	type entry struct {
		Timestamp int64  `json:"timestamp"`
		Data      string `json:"data"`
	}

	batch := struct {
		Data []entry `json:"data"`
	}{}

	for i, body := range bodies {
		batch.Data = append(batch.Data, entry{Timestamp: base + int64(i), Data: body})
	}

	postJSON(ctx, t, n.base+"/ingest", batch)
}

// queryResponse mirrors the /query envelope.
type queryResponse struct {
	Data []struct {
		Timestamp int64  `json:"timestamp"`
		Data      string `json:"data"`
	} `json:"data"`
}

// bodies queries the node and returns the matching record bodies, sorted.
func (n *node) bodies(ctx context.Context, t testing.TB, term string) []string {
	t.Helper()

	q := url.Values{"from": {"0"}, "to": {strconv.FormatInt(1<<62, 10)}}
	if term != "" {
		q.Set("term", term)
	}

	var resp queryResponse

	getJSON(ctx, t, n.base+"/query?"+q.Encode(), &resp)

	out := make([]string, 0, len(resp.Data))
	for _, e := range resp.Data {
		out = append(out, e.Data)
	}

	slices.Sort(out)

	return out
}

// waitForBodies polls until the node reports exactly want.
func (n *node) waitForBodies(ctx context.Context, t testing.TB, want []string) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)

	var got []string

	for time.Now().Before(deadline) {
		if got = n.bodies(ctx, t, ""); slices.Equal(got, want) {
			return
		}

		select {
		case <-ctx.Done():
			require.Fail(t, "context canceled while waiting for convergence")
		case <-time.After(200 * time.Millisecond):
		}
	}

	require.Equal(t, want, got, "node %s never converged", n.id)
}

func (n *node) post(ctx context.Context, t testing.TB, path string) {
	t.Helper()

	postJSON(ctx, t, n.base+path, struct{}{})
}

func TestSingleNodeIngestAndQuery(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)
	n := startNode(ctx, t, endpoint, "node1", t.TempDir())

	n.ingest(ctx, t, 1000, "alpha", "bravo", "charlie")

	// Readable straight from the head, before any flush.
	require.Equal(t, []string{"alpha", "bravo", "charlie"}, n.bodies(ctx, t, ""))

	// Still readable once it is a part on disk.
	n.post(ctx, t, "/flush")
	require.Equal(t, []string{"alpha", "bravo", "charlie"}, n.bodies(ctx, t, ""))
}

func TestQueryTermFiltersBody(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)
	n := startNode(ctx, t, endpoint, "node1", t.TempDir())

	n.ingest(ctx, t, 1000, "error: disk full", "info: all good", "ERROR: retrying")
	n.post(ctx, t, "/flush")

	require.Equal(t, []string{"ERROR: retrying", "error: disk full"}, n.bodies(ctx, t, "error"))
}

func TestPartReplicatesBetweenNodes(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)

	a := startNode(ctx, t, endpoint, "node1", t.TempDir())
	b := startNode(ctx, t, endpoint, "node2", t.TempDir())

	a.ingest(ctx, t, 1000, "from-a")
	b.ingest(ctx, t, 2000, "from-b")

	a.post(ctx, t, "/flush")
	b.post(ctx, t, "/flush")

	want := []string{"from-a", "from-b"}
	a.waitForBodies(ctx, t, want)
	b.waitForBodies(ctx, t, want)
}

func TestMergedPartReplacesSources(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)

	a := startNode(ctx, t, endpoint, "node1", t.TempDir())
	b := startNode(ctx, t, endpoint, "node2", t.TempDir())

	var want []string

	for i := range 3 {
		body := fmt.Sprintf("line-%d", i)
		want = append(want, body)

		a.ingest(ctx, t, int64(1000+i*100), body)
		a.post(ctx, t, "/flush")
	}

	slices.Sort(want)
	b.waitForBodies(ctx, t, want)

	// Compacting on the writer must leave both nodes with the same data and, on the peer, a
	// single part covering it.
	a.post(ctx, t, "/compact")

	a.waitForBodies(ctx, t, want)
	b.waitForBodies(ctx, t, want)

	require.Eventually(t, func() bool {
		state, err := b.node.Inspect(ctx)
		require.NoError(t, err)

		count := 0

		for _, p := range state.Parts {
			if p.Partition() == "node1" {
				count++
			}
		}

		return count == 1
	}, 60*time.Second, 200*time.Millisecond, "peer should converge on the merged part alone")
}

func TestLateJoinerClonesExistingData(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)

	a := startNode(ctx, t, endpoint, "node1", t.TempDir())
	a.ingest(ctx, t, 1000, "before-join")
	a.post(ctx, t, "/flush")

	// node2 joins a cluster that already has a log: it must clone rather than replay.
	b := startNode(ctx, t, endpoint, "node2", t.TempDir())
	b.waitForBodies(ctx, t, []string{"before-join"})
}

func TestRestartRecoversUnflushedRecords(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)
	dir := t.TempDir()

	func() {
		inner, cancel := context.WithCancel(ctx)
		defer cancel()

		n := startNode(inner, t, endpoint, "node1", dir)
		n.ingest(inner, t, 1000, "unflushed")
		require.Equal(t, []string{"unflushed"}, n.bodies(inner, t, ""))
	}()

	// Reopening the same directory must bring the record back from the write-ahead log even
	// though it was never flushed into a part.
	restarted := startNode(ctx, t, endpoint, "node1", dir)
	require.Contains(t, restarted.bodies(ctx, t, ""), "unflushed")
}
