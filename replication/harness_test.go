package replication_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
	"github.com/oteldb/shrimpd/replication/memkv"
)

// testBlock is the replicated unit under test: a part covering a block range in a partition,
// carrying a payload so a test can assert that the *data* moved, not just the metadata.
type testBlock struct {
	Part  string            `json:"partition"`
	R     replication.Range `json:"range"`
	Bytes int               `json:"bytes"`
}

var _ replication.Block = testBlock{}

func (b testBlock) Name() string      { return b.R.String() }
func (b testBlock) Partition() string { return b.Part }

func (b testBlock) Range() replication.Range { return b.R }

func newBlock(partition string, first, last, level uint64) testBlock {
	return testBlock{
		Part:  partition,
		R:     replication.Range{First: first, Last: last, Level: level},
		Bytes: int(last-first+1) * 100,
	}
}

// fakeStore is an in-memory [replication.Store]. Fetch reaches into the peer store registered
// under the given address, so a fetch exercises the real source-selection path without any
// transport.
type fakeStore struct {
	name  string
	peers *peerRegistry

	mu     sync.Mutex
	blocks map[string]testBlock

	// failFetch, when set, makes every Fetch fail — the knob for testing backoff and fallback
	// to another replica.
	failFetch bool
	// refuseMerge makes Merge decline, forcing the fetch path.
	refuseMerge bool

	fetched int
	merged  int
	dropped int
}

var _ replication.Store[testBlock] = (*fakeStore)(nil)

// peerRegistry maps a replica's advertised address to its store, standing in for the network.
type peerRegistry struct {
	mu     sync.Mutex
	stores map[string]*fakeStore
}

func newPeerRegistry() *peerRegistry {
	return &peerRegistry{stores: make(map[string]*fakeStore)}
}

func (p *peerRegistry) add(addr string, s *fakeStore) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stores[addr] = s
}

func (p *peerRegistry) get(addr string) (*fakeStore, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, ok := p.stores[addr]

	return s, ok
}

func newFakeStore(name string, peers *peerRegistry) *fakeStore {
	return &fakeStore{name: name, peers: peers, blocks: make(map[string]testBlock)}
}

func (s *fakeStore) Local(context.Context) ([]testBlock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := slices.Collect(maps.Values(s.blocks))
	replication.SortBlocks(out)

	return out, nil
}

func (s *fakeStore) Fetch(_ context.Context, addr string, b testBlock) error {
	s.mu.Lock()
	fail := s.failFetch
	s.mu.Unlock()

	if fail {
		return errors.New("fetch disabled")
	}

	peer, ok := s.peers.get(addr)
	if !ok {
		return errors.Errorf("no peer at %q", addr)
	}

	peer.mu.Lock()
	src, held := peer.blocks[replication.Key(b)]
	peer.mu.Unlock()

	if !held {
		return errors.Errorf("peer %s does not hold %s", peer.name, replication.Key(b))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.blocks[replication.Key(b)] = src
	s.fetched++

	return nil
}

func (s *fakeStore) Merge(_ context.Context, src []testBlock, dst testBlock) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.refuseMerge {
		return replication.ErrMergeUnsupported
	}

	total := 0

	for _, b := range src {
		held, ok := s.blocks[replication.Key(b)]
		if !ok {
			return errors.Errorf("missing merge source %s", replication.Key(b))
		}

		total += held.Bytes
	}

	dst.Bytes = total
	s.blocks[replication.Key(dst)] = dst
	s.merged++

	return nil
}

func (s *fakeStore) Drop(_ context.Context, b testBlock) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.blocks[replication.Key(b)]; ok {
		s.dropped++
	}

	delete(s.blocks, replication.Key(b))

	return nil
}

// put installs a block directly, as a local flush would.
func (s *fakeStore) put(b testBlock) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.blocks[replication.Key(b)] = b
}

func (s *fakeStore) keys() []string {
	blocks, _ := s.Local(context.Background())

	return keysOf(blocks)
}

func (s *fakeStore) counters() (fetched, merged, dropped int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.fetched, s.merged, s.dropped
}

func (s *fakeStore) set(fn func(*fakeStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fn(s)
}

// node is one replica in the test cluster.
type node struct {
	name  string
	addr  string
	kv    *memkv.Store
	store *fakeStore
	repl  *replication.Replication[testBlock]
}

// cluster is a set of replicas sharing one metadata keyspace, driven by explicit ticks rather
// than by wall-clock timers: every test is deterministic and finishes in microseconds.
type cluster struct {
	t     *testing.T
	space *memkv.Space
	peers *peerRegistry
	nodes map[string]*node
}

const testPrefix = "/test/blocks"

func newCluster(t *testing.T) *cluster {
	t.Helper()

	return &cluster{
		t:     t,
		space: memkv.NewSpace(),
		peers: newPeerRegistry(),
		nodes: make(map[string]*node),
	}
}

// add brings up a replica and runs its Start phase.
func (c *cluster) add(name string) *node {
	c.t.Helper()

	addr := name + ":8080"
	store := newFakeStore(name, c.peers)
	c.peers.add(addr, store)

	kv := c.space.Session()

	repl, err := replication.New(replication.Config[testBlock]{
		KV:      kv,
		Store:   store,
		Prefix:  testPrefix,
		Replica: name,
		Addr:    addr,
	})
	require.NoError(c.t, err)
	require.NoError(c.t, repl.Start(c.t.Context()))

	n := &node{name: name, addr: addr, kv: kv, store: store, repl: repl}
	c.nodes[name] = n

	c.t.Cleanup(func() { _ = kv.Close() })

	return n
}

// converge runs pull+execute on every replica until nothing changes, or fails the test. It
// replaces sleeping on the poll interval: the loops are the same, only the clock is not.
func (c *cluster) converge() {
	c.t.Helper()

	ctx := c.t.Context()

	const maxRounds = 50

	prev := ""

	for range maxRounds {
		for _, n := range c.nodes {
			require.NoError(c.t, replication.PullForTest(ctx, n.repl))
			replication.ExecuteForTest(ctx, n.repl)
		}

		state := c.snapshot()
		if state == prev {
			return
		}

		prev = state
	}

	c.t.Fatalf("cluster did not converge; last state:\n%s", prev)
}

// snapshot renders every replica's block set, for convergence detection and failure output.
func (c *cluster) snapshot() string {
	names := slices.Sorted(maps.Keys(c.nodes))

	var b []byte

	for _, name := range names {
		b = fmt.Appendf(b, "%s: %v\n", name, c.nodes[name].store.keys())
	}

	return string(b)
}

// requireConverged asserts that every replica holds exactly want.
func (c *cluster) requireConverged(want ...string) {
	c.t.Helper()

	for _, name := range slices.Sorted(maps.Keys(c.nodes)) {
		require.Equal(c.t, want, c.nodes[name].store.keys(), "replica %s", name)
	}
}

// flush simulates a local write on n: allocate a block number, install the part, announce it.
func (n *node) flush(t *testing.T, partition string) testBlock {
	t.Helper()

	num, err := n.repl.AllocateBlock(t.Context(), partition)
	require.NoError(t, err)

	b := newBlock(partition, num, num, 0)
	n.store.put(b)
	require.NoError(t, n.repl.Commit(t.Context(), b))

	return b
}

// merge simulates a local merge on n and announces it.
func (n *node) merge(t *testing.T, src ...testBlock) testBlock {
	t.Helper()

	require.NotEmpty(t, src)

	dst := testBlock{
		Part: src[0].Part,
		R: replication.Range{
			First: src[0].R.First,
			Last:  src[len(src)-1].R.Last,
			Level: src[0].R.Level + 1,
		},
	}

	require.NoError(t, n.store.Merge(t.Context(), src, dst))

	for _, b := range src {
		require.NoError(t, n.store.Drop(t.Context(), b))
	}

	require.NoError(t, n.repl.CommitMerge(t.Context(), src, dst))

	return dst
}

// waitFor is used only by the one test that exercises the real Run loop.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("condition not met within timeout")
}
