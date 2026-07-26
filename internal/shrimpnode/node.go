// Package shrimpnode assembles a shrimpd node from its two halves: the local record store
// ([shrimpengine.Engine]) and the replicated log ([replication.Replication]).
//
// It owns the one rule that ties them together — **nothing becomes visible to the cluster until
// it is durable locally**. A flush writes a part and only then announces it; a merge produces the
// merged part and only then announces that it supersedes its sources. Peers therefore never learn
// about a part before some replica can actually serve it.
package shrimpnode

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/query/fetch"
	slog "github.com/oteldb/storage/signal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/shrimpd/internal/shrimpengine"
	"github.com/oteldb/shrimpd/replication"
)

// Maintenance defaults.
const (
	// DefaultFlushInterval is how often an idle head is written out. It bounds how long a
	// record can sit unreplicated on the node that received it.
	DefaultFlushInterval = 5 * time.Second
	// DefaultFlushRecords flushes early once the head holds this many records, so a burst does
	// not wait out the interval.
	DefaultFlushRecords = 10_000
	// DefaultMergeInterval is how often flushed parts are compacted into one.
	DefaultMergeInterval = 60 * time.Second
)

// Options configures a [Node].
type Options struct {
	// Dir is the data directory.
	Dir string
	// ID is the node id: its replica name and the partition it writes.
	ID string
	// Addr is the advertised host:port peers fetch parts from.
	Addr string
	// KV is the metadata store replication coordinates through.
	KV replication.KV
	// Prefix is the KV key prefix for this cluster's replication state. Empty ⇒ "/shrimpd".
	Prefix string
	// Retention drops records older than this on merge. Zero disables it.
	Retention time.Duration

	FlushInterval time.Duration
	FlushRecords  int
	MergeInterval time.Duration

	Logger *zap.Logger
}

func (o *Options) setDefaults() {
	if o.Prefix == "" {
		o.Prefix = "/shrimpd"
	}

	if o.FlushInterval <= 0 {
		o.FlushInterval = DefaultFlushInterval
	}

	if o.FlushRecords <= 0 {
		o.FlushRecords = DefaultFlushRecords
	}

	if o.MergeInterval <= 0 {
		o.MergeInterval = DefaultMergeInterval
	}

	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
}

// Node is one shrimpd process's store and its participation in the cluster.
type Node struct {
	opts   Options
	engine *shrimpengine.Engine
	repl   *replication.Replication[shrimpengine.Part]
	lg     *zap.Logger
}

// New opens the node's local store and prepares its replication. It performs no cluster I/O;
// [Node.Run] does that.
func New(ctx context.Context, o Options) (*Node, error) {
	o.setDefaults()

	if o.KV == nil {
		return nil, errors.New("shrimpnode: KV is required")
	}

	engine, err := shrimpengine.Open(ctx, shrimpengine.Options{
		Dir:    o.Dir,
		Node:   o.ID,
		Logger: o.Logger,
	})
	if err != nil {
		return nil, err
	}

	repl, err := replication.New(replication.Config[shrimpengine.Part]{
		KV:      o.KV,
		Store:   shrimpengine.NewStore(engine, shrimpengine.NewClient(nil), o.Logger),
		Prefix:  o.Prefix + "/parts",
		Replica: o.ID,
		Addr:    o.Addr,
		Logger:  o.Logger,
	})
	if err != nil {
		_ = engine.Close(ctx)

		return nil, err
	}

	return &Node{opts: o, engine: engine, repl: repl, lg: o.Logger.With(zap.String("node", o.ID))}, nil
}

// Engine exposes the local store, for the HTTP handlers that read from it and serve its objects
// to peers.
func (n *Node) Engine() *shrimpengine.Engine { return n.engine }

// Replication exposes the replication state, for operator endpoints.
func (n *Node) Replication() *replication.Replication[shrimpengine.Part] { return n.repl }

// Close flushes and releases the local store.
func (n *Node) Close(ctx context.Context) error { return n.engine.Close(ctx) }

// Run joins the cluster and drives replication and maintenance until ctx is canceled.
func (n *Node) Run(ctx context.Context) error {
	if err := n.repl.Start(ctx); err != nil {
		return errors.Wrap(err, "start replication")
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return n.repl.Run(ctx) })
	eg.Go(func() error { return n.maintain(ctx) })

	return eg.Wait()
}

// maintain drives the local write side: flush the head on a timer or when it grows, compact
// periodically, and trim the replicated log.
func (n *Node) maintain(ctx context.Context) error {
	flushTick := time.NewTicker(n.opts.FlushInterval)
	mergeTick := time.NewTicker(n.opts.MergeInterval)

	defer flushTick.Stop()
	defer mergeTick.Stop()

	for {
		select {
		case <-ctx.Done():
			// A clean shutdown flushes what the head holds, so a restart does not have to
			// replay it — but the WAL still covers us if this fails.
			if err := n.Flush(context.WithoutCancel(ctx)); err != nil {
				n.lg.Warn("final flush", zap.Error(err))
			}

			return ctx.Err()
		case <-flushTick.C:
			if n.engine.HeadRecords() == 0 {
				continue
			}

			if err := n.Flush(ctx); err != nil {
				n.lg.Error("flush", zap.Error(err))
			}
		case <-mergeTick.C:
			if err := n.Merge(ctx); err != nil {
				n.lg.Error("merge", zap.Error(err))
			}

			if err := n.repl.CleanupLog(ctx); err != nil {
				n.lg.Warn("cleanup log", zap.Error(err))
			}
		}
	}
}

// Ingest writes a batch to the head, flushing early if it has grown past the threshold.
func (n *Node) Ingest(ctx context.Context, logs slog.Logs) (accepted, rejected int, err error) {
	accepted, rejected, err = n.engine.Ingest(ctx, logs)
	if err != nil {
		return 0, 0, err
	}

	if n.engine.HeadRecords() >= n.opts.FlushRecords {
		if err := n.Flush(ctx); err != nil {
			// The records are durable in the WAL and still queryable from the head; a failed
			// flush delays replication but loses nothing.
			n.lg.Warn("threshold flush", zap.Error(err))
		}
	}

	return accepted, rejected, nil
}

// Query reads records across every partition this node holds.
func (n *Node) Query(ctx context.Context, from, to int64, term string, limit int) ([]shrimpengine.Entry, error) {
	var cond []fetch.Condition
	if term != "" {
		cond = append(cond, shrimpengine.BodyContains(term))
	}

	return n.engine.Query(ctx, from, to, cond, limit)
}

// Flush writes the head to a part and announces it. Announcing second is the whole point: the
// part is on disk and servable before any peer is told it exists.
func (n *Node) Flush(ctx context.Context) error {
	part, ok, err := n.engine.Flush(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	if err := n.repl.Commit(ctx, part); err != nil {
		return errors.Wrapf(err, "announce part %s", part.Prefix())
	}

	n.lg.Info("flushed", zap.String("part", part.Prefix()))

	return nil
}

// Merge compacts this node's parts into one and announces that it supersedes the sources.
func (n *Node) Merge(ctx context.Context) error {
	var retainFrom int64
	if n.opts.Retention > 0 {
		retainFrom = time.Now().Add(-n.opts.Retention).UnixNano()
	}

	merged, sources, ok, err := n.engine.Merge(ctx, retainFrom)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	if err := n.repl.CommitMerge(ctx, sources, merged); err != nil {
		return errors.Wrapf(err, "announce merge %s", merged.Prefix())
	}

	n.lg.Info("merged", zap.String("part", merged.Prefix()), zap.Int("sources", len(sources)))

	return nil
}

// State is a node's operator-visible status.
type State struct {
	Node        string              `json:"node"`
	Addr        string              `json:"addr"`
	HeadRecords int                 `json:"head_records"`
	Parts       []shrimpengine.Part `json:"parts"`
	Replication replication.State   `json:"replication"`
	LagRecords  uint64              `json:"lag_records"`
	// AwaitingClone is true while this node must rebuild from a peer but none is reachable. It
	// holds untrustworthy data and is not serving the cluster until it clears.
	AwaitingClone bool `json:"awaiting_clone"`
}

// Inspect reports what this node holds and how far it trails the log.
func (n *Node) Inspect(ctx context.Context) (State, error) {
	parts, err := shrimpengine.NewStore(n.engine, nil, n.lg).Local(ctx)
	if err != nil {
		return State{}, err
	}

	lag, _, err := n.repl.Lag(ctx)
	if err != nil {
		return State{}, err
	}

	return State{
		Node:          n.opts.ID,
		Addr:          n.opts.Addr,
		HeadRecords:   n.engine.HeadRecords(),
		Parts:         parts,
		Replication:   n.repl.Inspect(),
		LagRecords:    lag,
		AwaitingClone: n.repl.AwaitingClone(),
	}, nil
}
