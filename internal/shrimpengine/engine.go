// Package shrimpengine is shrimpd's data plane: a thin, replication-aware shell around
// github.com/oteldb/storage's record engine.
//
// Storage owns everything about bytes on disk — the WAL, the columnar part format, blooms,
// merges, the fetch contract. shrimpengine adds exactly two things storage's single-node library
// leaves to its embedder:
//
//   - **Partitioning by writer.** Every node writes only into its own partition, which is a
//     separate record engine under the key prefix `{nodeID}/`. A record engine names its parts
//     from a node-local counter, so confining writes to one node per partition is what makes a
//     part's key identical on every replica — which in turn is what lets replication copy a part's
//     objects verbatim instead of re-encoding them.
//   - **A block descriptor per part.** The engine records a part's key and time range; shrimpd
//     writes a sidecar recording its logical block coverage, which is what [replication] needs to
//     decide that a merged part supersedes its sources.
//
// A node holds one writable engine (its own partition) and one read-only engine per peer
// partition, refreshed as replication installs parts into it. Queries fan out across all of them.
package shrimpengine

import (
	"context"
	"encoding/json"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	slog "github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/wal"
	"go.uber.org/zap"

	"github.com/oteldb/shrimpd/replication"
)

// Options configures an [Engine].
type Options struct {
	// Dir is the data directory. Parts live under Dir/parts, the write-ahead log under Dir/wal.
	Dir string
	// Node is this node's id, and the name of the partition it writes.
	Node string
	// OOOWindow rejects records older than the head's newest minus this (nanoseconds). Zero
	// disables the check.
	OOOWindow int64
	// Logger receives engine events. Nil ⇒ no logging.
	Logger *zap.Logger
}

// Engine is a node's log store: one writable partition plus a read-only view of every peer's.
type Engine struct {
	node string
	dir  string
	be   backend.Backend
	wal  *wal.SegmentWriter
	ooo  int64
	lg   *zap.Logger

	// mu guards the partition engine map. It is never held across engine I/O — a partition
	// engine is looked up under the lock and used outside it.
	mu    sync.RWMutex
	parts map[string]*recordengine.Engine
}

// Open creates or recovers the node's engine.
func Open(ctx context.Context, o Options) (*Engine, error) {
	if o.Node == "" {
		return nil, errors.New("shrimpengine: Node is required")
	}

	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}

	be, err := file.New(path.Join(o.Dir, "parts"))
	if err != nil {
		return nil, errors.Wrap(err, "open part backend")
	}

	w, err := wal.Create(path.Join(o.Dir, "wal"), 0)
	if err != nil {
		return nil, errors.Wrap(err, "open wal")
	}

	e := &Engine{
		node:  o.Node,
		dir:   o.Dir,
		be:    be,
		wal:   w,
		ooo:   o.OOOWindow,
		lg:    o.Logger.With(zap.String("node", o.Node)),
		parts: make(map[string]*recordengine.Engine),
	}

	// The own partition is the only one with a WAL: it is the only one accepting writes, so it
	// is the only one with an unflushed head worth recovering.
	own := recordengine.New(recordengine.Config{
		Schema:    slog.Schema,
		OOOWindow: o.OOOWindow,
		WAL:       w,
		Backend:   be,
		Prefix:    o.Node,
		Signal:    "log",
	})
	e.parts[o.Node] = own

	if err := own.LoadParts(ctx); err != nil {
		return nil, errors.Wrap(err, "load own parts")
	}

	if err := own.Replay(path.Join(o.Dir, "wal")); err != nil {
		return nil, errors.Wrap(err, "replay wal")
	}

	if err := e.discoverPartitions(ctx); err != nil {
		return nil, errors.Wrap(err, "discover partitions")
	}

	return e, nil
}

// Node returns this node's id, which is also the partition it writes.
func (e *Engine) Node() string { return e.node }

// Backend exposes the part object store, for the replication transport that copies objects
// between nodes.
func (e *Engine) Backend() backend.Backend { return e.be }

// Close flushes the writable partition and releases every engine.
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	engines := make([]*recordengine.Engine, 0, len(e.parts))

	for _, eng := range e.parts {
		engines = append(engines, eng)
	}

	e.mu.Unlock()

	var errs []error

	for _, eng := range engines {
		if err := eng.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return errors.Wrap(err, "close engines")
	}

	return nil
}

// discoverPartitions creates a read-only engine for every peer partition already present in the
// backend, so a restart serves replicated data before replication has run a single pass.
func (e *Engine) discoverPartitions(ctx context.Context) error {
	keys, err := e.be.List(ctx, "")
	if err != nil {
		return errors.Wrap(err, "list backend")
	}

	seen := make(map[string]struct{})

	for _, k := range keys {
		partition, ok := partitionOf(k)
		if !ok || partition == e.node {
			continue
		}

		seen[partition] = struct{}{}
	}

	for partition := range seen {
		if _, err := e.partitionEngine(ctx, partition); err != nil {
			return err
		}
	}

	return nil
}

// partitionEngine returns the engine for a partition, creating a read-only one on first use. A
// peer partition gets no WAL and no out-of-order window: nothing is ever appended to its head,
// only whole parts are installed underneath it.
func (e *Engine) partitionEngine(ctx context.Context, partition string) (*recordengine.Engine, error) {
	e.mu.RLock()
	eng, ok := e.parts[partition]
	e.mu.RUnlock()

	if ok {
		return eng, nil
	}

	e.mu.Lock()

	if eng, ok = e.parts[partition]; ok {
		e.mu.Unlock()

		return eng, nil
	}

	eng = recordengine.New(recordengine.Config{
		Schema:  slog.Schema,
		Backend: e.be,
		Prefix:  partition,
		Signal:  "log",
	})
	e.parts[partition] = eng
	e.mu.Unlock()

	if err := eng.LoadParts(ctx); err != nil {
		return nil, errors.Wrapf(err, "load parts of %q", partition)
	}

	e.lg.Info("opened peer partition", zap.String("partition", partition))

	return eng, nil
}

// engines snapshots every partition engine, own partition first.
func (e *Engine) engines() []*recordengine.Engine {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*recordengine.Engine, 0, len(e.parts))

	if own, ok := e.parts[e.node]; ok {
		out = append(out, own)
	}

	for name, eng := range e.parts {
		if name != e.node {
			out = append(out, eng)
		}
	}

	return out
}

// own returns the writable partition engine.
func (e *Engine) own() *recordengine.Engine {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.parts[e.node]
}

// Ingest appends a batch of log records to the writable partition's head. It is durable once it
// returns: the record engine writes to the WAL before acknowledging.
func (e *Engine) Ingest(_ context.Context, logs slog.Logs) (accepted, rejected int, err error) {
	eng := e.own()

	var appendErr error

	total := slog.Project(logs, func(b *recordengine.Batch) {
		if appendErr != nil {
			return
		}

		res, err := eng.AppendBatch(b, recordengine.AppendLimits{})
		if err != nil {
			appendErr = err

			return
		}

		rejected += res.Rejected()
	})

	if appendErr != nil {
		return 0, 0, errors.Wrap(appendErr, "append batch")
	}

	return total - rejected, rejected, nil
}

// HeadRecords reports how many unflushed records the writable partition holds — the signal the
// caller uses to decide when to flush.
func (e *Engine) HeadRecords() int { return e.own().HeadRecordCount() }

// Flush writes the writable partition's head to a new part and returns its descriptor, or false
// when there was nothing to flush.
//
// The descriptor is derived *after* the fact, from the part the engine actually wrote: the engine
// names its own parts, and shrimpd's job is to describe what it produced, not to dictate it. That
// is sound precisely because the partition has a single writer.
func (e *Engine) Flush(ctx context.Context) (Part, bool, error) {
	eng := e.own()

	before, err := e.partsOf(ctx, e.node)
	if err != nil {
		return Part{}, false, err
	}

	if err := eng.Flush(ctx); err != nil {
		return Part{}, false, errors.Wrap(err, "flush")
	}

	added, _, err := e.diff(ctx, before)
	if err != nil {
		return Part{}, false, err
	}

	if len(added) == 0 {
		return Part{}, false, nil
	}

	entry := added[0]

	p := flushed(e.node, seqOf(entry.Prefix), entry.MinTime, entry.MaxTime)
	if err := e.writeBlock(ctx, p); err != nil {
		return Part{}, false, err
	}

	e.lg.Debug("flushed part", zap.String("part", p.Prefix()))

	return p, true, nil
}

// Merge compacts the writable partition's parts into one, returning the merged descriptor and the
// sources it replaced. It reports false when there was nothing worth compacting.
func (e *Engine) Merge(ctx context.Context, retainFrom int64) (merged Part, sources []Part, ok bool, err error) {
	eng := e.own()

	before, err := e.partsOf(ctx, e.node)
	if err != nil {
		return Part{}, nil, false, err
	}

	if len(before) < 2 {
		return Part{}, nil, false, nil
	}

	if err := eng.Merge(ctx, retainFrom); err != nil {
		return Part{}, nil, false, errors.Wrap(err, "merge")
	}

	added, removed, err := e.diff(ctx, before)
	if err != nil {
		return Part{}, nil, false, err
	}

	if len(added) == 0 || len(removed) == 0 {
		return Part{}, nil, false, nil
	}

	entry := added[0]

	merged = mergedFrom(e.node, seqOf(entry.Prefix), removed, entry.MinTime, entry.MaxTime)
	if err := e.writeBlock(ctx, merged); err != nil {
		return Part{}, nil, false, err
	}

	e.lg.Debug("merged parts",
		zap.String("part", merged.Prefix()), zap.Int("sources", len(removed)))

	return merged, removed, true, nil
}

// diff compares the writable partition's current index against a previous snapshot, returning the
// entries added and the descriptors removed.
func (e *Engine) diff(ctx context.Context, before []Part) (added []bucketindex.Entry, removed []Part, err error) {
	ix, err := e.index(ctx, e.node)
	if err != nil {
		return nil, nil, err
	}

	had := make(map[string]Part, len(before))
	for _, p := range before {
		had[p.Prefix()] = p
	}

	now := make(map[string]struct{}, len(ix.Entries))

	for _, entry := range ix.Entries {
		now[entry.Prefix] = struct{}{}

		if _, existed := had[entry.Prefix]; !existed {
			added = append(added, entry)
		}
	}

	for prefix, p := range had {
		if _, still := now[prefix]; !still {
			removed = append(removed, p)
		}
	}

	replication.SortBlocks(removed)

	return added, removed, nil
}

// index reads a partition's bucket index, treating an absent one as empty.
func (e *Engine) index(ctx context.Context, partition string) (*bucketindex.Index, error) {
	ix, err := bucketindex.Load(ctx, e.be, partition+"/"+bucketindex.Object)
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return &bucketindex.Index{}, nil
		}

		return nil, errors.Wrapf(err, "load index of %q", partition)
	}

	return ix, nil
}

// writeBlock persists a part's block descriptor sidecar.
func (e *Engine) writeBlock(ctx context.Context, p Part) error {
	data, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "marshal block descriptor")
	}

	if err := e.be.Write(ctx, p.Prefix()+"/"+blockObject, data); err != nil {
		return errors.Wrap(err, "write block descriptor")
	}

	return nil
}

// readBlock reads a part's block descriptor sidecar.
func (e *Engine) readBlock(ctx context.Context, prefix string) (Part, error) {
	data, err := e.be.Read(ctx, prefix+"/"+blockObject)
	if err != nil {
		return Part{}, errors.Wrapf(err, "read block descriptor of %q", prefix)
	}

	var p Part
	if err := json.Unmarshal(data, &p); err != nil {
		return Part{}, errors.Wrapf(err, "decode block descriptor of %q", prefix)
	}

	return p, nil
}

// partsOf lists a partition's parts as block descriptors.
func (e *Engine) partsOf(ctx context.Context, partition string) ([]Part, error) {
	ix, err := e.index(ctx, partition)
	if err != nil {
		return nil, err
	}

	out := make([]Part, 0, len(ix.Entries))

	for _, entry := range ix.Entries {
		p, err := e.readBlock(ctx, entry.Prefix)
		if err != nil {
			// A part without a descriptor was written by a version that did not keep one, or
			// its sidecar was lost. Reconstruct the conservative equivalent: a level-0 part
			// covering its own sequence, which supersedes nothing and is superseded by any
			// merge spanning it.
			p = flushed(partition, seqOf(entry.Prefix), entry.MinTime, entry.MaxTime)
		}

		out = append(out, p)
	}

	return out, nil
}

// BodyContains returns the condition selecting records whose body contains term,
// case-insensitively.
//
// The token hint lets the engine skip whole parts using their body bloom before decoding
// anything; Match is still applied per row, so the hint only ever saves work.
func BodyContains(term string) fetch.Condition {
	lowered := strings.ToLower(term)

	return fetch.Condition{
		Column: slog.ColBody,
		Tokens: [][]byte{[]byte(lowered)},
		Match: func(v signal.Value) bool {
			// AppendText, not Bytes: the record engine hands a bytes column over as a *string*
			// value, and Value.Bytes returns nil for anything but KindBytes.
			return strings.Contains(strings.ToLower(string(v.AppendText(nil))), lowered)
		},
	}
}

// Entry is one log record as shrimpd's HTTP API presents it.
type Entry struct {
	Timestamp int64  `json:"timestamp"`
	Data      string `json:"data"`
}

// Query returns the records in the inclusive window matching cond, newest-agnostic (the caller
// sorts). It fans out across every partition — the local one and every replicated peer's.
func (e *Engine) Query(ctx context.Context, from, to int64, cond []fetch.Condition, limit int) ([]Entry, error) {
	req := fetch.Request{
		Signal:        signal.Log,
		Start:         from,
		End:           to,
		Conditions:    cond,
		AllConditions: len(cond) > 0,
		Projection:    []string{slog.ColBody},
	}

	var out []Entry

	for _, eng := range e.engines() {
		it, err := eng.Fetch(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "fetch")
		}

		err = drain(ctx, it, func(b *fetch.Batch) bool {
			body, ok := b.Column(slog.ColBody)
			for i, ts := range b.Timestamps {
				entry := Entry{Timestamp: ts}
				if ok && i < len(body.Bytes) {
					entry.Data = string(body.Bytes[i])
				}

				out = append(out, entry)

				if limit > 0 && len(out) >= limit {
					return false
				}
			}

			return true
		})
		if err != nil {
			return nil, err
		}

		if limit > 0 && len(out) >= limit {
			break
		}
	}

	return out, nil
}

// drain iterates batches until fn returns false or the iterator is exhausted.
func drain(ctx context.Context, it fetch.Iterator, fn func(*fetch.Batch) bool) error {
	defer func() { _ = it.Close() }()

	for {
		b, err := it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return errors.Wrap(err, "iterate batches")
		}

		if !fn(b) {
			return nil
		}
	}
}

// SyncWAL fsyncs the writable partition's write-ahead log.
func (e *Engine) SyncWAL() error {
	if err := e.own().SyncWAL(); err != nil {
		return errors.Wrap(err, "sync wal")
	}

	return nil
}

// seqOf parses the numeric last component of a part key prefix.
func seqOf(prefix string) int {
	n, err := strconv.Atoi(path.Base(prefix))
	if err != nil {
		return 0
	}

	return n
}

// partitionOf returns the partition component of a backend key — everything before the first
// separator — and whether the key had one at all.
func partitionOf(key string) (partition string, ok bool) {
	for i := range len(key) {
		if key[i] == '/' {
			return key[:i], true
		}
	}

	return "", false
}
