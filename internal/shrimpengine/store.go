package shrimpengine

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"go.uber.org/zap"

	"github.com/oteldb/shrimpd/replication"
)

// Store adapts an [Engine] to [replication.Store]. It moves *objects*, never engine state: a
// fetched part's column objects are copied byte-for-byte and the partition's bucket index is
// rewritten to name them, after which the record engine's ordinary part-loading path picks them
// up. That is the same commit discipline a local flush uses, so a crash mid-fetch leaves an
// unreferenced orphan rather than a half-visible part.
type Store struct {
	engine *Engine
	client *Client
	lg     *zap.Logger

	// indexMu serializes the read-modify-write of a partition's bucket index. Replication
	// executes non-conflicting queue entries concurrently, and two entries installing different
	// parts of the same partition would otherwise each load the index, add their own entry, and
	// save — silently losing one of the two parts. Only the index update is guarded; the object
	// copying that dominates a fetch happens outside it.
	indexMu sync.Mutex
}

var _ replication.Store[Part] = (*Store)(nil)

// NewStore wraps an engine for replication. client fetches objects from peers.
func NewStore(e *Engine, client *Client, lg *zap.Logger) *Store {
	if lg == nil {
		lg = zap.NewNop()
	}

	return &Store{engine: e, client: client, lg: lg}
}

// Local implements [replication.Store]: every part this node holds, across all partitions.
func (s *Store) Local(ctx context.Context) ([]Part, error) {
	partitions, err := s.partitions(ctx)
	if err != nil {
		return nil, err
	}

	var out []Part

	for _, partition := range partitions {
		parts, err := s.engine.partsOf(ctx, partition)
		if err != nil {
			return nil, err
		}

		out = append(out, parts...)
	}

	replication.SortBlocks(out)

	return out, nil
}

// Fetch implements [replication.Store]: copy a peer's part objects, then publish the part by
// adding it to the local partition index.
//
// The index is written **last**, exactly as flush does. Until then the copied objects are
// invisible to readers and to the engine, so an interrupted fetch costs a retry and some wasted
// disk, never a part that is referenced but incomplete.
func (s *Store) Fetch(ctx context.Context, addr string, p Part) error {
	if s.client == nil {
		return errors.New("shrimpengine: no fetch client configured")
	}

	be := s.engine.Backend()

	keys, err := s.client.List(ctx, addr, p.Prefix()+"/")
	if err != nil {
		return errors.Wrapf(err, "list %s on %s", p.Prefix(), addr)
	}

	if len(keys) == 0 {
		return errors.Errorf("peer %s has no objects for %s", addr, p.Prefix())
	}

	for _, key := range keys {
		data, err := s.client.Object(ctx, addr, key)
		if err != nil {
			return errors.Wrapf(err, "fetch %s from %s", key, addr)
		}

		if err := be.Write(ctx, key, data); err != nil {
			return errors.Wrapf(err, "write %s", key)
		}
	}

	// The part's columns are meaningless without the partition's stream identity index: a part
	// stores rows keyed by stream id, and only this object maps an id back to the resource and
	// scope that identify it. It lives beside the parts rather than inside one and grows as new
	// streams appear, so it is re-copied on every fetch — and always *before* the bucket index
	// makes the part visible, so a reader never resolves a row to an identity that is not there.
	if err := s.fetchStreams(ctx, addr, p.Node); err != nil {
		return err
	}

	if err := s.publish(ctx, p); err != nil {
		return err
	}

	s.lg.Debug("fetched part", zap.String("part", p.Prefix()), zap.String("from", addr))

	return nil
}

// fetchStreams copies a partition's stream identity index from a peer. An absent index means the
// peer has not written one yet, which is not an error — the part cannot reference identities that
// do not exist.
func (s *Store) fetchStreams(ctx context.Context, addr, partition string) error {
	key := partition + "/" + streamsObject

	data, found, err := s.client.ObjectIfExists(ctx, addr, key)
	if err != nil {
		return errors.Wrapf(err, "fetch %s from %s", key, addr)
	}

	if !found {
		return nil
	}

	if err := s.engine.Backend().Write(ctx, key, data); err != nil {
		return errors.Wrapf(err, "write %s", key)
	}

	return nil
}

// Merge implements [replication.Store] by declining.
//
// The merge has already been performed by the replica that logged it, and its result is a single
// immutable part sitting on that replica's disk. Copying it costs one transfer; reproducing it
// costs decoding and re-encoding every source part on every replica, to arrive at bytes that must
// match anyway. ClickHouse makes the same trade whenever the merged part can simply be fetched.
func (s *Store) Merge(context.Context, []Part, Part) error {
	return replication.ErrMergeUnsupported
}

// Drop implements [replication.Store]: unreference the part, then delete its objects.
//
// The order is the reverse of Fetch and for the same reason — a reader can only reach objects the
// index names, so removing the reference first means a concurrent reader never opens a part whose
// objects are being deleted underneath it.
func (s *Store) Drop(ctx context.Context, p Part) error {
	be := s.engine.Backend()
	indexKey := p.Node + "/" + bucketindex.Object

	if err := s.unreference(ctx, p, indexKey); err != nil {
		return errors.Wrapf(err, "unreference %s", p.Prefix())
	}

	keys, err := be.List(ctx, p.Prefix()+"/")
	if err != nil {
		return errors.Wrapf(err, "list objects of %s", p.Prefix())
	}

	for _, key := range keys {
		if err := be.Delete(ctx, key); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "delete %s", key)
		}
	}

	s.lg.Debug("dropped part", zap.String("part", p.Prefix()))

	return nil
}

// unreference removes a part from its partition index, under the index lock. Dropping a part that
// the index no longer names is a no-op, which is what makes [Store.Drop] idempotent.
func (s *Store) unreference(ctx context.Context, p Part, indexKey string) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	ix, err := s.engine.index(ctx, p.Node)
	if err != nil {
		return err
	}

	if !ix.Remove(p.Prefix()) {
		return nil
	}

	if err := ix.Save(ctx, s.engine.Backend(), indexKey); err != nil {
		return errors.Wrapf(err, "save index of %q", p.Node)
	}

	return s.reload(ctx, p.Node)
}

// publish adds a fetched part to its partition index — the commit point that makes it visible.
func (s *Store) publish(ctx context.Context, p Part) error {
	indexKey := p.Node + "/" + bucketindex.Object

	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	ix, err := s.engine.index(ctx, p.Node)
	if err != nil {
		return err
	}

	ix.Add(bucketindex.Entry{Prefix: p.Prefix(), MinTime: p.MinTime, MaxTime: p.MaxTime})

	if err := ix.Save(ctx, s.engine.Backend(), indexKey); err != nil {
		return errors.Wrapf(err, "save index of %q", p.Node)
	}

	return s.reload(ctx, p.Node)
}

// reload makes a partition's engine observe the current index.
func (s *Store) reload(ctx context.Context, partition string) error {
	eng, err := s.engine.partitionEngine(ctx, partition)
	if err != nil {
		return err
	}

	if partition == s.engine.node {
		// The writable partition holds an unflushed head; a full reload would be wrong. Its
		// index is only ever changed by its own flush and merge, which already publish.
		return nil
	}

	if err := eng.LoadParts(ctx); err != nil {
		return errors.Wrapf(err, "reload parts of %q", partition)
	}

	return nil
}

// partitions lists every partition present in the backend.
func (s *Store) partitions(ctx context.Context) ([]string, error) {
	keys, err := s.engine.Backend().List(ctx, "")
	if err != nil {
		return nil, errors.Wrap(err, "list backend")
	}

	seen := make(map[string]struct{})
	out := []string{}

	for _, k := range keys {
		partition, ok := partitionOf(k)
		if !ok {
			continue
		}

		if _, dup := seen[partition]; dup {
			continue
		}

		seen[partition] = struct{}{}

		out = append(out, partition)
	}

	return out, nil
}
