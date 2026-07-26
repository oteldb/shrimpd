package replication

import (
	"context"
	"strconv"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

// lostYes and lostNo are the two values of a replica's `lost` flag. A lost replica's local part
// set may be arbitrarily behind or ahead of the log, so it must not serve as a fetch source and
// must rebuild from a peer before it participates.
const (
	lostYes = "1"
	lostNo  = "0"
)

// register publishes this replica in the metadata store and takes an ephemeral liveness key.
//
// A replica appearing for the first time in a cluster that already has a log starts out lost: it
// holds nothing, and the log alone may no longer describe how to get everything (it is trimmed).
// Cloning from a peer is both correct and much faster than replaying.
func (r *Replication[B]) register(ctx context.Context) error {
	_, ptrRev, err := getUint(ctx, r.kv, r.pointerKey(r.name))
	if err != nil {
		return err
	}

	if ptrRev == 0 {
		logSeq, _, err := getUint(ctx, r.kv, r.logSeqKey())
		if err != nil {
			return err
		}

		lost := lostNo
		if logSeq > 0 {
			lost = lostYes
		}

		ok, err := r.kv.Txn(ctx, Txn{
			If: []Cond{{Key: r.pointerKey(r.name), Rev: 0}},
			Then: []Op{
				Put(r.pointerKey(r.name), []byte("0")),
				Put(r.lostKey(r.name), []byte(lost)),
			},
		})
		if err != nil {
			return errors.Wrap(err, "create replica")
		}

		if ok {
			r.lg.Info("registered new replica", zap.String("lost", lost))
		}
	}

	if _, err := r.kv.Txn(ctx, Txn{Then: []Op{Put(r.hostKey(r.name), []byte(r.cfg.Addr))}}); err != nil {
		return errors.Wrap(err, "publish host")
	}

	if err := r.kv.Ephemeral(ctx, r.activeKey(r.name), []byte(r.cfg.Addr)); err != nil {
		return errors.Wrap(err, "publish liveness")
	}

	return nil
}

// markLost flags this replica's local state as untrustworthy, so the next recovery pass clones
// it and peers stop choosing it as a fetch source.
func (r *Replication[B]) markLost(ctx context.Context) error {
	if _, err := r.kv.Txn(ctx, Txn{Then: []Op{Put(r.lostKey(r.name), []byte(lostYes))}}); err != nil {
		return errors.Wrap(err, "set lost")
	}

	return nil
}

// livePeers returns the address of every replica other than this one that currently holds a
// session. A replica without a session cannot serve a fetch, so there is no point asking it.
func (r *Replication[B]) livePeers(ctx context.Context) (map[string]string, error) {
	values, err := r.kv.List(ctx, r.replicasDir())
	if err != nil {
		return nil, errors.Wrap(err, "list replicas")
	}

	out := make(map[string]string)

	for _, v := range values {
		name, leaf, ok := cutReplicaKey(r.replicasDir(), v.Key)
		if !ok || leaf != "active" || name == r.name {
			continue
		}

		out[name] = string(v.Data)
	}

	return out, nil
}

// replicaBlocks reads the block set a replica has published for itself.
func (r *Replication[B]) replicaBlocks(ctx context.Context, name string) ([]B, error) {
	values, err := r.kv.List(ctx, r.partsDir(name))
	if err != nil {
		return nil, errors.Wrapf(err, "list parts of %q", name)
	}

	out := make([]B, 0, len(values))

	for _, v := range values {
		if len(v.Data) == 0 {
			continue
		}

		b, err := unmarshalBlock[B](v.Data)
		if err != nil {
			r.lg.Warn("skipping malformed published block", zap.String("key", v.Key), zap.Error(err))

			continue
		}

		out = append(out, b)
	}

	return out, nil
}

// recoverIfLost rebuilds this replica from a peer when it is flagged lost, and is a no-op
// otherwise.
//
// Cloning does not copy data: it copies the peer's *intent*. Every block the peer holds that this
// replica lacks becomes a bootstrap queue entry, as does every entry still pending in the peer's
// own queue, and the pointer jumps to the peer's. The ordinary executor then does the fetching,
// with the same conflict rules, backoff and source fallback as steady-state replication.
func (r *Replication[B]) recoverIfLost(ctx context.Context) error {
	lost, _, err := getString(ctx, r.kv, r.lostKey(r.name), lostNo)
	if err != nil {
		return err
	}

	if lost != lostYes {
		return nil
	}

	source, pointer, err := r.cloneSource(ctx)
	if err != nil {
		return err
	}

	r.lg.Info("cloning from peer", zap.String("source", source), zap.Uint64("pointer", pointer))

	local, err := r.store.Local(ctx)
	if err != nil {
		return errors.Wrap(err, "list local blocks")
	}

	peerBlocks, err := r.replicaBlocks(ctx, source)
	if err != nil {
		return err
	}

	records, err := r.cloneRecords(ctx, source, local, peerBlocks)
	if err != nil {
		return err
	}

	ops := []Op{
		DeletePrefix(r.queueDir(r.name)),
		DeletePrefix(r.bootstrapDir(r.name)),
		Put(r.pointerKey(r.name), []byte(strconv.FormatUint(pointer, 10))),
		Put(r.lostKey(r.name), []byte(lostNo)),
	}

	for i, rec := range records {
		data, err := marshalRecord(rec)
		if err != nil {
			return err
		}

		ops = append(ops, Put(r.bootstrapKey(r.name, uint64(i)), data))
	}

	if _, err := r.kv.Txn(ctx, Txn{Then: ops}); err != nil {
		return errors.Wrap(err, "install clone plan")
	}

	r.lg.Info("clone planned", zap.Int("blocks", len(records)), zap.String("source", source))

	return nil
}

// cloneSource picks the replica to clone from: live, not itself lost, and the furthest along the
// log. Cloning from a laggard would leave this replica needing a second catch-up pass.
func (r *Replication[B]) cloneSource(ctx context.Context) (name string, pointer uint64, err error) {
	peers, err := r.livePeers(ctx)
	if err != nil {
		return "", 0, err
	}

	var (
		best    string
		bestPtr uint64
		found   bool
	)

	for name := range peers {
		lost, _, err := getString(ctx, r.kv, r.lostKey(name), lostNo)
		if err != nil || lost == lostYes {
			continue
		}

		ptr, _, err := getUint(ctx, r.kv, r.pointerKey(name))
		if err != nil {
			continue
		}

		if !found || ptr > bestPtr {
			best, bestPtr, found = name, ptr, true
		}
	}

	if !found {
		return "", 0, errors.New("no healthy replica to clone from")
	}

	return best, bestPtr, nil
}

// cloneRecords is the work list for a clone: one create record per peer block this replica does
// not already have, plus the peer's own outstanding queue. Blocks the peer has already merged
// away are dropped by [MostCovering] — fetching a merge source *and* its result is pure waste.
func (r *Replication[B]) cloneRecords(ctx context.Context, source string, local, peerBlocks []B) ([]Record[B], error) {
	now := time.Now()
	wanted := MostCovering(peerBlocks)
	SortBlocks(wanted)

	queued := make(map[string]struct{}, len(local))
	for _, b := range local {
		queued[Key(b)] = struct{}{}
	}

	var records []Record[B]

	for _, b := range wanted {
		if _, have := queued[Key(b)]; have || Covered(local, b) {
			continue
		}

		queued[Key(b)] = struct{}{}
		records = append(records, Record[B]{
			Op:        OpCreate,
			Block:     b,
			Replica:   source,
			CreatedAt: now,
		})
	}

	peerQueue, err := r.kv.List(ctx, r.queueDir(source))
	if err != nil {
		return nil, errors.Wrap(err, "list source queue")
	}

	for _, v := range peerQueue {
		rec, err := unmarshalRecord[B](v.Data)
		if err != nil {
			continue
		}

		if _, have := queued[Key(rec.Block)]; have {
			continue
		}

		queued[Key(rec.Block)] = struct{}{}
		records = append(records, rec)
	}

	return records, nil
}

// cutReplicaKey splits a key under the replicas prefix into the replica name and the leaf
// component, e.g. ".../replicas/node1/active" ⇒ ("node1", "active").
func cutReplicaKey(prefix, key string) (name, leaf string, ok bool) {
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return "", "", false
	}

	rest := key[len(prefix):]

	for i := range len(rest) {
		if rest[i] == '/' {
			return rest[:i], rest[i+1:], true
		}
	}

	return "", "", false
}
