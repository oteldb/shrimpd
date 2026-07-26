package replication

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

// pullLimit bounds how many log records are copied into the queue per poll, so a replica that
// fell far behind makes steady progress instead of one enormous transaction.
const pullLimit = 512

// Retry backoff for a queue entry that failed. A fetch fails mostly because the source replica
// is down; retrying hard helps nobody, but giving up entirely loses data, so it backs off and
// keeps trying forever.
const (
	retryBase = 2 * time.Second
	retryMax  = 5 * time.Minute
)

// entryStatus is where a queue entry is in its lifecycle.
type entryStatus uint8

const (
	// statusReady means the entry is eligible to execute.
	statusReady entryStatus = iota
	// statusExecuting means a worker is applying it right now.
	statusExecuting
	// statusBlocked means it conflicts with an executing entry and must wait.
	statusBlocked
)

func (s entryStatus) String() string {
	switch s {
	case statusExecuting:
		return "executing"
	case statusBlocked:
		return "blocked"
	default:
		return "ready"
	}
}

// entry is one queued log record and this replica's attempt state for it.
type entry[B Block] struct {
	// key is the durable KV key holding this entry, deleted once it is applied. Entries copied
	// from the log and entries synthesized by a clone live in different subtrees, so the key is
	// carried rather than derived.
	key string
	// seq is the log sequence the entry came from, or zero for a clone-synthesized entry.
	seq      uint64
	rec      Record[B]
	status   entryStatus
	attempts int
	lastErr  string
	lastExec time.Time
	queuedAt time.Time
}

// retryAfter is when the entry may next be attempted: immediately on its first try, then
// exponential backoff capped at [retryMax].
func (e *entry[B]) retryAfter() time.Time {
	if e.attempts == 0 {
		return time.Time{}
	}

	d := retryBase << min(e.attempts-1, 8)

	return e.lastExec.Add(min(d, retryMax))
}

// loadQueue restores the durable queue and pointer written by a previous run. A queue entry
// survives a restart because it is the only record that this replica still owes work the log has
// already moved past.
func (r *Replication[B]) loadQueue(ctx context.Context) error {
	ptr, _, err := getUint(ctx, r.kv, r.pointerKey(r.name))
	if err != nil {
		return err
	}

	// Clone-synthesized work comes first: it is the backlog a rebuilding replica must clear
	// before the log records that follow it make sense.
	var queue []*entry[B]

	for _, dir := range []string{r.bootstrapDir(r.name), r.queueDir(r.name)} {
		values, err := r.kv.List(ctx, dir)
		if err != nil {
			return errors.Wrap(err, "list queue")
		}

		for _, v := range values {
			rec, err := unmarshalRecord[B](v.Data)
			if err != nil {
				r.lg.Warn("skipping malformed queue entry", zap.String("key", v.Key), zap.Error(err))

				continue
			}

			seq, _ := parseSeq(v.Key)
			if dir == r.bootstrapDir(r.name) {
				seq = 0
			}

			queue = append(queue, &entry[B]{key: v.Key, seq: seq, rec: rec, queuedAt: time.Now()})
		}
	}

	r.mu.Lock()
	r.queue = queue
	r.pointer = ptr
	r.mu.Unlock()

	return nil
}

// pull copies log records past this replica's pointer into its queue. Records this replica
// itself wrote are already applied locally, so they only advance the pointer.
//
// A gap — the first available record being past pointer+1 — means the log was trimmed while this
// replica was behind. There is no way to catch up incrementally from that, so the replica
// declares itself lost and clones from a peer.
func (r *Replication[B]) pull(ctx context.Context) error {
	ptr := r.Pointer()

	values, err := r.kv.ListFrom(ctx, r.logDir(), r.logKey(ptr+1), pullLimit)
	if err != nil {
		return errors.Wrap(err, "list log")
	}

	for _, v := range values {
		seq, ok := parseSeq(v.Key)
		if !ok || seq <= ptr {
			continue
		}

		if seq > ptr+1 {
			r.lg.Warn("log gap; replica fell behind retention, cloning",
				zap.Uint64("want", ptr+1), zap.Uint64("got", seq))

			if err := r.markLost(ctx); err != nil {
				return errors.Wrap(err, "mark lost")
			}

			if err := r.recoverIfLost(ctx); err != nil {
				return errors.Wrap(err, "clone after gap")
			}

			return nil
		}

		rec, err := unmarshalRecord[B](v.Data)
		if err != nil {
			// A record we cannot parse cannot be executed and will never become parseable.
			// Skipping it and advancing keeps the replica moving; the reconciliation in
			// publishLocalParts still reports the divergence.
			r.lg.Error("skipping unparseable log record", zap.Uint64("seq", seq), zap.Error(err))

			if err := r.advance(ctx, seq, nil); err != nil {
				return err
			}

			ptr = seq

			continue
		}

		if rec.Replica == r.name {
			if err := r.advance(ctx, seq, nil); err != nil {
				return err
			}

			ptr = seq

			continue
		}

		if err := r.advance(ctx, seq, v.Data); err != nil {
			return err
		}

		r.mu.Lock()
		r.queue = append(r.queue, &entry[B]{
			key:      r.queueKey(r.name, seq),
			seq:      seq,
			rec:      rec,
			queuedAt: time.Now(),
		})
		r.mu.Unlock()

		ptr = seq
	}

	return nil
}

// advance moves the durable pointer to seq, optionally enqueuing the record in the same
// transaction. Pairing them is what makes the queue crash-safe: a record is never skipped
// because the pointer moved without it being queued.
func (r *Replication[B]) advance(ctx context.Context, seq uint64, record []byte) error {
	ops := []Op{Put(r.pointerKey(r.name), []byte(strconv.FormatUint(seq, 10)))}
	if record != nil {
		ops = append(ops, Put(r.queueKey(r.name, seq), record))
	}

	if _, err := r.kv.Txn(ctx, Txn{Then: ops}); err != nil {
		return errors.Wrap(err, "advance pointer")
	}

	r.mu.Lock()
	r.pointer = seq
	r.mu.Unlock()

	return nil
}

// claim selects the entries to run now: ready, past their retry deadline, and not conflicting
// with anything already executing or claimed in this pass. Conflicting entries stay in the
// queue — order within a partition's overlapping block range is not negotiable.
func (r *Replication[B]) claim(now time.Time, limit int) []*entry[B] {
	r.mu.Lock()
	defer r.mu.Unlock()

	var claimed []*entry[B]

	for _, e := range r.queue {
		if len(claimed) >= limit {
			break
		}

		if e.status == statusExecuting {
			continue
		}

		if deadline := e.retryAfter(); now.Before(deadline) {
			continue
		}

		if r.conflictsLocked(e, claimed) {
			e.status = statusBlocked

			continue
		}

		e.status = statusExecuting
		e.attempts++
		e.lastExec = now
		claimed = append(claimed, e)
	}

	return claimed
}

// conflictsLocked reports whether e overlaps anything executing or just claimed. r.mu is held.
func (r *Replication[B]) conflictsLocked(e *entry[B], claimed []*entry[B]) bool {
	for _, other := range r.queue {
		if other != e && other.status == statusExecuting && e.rec.Conflicts(other.rec) {
			return true
		}
	}

	for _, other := range claimed {
		if e.rec.Conflicts(other.rec) {
			return true
		}
	}

	return false
}

// execute runs one pass of the queue: claim what can run, apply it concurrently, and retire the
// entries that succeeded.
func (r *Replication[B]) execute(ctx context.Context) {
	claimed := r.claim(time.Now(), r.cfg.Concurrency)
	if len(claimed) == 0 {
		return
	}

	var wg sync.WaitGroup

	for _, e := range claimed {
		wg.Go(func() {
			r.finish(ctx, e, r.apply(ctx, e.rec))
		})
	}

	wg.Wait()
}

// finish records the outcome of an attempt: a success retires the entry from both the in-memory
// and the durable queue, a failure leaves it queued for backoff retry.
func (r *Replication[B]) finish(ctx context.Context, e *entry[B], err error) {
	if err != nil {
		r.mu.Lock()
		e.status = statusReady
		e.lastErr = err.Error()
		r.mu.Unlock()

		r.lg.Warn("queue entry failed",
			zap.String("record", e.rec.String()),
			zap.Uint64("seq", e.seq),
			zap.Int("attempts", e.attempts),
			zap.Error(err))

		return
	}

	if _, err := r.kv.Txn(ctx, Txn{Then: []Op{Delete(e.key)}}); err != nil {
		// The work is done; only the bookkeeping failed. Leaving the entry queued makes the
		// next pass re-apply it, which is harmless because apply is idempotent.
		r.lg.Warn("dequeue", zap.Uint64("seq", e.seq), zap.Error(err))

		r.mu.Lock()
		e.status = statusReady
		r.mu.Unlock()

		return
	}

	r.mu.Lock()

	for i, other := range r.queue {
		if other == e {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)

			break
		}
	}

	// Unblock anything that was waiting on this entry's range.
	for _, other := range r.queue {
		if other.status == statusBlocked {
			other.status = statusReady
		}
	}

	r.mu.Unlock()

	r.lg.Debug("applied", zap.String("record", e.rec.String()), zap.Uint64("seq", e.seq))
}

// apply makes the local part set satisfy one log record. It is idempotent: it derives what to do
// from what is actually on disk, so re-running a partly applied record finishes it rather than
// duplicating it.
func (r *Replication[B]) apply(ctx context.Context, rec Record[B]) error {
	local, err := r.store.Local(ctx)
	if err != nil {
		return errors.Wrap(err, "list local blocks")
	}

	if rec.Op == OpDrop {
		for _, b := range local {
			if Key(b) == Key(rec.Block) {
				return r.store.Drop(ctx, b)
			}
		}

		return r.publishLocalParts(ctx)
	}

	if !Covered(local, rec.Block) {
		if err := r.acquire(ctx, rec, local); err != nil {
			return err
		}

		if local, err = r.store.Local(ctx); err != nil {
			return errors.Wrap(err, "list local blocks after acquire")
		}
	}

	// Whatever the new block covers is dead weight, whether this replica merged it or fetched
	// it. Dropping is best-effort: a leftover source is a waste of disk, not a correctness
	// problem, and the next pass retries it.
	for _, old := range SupersededBy(local, rec.Block) {
		if err := r.store.Drop(ctx, old); err != nil {
			r.lg.Warn("drop superseded block", zap.String("block", Key(old)), zap.Error(err))
		}
	}

	return r.publishLocalParts(ctx)
}

// acquire obtains rec's block: by reproducing the merge locally when this replica already holds
// every source, otherwise by fetching it from a replica that has it.
func (r *Replication[B]) acquire(ctx context.Context, rec Record[B], local []B) error {
	if rec.Op == OpMerge && len(rec.Sources) > 0 {
		if sources, ok := resolveSources(local, rec.Sources); ok {
			err := r.store.Merge(ctx, sources, rec.Block)
			if err == nil {
				return nil
			}

			if errors.Is(err, ErrMergeUnsupported) {
				r.lg.Debug("merge declined, fetching instead", zap.String("block", Key(rec.Block)))
			} else {
				r.lg.Warn("local merge failed, fetching instead",
					zap.String("block", Key(rec.Block)), zap.Error(err))
			}
		}
	}

	return r.fetch(ctx, rec)
}

// resolveSources maps a record's source descriptors onto the blocks this replica actually holds,
// reporting whether every one of them is present. A merge can only be reproduced from a complete
// source set.
func resolveSources[B Block](local, want []B) ([]B, bool) {
	byKey := make(map[string]B, len(local))
	for _, b := range local {
		byKey[Key(b)] = b
	}

	out := make([]B, 0, len(want))

	for _, w := range want {
		b, ok := byKey[Key(w)]
		if !ok {
			return nil, false
		}

		out = append(out, b)
	}

	return out, true
}

// fetch downloads rec's block, trying the replica that produced it first and then any other
// replica that has published a block covering it. Falling back matters: the originating replica
// is often exactly the one that just died.
func (r *Replication[B]) fetch(ctx context.Context, rec Record[B]) error {
	sources, err := r.fetchSources(ctx, rec)
	if err != nil {
		return err
	}

	if len(sources) == 0 {
		return errors.Errorf("no replica holds %s", Key(rec.Block))
	}

	var errs []error

	for _, src := range sources {
		if err := r.store.Fetch(ctx, src.addr, rec.Block); err != nil {
			r.lg.Debug("fetch from replica failed",
				zap.String("from", src.name), zap.String("block", Key(rec.Block)), zap.Error(err))

			errs = append(errs, errors.Wrapf(err, "from %s", src.name))

			continue
		}

		return nil
	}

	return errors.Wrapf(errors.Join(errs...), "fetch %s", Key(rec.Block))
}

type fetchSource struct {
	name string
	addr string
}

// fetchSources orders the replicas worth asking for rec's block: the originator first, then
// every other live replica whose published part set covers it.
func (r *Replication[B]) fetchSources(ctx context.Context, rec Record[B]) ([]fetchSource, error) {
	peers, err := r.livePeers(ctx)
	if err != nil {
		return nil, err
	}

	var out []fetchSource

	if addr, ok := peers[rec.Replica]; ok {
		out = append(out, fetchSource{name: rec.Replica, addr: addr})
	}

	for name, addr := range peers {
		if name == rec.Replica {
			continue
		}

		blocks, err := r.replicaBlocks(ctx, name)
		if err != nil {
			r.lg.Debug("read peer part set", zap.String("peer", name), zap.Error(err))

			continue
		}

		if Covered(blocks, rec.Block) {
			out = append(out, fetchSource{name: name, addr: addr})
		}
	}

	return out, nil
}
