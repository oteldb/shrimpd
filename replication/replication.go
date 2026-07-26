package replication

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

// Default timings. They are deliberately short: the log is small, and replication lag is the
// window in which a query can miss data another replica already has.
const (
	DefaultPollInterval = time.Second
	DefaultConcurrency  = 4
	// DefaultLogRetention is how many trailing log records are kept once every live replica has
	// consumed them. Records are the only way a replica catches up incrementally; a replica that
	// falls behind the retained window must clone from a peer instead.
	DefaultLogRetention = 10_000
)

// Config configures a [Replication]. KV, Store, Prefix and Replica are required.
type Config[B Block] struct {
	// KV is the metadata store all replicas coordinate through.
	KV KV
	// Store is this replica's local part storage.
	Store Store[B]
	// Prefix is the KV key prefix owning this replicated set, e.g. "/shrimpd/logs". Two
	// Replication instances with different prefixes are wholly independent, which is how a
	// deployment replicates data parts and index parts side by side.
	Prefix string
	// Replica is this replica's name, unique and stable across restarts (the node id).
	Replica string
	// Addr is what peers are told to fetch from — passed verbatim to [Store.Fetch]. Typically
	// host:port, but replication never interprets it.
	Addr string
	// PollInterval is how often the log is pulled. Zero ⇒ [DefaultPollInterval].
	PollInterval time.Duration
	// Concurrency bounds how many non-conflicting queue entries execute at once. Zero ⇒
	// [DefaultConcurrency].
	Concurrency int
	// LogRetention bounds the retained log. Zero ⇒ [DefaultLogRetention].
	LogRetention int
	// Logger receives replication events. Nil ⇒ no logging.
	Logger *zap.Logger
}

func (c *Config[B]) setDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}

	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}

	if c.LogRetention <= 0 {
		c.LogRetention = DefaultLogRetention
	}

	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}
}

func (c *Config[B]) validate() error {
	switch {
	case c.KV == nil:
		return errors.New("replication: KV is required")
	case c.Store == nil:
		return errors.New("replication: Store is required")
	case c.Prefix == "":
		return errors.New("replication: Prefix is required")
	case c.Replica == "":
		return errors.New("replication: Replica is required")
	}

	return nil
}

// Replication replicates a set of blocks across replicas through an ordered log in [KV].
//
// One instance owns one replicated set (one Prefix). Create it with [New], bring it online with
// [Replication.Start], then drive it with [Replication.Run]. The embedder announces its own
// writes with [Replication.Commit] and [Replication.CommitMerge]; everything else — copying peers'
// records into the queue, fetching, merging, dropping superseded parts — happens in Run.
type Replication[B Block] struct {
	cfg    Config[B]
	kv     KV
	store  Store[B]
	prefix string
	name   string
	lg     *zap.Logger

	// mu guards the queue and pointer, which the poll loop writes and the executor and
	// Inspect read. It is never held across KV or Store I/O.
	mu      sync.Mutex
	queue   []*entry[B]
	pointer uint64

	// commitMu serializes this replica's own log appends. Concurrent appends would contend on
	// the log-sequence compare-and-swap and retry each other for nothing.
	commitMu sync.Mutex

	// awaitingClone is set while the replica is lost and has no healthy peer to rebuild from.
	// It suspends the ordinary loop: pulling the log would be meaningless against local state
	// that is known to be wrong.
	awaitingClone atomic.Bool
}

// ErrCloneUnavailable reports that a replica must rebuild from a peer but none is currently
// reachable. It is transient by construction — the peers are registered, so they are expected
// back — and replication retries on its own. A replica that is the only one registered gets a
// plain error instead, because for it no amount of waiting can help.
var ErrCloneUnavailable = errors.New("replication: no healthy replica to clone from")

// New validates cfg and returns a Replication. It performs no I/O.
func New[B Block](cfg Config[B]) (*Replication[B], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	cfg.setDefaults()

	return &Replication[B]{
		cfg:    cfg,
		kv:     cfg.KV,
		store:  cfg.Store,
		prefix: cfg.Prefix,
		name:   cfg.Replica,
		lg:     cfg.Logger.With(zap.String("replica", cfg.Replica), zap.String("prefix", cfg.Prefix)),
	}, nil
}

// Name returns this replica's name.
func (r *Replication[B]) Name() string { return r.name }

// Start brings the replica online: it registers itself, recovers or clones its local state, and
// loads its durable queue. It must return before [Replication.Run] is called.
func (r *Replication[B]) Start(ctx context.Context) error {
	if err := r.register(ctx); err != nil {
		return errors.Wrap(err, "register replica")
	}

	// A replica that must rebuild but has no live peer right now is not a startup failure: the
	// peers are registered, so they are expected back. It comes up in the lost state and retries
	// in Run rather than making the process die and rely on a supervisor to try again.
	if err := r.recoverIfLost(ctx); err != nil {
		if !errors.Is(err, ErrCloneUnavailable) {
			return errors.Wrap(err, "recover replica")
		}

		r.awaitingClone.Store(true)
		r.lg.Warn("cannot rebuild yet, waiting for a healthy peer", zap.Error(err))
	}

	if err := r.loadQueue(ctx); err != nil {
		return errors.Wrap(err, "load queue")
	}

	if err := r.publishLocalParts(ctx); err != nil {
		return errors.Wrap(err, "publish local parts")
	}

	r.lg.Info("replication started", zap.Uint64("pointer", r.Pointer()))

	return nil
}

// Run drives replication until ctx is canceled or the KV session is lost. It pulls new log
// records into the queue and executes the queue, repeatedly.
func (r *Replication[B]) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.kv.Done():
			return errors.New("replication: metadata session lost")
		case <-ticker.C:
			if r.awaitingClone.Load() {
				if err := r.retryClone(ctx); err != nil {
					r.lg.Warn("still cannot rebuild", zap.Error(err))

					continue
				}
			}

			if err := r.pull(ctx); err != nil {
				r.lg.Warn("pull log", zap.Error(err))
			}

			r.execute(ctx)
		}
	}
}

// retryClone re-attempts a rebuild that could not run at startup, and adopts the resulting queue
// once it succeeds.
func (r *Replication[B]) retryClone(ctx context.Context) error {
	if err := r.recoverIfLost(ctx); err != nil {
		return err
	}

	if err := r.loadQueue(ctx); err != nil {
		return errors.Wrap(err, "load queue after clone")
	}

	r.awaitingClone.Store(false)
	r.lg.Info("rebuilt from peer", zap.Uint64("pointer", r.Pointer()))

	return nil
}

// AwaitingClone reports whether the replica is waiting for a healthy peer to rebuild from. While
// it is true the replica holds untrustworthy data and is not participating.
func (r *Replication[B]) AwaitingClone() bool { return r.awaitingClone.Load() }

// Pointer returns the highest log sequence this replica has copied into its queue.
func (r *Replication[B]) Pointer() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pointer
}

// AllocateBlock reserves the next block number in a partition. Every write must take its block
// number from here before producing a part, so a part's identity is agreed cluster-wide before
// any data exists under that name.
func (r *Replication[B]) AllocateBlock(ctx context.Context, partition string) (uint64, error) {
	key := r.blockSeqKey(partition)

	for range casRetries {
		cur, rev, err := getUint(ctx, r.kv, key)
		if err != nil {
			return 0, err
		}

		next := cur + 1

		ok, err := r.kv.Txn(ctx, Txn{
			If:   []Cond{{Key: key, Rev: rev}},
			Then: []Op{Put(key, []byte(strconv.FormatUint(next, 10)))},
		})
		if err != nil {
			return 0, errors.Wrap(err, "allocate block")
		}

		if ok {
			return next, nil
		}
	}

	return 0, errors.Errorf("allocate block in %q: too much contention", partition)
}

// Commit announces a block this replica just wrote. The log record and this replica's claim on
// the block are written in one transaction, so the log never advertises a block no replica
// admits to holding.
func (r *Replication[B]) Commit(ctx context.Context, b B) error {
	return r.append(ctx, Record[B]{
		Op:        OpCreate,
		Block:     b,
		Replica:   r.name,
		CreatedAt: time.Now(),
	})
}

// CommitMerge announces a merge this replica just performed: src have been combined into dst.
// Peers may reproduce the merge locally or fetch dst; either way they end up dropping src.
func (r *Replication[B]) CommitMerge(ctx context.Context, src []B, dst B) error {
	return r.append(ctx, Record[B]{
		Op:        OpMerge,
		Block:     dst,
		Sources:   src,
		Replica:   r.name,
		CreatedAt: time.Now(),
	})
}

// CommitDrop announces that a block has been removed for good (retention).
func (r *Replication[B]) CommitDrop(ctx context.Context, b B) error {
	return r.append(ctx, Record[B]{
		Op:        OpDrop,
		Block:     b,
		Replica:   r.name,
		CreatedAt: time.Now(),
	})
}

// casRetries bounds optimistic-concurrency retry loops. Contention is between replicas
// committing parts, which is rare and short; exceeding this many collisions means something is
// wrong rather than busy.
const casRetries = 32

// append writes rec at the next log sequence, together with the metadata changes it implies for
// this replica, in one transaction.
func (r *Replication[B]) append(ctx context.Context, rec Record[B]) error {
	data, err := marshalRecord(rec)
	if err != nil {
		return err
	}

	r.commitMu.Lock()
	defer r.commitMu.Unlock()

	for range casRetries {
		cur, rev, err := getUint(ctx, r.kv, r.logSeqKey())
		if err != nil {
			return err
		}

		seq := cur + 1

		ops := []Op{
			Put(r.logSeqKey(), []byte(strconv.FormatUint(seq, 10))),
			Put(r.logKey(seq), data),
		}

		// Record the local effect of our own operation, so peers can already see us as a
		// source and a restart does not have to re-derive it from the log.
		if rec.Op == OpDrop {
			ops = append(ops, Delete(r.partKey(r.name, rec.Block)))
		} else {
			blockData, err := marshalBlock(rec.Block)
			if err != nil {
				return err
			}

			ops = append(ops, Put(r.partKey(r.name, rec.Block), blockData))
			for _, s := range rec.Sources {
				ops = append(ops, Delete(r.partKey(r.name, s)))
			}
		}

		ok, err := r.kv.Txn(ctx, Txn{
			If:   []Cond{{Key: r.logSeqKey(), Rev: rev}},
			Then: ops,
		})
		if err != nil {
			return errors.Wrap(err, "append log record")
		}

		if ok {
			r.lg.Debug("committed", zap.String("record", rec.String()), zap.Uint64("seq", seq))

			return nil
		}
	}

	return errors.New("append log record: too much contention")
}

// publishLocalParts makes this replica's on-disk part set visible to peers. It is a full
// reconciliation rather than an incremental update, so a part restored or removed outside
// replication is still reflected.
func (r *Replication[B]) publishLocalParts(ctx context.Context) error {
	local, err := r.store.Local(ctx)
	if err != nil {
		return errors.Wrap(err, "list local blocks")
	}

	published, err := r.kv.List(ctx, r.partsDir(r.name))
	if err != nil {
		return errors.Wrap(err, "list published blocks")
	}

	want := make(map[string][]byte, len(local))

	for _, b := range local {
		data, err := marshalBlock(b)
		if err != nil {
			return err
		}

		want[r.partKey(r.name, b)] = data
	}

	var ops []Op

	have := make(map[string]struct{}, len(published))

	for _, v := range published {
		have[v.Key] = struct{}{}

		if _, keep := want[v.Key]; !keep {
			ops = append(ops, Delete(v.Key))
		}
	}

	for key, data := range want {
		if _, ok := have[key]; !ok {
			ops = append(ops, Put(key, data))
		}
	}

	if len(ops) == 0 {
		return nil
	}

	if _, err := r.kv.Txn(ctx, Txn{Then: ops}); err != nil {
		return errors.Wrap(err, "publish blocks")
	}

	return nil
}

// getUint reads an unsigned counter, treating an absent key as zero.
//
//nolint:unparam // rev is what the compare-and-swap callers condition on; unparam misses the uses inside the generic methods.
func getUint(ctx context.Context, kv KV, key string) (val uint64, rev int64, err error) {
	s, rev, err := getString(ctx, kv, key, "0")
	if err != nil {
		return 0, 0, err
	}

	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "parse counter %q", key)
	}

	return n, rev, nil
}
