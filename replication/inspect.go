package replication

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

// State is a point-in-time snapshot of this replica's replication, for operator endpoints.
type State struct {
	Replica string       `json:"replica"`
	Prefix  string       `json:"prefix"`
	Pointer uint64       `json:"pointer"`
	Queue   []EntryState `json:"queue"`
}

// EntryState describes one queued record and how its execution is going.
type EntryState struct {
	Seq       uint64    `json:"seq"`
	Op        Operation `json:"op"`
	Block     string    `json:"block"`
	Source    string    `json:"source"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	QueuedAt  time.Time `json:"queued_at"`
}

// Inspect returns the current replication state. It takes no locks the executor holds across
// I/O, so it is safe to call from an HTTP handler at any time.
func (r *Replication[B]) Inspect() State {
	r.mu.Lock()
	defer r.mu.Unlock()

	st := State{
		Replica: r.name,
		Prefix:  r.prefix,
		Pointer: r.pointer,
		Queue:   make([]EntryState, 0, len(r.queue)),
	}

	for _, e := range r.queue {
		st.Queue = append(st.Queue, EntryState{
			Seq:       e.seq,
			Op:        e.rec.Op,
			Block:     Key(e.rec.Block),
			Source:    e.rec.Replica,
			Status:    e.status.String(),
			Attempts:  e.attempts,
			LastError: e.lastErr,
			QueuedAt:  e.queuedAt,
		})
	}

	return st
}

// Lag reports how far this replica trails the log: the number of unconsumed records and the age
// of the oldest queued one. Both are zero when the replica is caught up.
func (r *Replication[B]) Lag(ctx context.Context) (records uint64, oldest time.Duration, err error) {
	head, _, err := getUint(ctx, r.kv, r.logSeqKey())
	if err != nil {
		return 0, 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if head > r.pointer {
		records = head - r.pointer
	}

	records += uint64(len(r.queue))

	now := time.Now()

	for _, e := range r.queue {
		if age := now.Sub(e.queuedAt); age > oldest {
			oldest = age
		}
	}

	return records, oldest, nil
}

// CleanupLog trims log records every live replica has already consumed, keeping at most
// Config.LogRetention of them.
//
// Trimming is bounded by the slowest *live* replica, never by a dead one: a replica that stays
// down past the retention window rebuilds by cloning rather than by replaying, so holding the log
// for it forever would only grow the metadata store. Call it periodically from one place; running
// it on every replica is harmless but wasteful.
func (r *Replication[B]) CleanupLog(ctx context.Context) error {
	head, _, err := getUint(ctx, r.kv, r.logSeqKey())
	if err != nil {
		return err
	}

	if head <= uint64(r.cfg.LogRetention) {
		return nil
	}

	peers, err := r.livePeers(ctx)
	if err != nil {
		return err
	}

	minPointer := r.Pointer()

	for name := range peers {
		ptr, _, err := getUint(ctx, r.kv, r.pointerKey(name))
		if err != nil {
			return err
		}

		minPointer = min(minPointer, ptr)
	}

	// Never trim past the retention floor even if everyone is caught up: the trailing window is
	// what lets a replica that restarts in the next moments catch up incrementally.
	cutoff := min(minPointer, head-uint64(r.cfg.LogRetention))
	if cutoff == 0 {
		return nil
	}

	values, err := r.kv.ListFrom(ctx, r.logDir(), "", pullLimit)
	if err != nil {
		return errors.Wrap(err, "list log")
	}

	var ops []Op

	for _, v := range values {
		seq, ok := parseSeq(v.Key)
		if !ok || seq > cutoff {
			continue
		}

		ops = append(ops, Delete(v.Key))
	}

	if len(ops) == 0 {
		return nil
	}

	if _, err := r.kv.Txn(ctx, Txn{Then: ops}); err != nil {
		return errors.Wrap(err, "trim log")
	}

	r.lg.Debug("trimmed log", zap.Int("records", len(ops)), zap.Uint64("cutoff", cutoff))

	return nil
}
