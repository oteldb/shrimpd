package replication

import (
	"context"
	"time"
)

// PullForTest runs one log-pull pass. Tests drive the loop step by step instead of waiting on
// the poll ticker, so they are deterministic and fast.
func PullForTest[B Block](ctx context.Context, r *Replication[B]) error { return r.pull(ctx) }

// ExecuteForTest runs one queue-execution pass.
func ExecuteForTest[B Block](ctx context.Context, r *Replication[B]) { r.execute(ctx) }

// MarkLostForTest flags the replica lost, as a detected log gap would.
func MarkLostForTest[B Block](ctx context.Context, r *Replication[B]) error { return r.markLost(ctx) }

// RecoverForTest runs the lost-replica recovery pass.
func RecoverForTest[B Block](ctx context.Context, r *Replication[B]) error {
	return r.recoverIfLost(ctx)
}

// ReloadQueueForTest re-reads the durable queue, as a restart would.
func ReloadQueueForTest[B Block](ctx context.Context, r *Replication[B]) error {
	return r.loadQueue(ctx)
}

// ClaimForTest runs entry selection alone and reports what a single execution pass would take
// on, so a test can assert the conflict rules without racing the executor.
func ClaimForTest[B Block](r *Replication[B], limit int) []EntryState {
	claimed := r.claim(time.Now(), limit)

	out := make([]EntryState, 0, len(claimed))
	for _, e := range claimed {
		out = append(out, EntryState{Seq: e.seq, Op: e.rec.Op, Block: Key(e.rec.Block)})
	}

	// Release the claim so the ordinary executor can pick these up afterwards.
	r.mu.Lock()
	for _, e := range claimed {
		e.status = statusReady
		e.attempts = 0
		e.lastExec = time.Time{}
	}
	r.mu.Unlock()

	return out
}

// ClearBackoffForTest makes every queued entry immediately eligible again.
func ClearBackoffForTest[B Block](r *Replication[B]) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, e := range r.queue {
		e.attempts = 0
		e.lastExec = time.Time{}
		e.status = statusReady
	}
}

// LogRecordsForTest counts the retained log records.
func LogRecordsForTest[B Block](ctx context.Context, r *Replication[B]) (int, error) {
	values, err := r.kv.List(ctx, r.logDir())

	return len(values), err
}
