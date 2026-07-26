package replication

import "context"

// Store is the physical half of replication: the embedder's local part storage. Replication
// decides *what* the local part set must become; Store makes it so.
//
// Every method must be idempotent — a queue entry is retried until it succeeds, and a replica
// that crashed mid-execution re-runs the entry on restart. Implementations must be safe for
// concurrent use; replication executes non-conflicting entries in parallel.
type Store[B Block] interface {
	// Local returns the blocks currently held on disk. It is the source of truth for what this
	// replica has: replication compares it against the log rather than trusting its own
	// bookkeeping, so a manually deleted or externally restored part is noticed.
	Local(ctx context.Context) ([]B, error)

	// Fetch downloads b from the peer reachable at addr and installs it locally. addr is the
	// value the source replica published for itself, so its meaning is entirely the embedder's.
	// Fetch must publish the block atomically: a reader must never see a half-installed part.
	Fetch(ctx context.Context, addr string, b B) error

	// Merge reproduces a merge locally: it combines src (all of which this replica holds) into
	// dst. Returning [ErrMergeUnsupported] — or any error — makes replication fall back to
	// fetching dst from the replica that performed it, so an implementation may decline any
	// merge it judges too expensive.
	Merge(ctx context.Context, src []B, dst B) error

	// Drop removes b from local storage. Dropping a block that is already absent is not an
	// error.
	Drop(ctx context.Context, b B) error
}

// ErrMergeUnsupported is the conventional [Store.Merge] error meaning "do not reproduce this
// merge locally, fetch the result instead". Any other error has the same effect but is reported
// as a failure; this one is expected and logged at debug level.
var ErrMergeUnsupported = errStatic("replication: merge unsupported, fetch instead")

type errStatic string

func (e errStatic) Error() string { return string(e) }
