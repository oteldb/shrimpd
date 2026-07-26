// Package shrimpd is the public face of the shrimpd module: its replication mechanism.
//
// shrimpd the daemon is a replicated log store, but the reusable part of it is the replication
// itself — a ClickHouse-style replicated log, generic over whatever unit you replicate. The
// daemon's own storage lives in internal packages; everything worth embedding is re-exported
// here from [github.com/oteldb/shrimpd/replication].
//
// A minimal embedding replicates any type that can name itself and state the block range it
// covers:
//
//	type MyPart struct{ … }
//
//	func (p MyPart) Name() string         { … }
//	func (p MyPart) Partition() string    { … }
//	func (p MyPart) Range() shrimpd.Range { … }
//
//	r, err := shrimpd.NewReplication(shrimpd.Config[MyPart]{
//	    KV: kv, Store: myStore, Prefix: "/myapp/parts", Replica: nodeID, Addr: addr,
//	})
package shrimpd

import "github.com/oteldb/shrimpd/replication"

type (
	// Block is the replicated unit: see [replication.Block].
	Block = replication.Block
	// Range is a block-number interval and merge level: see [replication.Range].
	Range = replication.Range
	// Store is the embedder's local part storage: see [replication.Store].
	Store[B Block] = replication.Store[B]
	// KV is the metadata store replicas coordinate through: see [replication.KV].
	KV = replication.KV
	// Config configures a [Replication].
	Config[B Block] = replication.Config[B]
	// Replication is the replicated log itself: see [replication.Replication].
	Replication[B Block] = replication.Replication[B]
	// Record is one entry in the replicated log.
	Record[B Block] = replication.Record[B]
	// State is a replica's operator-visible replication status.
	State = replication.State
)

// Log operations.
const (
	OpCreate = replication.OpCreate
	OpMerge  = replication.OpMerge
	OpDrop   = replication.OpDrop
)

// ErrMergeUnsupported tells replication to fetch a merge result rather than reproduce it.
var ErrMergeUnsupported = replication.ErrMergeUnsupported

// NewReplication builds a [Replication] from cfg.
func NewReplication[B Block](cfg Config[B]) (*Replication[B], error) {
	return replication.New(cfg)
}

// Range containment helpers, re-exported for implementations that reason about coverage.
var (
	Contains         = replication.Contains
	Equals           = replication.Equals
	ContainsOrEquals = replication.ContainsOrEquals
	Intersects       = replication.Intersects
)
