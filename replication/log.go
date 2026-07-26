package replication

import (
	"encoding/json"
	"time"

	"github.com/go-faster/errors"
)

// Operation is what a log record asks every replica to do.
type Operation string

// Log operations.
const (
	// OpCreate announces a newly written block. A replica that lacks it fetches it from
	// Record.SourceReplica.
	OpCreate Operation = "create"
	// OpMerge announces that Sources were merged into Block. A replica may reproduce the merge
	// locally from sources it already holds, or fetch the result — either way it ends up with
	// Block and drops the sources.
	OpMerge Operation = "merge"
	// OpDrop announces that Block was removed (retention, or an explicit partition drop). It
	// carries no data to fetch.
	OpDrop Operation = "drop"
)

// Record is one entry in the replicated log — the only thing replicas agree on. It names blocks,
// never bytes: the data itself moves replica-to-replica over the embedder's transport.
type Record[B Block] struct {
	Op Operation `json:"op"`
	// Block is the block the record produces (create, merge) or removes (drop).
	Block B `json:"block"`
	// Sources are the blocks a merge consumed. Empty for other operations.
	Sources []B `json:"sources,omitempty"`
	// Replica is the replica that performed the operation and can serve Block.
	Replica string `json:"replica"`
	// CreatedAt is when the originating replica appended the record, for lag reporting.
	CreatedAt time.Time `json:"created_at"`
}

// Partition returns the partition the record operates in.
func (r Record[B]) Partition() string { return r.Block.Partition() }

// Range returns the block range the record touches: for a merge that is the result's range,
// which by construction spans every source.
func (r Record[B]) Range() Range { return r.Block.Range() }

// Conflicts reports whether two records touch overlapping blocks in the same partition and so
// must not execute concurrently.
func (r Record[B]) Conflicts(other Record[B]) bool {
	return r.Partition() == other.Partition() && Intersects(r.Range(), other.Range())
}

// String renders the record for logs and operator output.
func (r Record[B]) String() string {
	return string(r.Op) + " " + Key(r.Block) + " @" + r.Replica
}

func marshalBlock[B Block](b B) ([]byte, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return nil, errors.Wrap(err, "marshal block")
	}

	return data, nil
}

func unmarshalBlock[B Block](data []byte) (B, error) {
	var b B
	if err := json.Unmarshal(data, &b); err != nil {
		return b, errors.Wrap(err, "unmarshal block")
	}

	return b, nil
}

func marshalRecord[B Block](rec Record[B]) ([]byte, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, errors.Wrap(err, "marshal record")
	}

	return data, nil
}

func unmarshalRecord[B Block](data []byte) (Record[B], error) {
	var rec Record[B]
	if err := json.Unmarshal(data, &rec); err != nil {
		return rec, errors.Wrap(err, "unmarshal record")
	}

	if rec.Op == "" {
		return rec, errors.New("record has no operation")
	}

	if !rec.Range().Valid() {
		return rec, errors.Errorf("record %s has invalid range %s", Key(rec.Block), rec.Range())
	}

	return rec, nil
}
