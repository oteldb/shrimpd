package shrimpengine

import (
	"fmt"

	"github.com/oteldb/shrimpd/replication"
)

// blockObject is the shrimpd-owned sidecar written next to every part's engine objects. The
// record engine's own metadata records a part's key prefix and time range but not its logical
// block coverage, and replication needs the coverage to decide obsolescence — so shrimpd stores
// it alongside the part and copies it with the rest of the part's objects.
const blockObject = "shrimp-block.json"

// streamsObject is the record engine's per-partition stream identity index. It is not part of any
// single part but every part depends on it, so replication copies it alongside them.
const streamsObject = "streams.bin"

// Part is shrimpd's replicated unit: one immutable record-engine part.
//
// Two identities live here and they are deliberately different. `Seq` is the *physical* name —
// the numeric key component the record engine gave the part, which is what a peer copies objects
// under. `First`/`Last`/`Level` are the *logical* coverage used for obsolescence: a flushed part
// covers the single block number equal to its sequence at level 0, and a merge covers the union
// of its sources at one level higher. Keeping them apart is what lets shrimpd replicate parts the
// engine named for itself while still reasoning about them the way ClickHouse does.
type Part struct {
	// Node is the partition: the id of the node that wrote this part. Each node writes only its
	// own partition, so the engine's node-local sequence allocation is unambiguous cluster-wide
	// and a part's objects live under the same key on every replica.
	Node string `json:"node"`
	// Seq is the record engine's part sequence, and the last component of its key prefix.
	Seq int `json:"seq"`

	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
	Level uint64 `json:"level"`

	MinTime int64 `json:"min_time"`
	MaxTime int64 `json:"max_time"`
}

var _ replication.Block = Part{}

// Name is the part's physical name — the numeric key component, zero-padded exactly as the
// record engine writes it.
func (p Part) Name() string { return fmt.Sprintf("%010d", p.Seq) }

// Partition returns the owning node's id.
func (p Part) Partition() string { return p.Node }

// Range returns the part's logical block coverage.
func (p Part) Range() replication.Range {
	return replication.Range{First: p.First, Last: p.Last, Level: p.Level}
}

// Prefix is the backend key prefix holding this part's objects, e.g. "node1/0000000007".
func (p Part) Prefix() string { return p.Node + "/" + p.Name() }

// Overlaps reports whether the part can contain records in the inclusive window.
func (p Part) Overlaps(from, to int64) bool { return p.MinTime <= to && p.MaxTime >= from }

// flushed builds the descriptor for a freshly written part: it covers the one block number its
// sequence names.
func flushed(node string, seq int, minTime, maxTime int64) Part {
	return Part{
		Node:    node,
		Seq:     seq,
		First:   uint64(seq),
		Last:    uint64(seq),
		Level:   0,
		MinTime: minTime,
		MaxTime: maxTime,
	}
}

// mergedFrom builds the descriptor for a merged part. The record engine compacts *every* live
// part into one, so the sources are exactly the partition's previous contents and the result
// covers their whole block interval with nothing surviving inside it — which is what makes plain
// interval containment a sound obsolescence test.
func mergedFrom(node string, seq int, sources []Part, minTime, maxTime int64) Part {
	p := Part{Node: node, Seq: seq, MinTime: minTime, MaxTime: maxTime}

	for i, s := range sources {
		if i == 0 || s.First < p.First {
			p.First = s.First
		}

		if i == 0 || s.Last > p.Last {
			p.Last = s.Last
		}

		if s.Level >= p.Level {
			p.Level = s.Level + 1
		}
	}

	return p
}
