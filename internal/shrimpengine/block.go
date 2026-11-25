package shrimpengine

import (
	"fmt"
	"path"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/backend/bucketindex"

	"github.com/oteldb/shrimpd/replication"
)

// blockObject is the shrimpd-owned sidecar written next to every part's engine objects. The
// record engine's own metadata records a part's key prefix and time range but not its logical
// block coverage, and replication needs the coverage to decide obsolescence — so shrimpd stores
// it alongside the part and copies it with the rest of the part's objects.
const blockObject = "shrimp-block.json"

// streamsObject is the legacy per-partition stream identity index.
const streamsObject = "streams.bin"

// Part is shrimpd's replicated unit: one immutable record-engine part.
//
// ID (or Seq for older parts) identifies the objects to copy. First, Last and Level describe
// the logical block range used by replication.
type Part struct {
	// Node is the id of the node that owns this partition.
	Node string `json:"node"`
	// ID is the opaque part name assigned by the record engine.
	ID string `json:"id,omitempty"`
	// Seq is the numeric name carried by older sidecars and replication records.
	Seq int `json:"seq"`

	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
	Level uint64 `json:"level"`

	MinTime int64 `json:"min_time"`
	MaxTime int64 `json:"max_time"`
}

var _ replication.Block = Part{}

// Name returns the last component of the part's object prefix.
func (p Part) Name() string {
	if p.ID != "" {
		return p.ID
	}

	return fmt.Sprintf("%010d", p.Seq)
}

// Partition returns the owning node's id.
func (p Part) Partition() string { return p.Node }

// Range returns the part's logical block coverage.
func (p Part) Range() replication.Range {
	return replication.Range{First: p.First, Last: p.Last, Level: p.Level}
}

// Prefix is the backend key prefix holding this part's objects.
func (p Part) Prefix() string { return p.Node + "/" + p.Name() }

// Overlaps reports whether the part can contain records in the inclusive window.
func (p Part) Overlaps(from, to int64) bool { return p.MinTime <= to && p.MaxTime >= from }

// The old engine used signed sequences. The new allocator starts at one independently of
// those sequences, so reserve the upper half of the range for its block numbers.
const storageBlockBase uint64 = 1 << 63

// partFromIndex describes a new part or reconstructs a missing sidecar from the index.
func partFromIndex(node string, entry bucketindex.Entry) (Part, error) {
	p := Part{Node: node, MinTime: entry.MinTime, MaxTime: entry.MaxTime}
	name := path.Base(entry.Prefix)
	if seq, err := strconv.Atoi(name); err == nil && seq >= 0 && fmt.Sprintf("%010d", seq) == name {
		p.Seq = seq
		p.First, p.Last = uint64(seq), uint64(seq)
		return p, nil
	}

	blocks := entry.Blocks
	if !blocks.Valid() || blocks.Min.Term != 0 || blocks.Max.Term != 0 || blocks.Max.N >= storageBlockBase {
		return Part{}, errors.Errorf("part %q has no supported block range", entry.Prefix)
	}

	p.ID = name
	p.First = storageBlockBase + blocks.Min.N
	p.Last = storageBlockBase + blocks.Max.N
	p.Level = uint64(entry.Level)

	return p, nil
}

// mergedFrom sets the range of a merge output from its sources, including legacy coverage
// recorded only in shrimpd's sidecars.
func mergedFrom(p Part, sources []Part) Part {
	p.Level = 0
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
