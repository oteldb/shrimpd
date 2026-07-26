package replication

import (
	"cmp"
	"fmt"
	"slices"
)

// Range is the identity of a part in block-number space: the inclusive interval of block numbers
// it covers within its partition, and the merge level that produced it.
//
// A freshly written part covers the single block it was allocated ([Replication.AllocateBlock])
// at level 0. Merging parts covering [1,3] and [4,7] yields [1,7] at level 1. Because merges only
// ever combine adjacent intervals, a part's interval plus its level is enough to decide
// obsolescence without any other coordination — see [Contains].
type Range struct {
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
	Level uint64 `json:"level"`
}

// String renders the range as `first_last_level`, the ClickHouse part-name convention.
func (r Range) String() string { return fmt.Sprintf("%d_%d_%d", r.First, r.Last, r.Level) }

// Valid reports whether the interval is well formed.
func (r Range) Valid() bool { return r.First <= r.Last }

// Contains reports whether a strictly supersedes b: a covers every block b covers, and a is the
// result of at least one more merge. The level comparison is what keeps the relation strict —
// without it a part would contain itself, and a replica would drop a part in favor of itself.
func Contains(a, b Range) bool {
	return a.First <= b.First && a.Last >= b.Last && a.Level > b.Level
}

// Equals reports whether a and b are the same part.
func Equals(a, b Range) bool { return a == b }

// ContainsOrEquals reports whether holding a makes b redundant.
func ContainsOrEquals(a, b Range) bool { return Equals(a, b) || Contains(a, b) }

// Intersects reports whether the two block-number intervals overlap at all, regardless of level.
// It is the conflict test for the execution queue: two entries touching overlapping blocks must
// not run concurrently, because each may drop parts the other is reading.
func Intersects(a, b Range) bool { return a.First <= b.Last && b.First <= a.Last }

// Block is the replicated unit — a part, identified by a name that is unique within its
// partition and by the block range it covers.
//
// Implementations must be plain, JSON-serializable values: a log record carries the block
// verbatim, so every replica reconstructs the same descriptor from the same bytes. Use a struct
// (or pointer to one) with exported, tagged fields.
type Block interface {
	// Name is the block's identity within its partition. It must be stable and derivable by
	// every replica from the same log record — conventionally the [Range] string.
	Name() string
	// Partition is the replication partition this block belongs to. Parts in different
	// partitions never merge and never conflict, so partitions replicate independently.
	Partition() string
	// Range is the block-number interval and level this block covers.
	Range() Range
}

// Key is the partition-qualified identity of a block, unique across the whole replicated set.
func Key[B Block](b B) string { return b.Partition() + "/" + b.Name() }

// Covered reports whether any block in have supersedes b (or is b itself) — the test a replica
// applies before doing any work for a log record: if the answer is yes, the record is already
// satisfied by what is on disk.
func Covered[B Block](have []B, b B) bool {
	br := b.Range()

	for _, h := range have {
		if h.Partition() == b.Partition() && ContainsOrEquals(h.Range(), br) {
			return true
		}
	}

	return false
}

// SupersededBy returns the blocks in have that b makes redundant. After installing b a replica
// drops exactly these — they are the merge sources, or any subset of them a previous merge had
// already combined.
func SupersededBy[B Block](have []B, b B) []B {
	br := b.Range()

	var out []B

	for _, h := range have {
		if h.Partition() == b.Partition() && Contains(br, h.Range()) {
			out = append(out, h)
		}
	}

	return out
}

// MostCovering drops every block that another block in the set already supersedes, leaving the
// minimal set with the same coverage. It is what turns a peer's raw part list into the set worth
// fetching when cloning a lost replica: fetching a merge source as well as the merge result is
// pure waste.
func MostCovering[B Block](blocks []B) []B {
	if len(blocks) < 2 {
		return blocks
	}

	out := make([]B, 0, len(blocks))

	for i, b := range blocks {
		covered := false

		for j, other := range blocks {
			if i == j || other.Partition() != b.Partition() {
				continue
			}

			if Contains(other.Range(), b.Range()) {
				covered = true

				break
			}
		}

		if !covered {
			out = append(out, b)
		}
	}

	return out
}

// SortBlocks orders blocks by partition, then by first block number, then by level — a stable,
// replica-independent order, so any two replicas presenting the same set present it identically.
func SortBlocks[B Block](blocks []B) {
	slices.SortFunc(blocks, func(a, b B) int {
		if c := cmp.Compare(a.Partition(), b.Partition()); c != 0 {
			return c
		}

		ar, br := a.Range(), b.Range()
		if c := cmp.Compare(ar.First, br.First); c != 0 {
			return c
		}

		if c := cmp.Compare(ar.Last, br.Last); c != 0 {
			return c
		}

		return cmp.Compare(ar.Level, br.Level)
	})
}
