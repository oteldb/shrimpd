package replication_test

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
)

// TestRecordRoundTrip checks that a log record survives the metadata store unchanged. Every
// replica reconstructs its work list from these bytes, so a field lost in encoding is a part that
// silently never replicates.
func TestRecordRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)

	for name, rec := range map[string]replication.Record[testBlock]{
		"create": {
			Op:        replication.OpCreate,
			Block:     newBlock("p", 7, 7, 0),
			Replica:   "node1",
			CreatedAt: now,
		},
		"merge": {
			Op:      replication.OpMerge,
			Block:   newBlock("p", 1, 4, 1),
			Sources: []testBlock{newBlock("p", 1, 2, 0), newBlock("p", 3, 4, 0)},
			Replica: "node2",

			CreatedAt: now,
		},
		"drop": {
			Op:        replication.OpDrop,
			Block:     newBlock("q", 9, 9, 0),
			Replica:   "node3",
			CreatedAt: now,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(rec)
			require.NoError(t, err)

			var got replication.Record[testBlock]
			require.NoError(t, json.Unmarshal(data, &got))

			require.Equal(t, rec.Op, got.Op)
			require.Equal(t, replication.Key(rec.Block), replication.Key(got.Block))
			require.Equal(t, rec.Block.Range(), got.Block.Range())
			require.Equal(t, rec.Replica, got.Replica)
			require.True(t, rec.CreatedAt.Equal(got.CreatedAt))
			require.Len(t, got.Sources, len(rec.Sources))

			for i := range rec.Sources {
				require.Equal(t, replication.Key(rec.Sources[i]), replication.Key(got.Sources[i]))
			}
		})
	}
}

// FuzzRecordDecode throws arbitrary bytes at the log-record decoder. A malformed record reaches
// this path whenever the metadata store is shared with anything else, or a record was written by
// a different version — it must be rejected, never panic and never decode into something that
// would make a replica act on nonsense.
func FuzzRecordDecode(f *testing.F) {
	for _, rec := range []replication.Record[testBlock]{
		{Op: replication.OpCreate, Block: newBlock("p", 1, 1, 0), Replica: "a"},
		{Op: replication.OpMerge, Block: newBlock("p", 1, 4, 1), Replica: "b"},
		{Op: replication.OpDrop, Block: newBlock("p", 2, 2, 0), Replica: "c"},
	} {
		data, err := json.Marshal(rec)
		require.NoError(f, err)
		f.Add(data)
	}

	f.Add([]byte(""))
	f.Add([]byte("{}"))
	f.Add([]byte(`{"op":"create"}`))
	f.Add([]byte(`{"op":"create","block":{"range":{"first":9,"last":1}}}`)) // inverted interval
	f.Add([]byte(`{"op":"","block":{}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		rec, err := replication.UnmarshalRecordForTest[testBlock](data)
		if err != nil {
			return
		}

		// A record that decoded must be one a replica can act on without further checks.
		require.NotEmpty(t, rec.Op, "an accepted record must name an operation")
		require.True(t, rec.Range().Valid(),
			"an accepted record must not carry an inverted interval: %s", rec.Range())

		// And it must survive a re-encode, since a clone copies records verbatim.
		again, err := json.Marshal(rec)
		require.NoError(t, err)

		reparsed, err := replication.UnmarshalRecordForTest[testBlock](again)
		require.NoError(t, err)
		require.Equal(t, rec.Op, reparsed.Op)
		require.Equal(t, rec.Range(), reparsed.Range())
	})
}

// TestRangeAlgebra asserts the properties the obsolescence rules rely on. They are stated as laws
// rather than examples because every one of them is load-bearing: containment decides what a
// replica deletes, and an asymmetry or a cycle there means deleting data that nothing replaces.
func TestRangeAlgebra(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test input

	ranges := make([]replication.Range, 0, 200)
	for range cap(ranges) {
		first := uint64(rng.Intn(20))
		ranges = append(ranges, replication.Range{
			First: first,
			Last:  first + uint64(rng.Intn(10)),
			Level: uint64(rng.Intn(4)),
		})
	}

	for _, a := range ranges {
		require.False(t, replication.Contains(a, a),
			"containment must be irreflexive, or a part would supersede itself: %s", a)

		for _, b := range ranges {
			// Antisymmetry: two parts cannot each make the other redundant.
			require.False(t, replication.Contains(a, b) && replication.Contains(b, a),
				"containment must be antisymmetric: %s and %s", a, b)

			// Containment implies overlap, so the queue's conflict rule always serializes a
			// merge against the sources it supersedes.
			if replication.Contains(a, b) {
				require.True(t, replication.Intersects(a, b),
					"a part that supersedes another must conflict with it: %s, %s", a, b)
			}

			require.Equal(t, replication.Intersects(a, b), replication.Intersects(b, a),
				"overlap must be symmetric: %s, %s", a, b)

			for _, c := range ranges {
				// Transitivity: dropping only what a new part directly supersedes must not
				// leave something the superseded part had itself superseded.
				if replication.Contains(a, b) && replication.Contains(b, c) {
					require.True(t, replication.Contains(a, c),
						"containment must be transitive: %s ⊃ %s ⊃ %s", a, b, c)
				}
			}
		}
	}
}

// TestMostCoveringIsIdempotentAndPreservesCoverage checks the reduction a clone plan is built
// from: it must be a fixed point, and it must not narrow what the set covers.
func TestMostCoveringIsIdempotentAndPreservesCoverage(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test input

	for range 200 {
		blocks := make([]testBlock, 0, 8)

		for range cap(blocks) {
			first := uint64(rng.Intn(10))
			blocks = append(blocks, newBlock(
				[]string{"p", "q"}[rng.Intn(2)],
				first,
				first+uint64(rng.Intn(5)),
				uint64(rng.Intn(3)),
			))
		}

		reduced := replication.MostCovering(blocks)

		once := keysOf(reduced)
		twice := keysOf(replication.MostCovering(reduced))
		require.Equal(t, once, twice, "reducing an already reduced set must change nothing")

		// Every original block is still covered by something in the reduction.
		for _, b := range blocks {
			require.True(t, replication.Covered(reduced, b),
				"reduction dropped coverage of %s", replication.Key(b))
		}
	}
}
