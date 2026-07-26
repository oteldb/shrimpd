package replication_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
)

func TestContains(t *testing.T) {
	t.Parallel()

	r := func(first, last, level uint64) replication.Range {
		return replication.Range{First: first, Last: last, Level: level}
	}

	for _, tt := range []struct {
		name string
		a, b replication.Range
		want bool
	}{
		{"merge result contains source", r(1, 4, 1), r(1, 1, 0), true},
		{"merge result contains last source", r(1, 4, 1), r(4, 4, 0), true},
		{"same range same level is not containment", r(1, 4, 1), r(1, 4, 1), false},
		{"same range higher level contains", r(1, 4, 2), r(1, 4, 1), true},
		{"disjoint intervals", r(1, 4, 1), r(5, 8, 0), false},
		{"partial overlap does not contain", r(1, 4, 1), r(3, 6, 0), false},
		{"lower level never contains", r(1, 8, 0), r(2, 3, 1), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, replication.Contains(tt.a, tt.b))
		})
	}
}

func TestIntersects(t *testing.T) {
	t.Parallel()

	r := func(first, last uint64) replication.Range {
		return replication.Range{First: first, Last: last}
	}

	require.True(t, replication.Intersects(r(1, 4), r(4, 9)))
	require.True(t, replication.Intersects(r(1, 9), r(3, 4)))
	require.False(t, replication.Intersects(r(1, 4), r(5, 9)))
	require.False(t, replication.Intersects(r(5, 9), r(1, 4)))
}

func TestMostCovering(t *testing.T) {
	t.Parallel()

	blocks := []testBlock{
		newBlock("p", 1, 1, 0),
		newBlock("p", 2, 2, 0),
		newBlock("p", 1, 2, 1), // supersedes both above
		newBlock("p", 3, 3, 0),
		newBlock("q", 1, 1, 0), // different partition: never superseded
	}

	got := replication.MostCovering(blocks)
	replication.SortBlocks(got)

	require.Equal(t, []string{"p/1_2_1", "p/3_3_0", "q/1_1_0"}, keysOf(got))
}

func TestSupersededByAndCovered(t *testing.T) {
	t.Parallel()

	have := []testBlock{
		newBlock("p", 1, 1, 0),
		newBlock("p", 2, 2, 0),
		newBlock("p", 9, 9, 0),
	}
	merged := newBlock("p", 1, 2, 1)

	require.False(t, replication.Covered(have, merged))
	require.Equal(t, []string{"p/1_1_0", "p/2_2_0"}, keysOf(replication.SupersededBy(have, merged)))

	// Once the merge result is held, both the result and its sources are covered.
	have = append(have, merged)
	require.True(t, replication.Covered(have, merged))
	require.True(t, replication.Covered(have, newBlock("p", 1, 1, 0)))
	require.False(t, replication.Covered(have, newBlock("p", 1, 9, 1)))
}

func keysOf(blocks []testBlock) []string {
	out := make([]string, 0, len(blocks))
	for _, b := range blocks {
		out = append(out, replication.Key(b))
	}

	return out
}
