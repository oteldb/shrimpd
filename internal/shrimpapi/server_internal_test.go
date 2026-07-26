package shrimpapi

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseLimit pins the query-parameter boundaries. The interesting cases are the ones a client
// controls: a limit too large for the platform's int must not wrap into a *smaller* number, which
// on a 32-bit build would turn a bounded query into an unbounded one.
func TestParseLimit(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		in   string
		want int
	}{
		"absent":            {"", 0},
		"zero means all":    {"0", 0},
		"ordinary":          {"250", 250},
		"negative":          {"-1", 0},
		"not a number":      {"many", 0},
		"float":             {"1.5", 0},
		"overflows int64":   {"99999999999999999999999", 0},
		"max int":           {strconv.Itoa(math.MaxInt), math.MaxInt},
		"just past 32 bits": {"4294967296", int64To32Safe(4294967296)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := parseLimit(tt.in)
			require.GreaterOrEqual(t, got, 0, "a limit must never be negative")
			require.Equal(t, tt.want, got)
		})
	}
}

// int64To32Safe is the expected result for a value beyond 32 bits: the real number where int is
// 64-bit, and "no limit" where parsing it cannot succeed.
func int64To32Safe(v int64) int {
	if math.MaxInt >= v {
		return int(v)
	}

	return 0
}

func TestParseInt(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(42), parseInt("42", 7))
	require.Equal(t, int64(7), parseInt("", 7), "an absent value falls back to the default")
	require.Equal(t, int64(7), parseInt("nonsense", 7), "an unparseable value falls back too")
	require.Equal(t, int64(-5), parseInt("-5", 0), "a negative timestamp is a legitimate window bound")
	require.Equal(t, int64(math.MaxInt64), parseInt(strconv.FormatInt(math.MaxInt64, 10), 0))
}
