package shrimpblock

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestBloomAddAndContain(t *testing.T) {
	var b shrimptypes.BloomFilter

	BloomAdd(&b, "compacted")
	require.True(t, BloomMightContain(&b, "compacted"),
		"must contain token that was added")
}

func TestBloomMissing(t *testing.T) {
	var b shrimptypes.BloomFilter

	BloomAdd(&b, "compacted")
	require.False(t, BloomMightContain(&b, "other"),
		"must not contain token that was never added")
}

func TestBloomMultiple(t *testing.T) {
	var b shrimptypes.BloomFilter

	for _, tok := range []string{"compacted", "index", "parts", "errors", "logs"} {
		BloomAdd(&b, tok)
	}

	for _, tok := range []string{"compacted", "index", "parts", "errors", "logs"} {
		require.True(t, BloomMightContain(&b, tok),
			"must contain added token %q", tok)
	}

	require.False(t, BloomMightContain(&b, "nonexistent"),
		"must not contain unadded token")
}
