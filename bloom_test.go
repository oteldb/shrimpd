package shrimpd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloomAddAndContain(t *testing.T) {
	var b [bloomBytes]byte

	bloomAdd(&b, "compacted")
	require.True(t, bloomMightContain(&b, "compacted"),
		"must contain token that was added")
}

func TestBloomMissing(t *testing.T) {
	var b [bloomBytes]byte

	bloomAdd(&b, "compacted")
	require.False(t, bloomMightContain(&b, "other"),
		"must not contain token that was never added")
}

func TestBloomMultiple(t *testing.T) {
	var b [bloomBytes]byte

	for _, tok := range []string{"compacted", "index", "parts", "errors", "logs"} {
		bloomAdd(&b, tok)
	}

	for _, tok := range []string{"compacted", "index", "parts", "errors", "logs"} {
		require.True(t, bloomMightContain(&b, tok),
			"must contain added token %q", tok)
	}

	require.False(t, bloomMightContain(&b, "nonexistent"),
		"must not contain unadded token")
}
