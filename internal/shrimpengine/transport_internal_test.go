package shrimpengine

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// encodeKeyList mirrors what ListHandler writes, for round-trip assertions.
func encodeKeyList(keys []string) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(keys)))
	for _, k := range keys {
		buf = binary.AppendUvarint(buf, uint64(len(k)))
		buf = append(buf, k...)
	}

	return buf
}

func TestDecodeKeyList(t *testing.T) {
	t.Parallel()

	for name, keys := range map[string][]string{
		"empty":     {},
		"one":       {"a/0000000001/manifest"},
		"several":   {"a/0000000001/marks", "a/0000000001/c/0", "a/streams.bin"},
		"empty key": {""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeKeyList(encodeKeyList(keys))
			require.NoError(t, err)
			require.Equal(t, keys, got)
		})
	}
}

// TestDecodeKeyListRejectsHostileInput covers what a broken or malicious peer can put on the
// wire. The count in particular must never be trusted for sizing: it is one varint that could
// claim 2^64 keys.
func TestDecodeKeyListRejectsHostileInput(t *testing.T) {
	t.Parallel()

	hugeCount := binary.AppendUvarint(nil, 1<<62)
	hugeCount = append(hugeCount, "a/1"...)

	hugeLength := binary.AppendUvarint(nil, 1)
	hugeLength = binary.AppendUvarint(hugeLength, 1<<62)
	hugeLength = append(hugeLength, "a/1"...)

	for name, data := range map[string][]byte{
		"empty":               {},
		"count only":          binary.AppendUvarint(nil, 1),
		"count exceeds body":  hugeCount,
		"length exceeds body": hugeLength,
		"truncated mid-key":   append(binary.AppendUvarint(binary.AppendUvarint(nil, 1), 10), "short"...),
		"incomplete uvarint":  {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeKeyList(data)
			require.Error(t, err, "hostile input must be rejected, not accepted or fatal")
		})
	}
}

// FuzzDecodeKeyList throws arbitrary bytes at the peer-response decoder. It must never panic and
// never allocate on a peer's say-so; anything it accepts must round-trip.
func FuzzDecodeKeyList(f *testing.F) {
	f.Add(encodeKeyList([]string{"a/0000000001/manifest", "a/streams.bin"}))
	f.Add(encodeKeyList(nil))
	f.Add([]byte{})
	f.Add(binary.AppendUvarint(nil, 1))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		keys, err := decodeKeyList(data)
		if err != nil {
			return
		}

		again, err := decodeKeyList(encodeKeyList(keys))
		require.NoError(t, err, "an accepted listing must re-encode to an acceptable one")
		require.Equal(t, keys, again)
	})
}
