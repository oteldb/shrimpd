package shrimpblock

import (
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"github.com/zeebo/xxh3"
)

// BloomAdd adds a token to the given bloom filter. The filter is modified in place.
func BloomAdd(b *shrimptypes.BloomFilter, token string) {
	h := xxh3.HashString128(token)
	h1, h2 := h.Lo, h.Hi
	for i := range shrimptypes.BloomK {
		setBit(b, h1, h2, i)
	}
}

// BloomMightContain checks whether the given token might be present in the bloom filter.
// Returns true if it might be present, false if it is definitely not present.
func BloomMightContain(b *shrimptypes.BloomFilter, token string) bool {
	h := xxh3.HashString128(token)
	h1, h2 := h.Lo, h.Hi
	for i := range shrimptypes.BloomK {
		if !getBit(b, h1, h2, i) {
			return false
		}
	}
	return true
}

func indexBit(h1, h2 uint64, i int) uint64 {
	return (h1 + uint64(i)*h2) % shrimptypes.BloomBits
}

func setBit(b *shrimptypes.BloomFilter, h1, h2 uint64, i int) {
	index := indexBit(h1, h2, i)
	b[index/8] |= 1 << (index % 8)
}

func getBit(b *shrimptypes.BloomFilter, h1, h2 uint64, i int) bool {
	index := indexBit(h1, h2, i)
	val := b[index/8] & (1 << (index % 8))
	return val != 0
}
