package shrimpblock

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/go-faster/jx"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"

	"github.com/klauspost/compress/zstd"
)

const (
	MagicShrimp = "SHMP"
	v2Version   = 0x01

	v2HeaderSize  = 16   // 4 + 1 + 3 + 8
	v2BlockDirRow = 1096 // per-block directory entry size
	v2BlockRows   = 512  // default rows per block
)

// PartFileV2 holds an open file descriptor and its block directory in memory.
type PartFileV2 struct {
	Meta    shrimptypes.PartMeta
	Headers []shrimptypes.BlockHeader
	fd      *os.File
}

// WritePartV2 splits entries into n-row blocks, builds bloom per block,
// compresses each block independently, and writes the header + directory + data.
// Returns the written headers.
func WritePartV2(path string, entries []shrimptypes.Entry) ([]shrimptypes.BlockHeader, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-v2-")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	name := tmp.Name()

	blockCount := (len(entries) + v2BlockRows - 1) / v2BlockRows
	if blockCount == 0 {
		blockCount = 1
	}
	blocks := make([][]shrimptypes.Entry, 0, blockCount)
	for i := 0; i < len(entries); i += v2BlockRows {
		end := min(i+v2BlockRows, len(entries))
		blocks = append(blocks, entries[i:end])
	}

	headers := make([]shrimptypes.BlockHeader, len(blocks))

	// Write magic header placeholder
	headerBuf := make([]byte, v2HeaderSize)
	copy(headerBuf[0:4], MagicShrimp)
	headerBuf[4] = v2Version
	// reserved: bytes 5-7 are zero
	binary.LittleEndian.PutUint64(headerBuf[8:16], uint64(len(blocks)))

	if _, err := tmp.Write(headerBuf); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("write header: %w", err)
	}

	// Write block directory placeholder
	dirOffset := int64(v2HeaderSize)
	dirSize := int64(len(blocks)) * v2BlockDirRow
	if _, err := tmp.Write(make([]byte, dirSize)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("write dir placeholder: %w", err)
	}

	// Write each block
	payloadOffset := dirOffset + dirSize
	enc := encoderPool.Get().(*zstd.Encoder)
	defer encoderPool.Put(enc)

	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	for bi, block := range blocks {
		// Build columnar JSON: {"ts":[...],"d":[...]} using jx.Writer (no reflection).
		jw.Reset()
		jw.ObjStart()
		jw.RawStr(`"ts":`)
		jw.ArrStart()
		for i, e := range block {
			if i != 0 {
				jw.Comma()
			}
			jw.Int64(e.Timestamp)
		}
		jw.ArrEnd()
		jw.RawStr(`,"d":`)
		jw.ArrStart()
		for i, e := range block {
			if i != 0 {
				jw.Comma()
			}
			jw.Str(e.Data)
		}
		jw.ArrEnd()
		jw.ObjEnd()

		enc.Reset(tmp)
		if _, err := enc.Write(jw.Buf); err != nil {
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, fmt.Errorf("compress block: %w", err)
		}
		if err := enc.Close(); err != nil {
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, fmt.Errorf("close zstd: %w", err)
		}
		compressedEnd, _ := tmp.Seek(0, io.SeekCurrent)
		compressedSz := compressedEnd - payloadOffset

		// Build bloom filter from block entries
		var bloom shrimptypes.BloomFilter
		for _, e := range block {
			for tok := range Tokenize(e.Data) {
				BloomAdd(&bloom, tok)
			}
		}

		headers[bi] = shrimptypes.BlockHeader{
			Offset:       payloadOffset,
			CompressedSz: compressedSz,
			Count:        int32(len(block)),
			MinTimestamp: block[0].Timestamp,
			MaxTimestamp: block[len(block)-1].Timestamp,
			Bloom:        bloom,
		}

		payloadOffset = compressedEnd
	}

	// Rewrite block directory
	dirBuf := make([]byte, dirSize)
	for bi, h := range headers {
		row := dirBuf[bi*v2BlockDirRow : (bi+1)*v2BlockDirRow]
		binary.LittleEndian.PutUint64(row[0:8], uint64(h.Offset))
		binary.LittleEndian.PutUint64(row[8:16], uint64(h.CompressedSz))
		binary.LittleEndian.PutUint32(row[16:20], uint32(h.Count))
		binary.LittleEndian.PutUint64(row[20:28], uint64(h.MinTimestamp))
		binary.LittleEndian.PutUint64(row[28:36], uint64(h.MaxTimestamp))
		copy(row[36:1060], h.Bloom[:])
	}

	if _, err := tmp.WriteAt(dirBuf, dirOffset); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("write dir: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	if err := os.Rename(name, path); err != nil {
		return nil, err
	}

	return headers, nil
}

// OpenPartV2 reads the magic, block directory, and returns a PartFileV2.
// If the file does not have the V2 magic, returns nil, nil.
func OpenPartV2(path string, meta shrimptypes.PartMeta) (*PartFileV2, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal part path
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	br := bufio.NewReaderSize(f, 512)
	head, err := br.Peek(4)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if string(head) != MagicShrimp {
		_ = f.Close()
		return nil, nil // legacy JSON file
	}

	// Read header
	var hdrBuf [v2HeaderSize]byte
	if _, err := io.ReadFull(br, hdrBuf[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read v2 header: %w", err)
	}

	blockCount := int(binary.LittleEndian.Uint64(hdrBuf[8:16]))

	// Read block directory
	dirSize := blockCount * v2BlockDirRow
	dirBuf := make([]byte, dirSize)
	if _, err := io.ReadFull(br, dirBuf); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read block dir: %w", err)
	}

	headers := make([]shrimptypes.BlockHeader, blockCount)
	for bi := range blockCount {
		row := dirBuf[bi*v2BlockDirRow : (bi+1)*v2BlockDirRow]
		headers[bi] = shrimptypes.BlockHeader{
			Offset:       int64(binary.LittleEndian.Uint64(row[0:8])),
			CompressedSz: int64(binary.LittleEndian.Uint64(row[8:16])),
			Count:        int32(binary.LittleEndian.Uint32(row[16:20])),
			MinTimestamp: int64(binary.LittleEndian.Uint64(row[20:28])),
			MaxTimestamp: int64(binary.LittleEndian.Uint64(row[28:36])),
		}
		copy(headers[bi].Bloom[:], row[36:1060])
	}

	return &PartFileV2{
		Meta:    meta,
		Headers: headers,
		fd:      f,
	}, nil
}

// Close closes the underlying file descriptor.
func (pf *PartFileV2) Close() error {
	return pf.fd.Close()
}

// ReadRowBlock pread-fetches exactly hdr.CompressedSz bytes at hdr.Offset,
// decompresses, decodes columnar JSON into RowBlock.
func ReadRowBlock(pf *PartFileV2, idx int) (*shrimptypes.RowBlock, error) {
	if idx < 0 || idx >= len(pf.Headers) {
		return nil, fmt.Errorf("block index %d out of range (0-%d)", idx, len(pf.Headers)-1)
	}
	hdr := pf.Headers[idx]

	compressed := make([]byte, hdr.CompressedSz)
	if _, err := pf.fd.ReadAt(compressed, hdr.Offset); err != nil {
		return nil, fmt.Errorf("read block %d: %w", idx, err)
	}

	dec := decoderPool.Get().(*zstd.Decoder)
	defer func() {
		_ = dec.Reset(nil)
		decoderPool.Put(dec)
	}()

	decoded, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", idx, err)
	}

	// Decode columnar JSON {"ts":[...],"d":[...]} without reflection.
	// decoded is fully in memory after zstd decompress, so DecodeBytes is zero-copy
	// for string slices where the source lives long enough (we copy via Str()).
	var (
		timestamps []int64
		data       []string
	)
	jd := jx.DecodeBytes(decoded)
	if err := jd.ObjBytes(func(jd *jx.Decoder, key []byte) error {
		switch string(key) {
		case "ts":
			return jd.Arr(func(jd *jx.Decoder) error {
				v, err := jd.Int64()
				if err != nil {
					return err
				}
				timestamps = append(timestamps, v)
				return nil
			})
		case "d":
			return jd.Arr(func(jd *jx.Decoder) error {
				v, err := jd.Str()
				if err != nil {
					return err
				}
				data = append(data, v)
				return nil
			})
		default:
			return jd.Skip()
		}
	}); err != nil {
		return nil, fmt.Errorf("decode block %d: %w", idx, err)
	}

	cost := uint32(len(timestamps) * 8)
	for _, s := range data {
		cost += uint32(len(s))
	}

	return &shrimptypes.RowBlock{
		Timestamps: timestamps,
		Data:       data,
		Cost:       cost,
	}, nil
}

// streamRowBlock decompresses block idx and calls fn for each entry that passes
// the timestamp range and term filter. Unlike readRowBlock, no RowBlock is built
// and no result slice is accumulated: only one string is allocated per matching
// entry via string(StrBytes(…)). The decoded buffer is discarded after return.
//
// Two-pass decode: pass 1 collects all timestamps (int64, zero string allocs),
// pass 2 calls StrBytes per data element — a subslice of the decoded buffer for
// plain strings — and only materializes a Go string for entries that match.
func StreamRowBlock(pf *PartFileV2, idx int, from, to int64, term string, fn func(shrimptypes.Entry) error) error {
	if idx < 0 || idx >= len(pf.Headers) {
		return fmt.Errorf("block index %d out of range (0-%d)", idx, len(pf.Headers)-1)
	}
	hdr := pf.Headers[idx]

	compressed := make([]byte, hdr.CompressedSz)
	if _, err := pf.fd.ReadAt(compressed, hdr.Offset); err != nil {
		return fmt.Errorf("read block %d: %w", idx, err)
	}

	dec := decoderPool.Get().(*zstd.Decoder)
	decoded, err := dec.DecodeAll(compressed, nil)
	_ = dec.Reset(nil)
	decoderPool.Put(dec)
	if err != nil {
		return fmt.Errorf("decompress block %d: %w", idx, err)
	}

	// Pass 1: collect timestamps (int64 — no string allocations).
	var timestamps []int64
	jd1 := jx.DecodeBytes(decoded)
	if err := jd1.ObjBytes(func(jd *jx.Decoder, key []byte) error {
		if string(key) == "ts" {
			return jd.Arr(func(jd *jx.Decoder) error {
				v, err := jd.Int64()
				if err == nil {
					timestamps = append(timestamps, v)
				}
				return err
			})
		}
		return jd.Skip()
	}); err != nil {
		return fmt.Errorf("decode ts block %d: %w", idx, err)
	}

	// Pass 2: decode data strings with StrBytes (subslice of decoded — no alloc for
	// plain strings). Skip the string entirely for out-of-range timestamps. Only
	// call string(sb) for entries that actually match.
	j := 0
	jd2 := jx.DecodeBytes(decoded)
	return jd2.ObjBytes(func(jd *jx.Decoder, key []byte) error {
		if string(key) != "d" {
			return jd.Skip()
		}
		return jd.Arr(func(jd *jx.Decoder) error {
			if j >= len(timestamps) {
				return jd.Skip()
			}
			ts := timestamps[j]
			j++
			if ts < from || ts > to {
				return jd.Skip()
			}
			sb, err := jd.StrBytes()
			if err != nil {
				return err
			}
			if term != "" && !shrimptypes.FoldContains(sb, term) {
				return nil
			}
			return fn(shrimptypes.Entry{Timestamp: ts, Data: string(sb)})
		})
	})
}

// V2ToBlock converts a V2 part file to a legacy Block for backward-compatible
// remote serving. It reads and merges all blocks.
func V2ToBlock(pf *PartFileV2) (shrimptypes.Block, error) {
	var entries []shrimptypes.Entry
	for i := range pf.Headers {
		rb, err := ReadRowBlock(pf, i)
		if err != nil {
			return shrimptypes.Block{}, err
		}
		for j := range rb.Timestamps {
			entries = append(entries, shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]})
		}
	}
	slices.SortFunc(entries, func(a, b shrimptypes.Entry) int {
		return int(a.Timestamp - b.Timestamp)
	})
	return shrimptypes.Block{
		SourceReplica: pf.Meta.NodeID,
		CreatedAt:     time.Now().UnixNano(),
		Data:          entries,
	}, nil
}
