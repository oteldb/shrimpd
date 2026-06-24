package shrimplication

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// Query returns entries within the given timestamp range, optionally filtered by term.
func (l *LSM) Query(ctx context.Context, from, to int64, term string) ([]shrimptypes.Entry, error) {
	l.mu.RLock()
	allParts := make([]shrimptypes.PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()

	// Step 1: Filter data parts by timestamp range
	timeParts := make([]shrimptypes.PartMeta, 0, 4) // preallocate for common case of 1-4 parts
	for _, meta := range allParts {
		if meta.Overlaps(from, to) {
			timeParts = append(timeParts, meta)
		}
	}
	normalizedTerm := strings.ToLower(term)

	// Step 2-4: Filter by index or fall back to old behavior
	useIndexFilter := false
	var indexedPartIDs map[string]struct{}
	if normalizedTerm != "" {
		matches, complete, err := l.idxEngine.Lookup(ctx, normalizedTerm, timeParts)
		if err != nil {
			slog.WarnContext(ctx, "index lookup failed, falling back to scanning", "error", err)
		} else if complete {
			useIndexFilter = true
			indexedPartIDs = matches
		}
	}

	var (
		result    = make([]shrimptypes.Entry, 0)
		getSparse = func(id string) []shrimptypes.SparseEntry {
			if s, ok := l.sparseCache.Get(id); ok {
				return s
			}
			s, _ := shrimpblock.ReadSidecar(l.sidecarPath(id))
			if s != nil {
				l.sparseCache.Set(id, s)
			}
			return s
		}
	)
	for _, meta := range timeParts {
		if useIndexFilter {
			if _, matched := indexedPartIDs[meta.ID]; !matched {
				continue
			}
		} else {
			if normalizedTerm != "" && !shrimpblock.HasToken(meta.Tokens, normalizedTerm) {
				continue
			}
		}

		// Try V2 path first
		if meta.FormatVersion == 1 {
			pf, err := l.partMgr.Get(meta.ID, meta)
			if err != nil {
				return nil, fmt.Errorf("open v2 part %s: %w", meta.ID, err)
			}
			if pf == nil {
				return nil, fmt.Errorf("v2 part %s not found on disk (replication pending?)", meta.ID)
			}
			for i, hdr := range pf.Headers {
				if hdr.MaxTimestamp < from || hdr.MinTimestamp > to {
					continue
				}
				if normalizedTerm != "" && !shrimpblock.BloomMightContain(&hdr.Bloom, normalizedTerm) {
					continue
				}

				ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
				rb, ok := l.rowBlockCache.Get(ck)
				if !ok {
					var err error
					rb, err = shrimpblock.ReadRowBlock(pf, i)
					if err != nil {
						slog.WarnContext(ctx, "read row block", "id", meta.ID, "block", i, "error", err)
						continue
					}
					l.rowBlockCache.Set(ck, rb)
				}

				for j := range rb.Timestamps {
					e := shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]}
					if e.Matches(from, to, normalizedTerm) {
						result = append(result, e)
					}
				}
			}
			continue
		}

		// Legacy path
		block, err := l.readLocalPart(meta.ID)
		if err != nil {
			slog.WarnContext(ctx, "skip part", "id", meta.ID, "error", err)
			continue
		}
		_ = getSparse(meta.ID)
		for _, e := range block.Data {
			if e.Matches(from, to, normalizedTerm) {
				result = append(result, e)
			}
		}
	}

	// Include memtable (not yet flushed to any part).
	l.mem.FilterTo(from, to, normalizedTerm, &result)

	slices.SortFunc(result, func(a, b shrimptypes.Entry) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return result, nil
}

// QueryStream calls fn for each entry matching [from, to] and term, streaming
// results without building a result slice. Peak memory is O(one decoded block)
// rather than O(all matching entries), which avoids OOM for high-cardinality
// terms like "error" across large data sets.
//
// Results arrive in part/block order (roughly ascending timestamp) but are NOT
// globally sorted. fn must not retain the Entry after returning.
//
// For cached blocks the existing RowBlock strings are reused. For uncached blocks,
// streamRowBlock decodes with StrBytes and only allocates a Go string per match.
func (l *LSM) QueryStream(ctx context.Context, from, to int64, term string, fn func(shrimptypes.Entry) error) error {
	l.mu.RLock()
	allParts := make([]shrimptypes.PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()

	timeParts := make([]shrimptypes.PartMeta, 0, 4)
	for _, meta := range allParts {
		if meta.Overlaps(from, to) {
			timeParts = append(timeParts, meta)
		}
	}
	normalizedTerm := strings.ToLower(term)

	useIndexFilter := false
	var indexedPartIDs map[string]struct{}
	if normalizedTerm != "" {
		matches, complete, err := l.idxEngine.Lookup(ctx, normalizedTerm, timeParts)
		if err != nil {
			slog.WarnContext(ctx, "index lookup failed, falling back to scanning", "error", err)
		} else if complete {
			useIndexFilter = true
			indexedPartIDs = matches
		}
	}

	getSparse := func(id string) []shrimptypes.SparseEntry {
		if s, ok := l.sparseCache.Get(id); ok {
			return s
		}
		s, _ := shrimpblock.ReadSidecar(l.sidecarPath(id))
		if s != nil {
			l.sparseCache.Set(id, s)
		}
		return s
	}

	for _, meta := range timeParts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if useIndexFilter {
			if _, matched := indexedPartIDs[meta.ID]; !matched {
				continue
			}
		} else {
			if normalizedTerm != "" && !shrimpblock.HasToken(meta.Tokens, normalizedTerm) {
				continue
			}
		}

		if meta.FormatVersion == 1 {
			pf, err := l.partMgr.Get(meta.ID, meta)
			if err != nil {
				return fmt.Errorf("open v2 part %s: %w", meta.ID, err)
			}
			if pf == nil {
				return fmt.Errorf("v2 part %s not found on disk (replication pending?)", meta.ID)
			}
			for i, hdr := range pf.Headers {
				if hdr.MaxTimestamp < from || hdr.MinTimestamp > to {
					continue
				}
				if normalizedTerm != "" && !shrimpblock.BloomMightContain(&hdr.Bloom, normalizedTerm) {
					continue
				}

				ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
				if rb, ok := l.rowBlockCache.Get(ck); ok {
					for j := range rb.Timestamps {
						e := shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]}
						if e.Matches(from, to, normalizedTerm) {
							if err := fn(e); err != nil {
								return err
							}
						}
					}
					continue
				}

				// Cache miss: stream without building a RowBlock or populating cache.
				if err := shrimpblock.StreamRowBlock(pf, i, from, to, normalizedTerm, fn); err != nil {
					slog.WarnContext(ctx, "stream row block", "id", meta.ID, "block", i, "error", err)
				}
			}
			continue
		}

		// Legacy path.
		block, err := l.readLocalPart(meta.ID)
		if err != nil {
			slog.WarnContext(ctx, "skip part", "id", meta.ID, "error", err)
			continue
		}
		_ = getSparse(meta.ID)
		for _, e := range block.Data {
			if e.Matches(from, to, normalizedTerm) {
				if err := fn(e); err != nil {
					return err
				}
			}
		}
	}

	return l.mem.StreamTo(from, to, normalizedTerm, fn)
}
