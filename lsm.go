// Package shrimpd provides a small LSM-backed distributed log store.
package shrimpd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	flushThreshold  = 100             // entries: eager flush when memtable exceeds this
	flushInterval   = 5 * time.Second // time-based flush regardless of size
	compactTrigger  = 4               // L0 parts before compaction kicks in
	compactInterval = 15 * time.Second
)

var remoteHTTP = &http.Client{Timeout: 10 * time.Second}

// LSM owns local writes, local parts, compaction, and distributed reads.
type LSM struct {
	nodeID  string
	addr    string
	dataDir string

	mem      *MemTable
	wal      *WAL
	reg      *Registry
	flushSig chan struct{} // buffered(1): signal from Write when threshold crossed

	mu    sync.RWMutex
	parts []PartMeta // all parts replicated locally, kept in sync with etcd log
}

// NewLSM creates an LSM instance and replays unflushed entries from the WAL.
func NewLSM(nodeID, addr, dataDir string, wal *WAL, reg *Registry) (*LSM, error) {
	l := &LSM{
		nodeID:   nodeID,
		addr:     addr,
		dataDir:  dataDir,
		mem:      &MemTable{},
		wal:      wal,
		reg:      reg,
		flushSig: make(chan struct{}, 1),
	}
	// Replay WAL to recover any entries not yet flushed to a part.
	entries, err := wal.Recover()
	if err != nil {
		return nil, fmt.Errorf("wal recover: %w", err)
	}
	if len(entries) > 0 {
		slog.Info("recovered entries from wal", "count", len(entries))
		l.mem.Write(entries)
	}
	return l, nil
}

// Write is safe for concurrent use. Durable after WAL fsync.
func (l *LSM) Write(_ context.Context, entries []Entry) error {
	if err := l.wal.Append(entries); err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	l.mem.Write(entries)
	if l.mem.Len() >= flushThreshold {
		select {
		case l.flushSig <- struct{}{}:
		default: // already signaled
		}
	}
	return nil
}

// Run registers the node, loads existing parts, then drives the flush/compact loop.
// Returns when ctx is canceled (after a final flush attempt).
func (l *LSM) Run(ctx context.Context) error {
	if err := l.startup(ctx); err != nil {
		return err
	}

	flushTick := time.NewTicker(flushInterval)
	compactTick := time.NewTicker(compactInterval)
	defer flushTick.Stop()
	defer compactTick.Stop()

	// Start background replication loop.
	go func() {
		if err := l.replicationLoop(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "replication loop failed", "error", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			if l.mem.Len() > 0 {
				_ = l.flush(context.Background())
			}
			return ctx.Err()
		case <-l.flushSig:
			if err := l.flush(ctx); err != nil {
				slog.ErrorContext(ctx, "flush failed", "error", err)
			}
		case <-flushTick.C:
			if l.mem.Len() > 0 {
				if err := l.flush(ctx); err != nil {
					slog.ErrorContext(ctx, "flush failed", "error", err)
				}
			}
		case <-compactTick.C:
			if err := l.compact(ctx, false); err != nil {
				slog.ErrorContext(ctx, "compact failed", "error", err)
			}
		}
	}
}

func (l *LSM) startup(ctx context.Context) error {
	if err := l.reg.RegisterNode(ctx, l.addr); err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// Scan local parts from the disk metadata files.
	files, err := filepath.Glob(filepath.Join(l.dataDir, "parts", "*.meta"))
	if err != nil {
		return fmt.Errorf("glob parts: %w", err)
	}

	var localParts []PartMeta
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			slog.WarnContext(ctx, "failed to open meta file", "file", file, "error", err)
			continue
		}
		var meta PartMeta
		decodeErr := json.NewDecoder(f).Decode(&meta)
		f.Close()
		if decodeErr != nil {
			slog.WarnContext(ctx, "failed to decode meta file", "file", file, "error", decodeErr)
			continue
		}
		localParts = append(localParts, meta)
	}

	l.mu.Lock()
	l.parts = localParts
	l.mu.Unlock()

	slog.InfoContext(ctx, "loaded local parts from disk", "count", len(l.parts))
	return nil
}

// replicationLoop polls etcd for global mutation log entries and applies them.
func (l *LSM) replicationLoop(ctx context.Context) error {
	pointer, err := l.reg.GetQueuePointer(ctx)
	if err != nil {
		return fmt.Errorf("get queue pointer: %w", err)
	}
	slog.InfoContext(ctx, "started replication loop", "pointer", pointer)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			entries, err := l.reg.GetLogs(ctx, pointer+1)
			if err != nil {
				slog.WarnContext(ctx, "failed to get logs from etcd", "error", err)
				continue
			}

			for _, entry := range entries {
				if entry.Index <= pointer {
					continue
				}

				if err := l.applyLogEntry(ctx, entry); err != nil {
					slog.ErrorContext(ctx, "failed to apply log entry", "index", entry.Index, "op", entry.Op, "error", err)
					break // Retry from the same pointer next time
				}

				pointer = entry.Index
				if err := l.reg.SetQueuePointer(ctx, pointer); err != nil {
					slog.WarnContext(ctx, "failed to save queue pointer", "pointer", pointer, "error", err)
				}
			}
		}
	}
}

func (l *LSM) applyLogEntry(ctx context.Context, entry LogEntry) error {
	if entry.NodeID == l.nodeID {
		slog.DebugContext(ctx, "skip own log entry", "index", entry.Index, "op", entry.Op)
		return nil
	}

	switch entry.Op {
	case OpPut:
		slog.InfoContext(ctx, "replicating PUT part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		block, err := fetchRemotePart(entry.Part)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeBlock(path, block); err != nil {
			return fmt.Errorf("write block: %w", err)
		}
		if err := writeMeta(metaPath, entry.Part); err != nil {
			os.Remove(path)
			return fmt.Errorf("write meta: %w", err)
		}

		l.mu.Lock()
		l.parts = append(l.parts, entry.Part)
		l.mu.Unlock()

	case OpMerge:
		slog.InfoContext(ctx, "replicating MERGE part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		block, err := fetchRemotePart(entry.Part)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeBlock(path, block); err != nil {
			return fmt.Errorf("write block: %w", err)
		}
		if err := writeMeta(metaPath, entry.Part); err != nil {
			os.Remove(path)
			return fmt.Errorf("write meta: %w", err)
		}

		oldSet := make(map[string]bool, len(entry.OldParts))
		for _, id := range entry.OldParts {
			oldSet[id] = true
			if err := os.Remove(l.partPath(id)); err != nil && !os.IsNotExist(err) {
				slog.WarnContext(ctx, "failed to remove old part file", "id", id, "error", err)
			}
			if err := os.Remove(l.partMetaPath(id)); err != nil && !os.IsNotExist(err) {
				slog.WarnContext(ctx, "failed to remove old meta file", "id", id, "error", err)
			}
		}

		l.mu.Lock()
		next := make([]PartMeta, 0, len(l.parts))
		for _, p := range l.parts {
			if !oldSet[p.ID] {
				next = append(next, p)
			}
		}
		next = append(next, entry.Part)
		l.parts = next
		l.mu.Unlock()
	}

	return nil
}

// flush drains the memtable, writes an immutable part file atomically,
// commits metadata, and appends a PUT operation to the etcd log.
func (l *LSM) flush(ctx context.Context) error {
	entries := l.mem.Snapshot()
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	slog.DebugContext(ctx, "creating new part", "id", id, "count", len(entries))
	if err := writeBlock(path, Block{SourceReplica: l.nodeID, CreatedAt: time.Now().UnixNano(), Data: entries}); err != nil {
		l.mem.Write(entries) // restore on failure so next flush retries
		return fmt.Errorf("write block: %w", err)
	}

	meta := PartMeta{
		ID:           id,
		NodeID:       l.nodeID,
		Level:        0,
		MinTimestamp: entries[0].Timestamp,
		MaxTimestamp: entries[len(entries)-1].Timestamp,
		Count:        len(entries),
		Addr:         l.addr,
	}

	if err := writeMeta(metaPath, meta); err != nil {
		os.Remove(path)
		l.mem.Write(entries)
		return fmt.Errorf("write meta: %w", err)
	}

	if _, err := l.reg.AppendLog(ctx, OpPut, meta, nil); err != nil {
		os.Remove(path)
		os.Remove(metaPath)
		l.mem.Write(entries)
		return fmt.Errorf("append log: %w", err)
	}

	// Safe to truncate WAL: entries are now durable in the part file and etcd log.
	if err := l.wal.Rotate(); err != nil {
		slog.WarnContext(ctx, "wal rotate failed", "error", err)
	}

	l.mu.Lock()
	l.parts = append(l.parts, meta)
	l.mu.Unlock()

	slog.InfoContext(ctx, "flushed part", "id", id, "level", 0, "count", meta.Count, "min_timestamp", meta.MinTimestamp, "max_timestamp", meta.MaxTimestamp)
	return nil
}

// compact merges all L0 parts for this node into a single L1 part.
// Emits a MERGE operation to the etcd log.
func (l *LSM) compact(ctx context.Context, force bool) error {
	l.mu.RLock()
	var l0 []PartMeta
	for _, p := range l.parts {
		if p.Level == 0 && p.NodeID == l.nodeID {
			l0 = append(l0, p)
		}
	}
	l.mu.RUnlock()

	if !force && len(l0) < compactTrigger {
		return nil
	}
	if len(l0) == 0 {
		if force {
			slog.DebugContext(ctx, "compaction skipped: no L0 parts to compact")
		}
		return nil
	}

	var merged []Entry
	for _, meta := range l0 {
		b, err := l.readLocalPart(meta.ID)
		if err != nil {
			return fmt.Errorf("read %s: %w", meta.ID, err)
		}
		merged = append(merged, b.Data...)
	}
	if len(merged) == 0 {
		if force {
			slog.DebugContext(ctx, "compaction skipped: no data in L0 parts")
		}
		return nil
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Timestamp < merged[j].Timestamp })

	oldIDs := make([]string, len(l0))
	for i, p := range l0 {
		oldIDs[i] = p.ID
	}

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	if err := writeBlock(path, Block{SourceReplica: l.nodeID, CreatedAt: time.Now().UnixNano(), SourceBlocks: oldIDs, Data: merged}); err != nil {
		return err
	}

	meta := PartMeta{
		ID:           id,
		NodeID:       l.nodeID,
		Level:        1,
		MinTimestamp: merged[0].Timestamp,
		MaxTimestamp: merged[len(merged)-1].Timestamp,
		Count:        len(merged),
		Addr:         l.addr,
	}

	if err := writeMeta(metaPath, meta); err != nil {
		os.Remove(path)
		return fmt.Errorf("write meta: %w", err)
	}

	slog.DebugContext(ctx, "compacting parts", "old_ids", oldIDs, "new_id", id, "count", len(merged))

	if _, err := l.reg.AppendLog(ctx, OpMerge, meta, oldIDs); err != nil {
		os.Remove(path)
		os.Remove(metaPath)
		return fmt.Errorf("append log: %w", err)
	}

	for _, p := range l0 {
		if err := os.Remove(l.partPath(p.ID)); err != nil && !os.IsNotExist(err) {
			slog.WarnContext(ctx, "remove old part failed", "id", p.ID, "error", err)
		}
		if err := os.Remove(l.partMetaPath(p.ID)); err != nil && !os.IsNotExist(err) {
			slog.WarnContext(ctx, "remove old meta failed", "id", p.ID, "error", err)
		}
	}

	oldSet := make(map[string]bool, len(l0))
	for _, p := range l0 {
		oldSet[p.ID] = true
	}
	l.mu.Lock()
	next := make([]PartMeta, 0, len(l.parts))
	for _, p := range l.parts {
		if !oldSet[p.ID] {
			next = append(next, p)
		}
	}
	next = append(next, meta)
	l.parts = next
	l.mu.Unlock()

	slog.InfoContext(ctx, "compacted parts", "level0_count", len(l0), "id", id, "level", 1, "count", len(merged))
	return nil
}

// Query returns all entries in [from, to] across all nodes by reading locally replicated parts.
func (l *LSM) Query(ctx context.Context, from, to int64) ([]Entry, error) {
	l.mu.RLock()
	allParts := make([]PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()

	result := make([]Entry, 0)
	for _, meta := range allParts {
		if !meta.overlaps(from, to) {
			continue
		}
		block, err := l.readLocalPart(meta.ID)
		if err != nil {
			slog.WarnContext(ctx, "skip part", "id", meta.ID, "error", err)
			continue
		}
		for _, e := range block.Data {
			if e.Timestamp >= from && e.Timestamp <= to {
				result = append(result, e)
			}
		}
	}

	// Include memtable (not yet flushed to any part).
	for _, e := range l.mem.All() {
		if e.Timestamp >= from && e.Timestamp <= to {
			result = append(result, e)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result, nil
}

// AllParts returns the copy of current memory parts list.
func (l *LSM) AllParts(ctx context.Context) ([]PartMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	copied := make([]PartMeta, len(l.parts))
	copy(copied, l.parts)
	return copied, nil
}

// ServeLocalPart streams the raw part file to w. Used by /part/{id}.
func (l *LSM) ServeLocalPart(id string, w io.Writer) error {
	f, err := os.Open(l.partPath(id))
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(w, f)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (l *LSM) partPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".json")
}

func (l *LSM) partMetaPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".meta")
}

func (l *LSM) readLocalPart(id string) (Block, error) {
	f, err := os.Open(l.partPath(id))
	if err != nil {
		return Block{}, err
	}
	var b Block
	decodeErr := json.NewDecoder(f).Decode(&b)
	closeErr := f.Close()
	if decodeErr != nil {
		return Block{}, decodeErr
	}
	return b, closeErr
}

// writeBlock writes b to path atomically via a temp-file + rename.
func writeBlock(path string, b Block) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(b); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// writeMeta writes meta to path atomically via a temp-file + rename.
func writeMeta(path string, meta PartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-meta-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(meta); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func fetchRemotePart(meta PartMeta) (Block, error) {
	resp, err := remoteHTTP.Get("http://" + meta.Addr + "/part/" + meta.ID)
	if err != nil {
		return Block{}, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return Block{}, fmt.Errorf("remote %s: HTTP %d", meta.ID, resp.StatusCode)
	}
	var b Block
	decodeErr := json.NewDecoder(resp.Body).Decode(&b)
	closeErr := resp.Body.Close()
	if decodeErr != nil {
		return Block{}, decodeErr
	}
	return b, closeErr
}

func newPartID(nodeID string) string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), nodeID)
}
