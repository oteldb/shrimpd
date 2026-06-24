// Package shrimpwal implements a segmented write-ahead log for the shrimpd project.
package shrimpwal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/go-faster/jx"
	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

type (
	Entry      = shrimptypes.Entry
	IndexEntry = shrimptypes.IndexEntry
)

// WAL is the per-node, segmented write-ahead log for pre-flush durability.
//
// Records are appended (NDJSON, one Entry per line) to the active segment and
// fsynced before Append returns. A flush proceeds as:
//
//	seq, _ := wal.Seal()                  // close active segment, open a fresh one
//	... write part file, publish to etcd  // slow; NO wal lock held
//	wal.Discard(seq)                      // delete the now-redundant sealed segments
//
// Because Seal redirects subsequent writes to a brand-new segment, the heavy
// flush work (disk + etcd) runs without blocking concurrent Append. The old
// design truncated the single WAL file after flush, which both blocked writers
// for the whole flush and could drop records appended during the flush window.
//
// Recover replays every segment, so a crash anywhere between Seal and Discard
// simply replays the sealed entries on the next startup. The invariant the
// caller must preserve is: the live in-memory set equals the union of every
// not-yet-discarded segment.
type WAL struct {
	mu        sync.Mutex
	dir       string
	prefix    string
	active    *os.File
	activeSeq uint64
}

// OpenWAL opens (or creates) the segmented write-ahead log rooted at path.
// The directory and base name of path determine where segments live and how
// they are named: "<dir>/wal.jsonl" yields segments "<dir>/wal-NNNNNN.jsonl".
func OpenWAL(path string) (*WAL, error) {
	dir := filepath.Dir(path)
	prefix := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)) // wal.jsonl -> wal
	if prefix == "" {
		return nil, fmt.Errorf("wal: cannot derive segment prefix from %q", path)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, prefix: prefix}

	// Migrate a legacy single-file WAL (pre-segments) into segment 0 so its
	// entries are still recovered and eventually discarded after the next flush.
	if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
		legacy := w.segPath(0)
		if _, statErr := os.Stat(legacy); os.IsNotExist(statErr) {
			if err := os.Rename(path, legacy); err != nil {
				return nil, fmt.Errorf("migrate legacy wal: %w", err)
			}
			if err := fsyncutil.SyncDir(dir); err != nil {
				return nil, err
			}
		}
	}

	seqs, err := w.listSegments()
	if err != nil {
		return nil, err
	}
	// Reuse the highest existing segment as the active one (it may hold
	// unflushed records from a previous run); otherwise start at segment 1.
	seq := uint64(1)
	if len(seqs) > 0 {
		seq = seqs[len(seqs)-1]
	}

	// #nosec G304 -- the daemon intentionally opens its configured local data path.
	f, err := os.OpenFile(w.segPath(seq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := fsyncutil.SyncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	w.active = f
	w.activeSeq = seq
	return w, nil
}

func (w *WAL) segPath(seq uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s-%06d.jsonl", w.prefix, seq))
}

// listSegments returns the sequence numbers of all segments on disk, ascending.
func (w *WAL) listSegments() ([]uint64, error) {
	ents, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	pfx := w.prefix + "-"
	var seqs []uint64
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, pfx) || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, pfx), ".jsonl")
		seq, err := strconv.ParseUint(mid, 10, 64)
		if err != nil {
			continue // not one of ours (e.g. a different prefix sharing the dir)
		}
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	return seqs, nil
}

// Append writes entries to the active segment and fsyncs before returning.
// All entries are encoded into a single pooled buffer and written in one syscall.
func (w *WAL) Append(entries []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	for _, e := range entries {
		jw.ObjStart()
		jw.RawStr(`"timestamp":`)
		jw.Int64(e.Timestamp)
		jw.RawStr(`,"data":`)
		jw.Str(e.Data)
		jw.ObjEnd()
		jw.Buf = append(jw.Buf, '\n')
	}
	if _, err := w.active.Write(jw.Buf); err != nil {
		return err
	}
	return w.active.Sync()
}

// Seal fsyncs and closes the active segment, then opens a fresh active segment
// for subsequent appends. It returns the sequence number of the sealed segment;
// pass it to Discard once the corresponding data is durable elsewhere.
func (w *WAL) Seal() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sealed := w.activeSeq
	if err := w.active.Sync(); err != nil {
		return 0, err
	}
	if err := w.active.Close(); err != nil {
		return 0, err
	}

	next := sealed + 1
	// #nosec G304 -- configured local data path.
	f, err := os.OpenFile(w.segPath(next), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		// Best effort: reopen the sealed segment so the WAL stays usable.
		// #nosec G304 -- configured local data path.
		if reopened, rerr := os.OpenFile(w.segPath(sealed), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600); rerr == nil {
			w.active = reopened
		}
		return 0, err
	}
	if err := fsyncutil.SyncDir(w.dir); err != nil {
		_ = f.Close()
		return 0, err
	}
	w.active = f
	w.activeSeq = next
	return sealed, nil
}

// Discard removes every sealed segment with sequence number <= uptoSeq. The
// active segment is never removed. Safe to call repeatedly: missing files are
// ignored, so a crash between Seal and Discard converges on retry.
func (w *WAL) Discard(uptoSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	seqs, err := w.listSegments()
	if err != nil {
		return err
	}
	removed := false
	for _, seq := range seqs {
		if seq > uptoSeq || seq == w.activeSeq {
			continue
		}
		if err := os.Remove(w.segPath(seq)); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}
	return fsyncutil.SyncDir(w.dir)
}

// Recover reads all entries from every segment, oldest first. Called once on
// startup. Skips corrupt lines silently — they indicate a mid-write crash.
func (w *WAL) Recover() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seqs, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var entries []Entry
	for _, seq := range seqs {
		// #nosec G304 -- configured local data path.
		f, err := os.Open(w.segPath(seq))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 4<<20), 4<<20) // 4 MiB max line (large log entries)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var e Entry
			d := jx.DecodeBytes(line)
			if err := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
				switch string(key) {
				case "timestamp":
					v, err := d.Int64()
					if err != nil {
						return err
					}
					e.Timestamp = v
				case "data":
					v, err := d.Str()
					if err != nil {
						return err
					}
					e.Data = v
				default:
					return d.Skip()
				}
				return nil
			}); err == nil {
				entries = append(entries, e)
			}
		}
		if scErr := sc.Err(); scErr != nil {
			_ = f.Close()
			return nil, scErr
		}
		_ = f.Close()
	}
	return entries, nil
}

// Close closes the active segment file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active.Close()
}
