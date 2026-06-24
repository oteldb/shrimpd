// Package shrimpwal implements a simple write-ahead log for the shrimpd project.
package shrimpwal

import (
	"bufio"
	"os"
	"sync"

	"github.com/go-faster/jx"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

type (
	Entry      = shrimptypes.Entry
	IndexEntry = shrimptypes.IndexEntry
)

// WAL is the per-node write-ahead log for pre-flush durability.
// Format: one JSON-encoded Entry per line (NDJSON). Truncated after a successful
// part flush; replayed on startup to rebuild the memtable after a crash.
type WAL struct {
	mu sync.Mutex
	f  *os.File
}

// OpenWAL opens the local write-ahead log at path.
func OpenWAL(path string) (*WAL, error) {
	// #nosec G304 -- the daemon intentionally opens its configured local data path.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil
}

// Append writes entries to the WAL and fsyncs before returning.
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
	if _, err := w.f.Write(jw.Buf); err != nil {
		return err
	}
	return w.f.Sync()
}

// Recover reads all entries from the WAL file. Called once on startup.
// Skips corrupt lines silently — they indicate a mid-write crash.
func (w *WAL) Recover() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	var entries []Entry
	sc := bufio.NewScanner(w.f)
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
	return entries, sc.Err()
}

// Rotate truncates the WAL after a successful flush. The entries are now
// durably committed in a part file and registered in etcd.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, 0)
	return err
}

// Close closes the WAL file.
func (w *WAL) Close() error { return w.f.Close() }
