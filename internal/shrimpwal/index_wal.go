package shrimpwal

import (
	"bufio"
	"os"
	"sync"

	"github.com/go-faster/jx"
)

// IndexWAL is the write-ahead log for pre-flush index entries.
type IndexWAL struct {
	mu sync.Mutex
	f  *os.File
}

// OpenIndexWAL opens the local index write-ahead log at path.
func OpenIndexWAL(path string) (*IndexWAL, error) {
	// #nosec G304 -- configured local path
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &IndexWAL{f: f}, nil
}

// Append writes entries to the IndexWAL and fsyncs before returning.
func (w *IndexWAL) Append(entries []IndexEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	for _, e := range entries {
		jw.ObjStart()
		jw.RawStr(`"token":`)
		jw.Str(e.Token)
		jw.RawStr(`,"data_id":`)
		jw.Str(e.DataID)
		jw.ObjEnd()
		jw.Buf = append(jw.Buf, '\n')
	}
	if _, err := w.f.Write(jw.Buf); err != nil {
		return err
	}
	return w.f.Sync()
}

// Recover reads all entries from the IndexWAL file. Called once on startup.
func (w *IndexWAL) Recover() ([]IndexEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	var entries []IndexEntry
	sc := bufio.NewScanner(w.f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e IndexEntry
		d := jx.DecodeBytes(line)
		if err := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
			switch string(key) {
			case "token":
				v, err := d.Str()
				if err != nil {
					return err
				}
				e.Token = v
			case "data_id":
				v, err := d.Str()
				if err != nil {
					return err
				}
				e.DataID = v
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

// Rotate truncates the IndexWAL after a successful flush.
func (w *IndexWAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, 0)
	return err
}

// Close closes the IndexWAL file.
func (w *IndexWAL) Close() error { return w.f.Close() }
