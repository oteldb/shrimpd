package shrimpblock

import (
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	"github.com/go-faster/jx"
	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// BuildIndexEntries tokenizes entries and returns sorted, deduplicated [shrimptypes.IndexEntry] values.
func BuildIndexEntries(dataID string, entries []shrimptypes.Entry) []shrimptypes.IndexEntry {
	seen := make(map[string]struct{})
	var out []shrimptypes.IndexEntry
	for _, e := range entries {
		for tok := range Tokenize(e.Data) {
			if _, ok := seen[tok]; !ok {
				seen[tok] = struct{}{}
				out = append(out, shrimptypes.IndexEntry{Token: tok, DataID: dataID})
			}
		}
	}
	slices.SortFunc(out, func(a, b shrimptypes.IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	return out
}

// ReadIndexMeta reads the index metadata from the specified path.
func ReadIndexMeta(path string) (shrimptypes.IndexPartMeta, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal path
	if err != nil {
		return shrimptypes.IndexPartMeta{}, err
	}
	defer func() { _ = f.Close() }()
	var meta shrimptypes.IndexPartMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return shrimptypes.IndexPartMeta{}, err
	}
	return meta, nil
}

// WriteIndexMeta writes the index metadata to the specified path atomically.
func WriteIndexMeta(path string, meta shrimptypes.IndexPartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-meta-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(meta); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}

// WriteIndexBlock writes an index block to the given path with the specified compression algorithm.
func WriteIndexBlock(path string, b shrimptypes.IndexBlock, algo string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	cw, err := NewCompressingWriter(tmp, algo)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	encErr := json.NewEncoder(cw).Encode(b)
	closeErr := cw.Close()
	if encErr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return encErr
	}
	if closeErr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return closeErr
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}

// ReadIndexBlock reads an index block from the given path and returns it.
func ReadIndexBlock(path string) (shrimptypes.IndexBlock, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal path
	if err != nil {
		return shrimptypes.IndexBlock{}, err
	}
	r, _, err := OpenBlockReader(f)
	if err != nil {
		_ = f.Close()
		return shrimptypes.IndexBlock{}, err
	}
	d := jx.GetDecoder()
	defer jx.PutDecoder(d)
	d.Reset(r)

	var b shrimptypes.IndexBlock
	decodeErr := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		switch string(key) {
		case "entries":
			return d.Arr(func(d *jx.Decoder) error {
				var e shrimptypes.IndexEntry
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
				}); err != nil {
					return err
				}
				b.Entries = append(b.Entries, e)
				return nil
			})
		default:
			return d.Skip()
		}
	})

	rCloseErr := r.Close()
	fCloseErr := f.Close()
	if decodeErr != nil {
		return shrimptypes.IndexBlock{}, decodeErr
	}
	if rCloseErr != nil {
		return shrimptypes.IndexBlock{}, rCloseErr
	}
	return b, fCloseErr
}
