package shrimplication

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func (l *LSM) partPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".json")
}

func (l *LSM) partMetaPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".meta")
}

func (l *LSM) sidecarPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".sparse.json")
}

func (l *LSM) readLocalPart(id string) (shrimptypes.Block, error) {
	f, err := os.Open(l.partPath(id)) // #nosec G304 -- trusted internal part path
	if err != nil {
		return shrimptypes.Block{}, err
	}
	r, _, err := shrimpblock.OpenBlockReader(f)
	if err != nil {
		_ = f.Close()
		return shrimptypes.Block{}, err
	}
	var b shrimptypes.Block
	decodeErr := json.NewDecoder(r).Decode(&b)
	rCloseErr := r.Close()
	fCloseErr := f.Close()
	if decodeErr != nil {
		return shrimptypes.Block{}, decodeErr
	}
	if rCloseErr != nil {
		return shrimptypes.Block{}, rCloseErr
	}
	return b, fCloseErr
}

// readPartBlock reads any local part (V2 binary or legacy JSON) and returns a Block.
func (l *LSM) readPartBlock(meta shrimptypes.PartMeta) (shrimptypes.Block, error) {
	if meta.FormatVersion == 1 {
		pf, err := l.partMgr.Get(meta.ID, meta)
		if err != nil {
			return shrimptypes.Block{}, err
		}
		if pf == nil {
			return shrimptypes.Block{}, fmt.Errorf("v2 part not found: %s", meta.ID)
		}
		return shrimpblock.V2ToBlock(pf)
	}
	return l.readLocalPart(meta.ID)
}

// readMeta reads PartMeta from a .meta file on disk.
func readMeta(path string) (shrimptypes.PartMeta, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal part path
	if err != nil {
		return shrimptypes.PartMeta{}, err
	}
	defer func() { _ = f.Close() }()
	var meta shrimptypes.PartMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return shrimptypes.PartMeta{}, err
	}
	return meta, nil
}

// writeMeta writes meta to path atomically via a temp-file + rename.
func writeMeta(path string, meta shrimptypes.PartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-meta-")
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
	return os.Rename(name, path)
}
