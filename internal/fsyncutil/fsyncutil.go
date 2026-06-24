package fsyncutil

import (
	"os"
)

// SyncDir fsyncs a directory so a rename is durable on disk.
func SyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = d.Close()
	}()
	return d.Sync()
}
