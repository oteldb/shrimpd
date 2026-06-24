package shrimpwal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "index-wal.jsonl")

	wal, err := OpenIndexWAL(walPath)
	require.NoError(t, err)

	entries := []IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
	}

	require.NoError(t, wal.Append(entries))
	require.NoError(t, wal.Close())

	// Corrupt trailing line by appending junk
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("invalid json line\n")
	require.NoError(t, err)
	f.Close()

	// Recover
	wal2, err := OpenIndexWAL(walPath)
	require.NoError(t, err)
	defer wal2.Close()

	recovered, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, entries, recovered, "should recover valid entries and skip corrupt trailing line")

	// Rotate
	require.NoError(t, wal2.Rotate())
	recovered2, err := wal2.Recover()
	require.NoError(t, err)
	require.Empty(t, recovered2)
}
