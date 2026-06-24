package shrimpblock

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/blevesearch/vellum"
	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func openFSTForTest(t *testing.T, path string, start, end []byte) (*vellum.FSTIterator, error) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	f, err := vellum.Load(data)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = f.Close() })
	return f.Iterator(start, end)
}

func TestWriteBlockZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	block := shrimptypes.Block{
		SourceReplica: "node1",
		Data: []shrimptypes.Entry{
			{Timestamp: 1, Data: "hello"},
			{Timestamp: 2, Data: "world"},
		},
	}
	path := filepath.Join(dir, "parts", "test.json")
	require.NoError(t, WriteBlock(path, block, CompressionZstd))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, data[:4], "zstd frame magic on disk")

	got, err := ReadBlock(path)
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestWriteBlockPlainRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	block := shrimptypes.Block{Data: []shrimptypes.Entry{{Timestamp: 7, Data: "plain"}}}
	path := filepath.Join(dir, "parts", "plain.json")
	require.NoError(t, WriteBlock(path, block, ""))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte('{'), data[0], "plain JSON starts with '{'")

	got, err := ReadBlock(path)
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestReadLocalPartLegacyPlain(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	plain := []byte(`{"data":[{"timestamp":1,"data":"foo"}]}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "parts", "legacy.json"), plain, 0o644))

	got, err := ReadBlock(filepath.Join(dir, "parts", "legacy.json"))
	require.NoError(t, err)
	require.Equal(t, []shrimptypes.Entry{{Timestamp: 1, Data: "foo"}}, got.Data)
}

func TestBuildIndexFSTRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "index"), 0o750))
	entries := []shrimptypes.IndexEntry{
		{Token: "hello", DataID: "p1"},
		{Token: "hello", DataID: "p2"},
		{Token: "world", DataID: "p1"},
	}
	path := filepath.Join(dir, "index", "test.fst")
	require.NoError(t, BuildIndexFST(path, entries))

	// prefix scan: all DataIDs for token "hello"
	start := compositeKey("hello", "")
	end := []byte("hello\x01")
	itr, err := openFSTForTest(t, path, start, end)
	require.NoError(t, err)
	var got []string
	for {
		k, _ := itr.Current()
		if k == nil {
			break
		}
		sep := bytes.IndexByte(k, '\x00')
		require.True(t, sep >= 0)
		got = append(got, string(k[sep+1:]))
		if err := itr.Next(); err != nil {
			break
		}
	}
	_ = itr.Close()
	require.Equal(t, []string{"p1", "p2"}, got)
}
