package shrimplication

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimpfilter"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"github.com/tdakkota/shrimpd/internal/shrimpwal"
)

func TestLSM_QueryMatcher_Termless(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	// Write OTLP-shaped entries (flattened labels). Stay in memtable to avoid needing etcd for flush.
	err = lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 1, Data: `{"severity_text":"INFO","body":"hello","resource":{"service.name":"svc-a"}}`},
		{Timestamp: 2, Data: `{"severity_text":"ERROR","body":"boom","resource":{"service.name":"svc-b"}}`},
		{Timestamp: 3, Data: `{"severity_text":"DEBUG","body":"trace","resource":{"service.name":"svc-a"}}`},
	})
	require.NoError(t, err)

	// Label eq: level=ERROR (memtable path)
	m, err := shrimpfilter.CompileMatcher(nil, []shrimpfilter.LabelFilter{
		{Label: "level", Op: shrimpfilter.OpLabelEq, Value: "ERROR"},
	})
	require.NoError(t, err)
	var got []shrimptypes.Entry
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, m, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 1)
	require.Equal(t, int64(2), got[0].Timestamp)

	// Label re + line eq
	m2, err := shrimpfilter.CompileMatcher(
		[]shrimpfilter.LineFilter{{Op: shrimpfilter.OpLineEq, Value: "trace"}},
		[]shrimpfilter.LabelFilter{{Label: "service_name", Op: shrimpfilter.OpLabelRe, Value: "svc-.*"}},
	)
	require.NoError(t, err)
	got = nil
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, m2, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 1)
	require.Equal(t, int64(3), got[0].Timestamp)

	// Empty matcher = full scan of memtable
	got = nil
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, shrimpfilter.Matcher{}, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 3)
}

func TestLSM_QueryMatcher_Memtable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	_ = lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 10, Data: `{"severity_text":"WARN","body":"m1","resource":{"service.name":"s1"}}`},
	})

	// Query directly from memtable (no flush -> no etcd needed)
	m, _ := shrimpfilter.CompileMatcher(nil, []shrimpfilter.LabelFilter{{Label: "service_name", Op: shrimpfilter.OpLabelEq, Value: "s1"}})
	var got []shrimptypes.Entry
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, m, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 1)
}

func TestLSM_QueryWithStats_Memtable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	err = lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello"},
		{Timestamp: 2, Data: "world"},
		{Timestamp: 3, Data: "hello shrimp"},
	})
	require.NoError(t, err)

	got, stats, err := lsm.QueryWithStats(context.Background(), 0, 1<<62, "hello")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotNil(t, stats)
	require.Equal(t, 0, stats.PartsTotal)
	require.Equal(t, 0, stats.PartsScanned)
	require.Equal(t, 3, stats.EntriesScanned)
	require.Equal(t, 2, stats.EntriesMatched)
	require.GreaterOrEqual(t, stats.DurationMs, int64(0))
}
