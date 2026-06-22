package shrimpd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
)

func TestDaemonSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	dataDir := t.TempDir()
	must.NoError(os.MkdirAll(filepath.Join(dataDir, "parts"), 0o755))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	wal, err := shrimpd.OpenWAL(filepath.Join(dataDir, "wal.jsonl"))
	must.NoError(err)
	defer func() {
		must.NoError(wal.Close())
	}()

	addr := freeLocalAddr(t)
	lsm, err := shrimpd.NewLSM("node1", addr, dataDir, wal, shrimpd.NewRegistry(cli, "node1"))
	must.NoError(err)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	eg, runCtx := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr, lsm).Run(runCtx) })
	defer func() {
		stop()
		err := eg.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	baseURL := "http://" + addr
	waitHTTP(ctx, t, baseURL+"/parts")

	postJSON(ctx, t, baseURL+"/ingest", shrimpd.Block{Data: []shrimpd.Entry{
		{Timestamp: 2, Data: "bar"},
		{Timestamp: 1, Data: "foo"},
	}})

	var got shrimpd.Block
	getJSON(ctx, t, baseURL+"/read?from=1&to=2", &got)
	must.Equal([]shrimpd.Entry{
		{Timestamp: 1, Data: "foo"},
		{Timestamp: 2, Data: "bar"},
	}, got.Data)
}

func startEtcd(ctx context.Context, t *testing.T) string {
	t.Helper()
	must := require.New(t)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/coreos/etcd:v3.5.13",
			ExposedPorts: []string{"2379/tcp", "2380/tcp"},
			Cmd: []string{
				"/usr/local/bin/etcd",
				"--name", "node1",
				"--listen-client-urls", "http://0.0.0.0:2379",
				"--advertise-client-urls", "http://0.0.0.0:2379",
				"--listen-peer-urls", "http://0.0.0.0:2380",
				"--initial-advertise-peer-urls", "http://0.0.0.0:2380",
				"--initial-cluster", "node1=http://0.0.0.0:2380",
				"--initial-cluster-state", "new",
			},
			WaitingFor: wait.ForListeningPort("2379/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	must.NoError(err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	host, err := container.Host(ctx)
	must.NoError(err)
	port, err := container.MappedPort(ctx, "2379/tcp")
	must.NoError(err)
	return net.JoinHostPort(host, port.Port())
}

func waitEtcd(ctx context.Context, t *testing.T, cli *clientv3.Client) {
	t.Helper()
	for {
		_, err := cli.Status(ctx, cli.Endpoints()[0])
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "wait for etcd", "endpoint %s: %v", cli.Endpoints()[0], ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	must := require.New(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.NoError(err)
	addr := ln.Addr().String()
	must.NoError(ln.Close())
	return addr
}

func waitHTTP(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	must := require.New(t)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		must.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "wait for http", "url %s: %v", url, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func postJSON(ctx context.Context, t *testing.T, url string, v any) {
	t.Helper()
	must := require.New(t)
	body, err := json.Marshal(v)
	must.NoError(err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	must.NoError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusNoContent, resp.StatusCode, "POST %s: %s", url, resp.Status)
}

func getJSON(ctx context.Context, t *testing.T, url string, v any) {
	t.Helper()
	must := require.New(t)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	must.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusOK, resp.StatusCode, "GET %s: %s", url, resp.Status)
	must.NoError(json.NewDecoder(resp.Body).Decode(v), fmt.Sprintf("decode %s", url))
}

func TestDaemonReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	// Create directories for node1 and node2
	tempDir := t.TempDir()
	dataDir1 := filepath.Join(tempDir, "node1")
	dataDir2 := filepath.Join(tempDir, "node2")
	must.NoError(os.MkdirAll(filepath.Join(dataDir1, "parts"), 0o755))
	must.NoError(os.MkdirAll(filepath.Join(dataDir2, "parts"), 0o755))

	wal1, err := shrimpd.OpenWAL(filepath.Join(dataDir1, "wal.jsonl"))
	must.NoError(err)
	defer wal1.Close()

	wal2, err := shrimpd.OpenWAL(filepath.Join(dataDir2, "wal.jsonl"))
	must.NoError(err)
	defer wal2.Close()

	addr1 := freeLocalAddr(t)
	addr2 := freeLocalAddr(t)

	lsm1, err := shrimpd.NewLSM("node1", addr1, dataDir1, wal1, shrimpd.NewRegistry(cli, "node1"))
	must.NoError(err)

	lsm2, err := shrimpd.NewLSM("node2", addr2, dataDir2, wal2, shrimpd.NewRegistry(cli, "node2"))
	must.NoError(err)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	eg, runCtx := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm1.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr1, lsm1).Run(runCtx) })
	eg.Go(func() error { return lsm2.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr2, lsm2).Run(runCtx) })

	defer func() {
		stop()
		err := eg.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	baseURL1 := "http://" + addr1
	baseURL2 := "http://" + addr2
	waitHTTP(ctx, t, baseURL1+"/parts")
	waitHTTP(ctx, t, baseURL2+"/parts")

	// 1. Ingest into node1.
	// Since we ingest only 2 entries (which is below the flushThreshold of 100),
	// it will stay in memtable first. To force a flush to a part, we either wait
	// for the flushInterval (5s) or we ingest 100 entries.
	// Let's write 100 entries to force an immediate flush and replication event.
	entries := make([]shrimpd.Entry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = shrimpd.Entry{Timestamp: int64(i + 1), Data: fmt.Sprintf("val-%d", i)}
	}
	postJSON(ctx, t, baseURL1+"/ingest", shrimpd.Block{Data: entries})

	// 2. Poll read on node2 until replicated
	var got shrimpd.Block
	for {
		getJSON(ctx, t, baseURL2+"/read?from=1&to=100", &got)
		if len(got.Data) == 100 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for replication: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	must.Equal(int64(1), got.Data[0].Timestamp)
	must.Equal("val-0", got.Data[0].Data)

	// 3. Trigger compaction on node1.
	// To trigger compaction, we need at least compactTrigger (4) L0 parts.
	// Let's ingest 3 more blocks of 100 entries to create 4 parts in total.
	for b := 1; b < 4; b++ {
		batchEntries := make([]shrimpd.Entry, 100)
		for i := 0; i < 100; i++ {
			ts := int64(b*100 + i + 1)
			batchEntries[i] = shrimpd.Entry{Timestamp: ts, Data: fmt.Sprintf("val-%d", ts)}
		}
		postJSON(ctx, t, baseURL1+"/ingest", shrimpd.Block{Data: batchEntries})
	}

	// Wait for Node 2 to replicate all 4 parts
	for {
		getJSON(ctx, t, baseURL2+"/read?from=1&to=400", &got)
		if len(got.Data) == 400 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for all 4 parts replication: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Trigger compact on node 1
	postJSON(ctx, t, baseURL1+"/compact", nil)

	// 4. Poll parts on node2 until compaction replicated (there should be 1 part with level=1)
	var parts []shrimpd.PartMeta
	for {
		getJSON(ctx, t, baseURL2+"/parts", &parts)
		if len(parts) == 1 && parts[0].Level == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for compaction replication: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
