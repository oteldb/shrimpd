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
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	must.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusOK, resp.StatusCode, "GET %s: %s", url, resp.Status)
	must.NoError(json.NewDecoder(resp.Body).Decode(v), fmt.Sprintf("decode %s", url))
}
