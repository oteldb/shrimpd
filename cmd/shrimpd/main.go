package main

// shrimpd is a replicated log store: each node owns its parts on local disk and etcd carries the
// replicated log that keeps every node's part set converging.
//
// Quick start:
//
//	docker run -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest
//
//	go run ./cmd/shrimpd -id=node1 -addr=localhost:8080 -data=./data1
//	go run ./cmd/shrimpd -id=node2 -addr=localhost:8081 -data=./data2
//
// Ingest into one node:
//
//	curl -sX POST localhost:8080/ingest \
//	  -H 'Content-Type: application/json' \
//	  -d '{"data":[{"timestamp":1,"data":"foo"}]}'
//
// Query from the other — replication has already carried the part across:
//
//	curl -s 'localhost:8081/query?from=0&to=9' | jq .
//
// Inspect this node's parts and replication lag:
//
//	curl -s localhost:8080/state | jq .

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	runtimepprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/sdk/app"
	slogzap "github.com/samber/slog-zap/v2"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/shrimpd/internal/shrimpapi"
	"github.com/oteldb/shrimpd/internal/shrimpnode"
	"github.com/oteldb/shrimpd/replication/etcdkv"
)

// bytesFlag is a custom flag type that allows human-readable byte sizes (e.g., "10MB") to be parsed into uint64 values.
type bytesFlag uint64

func BytesFlag(name, value, usage string) *bytesFlag {
	b := bytesFlag(0)
	if err := b.Set(value); err != nil {
		panic(fmt.Sprintf("invalid default value for %s: %v", name, err))
	}
	flag.Var(&b, name, usage)
	return &b
}

// String implements flag.Value interface, returning the value as a string.
func (b *bytesFlag) String() string {
	return humanize.Bytes(uint64(*b))
}

// Set implements flag.Value interface, allowing human-readable byte sizes like "10MB".
func (b *bytesFlag) Set(s string) error {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return err
	}
	*b = bytesFlag(v)
	return nil
}

func main() {
	var (
		nodeID     = flag.String("id", "node1", "unique node identifier")
		addr       = flag.String("addr", "localhost:8080", "HTTP listen + advertise address (host:port)")
		dataDir    = flag.String("data", "./data", "directory for parts and WAL")
		etcdEps    = flag.String("etcd", "localhost:2379", "etcd endpoints, comma-separated")
		etcdPrefix = flag.String("etcd-prefix", "/shrimpd", "etcd key prefix for this cluster's replication state")
		retention  = flag.Duration("retention", 0, "drop records older than this on compaction (0 = keep everything)")

		// Profiling flags
		pprofAddr          = flag.String("pprof", "", "expose net/http/pprof on this address (e.g. :6060); empty = disabled")
		cpuProfile         = flag.String("cpuprofile", "", "write CPU profile to file on exit")
		memProfileDir      = flag.String("memprofile-dir", "", "directory to write heap profiles; empty = disabled")
		memProfileInterval = flag.Duration("memprofile-interval", 30*time.Second, "how often to poll heap usage for threshold check")
		memProfileThresh   = BytesFlag("memprofile-threshold", "0", "auto-dump heap profile when HeapInuse exceeds this many bytes (0 = disabled)")
		memLimit           = BytesFlag("memlimit", "0", "soft memory limit in bytes passed to runtime/debug.SetMemoryLimit (0 = runtime default); also respects GOMEMLIMIT env var")
	)
	flag.Parse()
	if *pprofAddr != "" {
		if err := os.Setenv("PPROF_ADDR", *pprofAddr); err != nil {
			slog.Error("set pprof addr", "error", err)
			os.Exit(1)
		}
	}

	// Apply soft memory limit before anything allocates.
	if *memLimit > 0 {
		prev := debug.SetMemoryLimit(int64(*memLimit))
		slog.Info("set memory limit", "bytes", *memLimit, "previous", prev)
	}

	// CPU profiling.
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile) // #nosec G304
		if err != nil {
			slog.Error("create cpu profile", "error", err)
			os.Exit(1)
		}
		if err := runtimepprof.StartCPUProfile(f); err != nil {
			slog.Error("start cpu profile", "error", err)
			os.Exit(1)
		}
		defer func() {
			runtimepprof.StopCPUProfile()
			_ = f.Close()
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	app.Run(func(ctx context.Context, lg *zap.Logger, _ *app.Telemetry) error {
		slog.SetDefault(slog.New(slogzap.Option{Level: slog.LevelDebug, Logger: lg}.NewZapHandler()))

		if err := os.MkdirAll(*dataDir, 0o750); err != nil {
			return fmt.Errorf("create data directory: %w", err)
		}

		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   strings.Split(*etcdEps, ","),
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("connect etcd: %w", err)
		}
		defer func() {
			if err := cli.Close(); err != nil {
				lg.Warn("close etcd client", zap.Error(err))
			}
		}()

		kv, err := etcdkv.New(ctx, cli, 0)
		if err != nil {
			return fmt.Errorf("open metadata session: %w", err)
		}
		defer func() {
			if err := kv.Close(); err != nil {
				lg.Warn("close metadata session", zap.Error(err))
			}
		}()

		node, err := shrimpnode.New(ctx, shrimpnode.Options{
			Dir:       *dataDir,
			ID:        *nodeID,
			Addr:      *addr,
			KV:        kv,
			Prefix:    *etcdPrefix,
			Retention: *retention,
			Logger:    lg,
		})
		if err != nil {
			return fmt.Errorf("open node: %w", err)
		}
		defer func() {
			if err := node.Close(context.WithoutCancel(ctx)); err != nil {
				lg.Warn("close node", zap.Error(err))
			}
		}()

		srv, err := shrimpapi.NewServer(*addr, node, lg)
		if err != nil {
			return fmt.Errorf("create http server: %w", err)
		}

		// Background heap monitor: auto-dump when threshold crossed, and on SIGUSR1.
		if *memProfileDir != "" {
			if err := os.MkdirAll(*memProfileDir, 0o750); err != nil {
				return fmt.Errorf("create memprofile dir: %w", err)
			}
			go heapMonitor(ctx, *memProfileDir, uint64(*memProfileThresh), *memProfileInterval)
		}

		eg, ctx := errgroup.WithContext(ctx)
		eg.Go(func() error { return node.Run(ctx) })
		eg.Go(func() error { return srv.Run(ctx) })
		return eg.Wait()
	}, app.WithContext(ctx), app.WithServiceName("shrimpd"))
}

// heapMonitor polls heap stats and writes a heap profile when HeapInuse exceeds
// threshold. On platforms that support SIGUSR1, it also writes a profile on
// manual signal. Profiles land in dir as heap-<timestamp>.pprof. A zero
// threshold disables the automatic dump.
func heapMonitor(ctx context.Context, dir string, threshold uint64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var sigCh <-chan os.Signal
	if sig, _, ok := heapDumpSignal(); ok {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, sig)
		defer signal.Stop(ch)
		sigCh = ch
	}

	var lastDump time.Time
	const minDumpInterval = 30 * time.Second

	dump := func(reason string) {
		if time.Since(lastDump) < minDumpInterval {
			return
		}
		path := filepath.Join(dir, fmt.Sprintf("heap-%d.pprof", time.Now().UnixNano()))
		f, err := os.Create(path) // #nosec G304
		if err != nil {
			slog.Error("create heap profile", "error", err)
			return
		}
		runtime.GC() // get up-to-date stats
		if err := runtimepprof.WriteHeapProfile(f); err != nil {
			slog.Error("write heap profile", "error", err)
		} else {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			slog.Info("wrote heap profile",
				"reason", reason,
				"path", path,
				"heap_inuse_mb", ms.HeapInuse>>20,
				"heap_alloc_mb", ms.HeapAlloc>>20,
			)
		}
		_ = f.Close()
		lastDump = time.Now()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			_, reason, _ := heapDumpSignal()
			dump(reason)
		case <-ticker.C:
			if threshold <= 0 {
				continue
			}
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse >= threshold {
				dump(fmt.Sprintf("threshold(%dMiB)", threshold>>20))
			}
		}
	}
}
