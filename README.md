# shrimpd

A ClickHouse-style **replication mechanism**, generic over what it replicates — plus a small log
daemon that demonstrates it over [`github.com/oteldb/storage`](https://github.com/oteldb/storage).

The reusable part is [`replication`](./replication). Storage, encoding, indexing and querying are
not shrimpd's business: `oteldb/storage` does all of that. shrimpd answers one question — *how do
several nodes agree on the same set of immutable parts?*

## The model

Straight from ReplicatedMergeTree, reduced to essentials:

```
        ┌──────────── etcd (or any KV) ────────────┐
        │  /shrimpd/parts/log/000…001  create P1   │   the log: an ordered list of
        │  /shrimpd/parts/log/000…002  create P2   │   part-set mutations, nothing else
        │  /shrimpd/parts/log/000…003  merge P1,P2 → P3
        └──────────────────────────────────────────┘
             │ pointer                  │ pointer
        ┌────▼─────┐              ┌─────▼────┐
        │ replica A│◀── fetch ───▶│ replica B│    data moves node-to-node,
        │  queue   │   part P3    │  queue   │    never through the log
        └──────────┘              └──────────┘
```

- Every mutation of the part set is appended to **one ordered log**. A record names a part; it
  never carries data.
- Each replica keeps a **pointer** into that log and a durable **queue** of records it has copied
  but not yet executed. Executing means making local disk match: fetch the part from a replica
  that has it, reproduce a merge locally, or drop what is superseded.
- A part is identified by its **range**: the inclusive block-number interval `[First, Last]` it
  covers in its partition, plus a merge `Level`. Containment over ranges is the entire
  obsolescence calculus — `[1,4]@1` contains `[1,1]@0`, so a replica that fetched the merged part
  drops the sources without consulting anything.
- A replica whose state is untrustworthy (new, or fallen behind the retained log) flags itself
  **lost** and **clones** the healthiest peer's intent: the peer's most-covering parts plus its
  pending queue become this replica's work list.

## Using the replication mechanism

Implement three methods to say what a part *is*, and four to say what to *do* with one:

```go
type Part struct{ Shard string; First, Last, Level uint64 }

func (p Part) Name() string         { return p.Range().String() }
func (p Part) Partition() string    { return p.Shard }
func (p Part) Range() shrimpd.Range { return shrimpd.Range{First: p.First, Last: p.Last, Level: p.Level} }

type Store interface {
    Local(ctx) ([]Part, error)               // what is on disk
    Fetch(ctx, addr string, p Part) error    // copy p from the peer at addr
    Merge(ctx, src []Part, dst Part) error   // reproduce a merge, or decline
    Drop(ctx, p Part) error                  // p is superseded
}
```

Then run it:

```go
r, err := shrimpd.NewReplication(shrimpd.Config[Part]{
    KV:      kv,             // etcdkv.New(...) or memkv.New()
    Store:   myStore,
    Prefix:  "/myapp/parts",
    Replica: nodeID,
    Addr:    advertisedAddr, // passed verbatim to Store.Fetch
})
if err := r.Start(ctx); err != nil { return err }
go r.Run(ctx)

// After writing a part locally:
r.Commit(ctx, part)
// After merging locally:
r.CommitMerge(ctx, sources, merged)
```

Replication never touches the network itself. `Addr` is an opaque string it hands to
`Store.Fetch`; the transport is entirely yours.

`Store.Merge` may return `shrimpd.ErrMergeUnsupported` to say "don't reproduce this, fetch the
result" — which is what the daemon does, since the merge already happened elsewhere and its bytes
can simply be copied.

### The KV seam

Replication needs an ordered, revisioned key/value space with atomic conditional transactions and
session-scoped ephemeral keys — six methods, in [`replication.KV`](./replication/kv.go).

| Implementation | Use |
|---|---|
| [`replication/etcdkv`](./replication/etcdkv) | etcd v3, ephemeral keys on a session lease |
| [`replication/memkv`](./replication/memkv) | in-process; several sessions share one keyspace |

`memkv` deliberately enforces etcd's rules (including its refusal to range-delete a prefix and
write inside it in one transaction), so a bug cannot pass the fast tests and fail only in
production.

## The daemon

`shrimpd` is a log store built on the above: `oteldb/storage`'s record engine for the data,
`replication` for the cluster.

**Each node writes only its own partition.** A record engine names parts from a node-local
counter, so confining writes to one writer per partition is what makes a part's object keys
identical on every replica — which is what lets replication copy those objects verbatim instead
of re-encoding them. A node holds one writable engine and a read-only engine per peer partition;
queries fan out across all of them.

```
<dataDir>/
  wal/                             write-ahead log (storage/wal)
  parts/
    <node>/bucket-index.bin        the part list — written last, the commit point
    <node>/streams.bin             stream identity index
    <node>/<seq>/…                 one immutable part's column objects
```

### Binaries

| Binary | Description |
|---|---|
| `shrimpd` | the daemon: ingest, query, replication |
| `shrimply` | command-line query client |
| `shrimpgateway` | round-robin HTTP gateway across nodes |
| `ch2shrimpd` | one-shot importer from a ClickHouse `logs` table |

### HTTP API

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest` | `{"data":[{"timestamp":<ns>,"data":"line"}]}` |
| `POST` | `/ingest/otlp`, `/v1/logs` | OTLP logs (protobuf or JSON) |
| `GET` | `/query`, `/read` | `?from=<ns>&to=<ns>&term=<substring>&limit=<n>` |
| `GET` | `/state`, `/parts` | parts held, replication queue, lag |
| `POST` | `/flush` | write the head to a part and announce it |
| `POST` | `/compact` | merge this node's parts and announce it |
| `GET` | `/internal/parts/{list,object}` | how peers copy this node's part objects |

There is no query language: matching is the storage layer's job. `term` is a substring over the
record body, pushed down to the per-part body bloom so whole parts are skipped before scanning.

### Quick start

```bash
docker run -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest

go run ./cmd/shrimpd -id=node1 -addr=localhost:8080 -data=./data1
go run ./cmd/shrimpd -id=node2 -addr=localhost:8081 -data=./data2

curl -sX POST localhost:8080/ingest -H 'Content-Type: application/json' \
  -d '{"data":[{"timestamp":1,"data":"hello"}]}'
curl -sX POST localhost:8080/flush

# node2 has it, fetched from node1
curl -s 'localhost:8081/query?from=0&to=9' | jq .
```

`example/` has a three-node compose setup behind `shrimpgateway`.

### Flags

| Flag | Default | Description |
|---|---|---|
| `-id` | `node1` | node id: replica name and partition |
| `-addr` | `localhost:8080` | HTTP listen + advertise address |
| `-data` | `./data` | data directory |
| `-etcd` | `localhost:2379` | comma-separated etcd endpoints |
| `-etcd-prefix` | `/shrimpd` | etcd key prefix for cluster state |
| `-retention` | `0` | drop records older than this on compaction |
| `-memlimit` | `0` | soft memory limit, e.g. `400MiB` |
| `-pprof` | *(off)* | expose `net/http/pprof` on this address |

## Development

```bash
make test            # normal, purego, race
make test_fast       # go test -short ./...
E2E=1 go test ./e2e  # end-to-end; needs Docker
golangci-lint run ./...
```

The replication tests run a multi-replica cluster in-process over `memkv` — no Docker, a few
milliseconds. `e2e/` runs the same scenarios against real etcd and real HTTP.

## License

See [LICENSE](LICENSE).
