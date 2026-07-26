# Repository Instructions

## Project Shape
- Go module: `github.com/oteldb/shrimpd`.
- The point of the repo is `replication/`: a generic, ClickHouse-style replicated log
  (`Replication[B Block]`). The root package `shrimpd` re-exports it; everything else is the
  daemon that demonstrates it.
- Data storage is **not** shrimpd's: `github.com/oteldb/storage` owns the WAL, part format,
  indexing and the fetch contract. shrimpd drives `recordengine.Engine` directly, because the
  `storage.Storage` facade does not expose flush/merge/part-loading.
- Four CLI binaries under `cmd/`: `shrimpd` (daemon), `shrimply` (query client),
  `shrimpgateway` (round-robin proxy), `ch2shrimpd` (ClickHouse importer).
- Internal packages: `shrimpengine` (record-engine shell + replication Store + part transport),
  `shrimpnode` (engine + replication assembled, maintenance loop), `shrimpapi` (HTTP).

## Key Invariants
- **One writer per partition.** A partition is a node id. `recordengine` names parts from a
  node-local counter, so a single writer is what makes part keys identical across replicas and
  lets replication copy part objects verbatim. Do not add a second writer to a partition.
- **The bucket index commits last.** A part becomes visible only when the partition index names
  it. Fetch copies objects, then `streams.bin`, then the index. Drop removes the index entry
  first, then the objects.
- **Announce after durability.** `shrimpnode` flushes/merges locally and only then commits to the
  replicated log, so a peer never learns of a part nobody can serve.
- **`Replication.Run` is the whole lifecycle** — it joins the cluster and then loops; there is no
  separate Start to forget. Producers gate on `Ready()`: a node that has not joined must not flush,
  because its record engine restarts its part counter from the local index and would announce a
  part name peers already hold for different rows.
- **`Replication.Run` is the whole lifecycle** — it joins the cluster and then loops; there is no
  separate Start to forget. Producers gate on `Ready()`: a node that has not joined must not flush,
  because its record engine restarts its part counter from the local index and would announce a
  part name peers already hold for different rows.
- **Block ranges decide obsolescence.** `Contains(a, b)` (interval covers + strictly higher level)
  is the only supersession rule. `recordengine.Merge` compacts *every* part into one, which is
  what keeps a single interval a sound description of coverage.
- **A later log record cancels an earlier one it covers.** Without `dropSuperseded` a replica
  deadlocks: a fetch of a block that has since been merged away can never succeed, and the merge
  that would resolve it is blocked behind it by the range-conflict rule.
- **A drop removes what the dropped block covered**, not just the exact key. A lagging replica may
  still hold the merge sources the dropped part replaced, and no later record will mention them.
- **A replica that must rebuild waits, unless waiting cannot help.** Peers registered but offline ⇒
  start anyway, set `awaitingClone`, retry each tick. The only registered replica ⇒ fail with an
  unrecoverable error, since no source exists or ever will. A fresh single-node cluster has no log,
  so it is never lost and always starts.
- **A replica that must rebuild waits, unless waiting cannot help.** Peers registered but offline ⇒
  start anyway, set `awaitingClone`, retry each tick. The only registered replica ⇒ fail with an
  unrecoverable error, since no source exists or ever will. A fresh single-node cluster has no log,
  so it is never lost and always starts.
- `memkv` mirrors etcd's transaction rules exactly — two writes to one key conflict, a write inside
  a delete range conflicts, deletes never conflict with each other. Being *stricter* than etcd is
  as much a bug as being laxer; `replication/kvtest` holds both implementations to the same suite.

## Commands
- `go build ./cmd/...` builds all binaries.
- `make test` runs `./go.test.sh`: normal, `-tags purego`, then `-race`, each `--timeout 5m`.
- `make test_fast` runs `go test -short ./...`.
- `E2E=1 go test ./e2e/` runs end-to-end tests (requires Docker; etcd via testcontainers).
- `make coverage`, `make tidy`.
- Lint: `golangci-lint run ./...`; auto-fix: `--fix`; format: `golangci-lint fmt ./...`.

## Testing Notes
- `replication/` tests drive a multi-replica cluster in-process over `memkv` and step the loops
  explicitly (`PullForTest`/`ExecuteForTest` in `export_test.go`) instead of sleeping — keep new
  tests deterministic the same way.
- `converge_test.go` is the property suite: randomized flush/merge/drop workloads over 50 seeds,
  asserting convergence, no self-superseding parts, and exactly-once block coverage. A failure
  prints the event sequence that produced it. Both queue bugs above were found here.
- `chaos_test.go` injects transfer failures, a dying source, restarts mid-queue, and a trimmed log.
- `replication/kvtest` is the shared KV conformance suite: run it against `memkv` (fast) and
  against etcd in `e2e/kv_test.go`. Add anything replication relies on there, not to one side.
- `internal/shrimpengine` tests run real HTTP part transport over `httptest`.
- When a test starts `Replication.Run` in a goroutine with a `zaptest` logger, cancel and wait for
  it in `t.Cleanup`; logging after a test completes is a data race.

## Lint And CI
- CI uses reusable workflows from `go-faster/x` for test, cover, lint, commit checks, and CodeQL.
- `.golangci.yml` is v2 syntax and enables `gosec`, `gocritic`, `modernize`, `revive`,
  `staticcheck`, `goimports`, `gofumpt`.
- `errors.Wrap(f(), "msg")` as a return is a bug: `go-faster/errors.Wrap` does not nil-check.
  Always `if err := f(); err != nil { return errors.Wrap(err, "msg") }`.

## Agent Workflow Notes
- No generated files are present in the tree; generated-code exclusions in `.golangci.yml` are not
  evidence of any.
