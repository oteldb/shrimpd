// Package replication implements ClickHouse-style replicated-table replication, generic over
// the replicated unit.
//
// The model is ReplicatedMergeTree's, reduced to its essentials:
//
//   - Every mutation of the part set is appended to a single ordered **log** in the metadata
//     store ([KV], normally etcd). A log record says "part P was created" or "parts S were
//     merged into D" — never the data itself.
//   - Each replica keeps a **pointer** into that log and a private **queue** of records it has
//     copied but not yet executed. Executing a record means making the local part set match it:
//     fetch P from the replica that has it, or reproduce a merge locally and drop the sources.
//   - A part is identified by its [Range]: the half-open-free, inclusive block-number interval
//     `[First, Last]` it covers within its partition, plus its merge `Level`. Containment over
//     ranges ([Contains]) is the whole obsolescence calculus — a part that covers another
//     supersedes it, so a replica that fetched a merged part can drop the sources without
//     consulting the log, and a replica rejoining after an outage can skip every record already
//     subsumed by what it holds.
//   - Block numbers are allocated from the metadata store ([Replication.AllocateBlock]), so part
//     identity is agreed cluster-wide before any data is written and two replicas can never mint
//     different parts under the same name.
//
// The type parameter is the replicated unit: `Replication[B Block]` replicates data parts, index
// parts, symbol parts or anything else that can name itself and state the range it covers. The
// embedder supplies the physical half — how to fetch, merge and drop a block — as a [Store].
//
// Nothing here knows about HTTP, a part format, or a storage engine. Transport is the embedder's:
// a queue entry carries the source replica's advertised address, and [Store.Fetch] does whatever
// that address means.
package replication
