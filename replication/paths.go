package replication

import (
	"fmt"
	"path"
	"strconv"
)

// seqWidth zero-pads sequence numbers so lexicographic key order — the only order [KV.List]
// guarantees — equals numeric order. 20 digits covers uint64.
const seqWidth = 20

// seqKey formats n as a fixed-width, lexicographically sortable key component.
func seqKey(n uint64) string { return fmt.Sprintf("%0*d", seqWidth, n) }

// parseSeq reads a sequence number back out of a key's last component.
func parseSeq(key string) (uint64, bool) {
	n, err := strconv.ParseUint(path.Base(key), 10, 64)

	return n, err == nil
}

// The key layout under Config.Prefix:
//
//	log/{seq}                          one log record, seq ascending
//	log_seq                            the allocator for the above
//	blocks/{partition}                 the block-number allocator for a partition
//	replicas/{name}/host               the replica's advertised address
//	replicas/{name}/active             ephemeral; present while the replica holds a session
//	replicas/{name}/lost               "1" while the replica's local state is untrustworthy
//	replicas/{name}/pointer            the highest log seq this replica has copied to its queue
//	replicas/{name}/queue/{seq}        a copied-but-not-executed log record
//	replicas/{name}/parts/{part}/{key} a block this replica holds
//
// A replica publishes what it holds so peers can pick a fetch source and a rejoining replica can
// clone from the most advanced peer without reading the whole log.
func (r *Replication[B]) logKey(seq uint64) string { return r.prefix + "/log/" + seqKey(seq) }
func (r *Replication[B]) logDir() string           { return r.prefix + "/log/" }
func (r *Replication[B]) logSeqKey() string        { return r.prefix + "/log_seq" }

func (r *Replication[B]) blockSeqKey(partition string) string {
	return r.prefix + "/blocks/" + partition
}

func (r *Replication[B]) replicaDir(name string) string { return r.prefix + "/replicas/" + name }
func (r *Replication[B]) replicasDir() string           { return r.prefix + "/replicas/" }

func (r *Replication[B]) hostKey(name string) string    { return r.replicaDir(name) + "/host" }
func (r *Replication[B]) activeKey(name string) string  { return r.replicaDir(name) + "/active" }
func (r *Replication[B]) lostKey(name string) string    { return r.replicaDir(name) + "/lost" }
func (r *Replication[B]) pointerKey(name string) string { return r.replicaDir(name) + "/pointer" }

func (r *Replication[B]) queueDir(name string) string { return r.replicaDir(name) + "/queue/" }

func (r *Replication[B]) queueKey(name string, seq uint64) string {
	return r.queueDir(name) + seqKey(seq)
}

// bootstrap entries are queue work synthesized by a clone rather than copied from the log. They
// live in their own subtree because they have no log sequence: numbering them alongside real
// entries would collide with log records the replica has yet to pull.
func (r *Replication[B]) bootstrapDir(name string) string {
	return r.replicaDir(name) + "/bootstrap/"
}

func (r *Replication[B]) bootstrapKey(name string, n uint64) string {
	return r.bootstrapDir(name) + seqKey(n)
}

func (r *Replication[B]) partsDir(name string) string { return r.replicaDir(name) + "/parts/" }

func (r *Replication[B]) partKey(name string, b B) string {
	return r.partsDir(name) + Key(b)
}
