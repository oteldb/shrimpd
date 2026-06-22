package shrimpd

// Entry is the fundamental unit of data. Timestamp is used for ordering and pruning.
type Entry struct {
	Timestamp int64  `json:"timestamp"`
	Data      string `json:"data"`
}

// Block is the wire and file format for a collection of entries.
type Block struct {
	Data []Entry `json:"data"`
}

// PartMeta describes an immutable part stored on disk and registered in etcd.
// It lives at /lsm/parts/{id} and acts as both the part registry and the global WAL
// of committed parts (etcd revision gives total ordering across nodes).
type PartMeta struct {
	ID           string `json:"id"`
	NodeID       string `json:"node_id"`
	Level        int    `json:"level"` // 0 = freshly flushed, 1+ = compacted
	MinTimestamp int64  `json:"min_timestamp"`
	MaxTimestamp int64  `json:"max_timestamp"`
	Count        int    `json:"count"`
	Addr         string `json:"addr"` // host:port of the origin node's HTTP server
}

func (m PartMeta) overlaps(from, to int64) bool {
	return m.MaxTimestamp >= from && m.MinTimestamp <= to
}
