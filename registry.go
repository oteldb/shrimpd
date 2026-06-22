package shrimpd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcd key schema:
//
//	/lsm/log/{index}      → LogEntry JSON   (global mutation log; sequential %016d)
//	/lsm/nodes/{node-id}  → nodeInfo JSON   (ephemeral; disappears when lease expires on crash)
//	/lsm/nodes/{node-id}/pointer → string   (persistent; local node's processed log index)
const (
	logPrefix  = "/lsm/log"
	nodePrefix = "/lsm/nodes/"
	leaseTTL   = 10 // seconds
)

// LogOp represents the mutation operation.
type LogOp string

const (
	OpPut   LogOp = "PUT"
	OpMerge LogOp = "MERGE"
)

// LogEntry describes an operation appended to the global event log.
type LogEntry struct {
	Index    int64    `json:"index"`
	Op       LogOp    `json:"op"`
	Part     PartMeta `json:"part,omitempty"`
	OldParts []string `json:"old_parts,omitempty"`
	NodeID   string   `json:"node_id"`
}

// Registry stores node and part metadata in etcd.
type Registry struct {
	cli    *clientv3.Client
	nodeID string
}

// NewRegistry creates an etcd-backed metadata registry for nodeID.
func NewRegistry(cli *clientv3.Client, nodeID string) *Registry {
	return &Registry{cli: cli, nodeID: nodeID}
}

type nodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// RegisterNode writes an ephemeral node entry backed by a lease with keepalive.
// The entry disappears automatically if the node crashes and stops sending heartbeats.
func (r *Registry) RegisterNode(ctx context.Context, addr string) error {
	lease, err := r.cli.Grant(ctx, leaseTTL)
	if err != nil {
		return err
	}
	b, err := json.Marshal(nodeInfo{ID: r.nodeID, Addr: addr})
	if err != nil {
		return err
	}
	if _, err = r.cli.Put(ctx, nodePrefix+r.nodeID, string(b),
		clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	ch, err := r.cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	go func() {
		for range ch {
		}
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "etcd lease keepalive failed", "node_id", r.nodeID)
		}
	}()
	slog.InfoContext(ctx, "registered node", "node_id", r.nodeID, "addr", addr, "lease", lease.ID)
	return nil
}

// AppendLog appends a new mutation operation to the global log.
// Uses an optimistic transaction loop to determine the next sequential key.
func (r *Registry) AppendLog(ctx context.Context, op LogOp, part PartMeta, oldParts []string) (int64, error) {
	baseKey := "__" + logPrefix
	for i := 0; i < 100; i++ {
		resp, err := r.cli.Get(ctx, logPrefix+"/", clientv3.WithLastKey()...)
		if err != nil {
			return 0, err
		}

		newSeqNum := int64(1)
		var revision int64

		if len(resp.Kvs) != 0 {
			seqNumStr := path.Base(string(resp.Kvs[0].Key))
			seqNum, err := strconv.ParseInt(seqNumStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse seq num %q: %w", seqNumStr, err)
			}
			newSeqNum = seqNum + 1
			revision = resp.Header.Revision
		} else {
			respBase, err := r.cli.Get(ctx, baseKey)
			if err != nil {
				return 0, err
			}
			if len(respBase.Kvs) != 0 && string(respBase.Kvs[0].Value) != "" {
				seqNum, err := strconv.ParseInt(string(respBase.Kvs[0].Value), 10, 64)
				if err != nil {
					return 0, fmt.Errorf("parse base seq num: %w", err)
				}
				newSeqNum = seqNum + 1
			}
			revision = respBase.Header.Revision
		}

		newKey := fmt.Sprintf("%s/%016d", logPrefix, newSeqNum)
		entry := LogEntry{
			Index:    newSeqNum,
			Op:       op,
			Part:     part,
			OldParts: oldParts,
			NodeID:   r.nodeID,
		}
		b, err := json.Marshal(entry)
		if err != nil {
			return 0, err
		}

		cmp := clientv3.Compare(clientv3.ModRevision(baseKey), "<", revision+1)
		reqPrefix := clientv3.OpPut(baseKey, fmt.Sprintf("%016d", newSeqNum))
		reqNewLog := clientv3.OpPut(newKey, string(b))

		txnResp, err := r.cli.Txn(ctx).If(cmp).Then(reqPrefix, reqNewLog).Commit()
		if err != nil {
			return 0, err
		}
		if txnResp.Succeeded {
			return newSeqNum, nil
		}
	}
	return 0, fmt.Errorf("can't create serial log record, high concurrency")
}

// GetLogs retrieves sequential log entries starting from a given index (inclusive).
func (r *Registry) GetLogs(ctx context.Context, fromIndex int64) ([]LogEntry, error) {
	startKey := fmt.Sprintf("%s/%016d", logPrefix, fromIndex)
	endKey := fmt.Sprintf("%s/%016d", logPrefix, 9999999999999999)

	resp, err := r.cli.Get(ctx, startKey, clientv3.WithRange(endKey), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, err
	}

	var entries []LogEntry
	for _, kv := range resp.Kvs {
		var entry LogEntry
		if err := json.Unmarshal(kv.Value, &entry); err == nil {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

// GetQueuePointer retrieves the last processed log index for this node.
func (r *Registry) GetQueuePointer(ctx context.Context) (int64, error) {
	key := fmt.Sprintf("%s%s/pointer", nodePrefix, r.nodeID)
	resp, err := r.cli.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	val := string(resp.Kvs[0].Value)
	pointer, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse pointer %q: %w", val, err)
	}
	return pointer, nil
}

// SetQueuePointer stores the last processed log index for this node.
func (r *Registry) SetQueuePointer(ctx context.Context, index int64) error {
	key := fmt.Sprintf("%s%s/pointer", nodePrefix, r.nodeID)
	_, err := r.cli.Put(ctx, key, strconv.FormatInt(index, 10))
	return err
}

