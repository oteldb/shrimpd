// Package etcdkv implements [replication.KV] on etcd v3.
//
// The mapping is direct: a [replication.Txn] is one etcd transaction with ModRevision compares,
// [replication.KV.ListFrom] is a range read with a limit, and ephemeral keys are puts bound to a
// [concurrency.Session] lease — so a replica that dies stops advertising itself within the lease
// TTL without anything having to notice.
package etcdkv

import (
	"context"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/oteldb/shrimpd/replication"
)

// DefaultTTL is the session lease TTL in seconds: how long a crashed replica keeps appearing
// live to its peers.
const DefaultTTL = 10

// Store is an etcd-backed [replication.KV].
type Store struct {
	client  *clientv3.Client
	session *concurrency.Session
}

var _ replication.KV = (*Store)(nil)

// New opens a session on client and returns a store using it. The caller keeps ownership of
// client; [Store.Close] revokes only the session lease.
func New(ctx context.Context, client *clientv3.Client, ttl int) (*Store, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	session, err := concurrency.NewSession(client,
		concurrency.WithTTL(ttl),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create etcd session")
	}

	return &Store{client: client, session: session}, nil
}

// Get implements [replication.KV].
func (s *Store) Get(ctx context.Context, key string) (replication.Value, error) {
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return replication.Value{}, errors.Wrapf(err, "get %q", key)
	}

	if len(resp.Kvs) == 0 {
		return replication.Value{}, errors.Wrapf(replication.ErrNotFound, "get %q", key)
	}

	kv := resp.Kvs[0]

	return replication.Value{Key: string(kv.Key), Data: kv.Value, Rev: kv.ModRevision}, nil
}

// List implements [replication.KV].
func (s *Store) List(ctx context.Context, prefix string) ([]replication.Value, error) {
	return s.ListFrom(ctx, prefix, "", 0)
}

// ListFrom implements [replication.KV].
func (s *Store) ListFrom(ctx context.Context, prefix, start string, limit int) ([]replication.Value, error) {
	opts := []clientv3.OpOption{clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)}
	key := prefix

	if start != "" {
		// Range from start up to the end of the prefix, rather than by prefix from the
		// beginning: this is what makes reading the log from a pointer O(new records).
		key = start
		opts = append(opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)))
	} else {
		opts = append(opts, clientv3.WithPrefix())
	}

	if limit > 0 {
		opts = append(opts, clientv3.WithLimit(int64(limit)))
	}

	resp, err := s.client.Get(ctx, key, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "list %q", prefix)
	}

	out := make([]replication.Value, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out = append(out, replication.Value{
			Key:  string(kv.Key),
			Data: kv.Value,
			Rev:  kv.ModRevision,
		})
	}

	return out, nil
}

// Txn implements [replication.KV].
func (s *Store) Txn(ctx context.Context, t replication.Txn) (bool, error) {
	cmps := make([]clientv3.Cmp, 0, len(t.If))
	for _, c := range t.If {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(c.Key), "=", c.Rev))
	}

	ops := make([]clientv3.Op, 0, len(t.Then))

	for _, op := range t.Then {
		switch op.Kind {
		case replication.OpPut:
			ops = append(ops, clientv3.OpPut(op.Key, string(op.Data)))
		case replication.OpDelete:
			ops = append(ops, clientv3.OpDelete(op.Key))
		case replication.OpDeletePrefix:
			ops = append(ops, clientv3.OpDelete(op.Key, clientv3.WithPrefix()))
		}
	}

	resp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return false, errors.Wrap(err, "etcd txn")
	}

	return resp.Succeeded, nil
}

// Ephemeral implements [replication.KV].
func (s *Store) Ephemeral(ctx context.Context, key string, data []byte) error {
	if _, err := s.client.Put(ctx, key, string(data), clientv3.WithLease(s.session.Lease())); err != nil {
		return errors.Wrapf(err, "put ephemeral %q", key)
	}

	return nil
}

// Done implements [replication.KV].
func (s *Store) Done() <-chan struct{} { return s.session.Done() }

// Close implements [replication.KV]: it revokes the session lease, dropping every ephemeral key
// this replica holds.
func (s *Store) Close() error {
	if err := s.session.Close(); err != nil {
		return errors.Wrap(err, "close etcd session")
	}

	return nil
}
