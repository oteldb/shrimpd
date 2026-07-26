package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/shrimpd/replication"
	"github.com/oteldb/shrimpd/replication/etcdkv"
	"github.com/oteldb/shrimpd/replication/kvtest"
)

// TestEtcdKVConformance runs the shared KV suite against a real etcd.
//
// It is the counterpart to the same suite over memkv: the fast tests are only trustworthy while
// the two implementations agree, and every disagreement so far has been a bug that reached a real
// cluster before anything caught it.
func TestEtcdKVConformance(t *testing.T) {
	requireE2E(t)

	ctx := t.Context()
	endpoint := startEtcd(ctx, t)

	kvtest.Run(t, func(t *testing.T) func(*testing.T) replication.KV {
		// Each subtest gets its own keyspace, so sessions in one cannot see another's keys.
		prefix := "/kvtest/" + t.Name() + "/"

		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: 5 * time.Second,
		})
		require.NoError(t, err)

		waitEtcd(ctx, t, cli)

		_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, err)

		t.Cleanup(func() {
			_, _ = cli.Delete(context.WithoutCancel(ctx), prefix, clientv3.WithPrefix())
			_ = cli.Close()
		})

		return func(t *testing.T) replication.KV {
			// A short lease keeps the ephemeral-expiry assertion quick.
			kv, err := etcdkv.New(ctx, cli, 1)
			require.NoError(t, err)

			return prefixed{KV: kv, prefix: prefix}
		}
	})
}

// prefixed scopes a KV to a key prefix, so each subtest works in its own corner of the store.
type prefixed struct {
	replication.KV

	prefix string
}

func (p prefixed) Get(ctx context.Context, key string) (replication.Value, error) {
	v, err := p.KV.Get(ctx, p.prefix+key)
	v.Key = trim(v.Key, p.prefix)

	return v, err
}

func (p prefixed) List(ctx context.Context, prefix string) ([]replication.Value, error) {
	return p.strip(p.KV.List(ctx, p.prefix+prefix))
}

func (p prefixed) ListFrom(ctx context.Context, prefix, start string, limit int) ([]replication.Value, error) {
	if start != "" {
		start = p.prefix + start
	}

	return p.strip(p.KV.ListFrom(ctx, p.prefix+prefix, start, limit))
}

func (p prefixed) Txn(ctx context.Context, t replication.Txn) (bool, error) {
	scoped := replication.Txn{
		If:   make([]replication.Cond, 0, len(t.If)),
		Then: make([]replication.Op, 0, len(t.Then)),
	}

	for _, c := range t.If {
		c.Key = p.prefix + c.Key
		scoped.If = append(scoped.If, c)
	}

	for _, op := range t.Then {
		op.Key = p.prefix + op.Key
		scoped.Then = append(scoped.Then, op)
	}

	return p.KV.Txn(ctx, scoped)
}

func (p prefixed) Ephemeral(ctx context.Context, key string, data []byte) error {
	return p.KV.Ephemeral(ctx, p.prefix+key, data)
}

func (p prefixed) strip(values []replication.Value, err error) ([]replication.Value, error) {
	for i := range values {
		values[i].Key = trim(values[i].Key, p.prefix)
	}

	return values, err
}

func trim(key, prefix string) string {
	if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
		return key[len(prefix):]
	}

	return key
}
