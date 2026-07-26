package memkv_test

import (
	"testing"

	"github.com/oteldb/shrimpd/replication"
	"github.com/oteldb/shrimpd/replication/kvtest"
	"github.com/oteldb/shrimpd/replication/memkv"
)

// TestConformance runs the shared [replication.KV] suite. memkv stands in for etcd in every fast
// test, so any behavior it gets wrong is a bug those tests cannot see.
func TestConformance(t *testing.T) {
	t.Parallel()

	kvtest.Run(t, func(*testing.T) func(*testing.T) replication.KV {
		space := memkv.NewSpace()

		return func(*testing.T) replication.KV { return space.Session() }
	})
}
