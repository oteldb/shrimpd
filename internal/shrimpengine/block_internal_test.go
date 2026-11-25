package shrimpengine

import (
	"encoding/json"
	"testing"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/replication"
)

func TestPartFromIndex(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		entry bucketindex.Entry
		want  replication.Range
	}{
		{
			name:  "legacy sequence",
			entry: bucketindex.Entry{Prefix: "a/0000000001"},
			want:  replication.Range{First: 1, Last: 1},
		},
		{
			name: "opaque part ID",
			entry: bucketindex.Entry{
				Prefix: "a/01JZ1MVQPJFP08SQSBKNGTHZ51",
				Blocks: bucketindex.Blocks(1),
			},
			want: replication.Range{First: 1<<63 + 1, Last: 1<<63 + 1},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			part, err := partFromIndex("a", tt.entry)
			require.NoError(t, err)

			data, err := json.Marshal(part)
			require.NoError(t, err)

			var decoded Part
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.Equal(t, tt.entry.Prefix, decoded.Prefix())
			require.Equal(t, tt.want, decoded.Range())
		})
	}
}
