package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitHTTP(ctx context.Context, t testing.TB, url string) {
	t.Helper()
	must := require.New(t)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		must.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "wait for http", "url %s: %v", url, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func postJSON(ctx context.Context, t testing.TB, url string, v any) {
	t.Helper()
	must := require.New(t)
	body, err := json.Marshal(v)
	must.NoError(err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	must.NoError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusNoContent, resp.StatusCode, "POST %s: %s", url, resp.Status)
}

func getJSON(ctx context.Context, t testing.TB, url string, v any) {
	t.Helper()
	must := require.New(t)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	must.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusOK, resp.StatusCode, "GET %s: %s", url, resp.Status)
	must.NoError(json.NewDecoder(resp.Body).Decode(v), fmt.Sprintf("decode %s", url))
}
