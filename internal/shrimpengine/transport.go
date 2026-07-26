package shrimpengine

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/backend"
	"github.com/zeebo/xxh3"
)

// The two endpoints a node exposes so peers can copy its parts. They are deliberately dumb: one
// lists object keys under a prefix, the other returns one object verbatim. Replication decides
// *which* objects to ask for; the transport only moves them.
const (
	// ListPath serves the object keys under the "prefix" query parameter.
	ListPath = "/internal/parts/list"
	// ObjectPath serves one object, named by the "key" query parameter.
	ObjectPath = "/internal/parts/object"

	// checksumHeader carries the xxh3 of the body so the client can reject a corrupted transfer
	// rather than writing it into its own part store.
	checksumHeader = "X-Checksum-Xxh3"
)

// validKey reports whether a remotely supplied key or prefix is safe to hand to a backend.
// The file backend confines paths to its root as well; this is the same check at the network
// boundary, so a hostile peer or client cannot even express a traversal.
func validKey(k string) bool {
	return !strings.Contains(k, "..") &&
		!strings.HasPrefix(k, "/") &&
		!strings.ContainsAny(k, "\\\x00")
}

// ListHandler serves object keys under a prefix, framed as a uvarint count followed by
// uvarint-length-prefixed keys.
func ListHandler(be backend.Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		prefix := req.URL.Query().Get("prefix")
		if !validKey(prefix) {
			http.Error(w, "invalid prefix", http.StatusBadRequest)

			return
		}

		keys, err := be.List(req.Context(), prefix)
		if err != nil {
			http.Error(w, "list: "+err.Error(), http.StatusInternalServerError)

			return
		}

		buf := binary.AppendUvarint(nil, uint64(len(keys)))
		for _, k := range keys {
			buf = binary.AppendUvarint(buf, uint64(len(k)))
			buf = append(buf, k...)
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
		_, _ = w.Write(buf)
	})
}

// ObjectHandler serves one object verbatim, with its checksum in a header.
func ObjectHandler(be backend.Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" || !validKey(key) {
			http.Error(w, "invalid key", http.StatusBadRequest)

			return
		}

		data, err := be.Read(req.Context(), key)
		if err != nil {
			if errors.Is(err, backend.ErrNotExist) {
				http.Error(w, "no such object", http.StatusNotFound)

				return
			}

			http.Error(w, "read: "+err.Error(), http.StatusInternalServerError)

			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(checksumHeader, strconv.FormatUint(xxh3.Hash(data), 16))
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	})
}

// Client fetches part objects from peers over the endpoints above.
type Client struct {
	http *http.Client
}

// DefaultFetchTimeout bounds a single object transfer.
const DefaultFetchTimeout = 2 * time.Minute

// NewClient returns a fetch client. A nil http client gets one with [DefaultFetchTimeout].
func NewClient(hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: DefaultFetchTimeout}
	}

	return &Client{http: hc}
}

// List returns the peer's object keys under prefix.
func (c *Client) List(ctx context.Context, addr, prefix string) ([]string, error) {
	body, err := c.get(ctx, addr, ListPath, url.Values{"prefix": {prefix}})
	if err != nil {
		return nil, err
	}

	n, read := binary.Uvarint(body)
	if read <= 0 {
		return nil, errors.New("malformed key listing")
	}

	body = body[read:]
	out := make([]string, 0, n)

	for range n {
		l, read := binary.Uvarint(body)
		if read <= 0 || uint64(len(body[read:])) < l {
			return nil, errors.New("truncated key listing")
		}

		body = body[read:]
		out = append(out, string(body[:l]))
		body = body[l:]
	}

	return out, nil
}

// Object returns one of the peer's objects, verified against the checksum it advertised.
func (c *Client) Object(ctx context.Context, addr, key string) ([]byte, error) {
	data, found, err := c.ObjectIfExists(ctx, addr, key)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, errors.Errorf("peer %s has no object %q", addr, key)
	}

	return data, nil
}

// ObjectIfExists is [Client.Object] for objects a peer may legitimately not have yet, reporting
// absence rather than failing.
func (c *Client) ObjectIfExists(ctx context.Context, addr, key string) (data []byte, found bool, err error) {
	if !validKey(key) {
		return nil, false, errors.Errorf("refusing to fetch unsafe key %q", key)
	}

	body, err := c.getWithHeader(ctx, addr, ObjectPath, url.Values{"key": {key}})
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, false, nil
		}

		return nil, false, err
	}

	if want := body.checksum; want != "" {
		got := strconv.FormatUint(xxh3.Hash(body.data), 16)
		if got != want {
			return nil, false, errors.Errorf("checksum mismatch for %q: got %s, want %s", key, got, want)
		}
	}

	return body.data, true, nil
}

// errNotFound distinguishes a peer's 404 from a transport failure, so an object that legitimately
// does not exist yet is not retried as an error.
var errNotFound = errors.New("object not found on peer")

type response struct {
	data     []byte
	checksum string
}

func (c *Client) get(ctx context.Context, addr, path string, q url.Values) ([]byte, error) {
	resp, err := c.getWithHeader(ctx, addr, path, q)
	if err != nil {
		return nil, err
	}

	return resp.data, nil
}

func (c *Client) getWithHeader(ctx context.Context, addr, path string, q url.Values) (response, error) {
	endpoint := &url.URL{Scheme: "http", Host: addr, Path: path, RawQuery: q.Encode()}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), http.NoBody)
	if err != nil {
		return response{}, errors.Wrap(err, "build request")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return response{}, errors.Wrapf(err, "get %s", endpoint.Redacted())
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return response{}, errors.Wrapf(errNotFound, "get %s", endpoint.Redacted())
	}

	if resp.StatusCode != http.StatusOK {
		return response{}, errors.Errorf("get %s: %s", endpoint.Redacted(), resp.Status)
	}

	// maxObjectBytes caps a single transfer so a hostile or broken peer cannot exhaust memory.
	const maxObjectBytes = 1 << 30

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxObjectBytes))
	if err != nil {
		return response{}, errors.Wrapf(err, "read %s", endpoint.Redacted())
	}

	return response{data: data, checksum: resp.Header.Get(checksumHeader)}, nil
}
