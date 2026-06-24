// Package shrimpapi implements the HTTP API for the shrimpd daemon.
package shrimpapi

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/tdakkota/shrimpd/internal/shrimpfilter"
	"github.com/tdakkota/shrimpd/internal/shrimplication"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

// Server serves the daemon HTTP API for ingesting, reading, and sharing parts.
type Server struct {
	lsm *shrimplication.LSM
	srv *http.Server
}

// NewServer creates a daemon HTTP server bound to addr.
func NewServer(addr string, lsm *shrimplication.LSM) *Server {
	mux := http.NewServeMux()
	s := &Server{lsm: lsm}

	// POST /ingest          body: {"data":[{"timestamp":1,"data":"foo"}]}
	// GET  /read?from=&to=  timestamp range, inclusive; omit either for open bound
	// GET  /query?from=&to= same as /read, kept as a debug-friendly alias
	// GET  /part/{id}       raw part JSON (served to peer nodes)
	// GET  /parts           global part list from etcd (debugging)
	// POST /flush           forces immediate flush of memtable and index memtable
	// POST /compact         forces immediate compaction of data and index parts
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("POST /ingest/otlp", s.handleIngestOTLP)
	mux.HandleFunc("POST /v1/logs", s.handleIngestOTLP)
	mux.HandleFunc("GET /read", s.handleQuery)
	mux.HandleFunc("GET /query", s.handleQuery)
	mux.HandleFunc("GET /part/{id}", s.handlePart)
	mux.HandleFunc("GET /parts", s.handleParts)
	mux.HandleFunc("POST /flush", s.handleFlush)
	mux.HandleFunc("POST /compact", s.handleCompact)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.InfoContext(r.Context(), "incoming request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		mux.ServeHTTP(w, r)
	})

	s.srv = &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Run listens and serves until ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "http server listening", "addr", s.srv.Addr)
	go func() {
		<-ctx.Done()
		if err := s.srv.Shutdown(context.Background()); err != nil {
			slog.Warn("http server shutdown failed", "error", err)
		}
	}()
	if err := s.srv.Serve(ln); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ingestStreamBatch controls how many entries are decoded and written to WAL at
// a time. Keeping this small bounds peak memory regardless of request body size.
const ingestStreamBatch = 1000

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	const (
		maxIngestBody = 256 << 20 // 256 MiB hard ceiling
		jxBufSize     = 4096      // jx internal read buffer; covers any single token
	)

	d := jx.Decode(http.MaxBytesReader(w, r.Body, maxIngestBody), jxBufSize)

	var (
		batch    []shrimptypes.Entry
		total    int
		writeErr error
	)

	flush := func() {
		if writeErr != nil || len(batch) == 0 {
			return
		}
		if err := s.lsm.Write(r.Context(), batch); err != nil {
			writeErr = err
			return
		}
		total += len(batch)
		batch = batch[:0]
	}

	// Stream-decode {"data":[{"timestamp":N,"data":"..."},...]} entry by entry.
	decodeErr := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		if string(key) != "data" {
			return d.Skip()
		}
		return d.Arr(func(d *jx.Decoder) error {
			if writeErr != nil {
				return writeErr
			}
			var e shrimptypes.Entry
			if err := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
				switch string(key) {
				case "timestamp":
					v, err := d.Int64()
					if err != nil {
						return err
					}
					e.Timestamp = v
				case "data":
					v, err := d.Str()
					if err != nil {
						return err
					}
					e.Data = v
				default:
					return d.Skip()
				}
				return nil
			}); err != nil {
				return err
			}
			batch = append(batch, e)
			if len(batch) >= ingestStreamBatch {
				flush()
			}
			return writeErr
		})
	})

	if decodeErr != nil {
		http.Error(w, decodeErr.Error(), http.StatusBadRequest)
		return
	}
	if writeErr != nil {
		http.Error(w, writeErr.Error(), http.StatusInternalServerError)
		return
	}
	flush() // flush remaining tail
	if writeErr != nil {
		http.Error(w, writeErr.Error(), http.StatusInternalServerError)
		return
	}
	if total == 0 {
		http.Error(w, "empty block", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type otlpScopeJSON struct {
	Name       string         `json:"name,omitempty"`
	Version    string         `json:"version,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type otlpLogRecordJSON struct {
	Timestamp         uint64         `json:"timestamp"`
	ObservedTimestamp uint64         `json:"observed_timestamp,omitempty"`
	SeverityNumber    int32          `json:"severity_number,omitempty"`
	SeverityText      string         `json:"severity_text,omitempty"`
	Body              any            `json:"body,omitempty"`
	Attributes        map[string]any `json:"attributes,omitempty"`
	TraceID           string         `json:"trace_id,omitempty"`
	SpanID            string         `json:"span_id,omitempty"`
	Flags             uint32         `json:"flags,omitempty"`
	Resource          map[string]any `json:"resource,omitempty"`
	Scope             *otlpScopeJSON `json:"scope,omitempty"`
}

func (s *Server) handleIngestOTLP(w http.ResponseWriter, r *http.Request) {
	const maxBodySize = 32 << 20 // 32 MiB
	bodyReader := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		var err error
		bodyReader, err = gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer func() { _ = bodyReader.Close() }()
	}

	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, bodyReader, maxBodySize))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var logsData plog.Logs
	var unmarshalErr error
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/x-protobuf") {
		unmarshaler := &plog.ProtoUnmarshaler{}
		logsData, unmarshalErr = unmarshaler.UnmarshalLogs(bodyBytes)
	} else {
		// Default to JSON
		unmarshaler := &plog.JSONUnmarshaler{}
		logsData, unmarshalErr = unmarshaler.UnmarshalLogs(bodyBytes)
	}
	if unmarshalErr != nil {
		slog.WarnContext(r.Context(), "failed to unmarshal OTLP logs", "error", unmarshalErr, "content_type", contentType)
		http.Error(w, "failed to unmarshal logs: "+unmarshalErr.Error(), http.StatusBadRequest)
		return
	}

	var entries []shrimptypes.Entry
	now := time.Now().UnixNano()

	rls := logsData.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scope := sl.Scope()
			records := sl.LogRecords()
			for k := 0; k < records.Len(); k++ {
				record := records.At(k)
				ts := record.Timestamp()
				if ts == 0 {
					ts = record.ObservedTimestamp()
				}
				if ts == 0 {
					ts = pcommon.Timestamp(now)
				}

				rMap := resource.Attributes().AsRaw()
				sObj := &otlpScopeJSON{
					Name:       scope.Name(),
					Version:    scope.Version(),
					Attributes: scope.Attributes().AsRaw(),
				}

				bodyVal := record.Body().AsRaw()
				attrMap := record.Attributes().AsRaw()

				var traceIDHex string
				if !record.TraceID().IsEmpty() {
					traceIDHex = record.TraceID().String()
				}
				var spanIDHex string
				if !record.SpanID().IsEmpty() {
					spanIDHex = record.SpanID().String()
				}

				entryJSON := otlpLogRecordJSON{
					Timestamp:         uint64(record.Timestamp()),
					ObservedTimestamp: uint64(record.ObservedTimestamp()),
					SeverityNumber:    int32(record.SeverityNumber()),
					SeverityText:      record.SeverityText(),
					Body:              bodyVal,
					Attributes:        attrMap,
					TraceID:           traceIDHex,
					SpanID:            spanIDHex,
					Flags:             uint32(record.Flags()),
					Resource:          rMap,
					Scope:             sObj,
				}

				dataBytes, err := json.Marshal(entryJSON)
				if err != nil {
					slog.WarnContext(r.Context(), "skip OTLP record: marshal failed", "error", err)
					continue
				}

				entries = append(entries, shrimptypes.Entry{
					Timestamp: int64(ts),
					Data:      string(dataBytes),
				})
			}
		}
	}

	if len(entries) == 0 {
		s.writeOTLPResponse(w, contentType)
		return
	}

	if err := s.lsm.Write(r.Context(), entries); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeOTLPResponse(w, contentType)
}

func (s *Server) writeOTLPResponse(w http.ResponseWriter, contentType string) {
	if strings.Contains(contentType, "application/x-protobuf") {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		// Do not write body bytes to match prior behavior and test expectations.
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := plogotlp.NewExportResponse()
		b, _ := resp.MarshalJSON()
		_, _ = w.Write(b)
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := parseIntParam(q.Get("from"), 0)
	to := parseIntParam(q.Get("to"), 1<<62)
	term := q.Get("term")

	var m shrimpfilter.Matcher
	if qstr := q.Get("q"); qstr != "" {
		var qf struct {
			Line []struct {
				Op string `json:"op"`
				V  string `json:"v"`
			} `json:"line"`
			Labels []struct {
				L  string `json:"l"`
				Op string `json:"op"`
				V  string `json:"v"`
			} `json:"labels"`
		}
		if err := json.Unmarshal([]byte(qstr), &qf); err != nil {
			http.Error(w, "bad q: "+err.Error(), http.StatusBadRequest)
			return
		}
		var lines []shrimpfilter.LineFilter
		for _, lf := range qf.Line {
			op, ok := parseLineOp(lf.Op)
			if !ok {
				http.Error(w, "bad line op: "+lf.Op, http.StatusBadRequest)
				return
			}
			lines = append(lines, shrimpfilter.LineFilter{Op: op, Value: lf.V})
		}
		var labels []shrimpfilter.LabelFilter
		for _, lf := range qf.Labels {
			op, ok := parseLabelOp(lf.Op)
			if !ok {
				http.Error(w, "bad label op: "+lf.Op, http.StatusBadRequest)
				return
			}
			labels = append(labels, shrimpfilter.LabelFilter{Label: lf.L, Op: op, Value: lf.V})
		}
		var err error
		m, err = shrimpfilter.CompileMatcher(lines, labels)
		if err != nil {
			http.Error(w, "bad matcher: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	// Stream response: never accumulate a []Entry result slice.
	// Peak memory = O(one decoded block) regardless of match count.
	bw := bufio.NewWriterSize(w, 64<<10)
	_, _ = bw.WriteString(`{"data":[`)

	first := true
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	var err error
	emit := func(e shrimptypes.Entry) error {
		jw.Reset()
		jw.ObjStart()
		jw.RawStr(`"timestamp":`)
		jw.Int64(e.Timestamp)
		jw.RawStr(`,"data":`)
		jw.Str(e.Data)
		jw.ObjEnd()
		if !first {
			jw.Comma()
		}
		first = false
		_, werr := bw.Write(jw.Buf)
		return werr
	}
	if q.Get("q") != "" {
		var stats *shrimptypes.QueryStats
		stats, err = s.lsm.QueryMatcherWithStats(r.Context(), from, to, m, emit)
		if err != nil {
			slog.WarnContext(r.Context(), "stream query", "error", err)
		}

		_, _ = bw.WriteString(`],"stats":`)
		if statsBytes, merr := json.Marshal(stats); merr == nil {
			_, _ = bw.Write(statsBytes)
		} else {
			_, _ = bw.WriteString(`null`)
		}
		_, _ = bw.WriteString(`}`)
		if ferr := bw.Flush(); ferr != nil {
			slog.WarnContext(r.Context(), "flush query response", "error", ferr)
		}
		return
	} else {
		var stats *shrimptypes.QueryStats
		stats, err = s.lsm.QueryStreamWithStats(r.Context(), from, to, term, emit)
		if err != nil {
			slog.WarnContext(r.Context(), "stream query", "error", err)
		}

		_, _ = bw.WriteString(`],"stats":`)
		if statsBytes, merr := json.Marshal(stats); merr == nil {
			_, _ = bw.Write(statsBytes)
		} else {
			_, _ = bw.WriteString(`null`)
		}
		_, _ = bw.WriteString(`}`)
		if ferr := bw.Flush(); ferr != nil {
			slog.WarnContext(r.Context(), "flush query response", "error", ferr)
		}
		return
	}
}

func parseLineOp(s string) (shrimpfilter.LineOp, bool) {
	switch s {
	case "|=", "eq":
		return shrimpfilter.OpLineEq, true
	case "!=", "ne":
		return shrimpfilter.OpLineNotEq, true
	case "|~", "re":
		return shrimpfilter.OpLineRe, true
	case "!~", "nre":
		return shrimpfilter.OpLineNotRe, true
	}
	return 0, false
}

func parseLabelOp(s string) (shrimpfilter.LabelOp, bool) {
	switch s {
	case "eq":
		return shrimpfilter.OpLabelEq, true
	case "ne":
		return shrimpfilter.OpLabelNotEq, true
	case "re":
		return shrimpfilter.OpLabelRe, true
	case "nre":
		return shrimpfilter.OpLabelNotRe, true
	}
	return 0, false
}

func (s *Server) handlePart(w http.ResponseWriter, r *http.Request) {
	// Set Content-Type before writing; http.Error will override it on failure
	// (safe because os.Open failure occurs before any bytes are written to w).
	w.Header().Set("Content-Type", "application/json")
	if err := s.lsm.ServeLocalPart(r, w); err != nil {
		http.Error(w, "part not found: "+r.PathValue("id"), http.StatusNotFound)
	}
}

func (s *Server) handleParts(w http.ResponseWriter, r *http.Request) {
	parts, err := s.lsm.AllParts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(parts); err != nil {
		slog.WarnContext(r.Context(), "encode parts response", "error", err)
	}
}

func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if err := s.lsm.Flush(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if err := s.lsm.Compact(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseIntParam(s string, def int64) int64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}
