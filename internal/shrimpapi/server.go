// Package shrimpapi serves shrimpd's HTTP API: ingest, query, operator introspection, and the
// internal endpoints peers use to copy parts.
package shrimpapi

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/CAFxX/httpcompression"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/oteldb/storage/otlp/pdataconv"
	slog "github.com/oteldb/storage/signal/log"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.uber.org/zap"

	"github.com/oteldb/shrimpd/internal/shrimpengine"
	"github.com/oteldb/shrimpd/internal/shrimpnode"
)

// Body size limits. Ingest bodies are bounded because a single request is buffered before it is
// decoded; the ceiling is generous enough for any realistic OTLP batch.
const (
	maxIngestBody = 256 << 20
	jxBufSize     = 4096
)

// Server serves the daemon HTTP API.
type Server struct {
	node *shrimpnode.Node
	srv  *http.Server
	lg   *zap.Logger
}

// NewServer builds the API server bound to addr.
func NewServer(addr string, node *shrimpnode.Node, lg *zap.Logger) (*Server, error) {
	if lg == nil {
		lg = zap.NewNop()
	}

	s := &Server{node: node, lg: lg}

	compress, err := httpcompression.DefaultAdapter(httpcompression.MinSize(1024))
	if err != nil {
		return nil, errors.Wrap(err, "build compression adapter")
	}

	mux := http.NewServeMux()

	// Data plane.
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.Handle("POST /ingest/otlp", http.HandlerFunc(s.handleIngestOTLP))
	mux.Handle("POST /v1/logs", http.HandlerFunc(s.handleIngestOTLP))

	query := compress(http.HandlerFunc(s.handleQuery))
	mux.Handle("GET /query", query)
	mux.Handle("GET /read", query)

	// Operator plane.
	mux.HandleFunc("GET /parts", s.handleState)
	mux.HandleFunc("GET /state", s.handleState)
	mux.HandleFunc("POST /flush", s.handleFlush)
	mux.HandleFunc("POST /compact", s.handleMerge)

	// Replication transport: how a peer copies this node's part objects. These are the only
	// endpoints replication itself uses, and they know nothing about replication — they just
	// serve backend objects.
	be := node.Engine().Backend()
	mux.Handle("GET "+shrimpengine.ListPath, shrimpengine.ListHandler(be))
	mux.Handle("GET "+shrimpengine.ObjectPath, shrimpengine.ObjectHandler(be))

	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	return s, nil
}

// Run listens and serves until ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return errors.Wrapf(err, "listen on %s", s.srv.Addr)
	}

	s.lg.Info("http server listening", zap.String("addr", s.srv.Addr))

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			s.lg.Warn("http server shutdown", zap.Error(err))
		}
	}()

	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.Wrap(err, "serve")
	}

	return nil
}

// handleIngest accepts shrimpd's native batch shape:
//
//	{"data":[{"timestamp":1700000000000000000,"data":"hello"}]}
//
// Each entry becomes one log record whose body is the line. It is the plain-text door into a
// store whose native model is OTLP.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	d := jx.Decode(http.MaxBytesReader(w, r.Body, maxIngestBody), jxBufSize)

	var logs slog.Logs

	sl := logs.AddResource().AddScope()
	sl.Scope.Name = []byte("shrimpd")

	if err := decodeIngestBatch(d, func(ts int64, body []byte) {
		rec := sl.AddRecord()
		rec.Timestamp = ts
		rec.Body = body
	}); err != nil {
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)

		return
	}

	accepted, rejected, err := s.node.Ingest(r.Context(), logs)
	if err != nil {
		s.lg.Error("ingest", zap.Error(err))
		http.Error(w, "ingest: "+err.Error(), http.StatusInternalServerError)

		return
	}

	writeJSON(w, map[string]int{"accepted": accepted, "rejected": rejected})
}

// handleIngestOTLP accepts OTLP/HTTP logs in either protobuf or JSON, the shape an OpenTelemetry
// collector exports. The payload is converted straight into the storage library's ingest model.
func (s *Server) handleIngestOTLP(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, maxIngestBody)

	req := plogotlp.NewExportRequest()
	contentType := r.Header.Get("Content-Type")

	data, err := readAllLimited(body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)

		return
	}

	switch contentType {
	case "application/x-protobuf":
		err = req.UnmarshalProto(data)
	default:
		err = req.UnmarshalJSON(data)
	}

	if err != nil {
		http.Error(w, "decode otlp: "+err.Error(), http.StatusBadRequest)

		return
	}

	var logs slog.Logs

	dropped := pdataconv.AppendLogs(&logs, req.Logs())

	accepted, rejected, err := s.node.Ingest(r.Context(), logs)
	if err != nil {
		s.lg.Error("ingest otlp", zap.Error(err))
		http.Error(w, "ingest: "+err.Error(), http.StatusInternalServerError)

		return
	}

	s.lg.Debug("ingested otlp", zap.Int("accepted", accepted))
	s.writeOTLPResponse(w, contentType, rejected+dropped)
}

// writeOTLPResponse replies in the same encoding the request used, reporting any records the
// store refused as OTLP partial success.
func (s *Server) writeOTLPResponse(w http.ResponseWriter, contentType string, rejected int) {
	resp := plogotlp.NewExportResponse()
	if rejected > 0 {
		ps := resp.PartialSuccess()
		ps.SetRejectedLogRecords(int64(rejected))
		ps.SetErrorMessage("records rejected by storage")
	}

	var (
		data []byte
		err  error
	)

	if contentType == "application/x-protobuf" {
		data, err = resp.MarshalProto()
	} else {
		data, err = resp.MarshalJSON()
		contentType = "application/json"
	}

	if err != nil {
		http.Error(w, "encode response: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleQuery reads records in a time window, optionally narrowed to those whose body contains a
// substring.
//
//	GET /query?from=<ns>&to=<ns>&term=<substring>&limit=<n>
//
// There is no query language here by design: shrimpd is a replication mechanism, and matching is
// the storage layer's concern. `term` is passed to the engine, which prunes parts by their body
// bloom before scanning.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from := parseInt(q.Get("from"), 0)
	to := parseInt(q.Get("to"), math.MaxInt64)
	limit := int(parseInt(q.Get("limit"), 0))

	start := time.Now()

	entries, err := s.node.Query(r.Context(), from, to, q.Get("term"), limit)
	if err != nil {
		s.lg.Error("query", zap.Error(err))
		http.Error(w, "query: "+err.Error(), http.StatusInternalServerError)

		return
	}

	jw := &jx.Writer{}
	jw.ObjStart()
	jw.FieldStart("data")
	jw.ArrStart()

	for i, e := range entries {
		if i > 0 {
			jw.Comma()
		}

		jw.ObjStart()
		jw.FieldStart("timestamp")
		jw.Int64(e.Timestamp)
		jw.Comma()
		jw.FieldStart("data")
		jw.Str(e.Data)
		jw.ObjEnd()
	}

	jw.ArrEnd()
	jw.Comma()
	jw.FieldStart("stats")
	jw.ObjStart()
	jw.FieldStart("entries_matched")
	jw.Int(len(entries))
	jw.Comma()
	jw.FieldStart("duration_ms")
	jw.Int64(time.Since(start).Milliseconds())
	jw.ObjEnd()
	jw.ObjEnd()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(jw.Buf)
}

// handleState reports what this node holds and how far its replication trails the log.
func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	state, err := s.node.Inspect(r.Context())
	if err != nil {
		http.Error(w, "inspect: "+err.Error(), http.StatusInternalServerError)

		return
	}

	writeJSON(w, state)
}

// handleFlush writes the head to a part and announces it.
func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if err := s.node.Flush(r.Context()); err != nil {
		http.Error(w, "flush: "+err.Error(), http.StatusInternalServerError)

		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

// handleMerge compacts this node's parts and announces the result.
func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	if err := s.node.Merge(r.Context()); err != nil {
		http.Error(w, "merge: "+err.Error(), http.StatusInternalServerError)

		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func parseInt(s string, def int64) int64 {
	if s == "" {
		return def
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}

	return n
}
