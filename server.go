package shrimpd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Server serves the daemon HTTP API for ingesting, reading, and sharing parts.
type Server struct {
	lsm *LSM
	srv *http.Server
}

// NewServer creates a daemon HTTP server bound to addr.
func NewServer(addr string, lsm *LSM) *Server {
	mux := http.NewServeMux()
	s := &Server{lsm: lsm}

	// POST /ingest          body: {"data":[{"timestamp":1,"data":"foo"}]}
	// GET  /read?from=&to=  timestamp range, inclusive; omit either for open bound
	// GET  /query?from=&to= same as /read, kept as a debug-friendly alias
	// GET  /part/{id}       raw part JSON (served to peer nodes)
	// GET  /parts           global part list from etcd (debugging)
	// POST /compact         forces immediate compaction of L0 parts
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("GET /read", s.handleQuery)
	mux.HandleFunc("GET /query", s.handleQuery)
	mux.HandleFunc("GET /part/{id}", s.handlePart)
	mux.HandleFunc("GET /parts", s.handleParts)
	mux.HandleFunc("POST /compact", s.handleCompact)

	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
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

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var block Block
	if err := json.NewDecoder(r.Body).Decode(&block); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(block.Data) == 0 {
		http.Error(w, "empty block", http.StatusBadRequest)
		return
	}
	if err := s.lsm.Write(r.Context(), block.Data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := parseIntParam(q.Get("from"), 0)
	to := parseIntParam(q.Get("to"), 1<<62)

	entries, err := s.lsm.Query(r.Context(), from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(Block{Data: entries}); err != nil {
		slog.WarnContext(r.Context(), "encode query response", "error", err)
	}
}

func (s *Server) handlePart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Set Content-Type before writing; http.Error will override it on failure
	// (safe because os.Open failure occurs before any bytes are written to w).
	w.Header().Set("Content-Type", "application/json")
	if err := s.lsm.ServeLocalPart(id, w); err != nil {
		http.Error(w, "part not found: "+id, http.StatusNotFound)
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

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if err := s.lsm.compact(r.Context(), true); err != nil {
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
