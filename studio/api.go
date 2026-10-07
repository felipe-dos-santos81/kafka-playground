// HTTP API: flow CRUD over the file store and the health check. Responses
// are JSON; errors are {"error": msg} like producer/main.go, validation
// failures are 422 {"errors": [Problem...]}.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"time"

	"github.com/moby/moby/client"
)

type server struct {
	store  Store
	docker *client.Client // nil until main wires it; health then answers 503
}

type flowSummary struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

func newMux(s *server, ui fs.FS) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/flows", s.listFlows)
	mux.HandleFunc("POST /api/flows", s.createFlow)
	mux.HandleFunc("GET /api/flows/{id}", s.getFlow)
	mux.HandleFunc("PUT /api/flows/{id}", s.putFlow)
	mux.HandleFunc("DELETE /api/flows/{id}", s.deleteFlow)
	mux.Handle("GET /", http.FileServerFS(ui))
	return mux
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if s.docker == nil {
		fail(w, http.StatusServiceUnavailable, "docker: not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	v, err := dockerVersion(ctx, s.docker)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, "docker: "+err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]string{"docker": v})
}

func (s *server) listFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := s.store.List()
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]flowSummary, 0, len(flows))
	for _, f := range flows {
		out = append(out, flowSummary{ID: f.ID, Name: f.Name, Status: "stopped"}) // M2: status from the engine
	}
	reply(w, http.StatusOK, out)
}

func (s *server) createFlow(w http.ResponseWriter, r *http.Request) {
	f, ok := s.readFlow(w, r)
	if !ok {
		return
	}
	f.ID = NewID()
	if ps := Validate(&f, Save); ps != nil {
		reply(w, http.StatusUnprocessableEntity, map[string]any{"errors": ps})
		return
	}
	if err := s.store.Put(f); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	reply(w, http.StatusCreated, f)
}

func (s *server) getFlow(w http.ResponseWriter, r *http.Request) {
	f, err := s.store.Get(r.PathValue("id"))
	if s.storeErr(w, err) {
		return
	}
	reply(w, http.StatusOK, f)
}

func (s *server) putFlow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.Get(id); s.storeErr(w, err) {
		return
	}
	f, ok := s.readFlow(w, r)
	if !ok {
		return
	}
	f.ID = id // the URL wins over the body
	if ps := Validate(&f, Save); ps != nil {
		reply(w, http.StatusUnprocessableEntity, map[string]any{"errors": ps})
		return
	}
	if err := s.store.Put(f); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	reply(w, http.StatusOK, f)
}

func (s *server) deleteFlow(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.PathValue("id")); s.storeErr(w, err) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// readFlow decodes a flow from a body capped at 1 MiB; on failure it has already answered 400.
func (s *server) readFlow(w http.ResponseWriter, r *http.Request) (Flow, bool) {
	var f Flow
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&f); err != nil {
		fail(w, http.StatusBadRequest, "body: "+err.Error())
		return f, false
	}
	return f, true
}

// storeErr answers 404 or 500 for a store error and reports whether it did.
func (s *server) storeErr(w http.ResponseWriter, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrNotFound):
		fail(w, http.StatusNotFound, "flow not found")
	default:
		fail(w, http.StatusInternalServerError, err.Error())
	}
	return true
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	reply(w, status, map[string]string{"error": msg})
}
