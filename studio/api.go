// HTTP API: flow CRUD over the file store and the health check. Responses
// are JSON; errors are {"error": msg} like producer/main.go, validation
// failures are 422 {"errors": [Problem...]}.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"time"

	"github.com/moby/moby/client"
	"github.com/twmb/franz-go/pkg/kgo"
)

type server struct {
	store  Store
	docker *client.Client // nil until main wires it; health then answers 503
	kafka  *kgo.Client    // likewise
	engine *Engine        // nil in unit tests; deploy, stop and state need it
}

type flowSummary struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

func newMux(s *server, ui fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/flows", s.listFlows)
	mux.HandleFunc("POST /api/flows", s.createFlow)
	mux.HandleFunc("GET /api/flows/{id}", s.getFlow)
	mux.HandleFunc("PUT /api/flows/{id}", s.putFlow)
	mux.HandleFunc("DELETE /api/flows/{id}", s.deleteFlow)
	mux.HandleFunc("POST /api/flows/{id}/deploy", s.deploy)
	mux.HandleFunc("POST /api/flows/{id}/stop", s.stopFlow)
	mux.HandleFunc("GET /api/flows/{id}/state", s.flowState)
	mux.HandleFunc("POST /api/flows/{id}/nodes/{node}/send", s.nodeProxy("/send"))
	mux.HandleFunc("GET /api/flows/{id}/nodes/{node}/tail", s.nodeProxy("/tail"))
	// Method-less fallbacks keep every /api/ answer JSON: a known path with
	// the wrong method is 405, anything else under /api/ is 404.
	for _, path := range []string{"/api/health", "/api/flows", "/api/flows/{id}", "/api/flows/{id}/deploy", "/api/flows/{id}/stop", "/api/flows/{id}/state", "/api/flows/{id}/nodes/{node}/send", "/api/flows/{id}/nodes/{node}/tail"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			fail(w, http.StatusMethodNotAllowed, r.Method+" is not allowed on "+r.URL.Path)
		})
	}
	for _, path := range []string{"/api", "/api/"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			fail(w, http.StatusNotFound, "no such endpoint: "+r.URL.Path)
		})
	}
	// The UI is index.html plus Vite's assets/; narrower than "GET /" so it
	// cannot collide with the method-less /api/ routes above.
	files := http.FileServerFS(ui)
	for _, path := range []string{"GET /{$}", "GET /index.html", "GET /assets/"} {
		mux.Handle(path, files)
	}
	// A page on any other site could otherwise POST to 127.0.0.1:8082, and from
	// M2 on the API starts containers through the Docker socket. Requests without
	// browser headers (curl, node containers) pass.
	csrf := http.NewCrossOriginProtection()
	csrf.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fail(w, http.StatusForbidden, "cross-origin write refused")
	}))
	return csrf.Handler(mux)
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	switch {
	case s.docker == nil:
		fail(w, http.StatusServiceUnavailable, "docker: not configured")
		return
	case s.kafka == nil:
		fail(w, http.StatusServiceUnavailable, "kafka: not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	v, err := dockerVersion(ctx, s.docker)
	if err != nil {
		fail(w, http.StatusServiceUnavailable, "docker: "+err.Error())
		return
	}
	if err := s.kafka.Ping(ctx); err != nil {
		fail(w, http.StatusServiceUnavailable, "kafka: "+err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{"docker": v, "kafka": true})
}

func (s *server) listFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := s.store.List()
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	running := map[string]bool{}
	if s.engine != nil {
		if rs, err := s.engine.Running(r.Context()); err == nil {
			running = rs // Docker unreachable: every flow shows stopped and health says why
		}
	}
	out := make([]flowSummary, 0, len(flows))
	for _, f := range flows {
		status := "stopped"
		if running[f.ID] {
			status = "running"
		}
		out = append(out, flowSummary{ID: f.ID, Name: f.Name, Status: status})
	}
	reply(w, http.StatusOK, out)
}

func (s *server) createFlow(w http.ResponseWriter, r *http.Request) {
	f, ok := readFlow(w, r)
	if !ok {
		return
	}
	f.ID = NewID()
	s.saveFlow(w, f, http.StatusCreated)
}

func (s *server) getFlow(w http.ResponseWriter, r *http.Request) {
	f, err := s.store.Get(r.PathValue("id"))
	if storeErr(w, err) {
		return
	}
	reply(w, http.StatusOK, f)
}

func (s *server) putFlow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.Get(id); storeErr(w, err) {
		return
	}
	f, ok := readFlow(w, r)
	if !ok {
		return
	}
	f.ID = id // the URL wins over the body
	s.saveFlow(w, f, http.StatusOK)
}

// deleteFlow stops a running flow first, so no container outlives its file.
func (s *server) deleteFlow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.engine != nil {
		if err := s.engine.Stop(r.Context(), id); err != nil && !errors.Is(err, ErrNotRunning) && !errors.Is(err, ErrNotFound) {
			engineErr(w, err)
			return
		}
	}
	if err := s.store.Delete(id); storeErr(w, err) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) deploy(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.Deploy(r.Context(), r.PathValue("id")); err != nil {
		engineErr(w, err)
		return
	}
	reply(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *server) stopFlow(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.Stop(r.Context(), r.PathValue("id")); err != nil {
		engineErr(w, err)
		return
	}
	reply(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (s *server) flowState(w http.ResponseWriter, r *http.Request) {
	st, err := s.engine.State(r.Context(), r.PathValue("id"))
	if err != nil {
		engineErr(w, err)
		return
	}
	reply(w, http.StatusOK, st)
}

// nodeProxy forwards to path on the node's own container once it runs;
// otherwise it answers 409 naming the node's state.
func (s *server) nodeProxy(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, node := r.PathValue("id"), r.PathValue("node")
		if err := s.engine.NodeRunning(r.Context(), id, node); err != nil {
			engineErr(w, err)
			return
		}
		proxy(w, r, "http://"+containerName(id, node)+nodeAddr+path+"?"+r.URL.RawQuery)
	}
}

// proxy forwards r to url and copies the answer back; an unreachable node is 502.
func proxy(w http.ResponseWriter, r *http.Request, url string) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	var body io.Reader
	if r.Method != http.MethodGet {
		body = http.MaxBytesReader(w, r.Body, 1<<20)
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, url, body)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(w, http.StatusBadGateway, "node: "+err.Error())
		return
	}
	defer res.Body.Close()
	w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
	w.WriteHeader(res.StatusCode)
	io.Copy(w, res.Body)
}

// engineErr answers an engine error: 404 unknown flow, 422 not deployable,
// 409 already running or not running, 502 Docker, Kafka or a node.
func engineErr(w http.ResponseWriter, err error) {
	var ps Problems
	switch {
	case errors.Is(err, ErrNotFound):
		fail(w, http.StatusNotFound, err.Error())
	case errors.As(err, &ps):
		reply(w, http.StatusUnprocessableEntity, map[string]any{"errors": []Problem(ps)})
	case errors.Is(err, ErrRunning), errors.Is(err, ErrNotRunning):
		fail(w, http.StatusConflict, err.Error())
	default:
		fail(w, http.StatusBadGateway, err.Error())
	}
}

// saveFlow validates f at save level and stores it, answering 422, 500 or status with the flow.
func (s *server) saveFlow(w http.ResponseWriter, f Flow, status int) {
	if ps := Validate(&f, Save); ps != nil {
		reply(w, http.StatusUnprocessableEntity, map[string]any{"errors": ps})
		return
	}
	if err := s.store.Put(f); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	reply(w, status, f)
}

// readFlow decodes a flow from a body capped at 1 MiB; on failure it has already answered 400.
func readFlow(w http.ResponseWriter, r *http.Request) (Flow, bool) {
	var f Flow
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&f); err != nil {
		fail(w, http.StatusBadRequest, "body: "+err.Error())
		return f, false
	}
	f.normalize()
	return f, true
}

// storeErr answers 404 or 500 for a store error and reports whether it did.
func storeErr(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	status := http.StatusInternalServerError
	if errors.Is(err, ErrNotFound) {
		status = http.StatusNotFound
	}
	fail(w, status, err.Error())
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
