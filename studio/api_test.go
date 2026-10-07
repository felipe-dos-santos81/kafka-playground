package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := &server{store: Store{dir: t.TempDir()}}
	ui := fstest.MapFS{"index.html": {Data: []byte("<title>studio</title>")}}
	ts := httptest.NewServer(newMux(s, ui))
	t.Cleanup(ts.Close)
	return ts
}

// call sends body (nil, raw []byte, or anything JSON-marshalable) and returns status and body.
func call(t *testing.T, ts *httptest.Server, method, path string, body any) (int, []byte) {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		j, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(j)
	}
	req, err := http.NewRequest(method, ts.URL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, out
}

func TestFlowsAPI(t *testing.T) {
	ts := newTestServer(t)

	code, body := call(t, ts, "POST", "/api/flows", good)
	if code != 201 {
		t.Fatalf("create: %d %s", code, body)
	}
	var created Flow
	json.Unmarshal(body, &created)
	if !flowIDRe.MatchString(created.ID) || created.Name != "demo" {
		t.Fatalf("create returned %s", body)
	}
	id := created.ID

	code, body = call(t, ts, "GET", "/api/flows/"+id, nil)
	if code != 200 || !strings.Contains(string(body), `"name":"demo"`) {
		t.Fatalf("get: %d %s", code, body)
	}

	code, body = call(t, ts, "GET", "/api/flows", nil)
	want := `[{"id":"` + id + `","name":"demo","status":"stopped"}]`
	if code != 200 || string(bytes.TrimSpace(body)) != want {
		t.Fatalf("list: %d %s", code, body)
	}

	bad := clone(good)
	bad.Edges = []Edge{edge("topic-1", "producer-1")}
	code, body = call(t, ts, "PUT", "/api/flows/"+id, bad)
	var rej struct {
		Errors []Problem `json:"errors"`
	}
	json.Unmarshal(body, &rej)
	if code != 422 || len(rej.Errors) != 1 || rej.Errors[0].Edge != "topic-1-producer-1" ||
		!strings.Contains(rej.Errors[0].Message, "not allowed") {
		t.Fatalf("bad edge: %d %s", code, body)
	}

	renamed := clone(good)
	renamed.Name = "renamed"
	if code, body = call(t, ts, "PUT", "/api/flows/"+id, renamed); code != 200 {
		t.Fatalf("put: %d %s", code, body)
	}
	if _, body = call(t, ts, "GET", "/api/flows/"+id, nil); !strings.Contains(string(body), `"name":"renamed"`) {
		t.Fatalf("rename not stored: %s", body)
	}

	for _, m := range []string{"GET", "PUT", "DELETE"} {
		if code, _ = call(t, ts, m, "/api/flows/deadbeef", good); code != 404 {
			t.Errorf("%s unknown id: want 404, got %d", m, code)
		}
	}
	if code, _ = call(t, ts, "POST", "/api/flows", []byte("{not json")); code != 400 {
		t.Errorf("invalid body: want 400, got %d", code)
	}

	if code, body = call(t, ts, "DELETE", "/api/flows/"+id, nil); code != 204 {
		t.Fatalf("delete: %d %s", code, body)
	}
	if code, _ = call(t, ts, "GET", "/api/flows/"+id, nil); code != 404 {
		t.Fatalf("after delete: want 404, got %d", code)
	}

	for _, path := range []string{"/", "/index.html"} {
		if code, body = call(t, ts, "GET", path, nil); code != 200 || !strings.Contains(string(body), "studio") {
			t.Fatalf("ui %s: %d %s", path, code, body)
		}
	}
}

func TestFlowsAPICreateWithoutListsReturnsEmptyArrays(t *testing.T) {
	ts := newTestServer(t)
	code, body := call(t, ts, "POST", "/api/flows", map[string]any{"name": "t"})
	if code != 201 || !strings.Contains(string(body), `"nodes":[]`) || !strings.Contains(string(body), `"edges":[]`) {
		t.Fatalf("create: %d %s", code, body)
	}
}

func TestFlowsAPIBodyLimit(t *testing.T) {
	ts := newTestServer(t)
	big := clone(good)
	big.Name = strings.Repeat("x", 2<<20)
	if code, _ := call(t, ts, "POST", "/api/flows", big); code != 400 {
		t.Fatalf("want 400 for a 2 MiB body, got %d", code)
	}
}

func TestFlowsAPIPutIgnoresBodyID(t *testing.T) {
	ts := newTestServer(t)
	_, body := call(t, ts, "POST", "/api/flows", good)
	var created Flow
	json.Unmarshal(body, &created)
	other := clone(good)
	other.ID = "ffffffff"
	other.Name = "moved"
	if code, body := call(t, ts, "PUT", "/api/flows/"+created.ID, other); code != 200 {
		t.Fatalf("put: %d %s", code, body)
	}
	if code, _ := call(t, ts, "GET", "/api/flows/ffffffff", nil); code != 404 {
		t.Fatalf("body id created a second flow")
	}
	if _, body = call(t, ts, "GET", "/api/flows/"+created.ID, nil); !strings.Contains(string(body), `"name":"moved"`) {
		t.Fatalf("put under url id not stored: %s", body)
	}
}

func TestHealthWithoutDocker(t *testing.T) {
	ts := newTestServer(t)
	if code, body := call(t, ts, "GET", "/api/health", nil); code != 503 || !strings.Contains(string(body), "docker") {
		t.Fatalf("health without docker: %d %s", code, body)
	}
}

func TestUnroutedAPIAnswersJSON(t *testing.T) {
	ts := newTestServer(t)
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/api", 404},
		{"GET", "/api/flows/deadbeef/deploy", 405},
		{"GET", "/api/typo", 404},
		{"POST", "/api/typo", 404},
		{"POST", "/api/health", 405},
		{"POST", "/api/flows/deadbeef/events", 405},
		{"PATCH", "/api/flows/deadbeef", 405},
		{"PUT", "/api/flows/deadbeef/nodes/producer-1/send", 405},
	} {
		code, body := call(t, ts, c.method, c.path, nil)
		var e struct{ Error string }
		if code != c.want || json.Unmarshal(body, &e) != nil || e.Error == "" {
			t.Errorf("%s %s: want %d with a JSON error, got %d %s", c.method, c.path, c.want, code, body)
		}
	}
}

func TestCrossOriginWritesRefused(t *testing.T) {
	ts := newTestServer(t)
	for _, c := range []struct {
		method, site string
		want         int
	}{
		{"POST", "cross-site", 403},
		{"POST", "same-site", 403},
		{"POST", "same-origin", 201},
		{"POST", "", 201}, // curl and node containers send no Sec-Fetch-Site
		{"GET", "cross-site", 200},
	} {
		req, err := http.NewRequest(c.method, ts.URL+"/api/flows", strings.NewReader(`{"name":"x"}`))
		if err != nil {
			t.Fatal(err)
		}
		if c.site != "" {
			req.Header.Set("Sec-Fetch-Site", c.site)
		}
		res, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var e struct{ Error string }
		json.NewDecoder(res.Body).Decode(&e)
		res.Body.Close()
		if res.StatusCode != c.want || (c.want == 403 && e.Error == "") {
			t.Errorf("%s with Sec-Fetch-Site %q: want %d, got %d %+v", c.method, c.site, c.want, res.StatusCode, e)
		}
	}
}

// A DNS-rebinding page is same-origin to the browser but carries its own Host.
func TestBrowserWriteNeedsLocalHost(t *testing.T) {
	ts := newTestServer(t)
	for _, c := range []struct {
		host, site string
		want       int
	}{
		{"evil.example:8082", "same-origin", 403},
		{"localhost:8082", "same-origin", 201},
		{"studio:8082", "", 201}, // node containers and webhooks send no browser headers
	} {
		req, err := http.NewRequest("POST", ts.URL+"/api/flows", strings.NewReader(`{"name":"x"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = c.host
		if c.site != "" {
			req.Header.Set("Sec-Fetch-Site", c.site)
		}
		res, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var e struct{ Error string }
		json.NewDecoder(res.Body).Decode(&e)
		res.Body.Close()
		if res.StatusCode != c.want || (c.want == 403 && e.Error == "") {
			t.Errorf("Host %q, Sec-Fetch-Site %q: want %d, got %d %+v", c.host, c.site, c.want, res.StatusCode, e)
		}
	}
}

func TestEngineErrStatus(t *testing.T) {
	notDeployable := Problems{{Node: "producer-1", Message: "value is required"}}
	for _, c := range []struct {
		err  error
		want int
	}{
		{ErrNotFound, 404},
		{notDeployable, 422},
		{ErrRunning, 409},
		{ErrNotRunning, 409},
		{fmt.Errorf("node consumer-1 is exited: %w", ErrNotRunning), 409},
		{errors.New("Error response from daemon: Conflict"), 502},
	} {
		w := httptest.NewRecorder()
		engineErr(w, c.err)
		var body map[string]any
		if w.Code != c.want || json.Unmarshal(w.Body.Bytes(), &body) != nil {
			t.Errorf("%v: want %d with a JSON body, got %d %s", c.err, c.want, w.Code, w.Body)
		}
	}
	w := httptest.NewRecorder()
	engineErr(w, notDeployable)
	if !strings.Contains(w.Body.String(), `"errors":[{"node":"producer-1","message":"value is required"}]`) {
		t.Fatalf("422 body: %s", w.Body)
	}
}

func TestProxy(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		fmt.Fprintf(w, `{"method":%q,"query":%q,"body":%q}`, r.Method, r.URL.RawQuery, b)
	}))
	w := httptest.NewRecorder()
	proxy(w, httptest.NewRequest(http.MethodPost, "/api/flows/x/nodes/p/send?key=k", strings.NewReader(`{"a":1}`)), node.URL+"/send?key=k")
	if want := `{"method":"POST","query":"key=k","body":"{\"a\":1}"}`; w.Code != http.StatusTeapot || w.Body.String() != want {
		t.Fatalf("want the node's status and body passed through, got %d %s", w.Code, w.Body)
	}
	node.Close()
	w = httptest.NewRecorder()
	proxy(w, httptest.NewRequest(http.MethodGet, "/api/flows/x/nodes/p/tail", nil), node.URL+"/tail")
	if w.Code != http.StatusBadGateway || !strings.Contains(w.Body.String(), `"error":"node: `) {
		t.Fatalf("unreachable node: want 502 JSON, got %d %s", w.Code, w.Body)
	}
}
