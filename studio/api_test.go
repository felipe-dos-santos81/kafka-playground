package main

import (
	"bytes"
	"encoding/json"
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

	if code, body = call(t, ts, "GET", "/", nil); code != 200 || !strings.Contains(string(body), "studio") {
		t.Fatalf("ui: %d %s", code, body)
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
