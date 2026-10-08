package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRoutes(t *testing.T) {
	problem := "owner-verify-1: has 3 partitions, wants 2: partitions never decrease (delete the topic, or set PARTITIONS=3)"
	reg := prometheus.NewRegistry()
	reg.MustRegister(testCollector(topicState{}, nil, true))
	srv := httptest.NewServer(routes(func() string { return problem }, reg))
	defer srv.Close()

	get := func(path string) (int, string) {
		r, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r.StatusCode, string(b)
	}
	if code, body := get("/healthz"); code != 503 || strings.TrimSpace(body) != problem {
		t.Fatalf("unhealthy /healthz: %d %q", code, body)
	}
	if code, body := get("/metrics"); code != 200 || !strings.Contains(body, `topic_owner_info{base="orders",role="retry",topic="orders-1__retry",topic_instance="1"} 1`) {
		t.Fatalf("/metrics: %d %q", code, body)
	}
	if code := healthcheckQuiet(t, srv.URL+"/healthz"); code != 1 {
		t.Fatalf("healthcheck of an unhealthy owner exited %d, want 1", code)
	}

	problem = ""
	if code, body := get("/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("healthy /healthz: %d %q", code, body)
	}
	if code := healthcheckQuiet(t, srv.URL+"/healthz"); code != 0 {
		t.Fatalf("healthcheck of a healthy owner exited %d, want 0", code)
	}
}

// healthcheckQuiet runs healthcheck with its output discarded.
func healthcheckQuiet(t *testing.T, url string) int {
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)
	defer func() { os.Stdout = stdout }()
	return healthcheck(url)
}
