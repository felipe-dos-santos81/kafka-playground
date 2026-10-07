# Pipeline Studio M2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy a saved `Producer(manual) → Topic → Consumer(log)` flow as one container per node, stop it, send records and watch them arrive from the editor; a restarted control plane keeps running flows.

**Architecture:** The control plane gains an `Engine` (`engine.go`) that validates a flow, resolves its graph into one flat `NodeSpec` per producer/consumer (`resolve.go`), creates topics with `kadm` (`kafka.go`) and starts labelled containers from its own image on its own compose network (`docker.go`). Nothing about a running flow is stored: every call reads it back from the `studio.flow`/`studio.node` container labels, which is also what makes reconcile work. The same binary's `studio node` role (`node.go`) runs one franz-go client and serves `/send` and `/tail` on `:9000`. The UI gets Deploy/Stop, per-node container states polled from a new `/state` endpoint each second, and a tail drawer.

**Tech Stack:** Go 1.27.1 standard library (`net/http` `CrossOriginProtection`, `ServeMux` patterns), `github.com/moby/moby/client` v0.6.1 with `github.com/moby/moby/api/types/{container,network}`, `github.com/twmb/franz-go` v1.22.1 (`kgo`, `kerr`), `github.com/twmb/franz-go/pkg/kadm` v1.19.0; UI: existing Vite 8 + React 19 + `@xyflow/react` 12.12.0 + Zod 4.6.5, no new npm dependency.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` — sections 3.3 (API), 3.4 (runtime, deploy sequence, inside a node container), 3.5, 3.7, 6 (Makefile: `down`, `nodes`), 7 (M2) and 8 (risks 2–6). M1 shipped on `main` at `e89c564`.

## Global Constraints

- New Go dependencies are exactly `github.com/twmb/franz-go v1.22.1` and `github.com/twmb/franz-go/pkg/kadm v1.19.0` (the pins in `producer/go.mod`); `github.com/moby/moby/api` turns from indirect to direct at the version already in `studio/go.sum`. No new npm dependency. `go 1.27.1` stays.
- Images stay pinned as they are; the studio image stays `kafka-playground/studio:0.1.0` with `pull_policy: build`. `docker-compose.yml` does not change (the `studio` service already has `depends_on: *after-kafka` and `KAFKA_BROKERS`).
- Local-only, no auth. Browser writes from another origin are refused with a JSON 403 (Task 1); requests without browser headers (curl, node containers) are allowed.
- Node containers: name `studio-<flow>-<node>`; labels `studio.flow=<flow id>` and `studio.node=<node id>`; image = the control plane's own image id; command `["node"]`; env `STUDIO_NODE=<NodeSpec JSON>` and `KAFKA_BROKERS` copied from the control plane; attached to the control plane's network; no published ports; no restart policy.
- Stop: SIGTERM with a 5 s grace (`ContainerStop` timeout 5), then `ContainerRemove` with force. Topics and committed offsets are never deleted.
- Deploy: `Validate(f, Deploy)` and the M2 limits answer 422 `{"errors":[{node, edge?, message}]}`; a flow that has containers answers 409; Docker or Kafka failures answer 502 after rolling back every container of the flow. `kerr.TopicAlreadyExists` counts as success.
- Node `:9000` API: `POST /send` (producers; consumers answer 409), `GET /tail?since=N` (records with `seq > N`, oldest first). The tail keeps the last 100 records with values cut at 4096 bytes.
- Every `/api` answer is JSON, errors `{"error": "..."}`. New routes: `POST /api/flows/{id}/deploy` (200 `{"status":"running"}`), `POST /api/flows/{id}/stop` (200 `{"status":"stopped"}`, 409 not running), `GET /api/flows/{id}/state`, `POST /api/flows/{id}/nodes/{node}/send`, `GET /api/flows/{id}/nodes/{node}/tail` (409 unless that node's container is running). `GET /api/flows` reports `running` for every flow with containers; `DELETE /api/flows/{id}` stops a running flow first.
- `GET /api/health` answers `{"docker":"<engine version>","kafka":true}`, or 503 naming `docker:` or `kafka:`.
- After Go changes: `(cd studio && go vet ./... && gofmt -l . && go test ./...)`, `gofmt -l` prints nothing. After UI changes: `(cd studio/ui && npm run build)`. Before finishing a task that touches the Makefile, compose or the Dockerfile: `make down && make verify` ends with `STUDIO OK (...)` and `VERIFY OK`, then `make down`, then `docker ps -aq -f label=studio.flow` prints nothing.
- Makefile: GNU make 3.81, BSD tools, recipe lines start with a real tab, `## ` help comments, `# ── Section ──` rules, shell `$` written `$$`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Keep README.md in sync with behaviour; `studio/flow.go`, `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` change together (AGENTS.md).

## Review Focus

1. A deploy that fails halfway (the second container cannot be created because its name is taken) must answer 502 with Docker's error and leave no container of that flow behind. Pinned in Task 5, Step 6 (`verify-studio` decoy container).
2. Deploying a flow that is already running must answer 409 and must not create a second set of containers. Pinned in Task 5, Step 6 (second deploy, container count still 2).
3. Restarting the control plane while a flow runs must keep the flow running and stoppable. Pinned in Task 5, Step 6 (`docker compose restart studio`, then `state` says running, then Stop removes everything).
4. Deleting a running flow must remove its containers before its file. Pinned in Task 5, Step 6 (redeploy, `DELETE`, zero containers and no file).
5. Tail or send on a stopped flow, or on a node whose container is gone, must answer 409 JSON naming the state, not 502 or a hang. Pinned in Task 5, Step 1 (`TestEngineErrStatus`) and Task 6, Step 3 (`tail` after Stop is 409).

## Decisions this plan makes (Task 9 writes them into the spec)

1. **`GET /api/flows/{id}/state`** is new in M2: `{"status":"running"|"stopped","nodes":{"<id>":{"state":"running"|"exited"|…|"missing"}}}`. The UI polls it once a second; M3's SSE `tick` carries the same fields plus counters.
2. **`/send` with an empty body** renders the producer's own `key` and `value` templates with the next `.Seq` — that is the UI's Send button. A non-empty body is the value as is (curl, the M4 webhook), key from `?key=`.
3. **M2 deploy limits:** the timer source (M3), the http sink, consumer forwarding and `instances` above 1 (M4) answer 422 naming the node and the milestone, instead of starting containers that cannot do the job.
4. **Counters wait for M3:** `/stats` and the franz-go hooks arrive with the M3 poller that reads them; M2 nodes serve `/send` and `/tail` only.
5. **Demo wording:** `docker stop` on a node shows `exited`; `docker rm -f` removes the container, so the node shows `missing`.
6. **Cross-site writes:** the stdlib `http.CrossOriginProtection` (Go 1.25+) guards the whole mux, as the M1 final review asked before M2 wires the Docker socket to deploy.

## File map

| File | Status | Responsibility |
|---|---|---|
| `studio/api.go` | modify | routes, cross-origin guard, deploy/stop/state handlers, node proxy, `engineErr` |
| `studio/kafka.go` | create | `createTopics` and `topicErr` (idempotent topic creation) |
| `studio/resolve.go` | create | `NodeSpec`, `containerName`, `Resolve`, `notYetRunnable` (pure) |
| `studio/node.go` | create | `studio node`: tail ring, producer `/send`, consumer loop, `runNode` |
| `studio/docker.go` | modify | labels, `discoverSelf`, `startNode`, `flowContainers`, `removeContainers` |
| `studio/engine.go` | create | `Engine`: `Deploy`, `Stop`, `State`, `Running`, `Reconcile`, `NodeRunning` |
| `studio/flow.go` | modify | `render` shared by `checkTemplate` and producers |
| `studio/main.go` | modify | `node` role, Kafka client, engine wiring, reconcile on start |
| `studio/*_test.go` | create/modify | `api_test.go`, `kafka_test.go`, `resolve_test.go`, `node_test.go` |
| `studio/ui/src/flow/api.ts` | modify | `deploy`, `stop`, `state`, `send`, `tail`, `describe` |
| `studio/ui/src/nodes/StudioNodes.tsx` | modify | `RuntimeContext`, state badge |
| `studio/ui/src/App.tsx` | modify | Deploy/Stop, state polling, drawer placement |
| `studio/ui/src/TailDrawer.tsx` | create | tail polling, Send |
| `studio/ui/src/index.css` | modify | badge, drawer row |
| `Makefile` | modify | `down` removes node containers, `nodes`, `verify-studio` end to end |
| `README.md`, `AGENTS.md`, spec | modify | Task 9 |

---

### Task 1: Refuse cross-origin browser writes

**Files:**
- Modify: `studio/api.go` (`newMux`)
- Test: `studio/api_test.go`

**Interfaces:**
- Consumes: `newMux(s *server, ui fs.FS)`, `fail` (M1).
- Produces: `newMux` now returns `http.Handler` (callers `main.go` and `newTestServer` pass it straight to `http.ListenAndServe` / `httptest.NewServer`, so they compile unchanged).

`http.CrossOriginProtection` (Go 1.25+) rejects non-GET/HEAD/OPTIONS requests whose `Sec-Fetch-Site` is not `same-origin` or `none`, or whose `Origin` host differs from `Host`; requests with neither header (curl, node containers, the M4 webhook) pass. Checked with a probe on Go 1.27.1: `cross-site` 403, `same-site` 403, `same-origin` 200, `none` 200, no header 200, foreign `Origin` 403, `GET` with `cross-site` 200.

- [ ] **Step 1: Write the failing test**

Append to `studio/api_test.go`:

```go
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
```

- [ ] **Step 2: Run it and see it fail**

Run: `cd studio && go test -run TestCrossOriginWritesRefused ./...`
Expected: FAIL — the `cross-site` and `same-site` POSTs answer 201.

- [ ] **Step 3: Wrap the mux**

In `studio/api.go`, change the signature `func newMux(s *server, ui fs.FS) *http.ServeMux {` to `func newMux(s *server, ui fs.FS) http.Handler {` and replace its final `return mux` with:

```go
	// A page on any other site could otherwise POST to 127.0.0.1:8082, and from
	// M2 on the API starts containers through the Docker socket. Requests without
	// browser headers (curl, node containers) pass.
	csrf := http.NewCrossOriginProtection()
	csrf.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fail(w, http.StatusForbidden, "cross-origin write refused")
	}))
	return csrf.Handler(mux)
```

- [ ] **Step 4: Run the tests**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing.

- [ ] **Step 5: Commit**

```bash
git add studio/api.go studio/api_test.go
git commit -m "studio: refuse cross-origin browser writes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Kafka client, health reports the broker, idempotent topic creation

**Files:**
- Create: `studio/kafka.go`
- Test: `studio/kafka_test.go`
- Modify: `studio/api.go` (`server`, `health`), `studio/main.go`, `studio/go.mod`, `studio/go.sum`

**Interfaces:**
- Consumes: `server`, `reply`, `fail`, `dockerVersion` (M1); `TopicData` (`flow.go`).
- Produces: `server.kafka *kgo.Client` (nil allowed: health answers 503 `kafka: not configured`); `func createTopics(ctx context.Context, adm *kadm.Client, topics []TopicData) error`; `func topicErr(rs kadm.CreateTopicResponses) error`. Task 5's engine calls `createTopics`.

API shapes confirmed with Context7 (`/twmb/franz-go`) and `go doc` against v1.22.1 / kadm v1.19.0: `(*kgo.Client).Ping(ctx) error`; `(*kadm.Client).CreateTopics(ctx, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (CreateTopicResponses, error)`; `type CreateTopicResponses map[string]CreateTopicResponse` with fields `Topic string` and `Err error`; `kerr.TopicAlreadyExists`, `kerr.InvalidReplicationFactor`.

- [ ] **Step 1: Add the dependencies**

```bash
cd studio && go get github.com/twmb/franz-go@v1.22.1 github.com/twmb/franz-go/pkg/kadm@v1.19.0 && grep -E 'franz-go|^go ' go.mod
```

Expected: `go 1.27.1`, `github.com/twmb/franz-go v1.22.1`, `github.com/twmb/franz-go/pkg/kadm v1.19.0`. If `go get` raises the `go` line or picks other versions, stop and report.

- [ ] **Step 2: Write the failing test**

Create `studio/kafka_test.go`:

```go
package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestTopicErr(t *testing.T) {
	exists := kadm.CreateTopicResponse{Topic: "orders", Err: kerr.TopicAlreadyExists}
	created := kadm.CreateTopicResponse{Topic: "audit"}
	denied := kadm.CreateTopicResponse{Topic: "bad", Err: kerr.InvalidReplicationFactor}
	if err := topicErr(kadm.CreateTopicResponses{"orders": exists, "audit": created}); err != nil {
		t.Fatalf("an existing and a new topic: want nil, got %v", err)
	}
	err := topicErr(kadm.CreateTopicResponses{"orders": exists, "bad": denied})
	if !errors.Is(err, kerr.InvalidReplicationFactor) || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("want the bad topic's error, got %v", err)
	}
}
```

Run: `cd studio && go test -run TestTopicErr ./...`
Expected: FAIL, `undefined: topicErr`.

- [ ] **Step 3: Write `studio/kafka.go`**

```go
// Kafka for the control plane: topic creation at deploy and the health check's
// ping. Node containers run their own clients (node.go).
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// createTopics creates every topic. It is idempotent: a topic that already
// exists counts as created, whatever its partition count (spec risk 7).
func createTopics(ctx context.Context, adm *kadm.Client, topics []TopicData) error {
	for _, t := range topics {
		rs, err := adm.CreateTopics(ctx, int32(t.Partitions), int16(t.ReplicationFactor), nil, t.Name)
		if err != nil {
			return err
		}
		if err := topicErr(rs); err != nil {
			return err
		}
	}
	return nil
}

// topicErr is the first per-topic error in rs other than TopicAlreadyExists.
func topicErr(rs kadm.CreateTopicResponses) error {
	for _, r := range rs {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Health reports the broker**

In `studio/api.go`, add `"github.com/twmb/franz-go/pkg/kgo"` to the imports, give `server` the field below, and replace `health`:

```go
type server struct {
	store  Store
	docker *client.Client // nil until main wires it; health then answers 503
	kafka  *kgo.Client    // likewise
}
```

```go
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
```

In `studio/main.go`, add `"github.com/twmb/franz-go/pkg/kgo"` to the imports and replace `s := &server{store: Store{dir: dir}, docker: cli}` with:

```go
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		log.Fatal(err)
	}
	s := &server{store: Store{dir: dir}, docker: cli, kafka: kafka}
```

- [ ] **Step 5: Run the tests, then the real health check**

```bash
cd studio && go mod tidy && go vet ./... && gofmt -l . && go test ./...
cd .. && docker compose up -d --build --wait studio && curl -s localhost:8082/api/health; echo
```

Expected: `ok` (`TestHealthWithoutDocker` still passes: a nil docker client answers 503 `docker: not configured`); health prints `{"docker":"29.8.2","kafka":true}` (any version string). Leave the stack up or `make down`, either is fine.

- [ ] **Step 6: Commit**

```bash
git add studio/kafka.go studio/kafka_test.go studio/api.go studio/main.go studio/go.mod studio/go.sum
git commit -m "studio: Kafka client; health reports the broker; idempotent topic creation

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Resolve a flow into node specs; M2 limits

**Files:**
- Create: `studio/resolve.go`
- Test: `studio/resolve_test.go`

**Interfaces:**
- Consumes: `Flow`, `Node`, `ProducerData`, `TopicData`, `ConsumerData`, `Problem`; test helpers `node`, `edge`, `clone`, `good` from `flow_test.go`.
- Produces:
  - `type NodeSpec struct{ Flow, Node, Type, Topic, Group, AutoOffsetReset, Key, Value string }` (JSON tags `flow`, `node`, `type`, `topic`, `group,omitempty`, `auto_offset_reset,omitempty`, `key,omitempty`, `value,omitempty`) — the whole configuration of one node container, passed as env `STUDIO_NODE`.
  - `func containerName(flow, node string) string` → `"studio-" + flow + "-" + node`.
  - `func Resolve(f Flow) ([]NodeSpec, []TopicData)` — assumes `Validate(&f, Deploy)` passed; specs in edge order, topics in node order.
  - `func notYetRunnable(f Flow) []Problem` — nil when M2 can run the flow.

- [ ] **Step 1: Write the failing tests**

Create `studio/resolve_test.go`:

```go
package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	specs, topics := Resolve(f)
	want := []NodeSpec{
		{Flow: "0a1b2c3d", Node: "producer-1", Type: "producer", Topic: "orders", Value: `{"id": {{.Seq}}}`},
		{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "orders-workers"},
	}
	if !reflect.DeepEqual(specs, want) {
		t.Fatalf("specs:\n got %+v\nwant %+v", specs, want)
	}
	if want := []TopicData{{Name: "orders", Partitions: 3, ReplicationFactor: 1}}; !reflect.DeepEqual(topics, want) {
		t.Fatalf("topics: got %+v, want %+v", topics, want)
	}
	if got := containerName("0a1b2c3d", "consumer-1"); got != "studio-0a1b2c3d-consumer-1" {
		t.Fatalf("containerName: %s", got)
	}
}

func TestNotYetRunnable(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *Flow)
		node   string // the node of the single expected problem; "" means runnable
	}{
		{"manual producer and log consumer", func(*Flow) {}, ""},
		{"timer producer", func(f *Flow) {
			f.Nodes[0].Data = json.RawMessage(`{"source":"timer","interval_ms":100,"value":"{}"}`)
		}, "producer-1"},
		{"http sink", func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"http://x"}}`)
		}, "consumer-1"},
		{"two instances", func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":2,"sink":{"kind":"log"}}`)
		}, "consumer-1"},
		{"forward to a topic", func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"))
		}, "consumer-1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := clone(good)
			c.mutate(&f)
			ps := notYetRunnable(f)
			if c.node == "" {
				if ps != nil {
					t.Fatalf("want runnable, got %v", ps)
				}
				return
			}
			if len(ps) != 1 || ps[0].Node != c.node || !strings.Contains(ps[0].Message, " M") {
				t.Fatalf("want one problem on %s naming its milestone, got %v", c.node, ps)
			}
		})
	}
}
```

Run: `cd studio && go test -run 'TestResolve|TestNotYetRunnable' ./...`
Expected: FAIL, `undefined: Resolve`, `undefined: NodeSpec`.

- [ ] **Step 2: Write `studio/resolve.go`**

```go
// Resolve turns a deployable flow into what runs: the topics to create and one
// NodeSpec per producer or consumer container. A node container never sees the
// graph, only its NodeSpec (env STUDIO_NODE).
package main

import "encoding/json"

// NodeSpec is the whole configuration of one node container.
type NodeSpec struct {
	Flow            string `json:"flow"`
	Node            string `json:"node"`
	Type            string `json:"type"`  // "producer" or "consumer"
	Topic           string `json:"topic"` // produced to, or consumed from
	Group           string `json:"group,omitempty"`
	AutoOffsetReset string `json:"auto_offset_reset,omitempty"`
	Key             string `json:"key,omitempty"`   // producer key template
	Value           string `json:"value,omitempty"` // producer value template
}

// containerName is a node's container name and, on the compose network, its host name.
func containerName(flow, node string) string { return "studio-" + flow + "-" + node }

// Resolve assumes Validate(&f, Deploy) passed: every data field decodes and every
// producer and consumer has exactly one edge to or from a topic.
func Resolve(f Flow) ([]NodeSpec, []TopicData) {
	byID := map[string]Node{}
	topicName := map[string]string{} // topic node id → topic name
	var topics []TopicData
	for _, n := range f.Nodes {
		byID[n.ID] = n
		if n.Type == "topic" {
			var d TopicData
			json.Unmarshal(n.Data, &d)
			topicName[n.ID] = d.Name
			topics = append(topics, d)
		}
	}
	var specs []NodeSpec
	for _, e := range f.Edges {
		src, dst := byID[e.Source], byID[e.Target]
		switch {
		case src.Type == "producer" && dst.Type == "topic":
			var d ProducerData
			json.Unmarshal(src.Data, &d)
			specs = append(specs, NodeSpec{Flow: f.ID, Node: src.ID, Type: "producer", Topic: topicName[dst.ID], Key: d.Key, Value: d.Value})
		case src.Type == "topic" && dst.Type == "consumer":
			var d ConsumerData
			json.Unmarshal(dst.Data, &d)
			specs = append(specs, NodeSpec{Flow: f.ID, Node: dst.ID, Type: "consumer", Topic: topicName[src.ID], Group: d.Group, AutoOffsetReset: d.AutoOffsetReset})
		}
	}
	return specs, topics
}

// notYetRunnable lists what a deployable flow uses that this milestone's runtime
// cannot run yet. Each line goes when its milestone lands.
func notYetRunnable(f Flow) []Problem {
	var ps []Problem
	types := map[string]string{}
	for _, n := range f.Nodes {
		types[n.ID] = n.Type
		switch n.Type {
		case "producer":
			var d ProducerData
			json.Unmarshal(n.Data, &d)
			if d.Source == "timer" {
				ps = append(ps, Problem{Node: n.ID, Message: "the timer source runs from M3; use manual for now"})
			}
		case "consumer":
			var d ConsumerData
			json.Unmarshal(n.Data, &d)
			if d.Sink.Kind == "http" {
				ps = append(ps, Problem{Node: n.ID, Message: "the http sink runs from M4; use log for now"})
			}
			if d.Instances > 1 {
				ps = append(ps, Problem{Node: n.ID, Message: "more than one instance runs from M4"})
			}
		}
	}
	for _, e := range f.Edges {
		if types[e.Source] == "consumer" {
			ps = append(ps, Problem{Node: e.Source, Edge: e.ID, Message: "forwarding from a consumer runs from M4 (through a transform from M5)"})
		}
	}
	return ps
}
```

- [ ] **Step 3: Run the tests**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing.

- [ ] **Step 4: Commit**

```bash
git add studio/resolve.go studio/resolve_test.go
git commit -m "studio: resolve flows into node specs; M2 deploy limits

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The node runner, `studio node`

**Files:**
- Create: `studio/node.go`
- Test: `studio/node_test.go`
- Modify: `studio/flow.go` (`render`, `checkTemplate`), `studio/main.go` (`node` role)

**Interfaces:**
- Consumes: `NodeSpec`, `containerName` (Task 3); `templateData` (`flow.go`); `reply`, `fail` (`api.go`).
- Produces: `func render(src string, d templateData) (string, error)`; `const nodeAddr = ":9000"` (Task 6 builds node URLs with it); `type tailEntry struct{ Seq int64; Time string; Partition int32; Offset int64; Key, Value string }` (JSON `seq`, `time`, `partition`, `offset`, `key`, `value` — Task 8's `TailEntry` mirrors it); `func runNode()`.

franz-go shapes confirmed with Context7 (`/twmb/franz-go`) and `go doc` on v1.22.1: `kgo.ConsumerGroup(group)`, `kgo.ConsumeTopics(topics...)`, `kgo.ConsumeResetOffset(kgo.NewOffset().AtStart() | .AtEnd())`, `kgo.ClientID(id)`, `(*Client).PollFetches(ctx) Fetches`, `Fetches.IsClientClosed()`, `Fetches.EachError(func(string, int32, error))`, `Fetches.EachRecord(func(*Record))`, `(*Client).ProduceSync(ctx, recs...).FirstErr()`. With default options a consumer group autocommits and `Close()` commits and leaves the group, which is what Stop's SIGTERM relies on.

- [ ] **Step 1: Write the failing tests**

Create `studio/node_test.go`:

```go
package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTailKeepsTheLastHundred(t *testing.T) {
	tl := &tail{}
	for range 150 {
		tl.push(&kgo.Record{Value: []byte(`{}`)})
	}
	all := tl.since(0)
	if len(all) != tailSize || all[0].Seq != 51 || all[len(all)-1].Seq != 150 {
		t.Fatalf("want seq 51..150, got %d entries starting at %d", len(all), all[0].Seq)
	}
	if got := tl.since(140); len(got) != 10 || got[0].Seq != 141 {
		t.Fatalf("since 140: want 141..150, got %+v", got)
	}
	if got := tl.since(150); got == nil || len(got) != 0 {
		t.Fatalf("since the last seq: want an empty list, got %#v", got)
	}
	tl.push(&kgo.Record{Value: []byte(strings.Repeat("x", tailValueMax+10))})
	if got := tl.since(150); len(got[0].Value) != tailValueMax {
		t.Fatalf("want the value cut to %d bytes, got %d", tailValueMax, len(got[0].Value))
	}
}

func TestProducerSend(t *testing.T) {
	var sent []*kgo.Record
	brokerDown := false
	p := &producer{
		spec: NodeSpec{Topic: "orders", Key: "k{{.Seq}}", Value: `{"id": {{.Seq}}}`},
		tail: &tail{},
		produce: func(_ context.Context, r *kgo.Record) error {
			if brokerDown {
				return errors.New("broker down")
			}
			r.Partition, r.Offset = 2, int64(len(sent))
			sent = append(sent, r)
			return nil
		},
	}
	post := func(query, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		p.send(w, httptest.NewRequest(http.MethodPost, "/send"+query, strings.NewReader(body)))
		return w
	}

	// An empty body renders the node's templates with the next .Seq.
	for i, want := range []struct{ value, key string }{{`{"id": 1}`, "k1"}, {`{"id": 2}`, "k2"}} {
		if w := post("", ""); w.Code != http.StatusOK {
			t.Fatalf("template send %d: %d %s", i+1, w.Code, w.Body)
		}
		if r := sent[i]; string(r.Value) != want.value || string(r.Key) != want.key || r.Topic != "orders" {
			t.Fatalf("template send %d: got %s %q=%q", i+1, r.Topic, r.Key, r.Value)
		}
	}
	// A body is the value as is; the key comes from ?key=.
	if w := post("?key=a", `{"x": 1}`); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"partition":2`) {
		t.Fatalf("body send: %d %s", w.Code, w.Body)
	}
	if r := sent[2]; string(r.Key) != "a" || string(r.Value) != `{"x": 1}` {
		t.Fatalf("body send produced %q=%q", r.Key, r.Value)
	}
	if w := post("", "{"); w.Code != http.StatusBadRequest || len(sent) != 3 {
		t.Fatalf("invalid JSON: want 400 and nothing produced, got %d with %d records", w.Code, len(sent))
	}
	brokerDown = true
	if w := post("", `{}`); w.Code != http.StatusBadGateway {
		t.Fatalf("broker down: want 502, got %d %s", w.Code, w.Body)
	}
	if got := p.tail.since(0); len(got) != 3 {
		t.Fatalf("want the 3 produced records in the tail, got %d", len(got))
	}
}
```

Run: `cd studio && go test -run 'TestTail|TestProducerSend' ./...`
Expected: FAIL, `undefined: tail`, `undefined: producer`.

- [ ] **Step 2: Share template rendering**

In `studio/flow.go`, replace `checkTemplate` (the whole function and its comment) with:

```go
// render executes a producer key or value template with d.
func render(src string, d templateData) (string, error) {
	t, err := template.New("").Parse(src)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, d); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// checkTemplate renders a producer template once with Seq=1; with mustBeJSON the
// result must be valid JSON.
func checkTemplate(src string, mustBeJSON bool) error {
	out, err := render(src, templateData{Seq: 1, Now: "2026-01-01T00:00:00Z"})
	if err != nil {
		return err
	}
	if mustBeJSON && !json.Valid([]byte(out)) {
		return fmt.Errorf("renders to invalid JSON: %s", out)
	}
	return nil
}
```

- [ ] **Step 3: Write `studio/node.go`**

```go
// `studio node`: one producer or consumer of a deployed flow, in its own
// container. Its whole configuration is env STUDIO_NODE (a NodeSpec) plus
// KAFKA_BROKERS. It serves the control plane on :9000 inside the compose
// network: POST /send (producers) and GET /tail?since=N.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	nodeAddr     = ":9000"
	tailSize     = 100     // records a node keeps for the tail
	tailValueMax = 4 << 10 // bytes of a value the tail keeps
)

// tailEntry is one record as the tail drawer shows it.
type tailEntry struct {
	Seq       int64  `json:"seq"`
	Time      string `json:"time"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// tail keeps the last tailSize records; seq numbers every record ever pushed.
type tail struct {
	mu      sync.Mutex
	seq     int64
	entries []tailEntry
}

func (t *tail) push(r *kgo.Record) {
	v := r.Value
	if len(v) > tailValueMax {
		v = v[:tailValueMax]
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	t.entries = append(t.entries, tailEntry{
		Seq:       t.seq,
		Time:      r.Timestamp.UTC().Format(time.RFC3339Nano),
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       string(r.Key),
		Value:     string(v),
	})
	if len(t.entries) > tailSize {
		t.entries = t.entries[len(t.entries)-tailSize:]
	}
}

// since returns the kept records with Seq > n, oldest first; never nil, so it encodes as [].
func (t *tail) since(n int64) []tailEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := []tailEntry{}
	for _, e := range t.entries {
		if e.Seq > n {
			out = append(out, e)
		}
	}
	return out
}

// producer serves /send for one producer node.
type producer struct {
	spec    NodeSpec
	tail    *tail
	seq     atomic.Int64 // .Seq of the last rendered send
	produce func(context.Context, *kgo.Record) error
}

// send produces one record. A body is the value as is (curl, webhooks), keyed by
// ?key=; an empty body renders the node's own key and value templates with the
// next .Seq (the UI's Send button). It answers {partition, offset}.
func (p *producer) send(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		fail(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	key := r.URL.Query().Get("key")
	if len(bytes.TrimSpace(value)) == 0 {
		d := templateData{Seq: int(p.seq.Add(1)), Now: time.Now().UTC().Format(time.RFC3339), Rand: rand.IntN(1000)}
		v, err := render(p.spec.Value, d)
		if err != nil {
			fail(w, http.StatusInternalServerError, "value template: "+err.Error())
			return
		}
		if key, err = render(p.spec.Key, d); err != nil {
			fail(w, http.StatusInternalServerError, "key template: "+err.Error())
			return
		}
		value = []byte(v)
	}
	if !json.Valid(value) {
		fail(w, http.StatusBadRequest, "value is not valid JSON")
		return
	}
	rec := &kgo.Record{Topic: p.spec.Topic, Value: value}
	if key != "" {
		rec.Key = []byte(key)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := p.produce(ctx, rec); err != nil {
		fail(w, http.StatusBadGateway, err.Error())
		return
	}
	p.tail.push(rec)
	reply(w, http.StatusOK, map[string]any{"partition": rec.Partition, "offset": rec.Offset})
}

// consume polls the group until ctx ends, feeding every record to the tail.
func consume(ctx context.Context, cl *kgo.Client, t *tail) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch %s[%d]: %v", topic, partition, err)
		})
		fs.EachRecord(t.push)
	}
}

// runNode is `studio node`: it runs until SIGTERM (Stop), then closes its client,
// which for a consumer commits its offsets and leaves the group.
func runNode() {
	var spec NodeSpec
	if err := json.Unmarshal([]byte(os.Getenv("STUDIO_NODE")), &spec); err != nil {
		log.Fatal("STUDIO_NODE: ", err)
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers), kgo.ClientID(containerName(spec.Flow, spec.Node))}
	if spec.Type == "consumer" {
		reset := kgo.NewOffset().AtStart()
		if spec.AutoOffsetReset == "latest" {
			reset = kgo.NewOffset().AtEnd()
		}
		opts = append(opts, kgo.ConsumerGroup(spec.Group), kgo.ConsumeTopics(spec.Topic), kgo.ConsumeResetOffset(reset))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	t := &tail{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /tail", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		reply(w, http.StatusOK, t.since(since))
	})
	if spec.Type == "producer" {
		p := &producer{spec: spec, tail: t, produce: func(ctx context.Context, rec *kgo.Record) error {
			return cl.ProduceSync(ctx, rec).FirstErr()
		}}
		mux.HandleFunc("POST /send", p.send)
	} else {
		mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
			fail(w, http.StatusConflict, "only producer nodes send")
		})
		go consume(ctx, cl, t)
	}
	srv := &http.Server{Addr: nodeAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("node %s (%s) on topic %s", spec.Node, spec.Type, spec.Topic)

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	cl.Close()
}
```

- [ ] **Step 4: Dispatch the `node` role**

In `studio/main.go`, replace the package comment (the first three lines, which end "M2 adds the `node` role.") with:

```go
// Pipeline Studio: build Kafka pipelines on a canvas and run them. `studio`
// serves the UI and API on :8082; `studio -healthcheck` is the compose
// healthcheck (the scratch image has no curl); `studio node` runs one node of
// a deployed flow (node.go).
```

and insert right after the `-healthcheck` block in `main`:

```go
	if len(os.Args) > 1 && os.Args[1] == "node" {
		runNode()
		return
	}
```

- [ ] **Step 5: Run the tests**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok` (`TestValidate` still passes through the new `render`), `gofmt -l` prints nothing. The node runs against a real broker only in Task 5's end-to-end check.

- [ ] **Step 6: Commit**

```bash
git add studio/node.go studio/node_test.go studio/flow.go studio/main.go
git commit -m "studio: node runner with send, tail and a consumer group loop

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Engine — deploy, stop, state, reconcile; Makefile end to end

**Files:**
- Create: `studio/engine.go`
- Modify: `studio/docker.go`, `studio/api.go`, `studio/main.go`, `studio/api_test.go`, `Makefile`
- Test: `studio/docker_test.go` (create)

**Interfaces:**
- Consumes: `Store`, `ErrNotFound`, `Validate`, `Deploy` level, `Problem` (M1); `createTopics` (Task 2); `NodeSpec`, `containerName`, `Resolve`, `notYetRunnable` (Task 3); `server.docker`, `server.kafka` (Task 2).
- Produces:
  - `docker.go`: `const labelFlow = "studio.flow"`, `const labelNode = "studio.node"`, `type self struct{ image, network string }`, `func discoverSelf(ctx, *client.Client) (self, error)`, `func startNode(ctx, *client.Client, self, brokers string, NodeSpec) error`, `func flowContainers(ctx, *client.Client, flow string) ([]container.Summary, error)` (`flow == ""` means every flow), `func removeContainers(ctx, *client.Client, []container.Summary) error`.
  - `engine.go`: `var ErrRunning, ErrNotRunning error`; `type Problems []Problem` (an `error`); `type Engine struct{ store Store; docker *client.Client; adm *kadm.Client; brokers string; me self; mu sync.Mutex }`; methods `Deploy(ctx, id) error`, `Stop(ctx, id) error`, `State(ctx, id) (FlowState, error)`, `Running(ctx) (map[string]bool, error)`, `Reconcile(ctx) error`; `type FlowState struct{ Status string; Nodes map[string]NodeState }` (JSON `status`, `nodes`), `type NodeState struct{ State string }` (JSON `state`).
  - `api.go`: `server.engine *Engine`; routes deploy/stop/state; `func engineErr(w, err)`. Task 6 adds `Engine.NodeRunning` and the node proxy.

Moby shapes confirmed with Context7 (`/websites/pkg_go_dev_github_com_moby_moby_client`) and `go doc` on v0.6.1: `ContainerCreate(ctx, client.ContainerCreateOptions{Config *container.Config, HostConfig *container.HostConfig, NetworkingConfig *network.NetworkingConfig, Name string}) (ContainerCreateResult{ID}, error)`; `ContainerStart(ctx, id, client.ContainerStartOptions{})`; `ContainerList(ctx, client.ContainerListOptions{All bool, Filters client.Filters}) (ContainerListResult{Items []container.Summary}, error)` with `client.Filters` a map whose zero value is read-only (build it with `make(client.Filters).Add("label", …)`); `container.Summary{ID, Labels map[string]string, State container.ContainerState}`; `ContainerStop(ctx, id, client.ContainerStopOptions{Timeout *int})`; `ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force bool})`; `ContainerInspect(ctx, id, client.ContainerInspectOptions{}) (ContainerInspectResult{Container container.InspectResponse}, error)` where `InspectResponse.Image` is the image id and `InspectResponse.NetworkSettings.Networks` is keyed by network name; `container.NetworkMode` is a string type; `network.NetworkingConfig{EndpointsConfig map[string]*network.EndpointSettings}`.

- [ ] **Step 1: Write the failing test**

In `studio/api_test.go`, add `"errors"` and `"fmt"` to the imports, add `{"GET", "/api/flows/deadbeef/deploy", 405},` to the case list of `TestUnroutedAPIAnswersJSON`, and append:

```go
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
```

Create `studio/docker_test.go` (spec risk 3: the override path for a control plane outside compose; the inspect path runs in Step 6):

```go
package main

import (
	"context"
	"testing"
)

func TestDiscoverSelfFromEnv(t *testing.T) {
	t.Setenv("STUDIO_IMAGE", "kafka-playground/studio:0.1.0")
	t.Setenv("STUDIO_NETWORK", "kafka-playground_default")
	me, err := discoverSelf(context.Background(), nil) // both set: Docker is never asked
	if err != nil || me != (self{image: "kafka-playground/studio:0.1.0", network: "kafka-playground_default"}) {
		t.Fatalf("got %+v %v", me, err)
	}
}
```

Run: `cd studio && go test -run 'TestEngineErrStatus|TestUnrouted|TestDiscoverSelf' ./...`
Expected: FAIL, `undefined: Problems`, `undefined: engineErr`, `undefined: discoverSelf`.

- [ ] **Step 2: Container operations in `studio/docker.go`**

Replace the whole file with:

```go
// Docker Engine access: the health check's version, and the node containers
// that deployed flows run in. Every node container carries the labels
// studio.flow and studio.node; nothing else about it is stored.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const (
	labelFlow = "studio.flow"
	labelNode = "studio.node"
)

// newDocker builds a client from DOCKER_HOST etc.; the default is the socket
// at /var/run/docker.sock, which compose mounts into the studio container.
func newDocker() (*client.Client, error) {
	return client.New(client.FromEnv, client.WithAPIVersionNegotiation())
}

// dockerVersion returns the engine version; the client negotiates the API
// version on its first request.
func dockerVersion(ctx context.Context, cli *client.Client) (string, error) {
	v, err := cli.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return "", err
	}
	return v.Version, nil
}

// self is what node containers copy from the control plane's own container.
type self struct{ image, network string }

// discoverSelf inspects the control plane's own container (its hostname is the
// container id) for its image and compose network. STUDIO_IMAGE and
// STUDIO_NETWORK override both, for a control plane run outside compose.
func discoverSelf(ctx context.Context, cli *client.Client) (self, error) {
	s := self{image: os.Getenv("STUDIO_IMAGE"), network: os.Getenv("STUDIO_NETWORK")}
	if s.image != "" && s.network != "" {
		return s, nil
	}
	host, err := os.Hostname()
	if err != nil {
		return s, err
	}
	res, err := cli.ContainerInspect(ctx, host, client.ContainerInspectOptions{})
	if err != nil {
		return s, fmt.Errorf("inspect own container %s (outside compose set STUDIO_IMAGE and STUDIO_NETWORK): %w", host, err)
	}
	c := res.Container
	if s.image == "" {
		s.image = c.Image // the image id: a rebuilt studio image does not change running flows
	}
	if s.network == "" && c.NetworkSettings != nil {
		for name := range c.NetworkSettings.Networks {
			s.network = name // ponytail: compose gives the studio one network; the first wins if there are more
			break
		}
	}
	if s.image == "" || s.network == "" {
		return s, fmt.Errorf("own container %s: no image or network", host)
	}
	return s, nil
}

// startNode creates and starts one node container on the control plane's network.
func startNode(ctx context.Context, cli *client.Client, me self, brokers string, spec NodeSpec) error {
	env, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	name := containerName(spec.Flow, spec.Node)
	res, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:  me.image,
			Cmd:    []string{"node"},
			Env:    []string{"STUDIO_NODE=" + string(env), "KAFKA_BROKERS=" + brokers},
			Labels: map[string]string{labelFlow: spec.Flow, labelNode: spec.Node},
		},
		HostConfig: &container.HostConfig{NetworkMode: container.NetworkMode(me.network)},
		NetworkingConfig: &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{me.network: {}},
		},
	})
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if _, err := cli.ContainerStart(ctx, res.ID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	return nil
}

// flowContainers lists the node containers of one flow, or of every flow when
// flow is "", in any state.
func flowContainers(ctx context.Context, cli *client.Client, flow string) ([]container.Summary, error) {
	label := labelFlow
	if flow != "" {
		label += "=" + flow
	}
	res, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: make(client.Filters).Add("label", label)})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// removeContainers stops each running container (SIGTERM, 5 s grace so a
// consumer commits and leaves its group) and removes it. It tries every
// container and returns the first error.
func removeContainers(ctx context.Context, cli *client.Client, cs []container.Summary) error {
	grace := 5
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	for _, c := range cs { // ponytail: one at a time; parallel if flows grow past a handful of nodes
		if c.State == container.StateRunning {
			_, err := cli.ContainerStop(ctx, c.ID, client.ContainerStopOptions{Timeout: &grace})
			keep(err)
		}
		_, err := cli.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{Force: true})
		keep(err)
	}
	return first
}
```

- [ ] **Step 3: Write `studio/engine.go`**

```go
// Engine runs flows as node containers. What runs is never stored: every call
// reads it back from the containers' labels, so a restarted control plane
// carries on where it was (spec 3.4, reconcile).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/twmb/franz-go/pkg/kadm"
)

var (
	ErrRunning    = errors.New("flow is already running")
	ErrNotRunning = errors.New("flow is not running")
)

// Problems is a deploy refused before anything started; the API answers 422.
type Problems []Problem

func (ps Problems) Error() string {
	return fmt.Sprintf("flow is not deployable (%d problems)", len(ps))
}

type Engine struct {
	store   Store
	docker  *client.Client
	adm     *kadm.Client
	brokers string     // KAFKA_BROKERS handed to every node container
	me      self       // found on the first deploy
	mu      sync.Mutex // ponytail: one lock for every deploy and stop; per-flow locks if deploys ever queue
}

// FlowState is what a flow's containers are doing. M3's SSE tick extends it.
type FlowState struct {
	Status string               `json:"status"` // "running" (it has node containers) or "stopped"
	Nodes  map[string]NodeState `json:"nodes"`
}

// NodeState.State is Docker's container state (running, exited, …) or "missing".
type NodeState struct {
	State string `json:"state"`
}

// Deploy validates the saved flow, creates its topics and starts one container
// per producer and consumer; on any failure it removes what it started.
func (e *Engine) Deploy(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, err := e.store.Get(id)
	if err != nil {
		return err
	}
	if ps := Validate(&f, Deploy); ps != nil {
		return Problems(ps)
	}
	if ps := notYetRunnable(f); ps != nil {
		return Problems(ps)
	}
	cs, err := flowContainers(ctx, e.docker, id)
	if err != nil {
		return err
	}
	if len(cs) > 0 {
		return ErrRunning
	}
	if e.me.image == "" {
		me, err := discoverSelf(ctx, e.docker)
		if err != nil {
			return err
		}
		e.me = me
	}
	specs, topics := Resolve(f)
	if err := createTopics(ctx, e.adm, topics); err != nil {
		return err
	}
	for _, spec := range specs {
		if err := startNode(ctx, e.docker, e.me, e.brokers, spec); err != nil {
			// Roll back even if the request was cancelled; reconcile catches what this misses.
			if rbErr := e.stop(context.WithoutCancel(ctx), id); rbErr != nil && !errors.Is(rbErr, ErrNotRunning) {
				return fmt.Errorf("%w (rollback: %v)", err, rbErr)
			}
			return err
		}
	}
	return nil
}

// Stop removes every container of the flow. Topics and committed offsets stay.
func (e *Engine) Stop(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.store.Get(id); err != nil {
		return err
	}
	return e.stop(ctx, id)
}

func (e *Engine) stop(ctx context.Context, id string) error {
	cs, err := flowContainers(ctx, e.docker, id)
	if err != nil {
		return err
	}
	if len(cs) == 0 {
		return ErrNotRunning
	}
	return removeContainers(ctx, e.docker, cs)
}

// State reports the flow's container states. A producer or consumer with no
// container in a running flow is "missing" (removed with docker rm -f, or added
// to the file after the deploy).
func (e *Engine) State(ctx context.Context, id string) (FlowState, error) {
	f, err := e.store.Get(id)
	if err != nil {
		return FlowState{}, err
	}
	cs, err := flowContainers(ctx, e.docker, id)
	if err != nil {
		return FlowState{}, err
	}
	st := FlowState{Status: "stopped", Nodes: map[string]NodeState{}}
	if len(cs) == 0 {
		return st, nil
	}
	st.Status = "running"
	for _, n := range f.Nodes {
		if n.Type == "producer" || n.Type == "consumer" {
			st.Nodes[n.ID] = NodeState{State: "missing"}
		}
	}
	for _, c := range cs {
		st.Nodes[c.Labels[labelNode]] = NodeState{State: string(c.State)}
	}
	return st, nil
}

// Running is the set of flows that have node containers, in any state.
func (e *Engine) Running(ctx context.Context) (map[string]bool, error) {
	cs, err := flowContainers(ctx, e.docker, "")
	if err != nil {
		return nil, err
	}
	running := map[string]bool{}
	for _, c := range cs {
		running[c.Labels[labelFlow]] = true
	}
	return running, nil
}

// Reconcile runs at start: containers of flows whose file is gone (deleted
// while the control plane was down) are removed; the rest keep running.
func (e *Engine) Reconcile(ctx context.Context) error {
	cs, err := flowContainers(ctx, e.docker, "")
	if err != nil {
		return err
	}
	var orphans []container.Summary
	for _, c := range cs {
		if _, err := e.store.Get(c.Labels[labelFlow]); errors.Is(err, ErrNotFound) {
			orphans = append(orphans, c)
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	log.Printf("reconcile: removing %d containers of deleted flows", len(orphans))
	return removeContainers(ctx, e.docker, orphans)
}
```

- [ ] **Step 4: Routes, handlers and wiring**

In `studio/api.go`:

1. Give `server` its engine:

```go
type server struct {
	store  Store
	docker *client.Client // nil until main wires it; health then answers 503
	kafka  *kgo.Client    // likewise
	engine *Engine        // nil in unit tests; deploy, stop and state need it
}
```

2. In `newMux`, after `mux.HandleFunc("DELETE /api/flows/{id}", s.deleteFlow)` add:

```go
	mux.HandleFunc("POST /api/flows/{id}/deploy", s.deploy)
	mux.HandleFunc("POST /api/flows/{id}/stop", s.stopFlow)
	mux.HandleFunc("GET /api/flows/{id}/state", s.flowState)
```

   and extend the method-less fallback list to `[]string{"/api/health", "/api/flows", "/api/flows/{id}", "/api/flows/{id}/deploy", "/api/flows/{id}/stop", "/api/flows/{id}/state"}`.
3. Replace `listFlows` and `deleteFlow`:

```go
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
```

```go
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
```

4. Add the handlers and `engineErr` after `deleteFlow`:

```go
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
```

In `studio/main.go`, add `"context"` and `"github.com/twmb/franz-go/pkg/kadm"` to the imports and replace `s := &server{store: Store{dir: dir}, docker: cli, kafka: kafka}` with:

```go
	store := Store{dir: dir}
	engine := &Engine{store: store, docker: cli, adm: kadm.NewClient(kafka), brokers: brokers}
	if err := engine.Reconcile(context.Background()); err != nil {
		log.Println("reconcile:", err) // health says why; orphans stay until the next start
	}
	s := &server{store: store, docker: cli, kafka: kafka, engine: engine}
```

Run: `cd studio && go mod tidy && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing; `grep 'moby/moby/api' go.mod` shows it without `// indirect`.

- [ ] **Step 5: Makefile — `down` removes node containers, `nodes` lists them**

In `Makefile`:

1. `.PHONY` becomes `.PHONY: help up down ps logs topics groups nodes produce scale verify verify-studio`.
2. Replace the `down` rule with:

```make
down: ## Remove every container, Studio node containers first (topics and messages are lost)
	@ids=$$(docker ps -aq -f label=studio.flow); [ -z "$$ids" ] || docker rm -f $$ids >/dev/null
	$(COMPOSE) down --remove-orphans
```

   (Node containers are not compose services; the compose network cannot be removed while they are attached.)
3. After the `groups` rule in `# ── Inspect ──` add:

```make
nodes: ## List Studio node containers: one per producer and consumer of each deployed flow
	docker ps -a -f label=studio.flow
```

- [ ] **Step 6: Makefile — `verify-studio` deploys**

Replace the whole `verify-studio` rule with the recipe below. `nodes()` counts the flow's containers; the `trap` deletes the flow (which stops it) and removes the decoy on any exit, so a failed run leaves nothing behind. The decoy is an unlabelled container that takes `studio-<id>-consumer-1`: the deploy starts the producer, fails on the name, and must roll the producer back.

```make
verify-studio: up ## Check the studio end to end: health, cross-site refusal, save rules, deploy (rollback, 409, restart), stop, delete
	@health=$$(curl -sS --fail-with-body $(STUDIO_URL)/api/health) || { echo "STUDIO FAILED: health: $$health"; exit 1; }; \
	echo "studio health: $$health"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows -H 'Sec-Fetch-Site: cross-site' --data '{"name":"x"}'); \
	[ "$$code" = 403 ] || { echo "STUDIO FAILED: cross-site write accepted ($$code)"; exit 1; }; \
	flow='{"name":"verify","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"manual","key":"","value":"{}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify","partitions":1,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify","auto_offset_reset":"earliest","sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"}]}'; \
	created=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$flow") || { echo "STUDIO FAILED: create: $$created"; exit 1; }; \
	id=$$(echo "$$created" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	trap "curl -sS -X DELETE $(STUDIO_URL)/api/flows/$$id >/dev/null 2>&1; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1" EXIT; \
	nodes() { docker ps -aq -f label=studio.flow=$$id | wc -l | tr -d ' '; }; \
	[ -f "flows/$$id.json" ] || { echo "STUDIO FAILED: flows/$$id.json not written ($$created)"; exit 1; }; \
	bad=$$(echo "$$flow" | sed 's/"source":"producer-1","target":"topic-1"/"source":"topic-1","target":"producer-1"/'); \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X PUT $(STUDIO_URL)/api/flows/$$id -H 'Content-Type: application/json' --data "$$bad"); \
	[ "$$code" = 422 ] || { echo "STUDIO FAILED: bad edge accepted ($$code)"; exit 1; }; \
	curl -sS --fail-with-body $(STUDIO_URL)/api/flows/$$id | grep -q '"name":"verify"' || { echo "STUDIO FAILED: round trip"; exit 1; }; \
	docker create --name studio-$$id-consumer-1 $$(docker inspect -f '{{.Image}}' $$($(COMPOSE) ps -q studio)) >/dev/null; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows/$$id/deploy); \
	docker rm studio-$$id-consumer-1 >/dev/null; \
	[ "$$code" = 502 ] && [ "$$(nodes)" = 0 ] || { echo "STUDIO FAILED: deploy into a taken name: $$code with $$(nodes) containers left (want 502, 0)"; exit 1; }; \
	deployed=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/deploy) || { echo "STUDIO FAILED: deploy: $$deployed"; exit 1; }; \
	[ "$$(nodes)" = 2 ] || { echo "STUDIO FAILED: $$(nodes) node containers after deploy, want 2"; exit 1; }; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows/$$id/deploy); \
	[ "$$code" = 409 ] && [ "$$(nodes)" = 2 ] || { echo "STUDIO FAILED: second deploy: $$code with $$(nodes) containers (want 409, 2)"; exit 1; }; \
	$(COMPOSE) restart studio >/dev/null 2>&1 && $(COMPOSE) up -d --wait studio >/dev/null 2>&1 || { echo "STUDIO FAILED: restart"; exit 1; }; \
	curl -sS --fail-with-body $(STUDIO_URL)/api/flows/$$id/state | grep -q '"status":"running"' || { echo "STUDIO FAILED: flow not running after a studio restart"; exit 1; }; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/stop >/dev/null || { echo "STUDIO FAILED: stop"; exit 1; }; \
	[ "$$(nodes)" = 0 ] || { echo "STUDIO FAILED: $$(nodes) node containers left after stop"; exit 1; }; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/deploy >/dev/null || { echo "STUDIO FAILED: redeploy"; exit 1; }; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$id || { echo "STUDIO FAILED: delete"; exit 1; }; \
	[ "$$(nodes)" = 0 ] && [ ! -f "flows/$$id.json" ] || { echo "STUDIO FAILED: delete left $$(nodes) containers or flows/$$id.json"; exit 1; }; \
	echo "STUDIO OK ($$id)"
```

Run (all recipe lines start with a real tab — check with `grep -n '^ ' Makefile`, which must print nothing new):

```bash
docker compose config --quiet && make down && make verify; make down; docker ps -aq -f label=studio.flow
```

Expected, in order: `studio health: {"docker":"…","kafka":true}`, `STUDIO OK (<id>)`, the existing Kafka check ending in `VERIFY OK`; after `make down` the last command prints nothing; `git status --short flows` prints nothing.

If the deploy answers 502 with an `inspect own container` error, the studio container's hostname is not its id (check `docker compose ps studio`); fix the cause, not the check. If a node container exits at once, `docker logs studio-<id>-producer-1` names the problem.

- [ ] **Step 7: Commit**

```bash
git add studio/engine.go studio/docker.go studio/docker_test.go studio/api.go studio/api_test.go studio/main.go studio/go.mod studio/go.sum Makefile
git commit -m "studio: deploy, stop and state; reconcile on start; make down removes node containers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Send and tail through the control plane

**Files:**
- Modify: `studio/engine.go` (`NodeRunning`), `studio/api.go` (routes, `nodeProxy`, `proxy`), `studio/api_test.go`, `Makefile` (`verify-studio`)

**Interfaces:**
- Consumes: `Engine.State`, `ErrNotRunning`, `engineErr` (Task 5); `containerName` (Task 3); `nodeAddr` (Task 4).
- Produces: `func (e *Engine) NodeRunning(ctx, id, node string) error`; `func proxy(w, r, url string)`; routes `POST /api/flows/{id}/nodes/{node}/send` and `GET /api/flows/{id}/nodes/{node}/tail` (Tasks 7–8 call them).

Only node ids that appear in the flow's state (its own producers and consumers) are ever proxied, so a crafted `{node}` cannot point the control plane at another host.

- [ ] **Step 1: Write the failing test**

In `studio/api_test.go`, add `{"PUT", "/api/flows/deadbeef/nodes/producer-1/send", 405},` to `TestUnroutedAPIAnswersJSON` and append:

```go
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
```

Run: `cd studio && go test -run 'TestProxy|TestUnrouted' ./...`
Expected: FAIL, `undefined: proxy`.

- [ ] **Step 2: Implement**

Append to `studio/engine.go`:

```go
// NodeRunning is nil when node's container in flow id is running; otherwise
// ErrNotRunning, wrapped with the node's state when the flow itself runs.
func (e *Engine) NodeRunning(ctx context.Context, id, node string) error {
	st, err := e.State(ctx, id)
	if err != nil {
		return err
	}
	if st.Status != "running" {
		return ErrNotRunning
	}
	switch state := st.Nodes[node].State; state {
	case "running":
		return nil
	case "":
		return fmt.Errorf("node %s has no container: %w", node, ErrNotRunning)
	default:
		return fmt.Errorf("node %s is %s: %w", node, state, ErrNotRunning)
	}
}
```

In `studio/api.go`, add `"io"` to the imports; after the `state` route add:

```go
	mux.HandleFunc("POST /api/flows/{id}/nodes/{node}/send", s.nodeProxy("/send"))
	mux.HandleFunc("GET /api/flows/{id}/nodes/{node}/tail", s.nodeProxy("/tail"))
```

append `"/api/flows/{id}/nodes/{node}/send", "/api/flows/{id}/nodes/{node}/tail"` to the method-less fallback list, and add after `flowState`:

```go
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
```

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing.

- [ ] **Step 3: `verify-studio` sends and tails**

In `Makefile`, change the `verify-studio` help comment to `## Check the studio end to end: health, cross-site refusal, save rules, deploy (rollback, 409, restart), send, tail, stop, delete`. Insert these lines right after the line `	[ "$$(nodes)" = 2 ] || { echo "STUDIO FAILED: $$(nodes) node containers after deploy, want 2"; exit 1; }; \`:

```make
	rec="verify-$$(date +%s)"; \
	sent=$$(curl -sS --fail-with-body -X POST "$(STUDIO_URL)/api/flows/$$id/nodes/producer-1/send?key=$$rec" --data "{\"id\":\"$$rec\"}") || { echo "STUDIO FAILED: send: $$sent"; exit 1; }; \
	echo "studio sent $$rec: $$sent"; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/nodes/producer-1/send >/dev/null || { echo "STUDIO FAILED: template send"; exit 1; }; \
	for i in $$(seq 30); do \
		curl -sS "$(STUDIO_URL)/api/flows/$$id/nodes/consumer-1/tail?since=0" | grep -q "\"key\":\"$$rec\"" && break; \
		[ "$$i" = 30 ] && { echo "STUDIO FAILED: the consumer tail never showed $$rec"; exit 1; }; sleep 1; \
	done; \
```

and these right after the line `	[ "$$(nodes)" = 0 ] || { echo "STUDIO FAILED: $$(nodes) node containers left after stop"; exit 1; }; \`:

```make
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$id/nodes/consumer-1/tail?since=0"); \
	[ "$$code" = 409 ] || { echo "STUDIO FAILED: tail of a stopped flow: $$code, want 409"; exit 1; }; \
```

Run:

```bash
docker compose config --quiet && make down && make verify; make down; docker ps -aq -f label=studio.flow
```

Expected: `studio sent verify-<n>: {"offset":…,"partition":0}`, `STUDIO OK (<id>)`, `VERIFY OK`; the last command prints nothing; `git status --short flows` prints nothing.

- [ ] **Step 4: Commit**

```bash
git add studio/engine.go studio/api.go studio/api_test.go Makefile
git commit -m "studio: send and tail through the control plane

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: UI — Deploy, Stop and node states

**Files:**
- Modify: `studio/ui/src/flow/api.ts`, `studio/ui/src/nodes/StudioNodes.tsx`, `studio/ui/src/App.tsx`, `studio/ui/src/index.css`

**Interfaces:**
- Consumes: `POST …/deploy`, `POST …/stop`, `GET …/state` (Task 5).
- Produces: `type FlowState = { status: 'running' | 'stopped'; nodes: Record<string, { state: string }> }` and `api.deploy(id)`, `api.stop(id)`, `api.state(id)` in `api.ts`; `export const RuntimeContext` (node id → container state) in `nodes/StudioNodes.tsx`; in `App.tsx` the values `flowState` and `running` that Task 8 reads.

Runtime state goes through a React context, not into `node.data`: `data` is the saved file and drives the unsaved-changes snapshot, so a state badge must never touch it.

- [ ] **Step 1: API client**

In `studio/ui/src/flow/api.ts`, after the `FlowSummary` type add:

```ts
// GET /api/flows/{id}/state: each producer's and consumer's container state while deployed.
export type FlowState = { status: 'running' | 'stopped'; nodes: Record<string, { state: string }> }
```

and add to the `api` object:

```ts
  deploy: (id: string) => call<{ status: string }>(`/api/flows/${id}/deploy`, { method: 'POST' }),
  stop: (id: string) => call<{ status: string }>(`/api/flows/${id}/stop`, { method: 'POST' }),
  state: (id: string) => call<FlowState>(`/api/flows/${id}/state`),
```

- [ ] **Step 2: State badge on every node**

In `studio/ui/src/nodes/StudioNodes.tsx`, replace the `react` import with `import { createContext, useContext, type ReactNode } from 'react'`, add above `Shell`:

```tsx
// Container state per deployed node id (running, exited, missing…); empty while the flow is stopped.
export const RuntimeContext = createContext<Record<string, string>>({})
```

replace `Shell` with:

```tsx
// The frame every node shares: type as title (plus its container state while
// deployed), a one-line summary, and the input/output handles the allowed-edge
// table gives its type.
function Shell({ id, type, selected, children }: { id: string; type: NodeType; selected?: boolean; children: ReactNode }) {
  const state = useContext(RuntimeContext)[id]
  return (
    <div className={`node ${type}${selected ? ' selected' : ''}`}>
      <div className="node-title">
        {type} {state && <span className={`node-state ${state}`}>{state}</span>}
      </div>
      <div className="node-summary">{children}</div>
      {hasInput(type) && <Handle type="target" position={Position.Left} />}
      {hasOutput(type) && <Handle type="source" position={Position.Right} />}
    </div>
  )
}
```

and in each of the four `nodeTypes` entries take `id` from the props and pass it on, e.g. `producer: ({ id, data, selected }: NodeProps<ProducerNode>) => (<Shell id={id} type="producer" selected={selected}>…`; the same for `topic`, `consumer` and `transform`.

- [ ] **Step 3: App — poll the state, Deploy and Stop**

In `studio/ui/src/App.tsx`:

1. Imports: `import { useCallback, useEffect, useMemo, useState } from 'react'`; `import { api, ApiError, type FlowState, type FlowSummary } from './flow/api'`; add `import { RuntimeContext } from './nodes/StudioNodes'`.
2. After `const [error, setError] = useState('')` add:

```tsx
  const [flowState, setFlowState] = useState<FlowState | null>(null)
  const [deployedSnapshot, setDeployedSnapshot] = useState('') // savedSnapshot at the last Deploy from this page
```

3. After the `useEffect` that calls `refresh()` add:

```tsx
  // The open flow's container states, once a second (M3 replaces this poll with SSE).
  const flowId = current?.id
  useEffect(() => {
    setFlowState(null)
    if (!flowId) return
    let live = true
    const poll = () =>
      api.state(flowId).then(
        (s) => live && setFlowState(s),
        () => live && setFlowState(null),
      )
    poll()
    const timer = setInterval(poll, 1000)
    return () => {
      live = false
      clearInterval(timer)
    }
  }, [flowId])
  const running = flowState?.status === 'running'
  const nodeStates = useMemo(
    () => Object.fromEntries(Object.entries(flowState?.nodes ?? {}).map(([id, n]) => [id, n.state])),
    [flowState],
  )
```

4. In `load`, after `setSavedSnapshot(snapshot(f.name, f.nodes, f.edges))` add `setDeployedSnapshot('')`.
5. After `save` add:

```tsx
  // Deploy runs the saved file, so the button waits for Save.
  const deploy = () =>
    withTopBarError(async () => {
      if (!current) return
      await api.deploy(current.id)
      setDeployedSnapshot(savedSnapshot)
      setFlowState(await api.state(current.id))
      setError('')
      refresh()
    })

  const stop = () =>
    withTopBarError(async () => {
      if (!current) return
      await api.stop(current.id)
      setDeployedSnapshot('')
      setFlowState(await api.state(current.id))
      setError('')
      refresh()
    })
```

6. In the top bar, after the Save button add:

```tsx
            <button onClick={deploy} disabled={dirty || running} title={dirty ? 'Save before deploying' : undefined}>
              Deploy
            </button>
            <button onClick={stop} disabled={!running}>
              Stop
            </button>
            <span className="status">{running ? 'running' : 'stopped'}</span>
            {running && deployedSnapshot !== '' && savedSnapshot !== deployedSnapshot && (
              <span className="hint">saved changes apply on redeploy</span>
            )}
```

7. Wrap the `<Canvas … />` element in `<RuntimeContext.Provider value={nodeStates}>` … `</RuntimeContext.Provider>`.

- [ ] **Step 4: CSS**

Append to `studio/ui/src/index.css`:

```css
.topbar .status { color: #555; }
.node-state { font-weight: 400; font-size: 11px; padding: 0 4px; border-radius: 3px; background: #eee; text-transform: none; }
.node-state.running { background: #c8e6c9; }
.node-state.exited, .node-state.dead, .node-state.missing { background: #ffcdd2; }
```

- [ ] **Step 5: Build, then check against the running stack**

```bash
cd studio/ui && npm run build
cd ../.. && make up >/dev/null && curl -s localhost:8082/ | grep -o '<title>[^<]*</title>'
```

Expected: the build passes with no TypeScript error or warning; the page title is `Pipeline Studio`. The click-through (Deploy, badges turn `running`, `docker stop studio-<id>-consumer-1` turns the badge `exited`, Stop clears them) is part of the human's M2 demo. `make down` afterwards.

- [ ] **Step 6: Commit**

```bash
git add studio/ui/src
git commit -m "studio ui: Deploy, Stop and node container states

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: UI — tail drawer with Send

**Files:**
- Create: `studio/ui/src/TailDrawer.tsx`
- Modify: `studio/ui/src/flow/api.ts`, `studio/ui/src/App.tsx`, `studio/ui/src/index.css`

**Interfaces:**
- Consumes: `POST …/nodes/{node}/send`, `GET …/nodes/{node}/tail?since=` (Task 6); `flowState`, `running` and the selected `node` in `App.tsx` (Task 7).
- Produces: `type TailEntry = { seq: number; time: string; partition: number; offset: number; key: string; value: string }` (mirrors Go's `tailEntry`), `api.send(id, node)`, `api.tail(id, node, since)`, `export function describe(e: unknown): string` in `api.ts`; `TailDrawer` with props `{ flowId: string; node: StudioNode }`.

- [ ] **Step 1: API client**

In `studio/ui/src/flow/api.ts`, add after `FlowState`:

```ts
// One record of a node's tail (Go: tailEntry in node.go).
export type TailEntry = { seq: number; time: string; partition: number; offset: number; key: string; value: string }
```

add to the `api` object:

```ts
  // An empty body makes the producer render its own key and value templates.
  send: (id: string, node: string) =>
    call<{ partition: number; offset: number }>(`/api/flows/${id}/nodes/${node}/send`, { method: 'POST' }),
  tail: (id: string, node: string, since: number) =>
    call<TailEntry[]>(`/api/flows/${id}/nodes/${node}/tail?since=${since}`),
```

and move `describe` here from `App.tsx`, exported:

```ts
export function describe(e: unknown): string {
  return e instanceof ApiError ? `${e.status}: ${e.message}` : String(e)
}
```

In `App.tsx`, delete the local `describe` function and change the import to `import { api, describe, type FlowState, type FlowSummary } from './flow/api'` (`ApiError` is no longer used there).

- [ ] **Step 2: Write `studio/ui/src/TailDrawer.tsx`**

```tsx
import { useEffect, useState } from 'react'
import { api, describe, type TailEntry } from './flow/api'
import type { StudioNode } from './nodes/types'

type Props = { flowId: string; node: StudioNode }

// The selected node's last records, fetched once a second (M3 fetches only when
// the node's tailSeq moves). A producer's drawer also has Send, which renders the
// node's own key and value templates.
export default function TailDrawer({ flowId, node }: Props) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [sent, setSent] = useState('')

  useEffect(() => {
    let live = true
    let since = 0
    let busy = false // a slow answer must not be fetched twice
    const poll = () => {
      if (busy) return
      busy = true
      api
        .tail(flowId, node.id, since)
        .then(
          (got) => {
            if (!live) return
            setError('')
            if (got.length === 0) return
            since = got[got.length - 1].seq
            setEntries((es) => [...es, ...got].slice(-100))
          },
          (e) => live && setError(describe(e)),
        )
        .finally(() => {
          busy = false
        })
    }
    poll()
    const timer = setInterval(poll, 1000)
    return () => {
      live = false
      clearInterval(timer)
    }
  }, [flowId, node.id])

  const send = () =>
    api.send(flowId, node.id).then(
      (r) => setSent(`sent to partition ${r.partition} at offset ${r.offset}`),
      (e) => setSent(describe(e)),
    )

  return (
    <section className="drawer">
      <header>
        <strong>{node.id}</strong> tail
        {node.type === 'producer' && <button onClick={send}>Send</button>}
        <span className="hint">{error || sent}</span>
      </header>
      {entries.length === 0 ? (
        <p className="hint">No records yet.</p>
      ) : (
        <ol>
          {entries.map((e) => (
            <li key={e.seq}>
              <code>
                p{e.partition}@{e.offset}
              </code>
              {e.key !== '' && <code>key={e.key}</code>}
              <code>{e.value}</code>
            </li>
          ))}
        </ol>
      )}
    </section>
  )
}
```

- [ ] **Step 3: Place the drawer**

In `App.tsx`, add `import TailDrawer from './TailDrawer'` and, right after the closing `</aside>` of the inspector, add:

```tsx
      {current && running && node && (node.type === 'producer' || node.type === 'consumer') && (
        <TailDrawer key={`${current.id}/${node.id}`} flowId={current.id} node={node} />
      )}
```

In `studio/ui/src/index.css`, change the `.studio` rule's grid to three rows with the drawer under canvas and inspector:

```css
.studio {
  display: grid; height: 100%;
  grid-template-rows: 44px 1fr auto;
  grid-template-columns: 220px 1fr 320px;
  grid-template-areas: "top top top" "side canvas inspector" "side drawer drawer";
}
```

and append:

```css
.drawer { grid-area: drawer; max-height: 220px; overflow: auto; padding: 8px 12px; border-top: 1px solid #ddd; font-size: 12px; }
.drawer header { display: flex; gap: 8px; align-items: center; }
.drawer ol { list-style: none; margin: 6px 0 0; padding: 0; }
.drawer li { padding: 2px 0; }
.drawer code { margin-right: 6px; }
```

- [ ] **Step 4: Build**

Run: `cd studio/ui && npm run build`
Expected: no TypeScript error or warning. The click-through (select the deployed producer, Send, select the consumer, see the record) is part of the human's M2 demo.

- [ ] **Step 5: Commit**

```bash
git add studio/ui/src
git commit -m "studio ui: tail drawer with Send

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: README, AGENTS and the spec

**Files:**
- Modify: `README.md`, `AGENTS.md`, `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`

- [ ] **Step 1: README**

Replace the whole `## Pipeline Studio` section (up to, not including, `## Connect from the host`) with:

```markdown
## Pipeline Studio

On http://localhost:8082: drag Producer, Topic and Consumer nodes from the palette, wire them, edit the selected node on the right, Save, Deploy.

- Allowed edges: Producer → Topic, Topic → Consumer, Consumer → Topic. Consumer → Transform → Topic is reserved for a later milestone: a Transform node in a flow file shows and edits, but the palette doesn't offer it yet. The editor refuses other wires; the server rejects them on save.
- Opening a flow file that lacks some node fields fills in the defaults and marks the flow unsaved; the file changes only when you Save.
- Each flow is `flows/<id>.json`: React Flow's nodes and edges (each edge with a unique `id`) plus `id`, `name` and `viewport`. Edit, copy or commit them; a file that does not parse is skipped and logged. `flows/0a1b2c3d.json` is an example.
- **Deploy** runs the saved flow: it creates the topics (a topic that exists is used as it is) and starts one container per producer and consumer, named `studio-<flow>-<node>` and labelled `studio.flow`. `make nodes` lists them; `docker logs`, `docker stop` and `docker rm -f` work on them, and the node's badge turns `exited` or `missing`. **Stop** removes them. Topics and committed offsets stay, so a redeployed consumer carries on where its group left off.
- Select a deployed producer or consumer to open its tail: its last 100 records, refreshed every second. A producer's tail has **Send**, which renders its key and value templates. From the command line, `curl -X POST 'localhost:8082/api/flows/<id>/nodes/producer-1/send?key=k1' --data '{"id": 1}'` sends that body as it is.
- This milestone deploys manual producers and log consumers. A deploy refuses the timer source (M3), the http sink, consumer forwarding and more than one instance (M4), and transforms (M5), naming the node.
- Node containers outlive the studio: `docker compose restart studio` keeps flows running. They are not compose services, so stop the stack with `make down`, which removes them first; `docker compose down` alone cannot remove the network while they are attached.
- Consumers commit their offsets on Stop. `docker rm -f` skips that, so their uncommitted records are delivered again on the next deploy (at-least-once).
- The API lives under `/api` and always answers JSON: an unknown path is 404, a wrong method 405, errors are `{"error": "..."}`. Browsers may only write from the studio's own page: a cross-site POST, PUT or DELETE gets 403. curl and the node containers are not affected.
- `GET /api/health` reports the Docker Engine version reachable through the mounted `/var/run/docker.sock` and whether the broker answers. That socket is root-equivalent on the host, another reason the studio stays on `127.0.0.1`.
- Native Linux: the setup assumes Docker Desktop. There the socket is `root:docker 0660`, so `/api/health` stays 503 and `make up` fails. Add `user: "0"` to the `studio` service (the socket already grants root), or `group_add` the host's docker gid and make `./flows` writable by that user.
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the running `studio` container. Go changes need `make up` (rebuilds the image).
```

- [ ] **Step 2: AGENTS.md**

Replace the `studio/` line under Layout with:

```markdown
- `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `docker.go`, `kafka.go`) and node runner (`node.go`, run as `studio node` in one container per producer and consumer), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
```

and add to Rules, after the `.gitignore` line:

```markdown
- Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). Keep that line in `down`; use `make nodes` to see them.
```

- [ ] **Step 3: The spec**

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`:

1. §3.3 table — replace the `send` row's middle cell with: ``body = JSON value (key from `?key=`), or empty to render the node's own key and value templates with the next `.Seq` (the UI's Send button); proxied to the producer container's `/send`; returns `{partition, offset}`. The body form is the webhook URL``. After the `tail` row add:

```markdown
| `GET /api/flows/{id}/state` | `{status, nodes: {<id>: {state}}}`: the flow's container states (Docker's `running`, `exited`, … or `missing`). From M2; the UI polls it once a second until M3's `events` | 404 |
```

2. §3.3 file list — after the `store.go` line add `  resolve.go     flow → topics and one NodeSpec per container; what this milestone cannot run yet` and `  kafka.go       idempotent topic creation`; replace the `store_test.go, api_test.go` line with `  store_test.go, api_test.go, resolve_test.go, kafka_test.go, node_test.go   everything that runs without Docker or a broker`.
3. §3.4, "Inside a node container" — change the start of the Stats bullet from `- **Stats:**` to `- **Stats (M3, with the poller that reads them; M2 nodes serve only /send and /tail):**`.
4. §3.7 — append to the paragraph: ``Browser writes from other origins are already refused (`http.CrossOriginProtection` around the mux, M2), because the API drives the Docker socket.``
5. §7 M2 — add a bullet after the first one: `- Until their milestone, a deploy refuses (422, naming the node) the timer source, the http sink, consumer forwarding and instances above 1.` In the demo, replace ``docker rm -f` the consumer → its node turns `exited` in the UI`` with ``docker stop` the consumer → its node turns `exited`; `docker rm -f` → `missing``.
6. §9 — replace `` `store_test.go` and `api_test.go` drive the file store and the HTTP API through `httptest`. `` with `` `store_test.go` and `api_test.go` drive the file store and the HTTP API through `httptest`; `resolve_test.go`, `kafka_test.go` and `node_test.go` cover the runtime's pure parts. ``

- [ ] **Step 4: Check and commit**

```bash
docker compose config --quiet && (cd studio && go vet ./... && gofmt -l . && go test ./...)
git add README.md AGENTS.md docs/superpowers/specs/2026-10-06-pipeline-studio-design.md
git commit -m "Docs for Studio M2: deploy, nodes, send, tail; spec amendments

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

M2 demo for the human (spec §7): `make up`, open http://localhost:8082, build or open a manual Producer → Topic → Consumer(log) flow, Save, Deploy; `make nodes` lists `studio-<flow>-producer-1` and `…-consumer-1`; `make topics` shows the topic; select the producer, Send; select the consumer, the record shows in the tail; `make groups` shows the group; `docker stop studio-<flow>-consumer-1` turns its badge `exited`; `docker compose restart studio` keeps the flow running; Stop removes the rest.
