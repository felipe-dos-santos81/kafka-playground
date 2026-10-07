# Pipeline Studio M1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship milestone M1 of the Pipeline Studio: a `studio` compose service that serves a React Flow editor where Producer/Topic/Consumer nodes are dragged, wired (invalid edges refused), configured, saved to `flows/<id>.json` and reloaded; plus a Docker-socket smoke check through `/api/health`. No Kafka client yet.

**Architecture:** One Go binary (`studio/`, standard library + the moby Docker client) embeds the built UI and exposes `/api/flows` CRUD over JSON files written atomically; `Validate(flow, level)` is the single authority on the data model and edge rules. The UI (`studio/ui/`, Vite + React + TypeScript + `@xyflow/react` + Zod) stores the flow in React Flow's own JSON shape so save/load is `toObject()` with no mapping layer. A three-stage Dockerfile (`node` → `golang` → `scratch`) builds it; compose mounts the Docker socket and `./flows`.

**Tech Stack:** Go 1.27.1 (`net/http` ServeMux patterns, `embed`), `github.com/moby/moby/client` v0.6.1, Vite 8.3.3 `react-ts` template, `@xyflow/react` 12.12.0, `zod` 4.6.5, images `node:24.21.0-alpine`, `golang:1.27.1-alpine`, `scratch`.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` — sections 3.2, 3.3, 3.5, 4, 5, 6 and M1 in 7. M2–M5 get their own plans after M1 is green.

## Global Constraints

- Backend Go, frontend TypeScript. Standard library first; the only new Go dependency is `github.com/moby/moby/client v0.6.1`; the only runtime npm dependencies are `@xyflow/react@12.12.0` and `zod@4.6.5` plus what the Vite `react-ts` template installs.
- Pin every image exactly: `node:24.21.0-alpine`, `golang:1.27.1-alpine`, `scratch`. Never `latest`. The studio image is local: `image: kafka-playground/studio:0.1.0` with `pull_policy: build`.
- Local-only: the studio port binds to `127.0.0.1:8082`; no auth, no TLS.
- Flow files live in `./flows` on the host, bind-mounted to `/data`; file name is `<id>.json`, id is 8 lowercase hex characters.
- The flow JSON is React Flow's shape: nodes `{id, type, position:{x,y}, data}`, edges `{id, source, target}`, optional `viewport`, plus `id` and `name`. Node ids match `^[a-z0-9][a-z0-9-]{0,30}$`. Allowed edges: producer→topic, topic→consumer, consumer→topic, consumer→transform, transform→topic.
- Inside compose `command`/`entrypoint` strings a shell `$` is `$$`. Makefile: GNU make 3.81, BSD tools, real tabs, `## ` help comments, `# ── Section ──` rules, `$(call shq,$(value var))` for user text.
- After Go changes: `(cd studio && go vet ./... && gofmt -l . && go test ./...)`; `gofmt -l` must print nothing. After UI changes: `(cd studio/ui && npm run build)`. After compose changes: `docker compose config --quiet`. Before finishing any task that touches compose, the Makefile or the Dockerfile: `make down && make verify` must end with `VERIFY OK`, then `make down`.
- Every commit message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Keep README.md in sync with behaviour changes.

## Review Focus

1. A hand-edited flow file that is not valid JSON must not break `GET /api/flows` for every other flow: the list skips it and logs the file name. Pinned in Task 2, Step 1 (`TestStoreListSkipsBrokenFile`).
2. A flow id in the URL such as `../etc/passwd` or `0a1b2c3d.json` must answer 404 and never touch a path outside the data directory. Pinned in Task 2, Step 1 (`TestStoreRejectsBadID`).
3. A request body larger than 1 MiB must answer 400, not hang or 500. Pinned in Task 3, Step 1 (`TestFlowsAPIBodyLimit`).
4. A topic named `.` or `..` is accepted by the name regex but is illegal in Kafka; deploy-level validation must reject it. Pinned in Task 1, Step 1 (cases `topic name dot` and `topic name dotdot`).
5. A `PUT /api/flows/{id}` whose body carries a different `id` must store under the URL id, never create a second file. Pinned in Task 3, Step 1 (`TestFlowsAPIPutIgnoresBodyID`).

---

### Task 1: Go module, flow model and `Validate`

**Files:**
- Create: `studio/go.mod`
- Create: `studio/flow.go`
- Test: `studio/flow_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `type Flow struct{ID, Name string; Nodes []Node; Edges []Edge; Viewport json.RawMessage}`, `type Node struct{ID, Type string; Position Position; Data json.RawMessage}`, `type Edge struct{ID, Source, Target string}`, `type Problem struct{Node, Edge, Message string}`, `type Level int` with constants `Save` and `Deploy`, `func Validate(f *Flow, level Level) []Problem` (nil when valid), per-type data structs `ProducerData`, `TopicData`, `ConsumerData`, `Sink`, `TransformData`, and `type templateData struct{Seq int; Now string; Rand int}` (reused by M3's timer producer). Test helpers `node(id, typ, data string) Node`, `edge(source, target string) Edge`, `clone(Flow) Flow` and the fixture `good` live in `flow_test.go` and are reused by Tasks 2 and 3.

- [ ] **Step 1: Write the failing test**

Create `studio/flow_test.go`:

```go
package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func node(id, typ, data string) Node {
	return Node{ID: id, Type: typ, Data: json.RawMessage(data)}
}

func edge(source, target string) Edge {
	return Edge{ID: source + "-" + target, Source: source, Target: target}
}

// clone deep-copies through JSON so a test can mutate its own flow.
func clone(f Flow) Flow {
	b, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	var c Flow
	if err := json.Unmarshal(b, &c); err != nil {
		panic(err)
	}
	return c
}

// good is a complete producer → topic → consumer flow that deploys cleanly.
var good = Flow{Name: "demo", Nodes: []Node{
	node("producer-1", "producer", `{"source":"manual","value":"{\"id\": {{.Seq}}}"}`),
	node("topic-1", "topic", `{"name":"orders","partitions":3,"replication_factor":1}`),
	node("consumer-1", "consumer", `{"group":"orders-workers","sink":{"kind":"log"}}`),
}, Edges: []Edge{edge("producer-1", "topic-1"), edge("topic-1", "consumer-1")}}

func TestValidate(t *testing.T) {
	topic := func(name string) string {
		return `{"name":"` + name + `","partitions":1,"replication_factor":1}`
	}
	cases := []struct {
		name   string
		level  Level
		mutate func(f *Flow)
		want   string // substring of the single expected message; "" means valid
	}{
		{"valid at deploy", Deploy, func(*Flow) {}, ""},
		{"half built saves", Save, func(f *Flow) { f.Edges = nil; f.Nodes[1].Data = json.RawMessage(`{}`) }, ""},
		{"empty name", Save, func(f *Flow) { f.Name = " " }, "name is required"},
		{"bad node id", Save, func(f *Flow) { f.Nodes = append(f.Nodes, node("Bad Id", "topic", `{}`)) }, "must match"},
		{"duplicate node id", Save, func(f *Flow) { f.Nodes = append(f.Nodes, node("topic-1", "topic", `{}`)) }, "duplicate node id"},
		{"unknown type", Save, func(f *Flow) { f.Nodes = append(f.Nodes, node("sink-1", "sink", `{}`)) }, "unknown node type"},
		{"topic to producer", Save, func(f *Flow) { f.Edges = []Edge{edge("topic-1", "producer-1")} }, "not allowed"},
		{"consumer to consumer", Save, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("consumer-2", "consumer", `{}`))
			f.Edges = []Edge{edge("consumer-1", "consumer-2")}
		}, "not allowed"},
		{"self edge", Save, func(f *Flow) { f.Edges = []Edge{edge("topic-1", "topic-1")} }, "self edge"},
		{"duplicate edge", Save, func(f *Flow) { f.Edges = append(f.Edges, edge("producer-1", "topic-1")) }, "duplicate"},
		{"unknown endpoint", Save, func(f *Flow) { f.Edges[0].Target = "nope" }, "unknown node"},
		{"producer without topic", Deploy, func(f *Flow) { f.Edges = f.Edges[1:] }, "exactly one edge to a topic"},
		{"bad source", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"cron","value":"{}"}`) }, "source must be"},
		{"timer too fast", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"timer","interval_ms":5,"value":"{}"}`) }, "interval_ms"},
		{"value missing", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual"}`) }, "value is required"},
		{"value not json", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual","value":"hello {{.Seq}}"}`) }, "invalid JSON"},
		{"value unknown field", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual","value":"{{.Nope}}"}`) }, "value:"},
		{"key bad template", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual","value":"{}","key":"{{.Seq"}`) }, "key:"},
		{"topic name invalid", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic("bad topic")) }, "invalid"},
		{"topic name dot", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic(".")) }, "invalid"},
		{"topic name dotdot", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic("..")) }, "invalid"},
		{"duplicate topic name", Deploy, func(f *Flow) { f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("orders"))) }, "also used"},
		{"replication factor 2", Deploy, func(f *Flow) {
			f.Nodes[1].Data = json.RawMessage(`{"name":"orders","partitions":1,"replication_factor":2}`)
		}, "replication_factor must be 1"},
		{"partitions 0", Deploy, func(f *Flow) {
			f.Nodes[1].Data = json.RawMessage(`{"name":"orders","partitions":0,"replication_factor":1}`)
		}, "partitions"},
		{"consumer without topic", Deploy, func(f *Flow) { f.Edges = f.Edges[:1] }, "exactly one edge from a topic"},
		{"empty group", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"  "}`) }, "group is required"},
		{"bad offset reset", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","auto_offset_reset":"middle"}`) }, "auto_offset_reset"},
		{"bad sink url", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"ftp://x"}}`)
		}, "sink url"},
		{"bad sink kind", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"mail"}}`) }, "sink kind"},
		{"too many instances", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":11}`) }, "instances"},
		{"consumer forwards twice", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("a")), node("topic-3", "topic", topic("b")))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"), edge("consumer-1", "topic-3"))
		}, "at most one"},
		{"transform chain valid", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`), node("topic-2", "topic", topic("orders-archive")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, ""},
		{"transform without output", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"))
		}, "a transform needs"},
		{"transform empty expr", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":" "}`), node("topic-2", "topic", topic("x")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, "expr is required"},
		{"data missing", Deploy, func(f *Flow) { f.Nodes[0].Data = nil }, "data is required"},
		{"data wrong shape", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(`{"partitions":"three"}`) }, "data:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := clone(good)
			c.mutate(&f)
			ps := Validate(&f, c.level)
			if c.want == "" {
				if len(ps) != 0 {
					t.Fatalf("want valid, got %v", ps)
				}
				return
			}
			if len(ps) != 1 || !strings.Contains(ps[0].Message, c.want) {
				t.Fatalf("want one problem containing %q, got %v", c.want, ps)
			}
		})
	}
}

func TestValidateProblemNamesTheNode(t *testing.T) {
	f := clone(good)
	f.Nodes[2].Data = json.RawMessage(`{"group":""}`)
	ps := Validate(&f, Deploy)
	if len(ps) != 1 || ps[0].Node != "consumer-1" || ps[0].Edge != "" {
		t.Fatalf("want problem on consumer-1, got %+v", ps)
	}
	f = clone(good)
	f.Edges = []Edge{edge("topic-1", "producer-1")}
	ps = Validate(&f, Save)
	if len(ps) != 1 || ps[0].Edge != "topic-1-producer-1" || ps[0].Node != "" {
		t.Fatalf("want problem on the edge, got %+v", ps)
	}
}
```

- [ ] **Step 2: Create the module and run the test to see it fail**

```bash
mkdir -p studio && cd studio && cat > go.mod <<'EOF'
module kafka-playground/studio

go 1.27.1
EOF
go test ./...
```

Expected: compile errors `undefined: Node`, `undefined: Flow`, `undefined: Validate`.

- [ ] **Step 3: Write `studio/flow.go`**

```go
// Flow model and validation. The JSON shape is React Flow's own (nodes with
// id/type/position/data, edges with id/source/target, viewport) plus id and
// name, so the editor's toObject() round-trips without a mapping layer.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"text/template"
)

type Flow struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Nodes    []Node          `json:"nodes"`
	Edges    []Edge          `json:"edges"`
	Viewport json.RawMessage `json:"viewport,omitempty"`
}

type Node struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Position Position        `json:"position"`
	Data     json.RawMessage `json:"data"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Edge struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
}

// Contents of Node.Data, one struct per node type.
type ProducerData struct {
	Source     string `json:"source"`      // "manual" | "timer"
	IntervalMS int    `json:"interval_ms"` // timer only
	Key        string `json:"key"`         // text/template, empty = keyless
	Value      string `json:"value"`       // text/template that renders JSON
}

type TopicData struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
}

type ConsumerData struct {
	Group           string `json:"group"`
	AutoOffsetReset string `json:"auto_offset_reset"` // "earliest" (default) | "latest"
	Instances       int    `json:"instances"`         // 0 = 1
	Sink            Sink   `json:"sink"`
}

type Sink struct {
	Kind string `json:"kind"` // "" or "log" | "http"
	URL  string `json:"url"`
}

type TransformData struct {
	Expr string `json:"expr"`
}

// templateData is what producer key/value templates are rendered with.
type templateData struct {
	Seq  int
	Now  string
	Rand int
}

// Problem is one validation failure; Node or Edge names the culprit when there is one.
type Problem struct {
	Node    string `json:"node,omitempty"`
	Edge    string `json:"edge,omitempty"`
	Message string `json:"message"`
}

// Level selects how strict Validate is: Save accepts a half-built flow
// (shape and edge pairs only), Deploy accepts only a runnable one.
type Level int

const (
	Save Level = iota
	Deploy
)

// allowedEdges is the authority on which node types may be wired; the UI mirrors it.
var allowedEdges = map[string]map[string]bool{
	"producer":  {"topic": true},
	"topic":     {"consumer": true},
	"consumer":  {"topic": true, "transform": true},
	"transform": {"topic": true},
}

var (
	nodeIDRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,30}$`) // node ids become part of container names
	topicNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
)

// Validate returns every problem found; nil means valid.
func Validate(f *Flow, level Level) []Problem {
	var ps []Problem
	add := func(node, edge, format string, args ...any) {
		ps = append(ps, Problem{Node: node, Edge: edge, Message: fmt.Sprintf(format, args...)})
	}
	if strings.TrimSpace(f.Name) == "" {
		add("", "", "name is required")
	}
	types := map[string]string{} // node id → type
	for _, n := range f.Nodes {
		switch {
		case !nodeIDRe.MatchString(n.ID):
			add(n.ID, "", "node id %q must match %s", n.ID, nodeIDRe)
		case types[n.ID] != "":
			add(n.ID, "", "duplicate node id %q", n.ID)
		case allowedEdges[n.Type] == nil:
			add(n.ID, "", "unknown node type %q", n.Type)
		default:
			types[n.ID] = n.Type
		}
	}
	in := map[string]int{}       // node id → number of incoming edges
	out := map[string][]string{} // node id → target node ids
	seen := map[[2]string]bool{}
	for _, e := range f.Edges {
		st, tt := types[e.Source], types[e.Target]
		switch {
		case st == "" || tt == "":
			add("", e.ID, "edge %s → %s: unknown node", e.Source, e.Target)
			continue
		case e.Source == e.Target:
			add("", e.ID, "edge %s → %s: self edge", e.Source, e.Target)
			continue
		case !allowedEdges[st][tt]:
			add("", e.ID, "edge %s → %s: %s → %s is not allowed", e.Source, e.Target, st, tt)
			continue
		case seen[[2]string{e.Source, e.Target}]:
			add("", e.ID, "edge %s → %s: duplicate", e.Source, e.Target)
			continue
		}
		seen[[2]string{e.Source, e.Target}] = true
		in[e.Target]++
		out[e.Source] = append(out[e.Source], e.Target)
	}
	if level == Save {
		return ps
	}
	topicNames := map[string]string{} // topic name → node id
	for _, n := range f.Nodes {
		if types[n.ID] == "" {
			continue // already reported
		}
		switch n.Type {
		case "producer":
			var d ProducerData
			if !decodeData(n, &d, add) {
				continue
			}
			if d.Source != "manual" && d.Source != "timer" {
				add(n.ID, "", `source must be "manual" or "timer"`)
			}
			if d.Source == "timer" && d.IntervalMS < 10 {
				add(n.ID, "", "interval_ms must be at least 10")
			}
			if d.Value == "" {
				add(n.ID, "", "value is required")
			} else if err := checkTemplate(d.Value, true); err != nil {
				add(n.ID, "", "value: %v", err)
			}
			if err := checkTemplate(d.Key, false); err != nil {
				add(n.ID, "", "key: %v", err)
			}
			if len(out[n.ID]) != 1 {
				add(n.ID, "", "a producer needs exactly one edge to a topic")
			}
		case "topic":
			var d TopicData
			if !decodeData(n, &d, add) {
				continue
			}
			switch other := topicNames[d.Name]; {
			case !topicNameRe.MatchString(d.Name) || d.Name == "." || d.Name == "..":
				add(n.ID, "", "topic name %q is invalid", d.Name)
			case other != "":
				add(n.ID, "", "topic %q is also used by node %s", d.Name, other)
			default:
				topicNames[d.Name] = n.ID
			}
			if d.Partitions < 1 {
				add(n.ID, "", "partitions must be at least 1")
			}
			if d.ReplicationFactor != 1 {
				add(n.ID, "", "single-broker playground: replication_factor must be 1")
			}
		case "consumer":
			var d ConsumerData
			if !decodeData(n, &d, add) {
				continue
			}
			if strings.TrimSpace(d.Group) == "" || len(d.Group) > 255 {
				add(n.ID, "", "group is required (at most 255 characters)")
			}
			if d.AutoOffsetReset != "" && d.AutoOffsetReset != "earliest" && d.AutoOffsetReset != "latest" {
				add(n.ID, "", `auto_offset_reset must be "earliest" or "latest"`)
			}
			if d.Instances < 0 || d.Instances > 10 {
				add(n.ID, "", "instances must be between 1 and 10")
			}
			switch d.Sink.Kind {
			case "", "log":
			case "http":
				if u, err := url.Parse(d.Sink.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
					add(n.ID, "", "sink url must be an http(s) URL")
				}
			default:
				add(n.ID, "", `sink kind must be "log" or "http"`)
			}
			if in[n.ID] != 1 {
				add(n.ID, "", "a consumer needs exactly one edge from a topic")
			}
			if len(out[n.ID]) > 1 {
				add(n.ID, "", "a consumer may forward to at most one topic or transform")
			}
		case "transform":
			var d TransformData
			if !decodeData(n, &d, add) {
				continue
			}
			if strings.TrimSpace(d.Expr) == "" {
				add(n.ID, "", "expr is required")
			}
			if in[n.ID] != 1 || len(out[n.ID]) != 1 {
				add(n.ID, "", "a transform needs one edge from a consumer and one edge to a topic")
			}
		}
	}
	return ps
}

func decodeData(n Node, into any, add func(node, edge, format string, args ...any)) bool {
	if len(n.Data) == 0 || string(n.Data) == "null" {
		add(n.ID, "", "data is required")
		return false
	}
	if err := json.Unmarshal(n.Data, into); err != nil {
		add(n.ID, "", "data: %v", err)
		return false
	}
	return true
}

// checkTemplate parses a producer template and renders it once with Seq=1;
// with mustBeJSON the rendering must be valid JSON.
func checkTemplate(src string, mustBeJSON bool) error {
	t, err := template.New("").Parse(src)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, templateData{Seq: 1, Now: "2026-01-01T00:00:00Z"}); err != nil {
		return err
	}
	if mustBeJSON && !json.Valid(buf.Bytes()) {
		return fmt.Errorf("renders to invalid JSON: %s", buf.String())
	}
	return nil
}
```

- [ ] **Step 4: Run the tests and `vet`**

```bash
cd studio && go vet ./... && gofmt -l . && go test ./... -run 'TestValidate' -v
```

Expected: every subtest `PASS`, `gofmt -l` prints nothing. If `value unknown field` fails, check that `templateData` has no `Nope` field and that `Execute` returns the error (it does for struct field misses).

- [ ] **Step 5: Commit**

```bash
git add studio/go.mod studio/flow.go studio/flow_test.go
git commit -m "studio: flow model and Validate with edge rules

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: JSON file store

**Files:**
- Create: `studio/store.go`
- Test: `studio/store_test.go`

**Interfaces:**
- Consumes: `Flow`, test helpers `good`, `clone` from Task 1.
- Produces: `type Store struct{ dir string }`, `var ErrNotFound`, `func NewID() string` (8 lowercase hex), `func (s Store) List() ([]Flow, error)` (sorted by name, skips unreadable files), `func (s Store) Get(id string) (Flow, error)`, `func (s Store) Put(f Flow) error` (atomic), `func (s Store) Delete(id string) error`.

- [ ] **Step 1: Write the failing tests**

Create `studio/store_test.go`:

```go
package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestNewID(t *testing.T) {
	a, b := NewID(), NewID()
	if !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(a) || a == b {
		t.Fatalf("want two distinct 8-hex ids, got %q %q", a, b)
	}
}

func TestStoreRoundTrip(t *testing.T) {
	s := Store{dir: t.TempDir()}
	f := clone(good)
	f.ID = NewID()
	if err := s.Put(f); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(f.ID)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(f)
	have, _ := json.Marshal(got)
	if string(want) != string(have) {
		t.Fatalf("round trip changed the flow:\n%s\n%s", want, have)
	}
	list, err := s.List()
	if err != nil || len(list) != 1 || list[0].ID != f.ID {
		t.Fatalf("want one listed flow, got %v %v", list, err)
	}
	entries, _ := os.ReadDir(s.dir)
	if len(entries) != 1 || entries[0].Name() != f.ID+".json" {
		t.Fatalf("want only %s.json on disk, got %v", f.ID, entries)
	}
	if err := s.Delete(f.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(f.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	if err := s.Delete(f.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound on second delete, got %v", err)
	}
}

func TestStoreListSkipsBrokenFile(t *testing.T) {
	s := Store{dir: t.TempDir()}
	f := clone(good)
	f.ID = NewID()
	if err := s.Put(f); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(s.dir, "deadbeef.json"), []byte("{not json"), 0o644)
	os.WriteFile(filepath.Join(s.dir, "notes.txt"), []byte("ignored"), 0o644)
	list, err := s.List()
	if err != nil || len(list) != 1 || list[0].ID != f.ID {
		t.Fatalf("want the one good flow, got %v %v", list, err)
	}
}

func TestStoreRejectsBadID(t *testing.T) {
	s := Store{dir: t.TempDir()}
	for _, id := range []string{"../etc/passwd", "0a1b2c3d.json", "ABCDEF12", "", "0a1b2c3d4"} {
		if _, err := s.Get(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%q): want ErrNotFound, got %v", id, err)
		}
		if err := s.Delete(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Delete(%q): want ErrNotFound, got %v", id, err)
		}
	}
}
```

- [ ] **Step 2: Run the tests to see them fail**

```bash
cd studio && go test ./... -run 'TestNewID|TestStore' 2>&1 | head
```

Expected: `undefined: Store`, `undefined: NewID`, `undefined: ErrNotFound`.

- [ ] **Step 3: Write `studio/store.go`**

```go
// Store keeps one JSON file per flow under dir: <id>.json, written through a
// temp file and os.Rename so a reader never sees a half-written flow.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Store struct{ dir string }

var ErrNotFound = errors.New("flow not found")

var flowIDRe = regexp.MustCompile(`^[0-9a-f]{8}$`)

// NewID returns 8 random lowercase hex characters.
func NewID() string {
	b := make([]byte, 4)
	rand.Read(b) // cannot fail on supported platforms
	return hex.EncodeToString(b)
}

func (s Store) path(id string) string { return filepath.Join(s.dir, id+".json") }

// List returns every readable flow sorted by name; a file that does not parse
// is logged and skipped so one hand-edited mistake does not hide the rest.
func (s Store) List() ([]Flow, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	flows := []Flow{}
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), ".json")
		if e.IsDir() || !ok {
			continue
		}
		f, err := s.Get(id)
		if err != nil {
			log.Printf("skipping %s: %v", e.Name(), err)
			continue
		}
		flows = append(flows, f)
	}
	sort.Slice(flows, func(i, j int) bool { return flows[i].Name < flows[j].Name })
	return flows, nil
}

func (s Store) Get(id string) (Flow, error) {
	var f Flow
	if !flowIDRe.MatchString(id) {
		return f, ErrNotFound
	}
	b, err := os.ReadFile(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return f, ErrNotFound
	}
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return f, err
	}
	f.ID = id // the file name wins over whatever the file says
	return f, nil
}

func (s Store) Put(f Flow) error {
	if !flowIDRe.MatchString(f.ID) {
		return ErrNotFound
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, f.ID+".*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), s.path(f.ID))
}

func (s Store) Delete(id string) error {
	if !flowIDRe.MatchString(id) {
		return ErrNotFound
	}
	err := os.Remove(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	return err
}
```

- [ ] **Step 4: Run the tests and `vet`**

```bash
cd studio && go vet ./... && gofmt -l . && go test ./... -v 2>&1 | grep -E '^(---|ok|FAIL)'
```

Expected: all `--- PASS`, final `ok`. `TestStoreRoundTrip` compares `json.Marshal` of both flows because `MarshalIndent` re-indents `RawMessage` contents on disk; `Marshal` compacts both sides again.

- [ ] **Step 5: Commit**

```bash
git add studio/store.go studio/store_test.go
git commit -m "studio: JSON file store with atomic writes

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: HTTP API, embedded UI and the `serve` entry point

**Files:**
- Create: `studio/api.go`
- Create: `studio/main.go`
- Create: `studio/ui/dist/.gitkeep` (empty; keeps `go:embed all:ui/dist` non-empty before the UI exists)
- Test: `studio/api_test.go`

**Interfaces:**
- Consumes: `Flow`, `Problem`, `Validate`, `Save` (Task 1); `Store`, `ErrNotFound`, `NewID`, `flowIDRe` (Task 2).
- Produces: `type server struct{ store Store }` (Task 4 adds a `docker` field), `func newMux(s *server, ui fs.FS) *http.ServeMux`, handlers `health`, `listFlows`, `createFlow`, `getFlow`, `putFlow`, `deleteFlow`, helpers `reply(w, status, v)` and `fail(w, status, msg)`, and `type flowSummary struct{ID, Name, Status string}`. Routes exactly as spec section 3.3: `GET /api/health`, `GET|POST /api/flows`, `GET|PUT|DELETE /api/flows/{id}`, `GET /` static.

- [ ] **Step 1: Write the failing tests**

Create `studio/api_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests to see them fail**

```bash
cd studio && go test ./... -run 'API|Health' 2>&1 | head -5
```

Expected: `undefined: server`, `undefined: newMux`.

- [ ] **Step 3: Write `studio/api.go`**

```go
// HTTP API: flow CRUD over the file store and the health check. Responses
// are JSON; errors are {"error": msg} like producer/main.go, validation
// failures are 422 {"errors": [Problem...]}.
package main

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
)

type server struct {
	store Store
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
	fail(w, http.StatusServiceUnavailable, "docker: not configured") // Task 4 wires the Docker client
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
```

- [ ] **Step 4: Write `studio/main.go` and the embed placeholder**

```bash
mkdir -p studio/ui/dist && touch studio/ui/dist/.gitkeep
```

`studio/main.go`:

```go
// Pipeline Studio: build Kafka pipelines on a canvas and run them. `studio`
// serves the UI and API on :8082; `studio -healthcheck` is the compose
// healthcheck (the scratch image has no curl). M2 adds the `node` role.
package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"os"
)

// all: keeps the pattern valid while dist holds only .gitkeep.
//
//go:embed all:ui/dist
var uiFiles embed.FS

const addr = ":8082"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		r, err := http.Get("http://localhost" + addr + "/api/health")
		if err != nil || r.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	dir := os.Getenv("STUDIO_DATA")
	if dir == "" {
		dir = "/data"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	ui, err := fs.Sub(uiFiles, "ui/dist")
	if err != nil {
		log.Fatal(err)
	}
	s := &server{store: Store{dir: dir}}
	log.Println("studio on", addr, "flows in", dir)
	log.Fatal(http.ListenAndServe(addr, newMux(s, ui)))
}
```

- [ ] **Step 5: Run the tests, `vet` and a manual round trip**

```bash
cd studio && go vet ./... && gofmt -l . && go test ./... 2>&1 | tail -3
STUDIO_DATA=$(mktemp -d) go run . &
sleep 1
curl -s -X POST localhost:8082/api/flows -H 'Content-Type: application/json' \
  --data '{"name":"t","nodes":[],"edges":[]}'; echo
curl -s localhost:8082/api/flows; echo
curl -s -o /dev/null -w '%{http_code}\n' localhost:8082/api/health
kill %1
```

Expected: tests `ok`; the POST prints `{"id":"<8 hex>","name":"t","nodes":[],"edges":[]}`; the list prints `[{"id":"…","name":"t","status":"stopped"}]`; health prints `503`.

- [ ] **Step 6: Commit**

```bash
git add studio/api.go studio/api_test.go studio/main.go studio/ui/dist/.gitkeep
git commit -m "studio: flows API, embedded UI and serve entry point

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Docker smoke through `/api/health`

**Files:**
- Create: `studio/docker.go`
- Modify: `studio/api.go` (`server` struct, `health` handler)
- Modify: `studio/main.go` (construct the client)
- Modify: `studio/go.mod`, create `studio/go.sum` (via `go get`)
- Test: `studio/api_test.go` (`TestHealthWithoutDocker` keeps passing: nil client → 503)

**Interfaces:**
- Consumes: `server`, `reply`, `fail` (Task 3).
- Produces: `server.docker *client.Client` (nil allowed), `func newDocker() (*client.Client, error)`, `func dockerVersion(ctx context.Context, cli *client.Client) (string, error)`. M2's `docker.go` grows container calls on the same client.

API shapes verified with Context7 (`/websites/pkg_go_dev_github_com_moby_moby_client`): `client.New(ops ...Opt) (*Client, error)`, `client.FromEnv`, `(*Client).Ping(ctx, PingOptions{NegotiateAPIVersion: true}) (PingResult, error)`, `(*Client).ServerVersion(ctx, ServerVersionOptions{}) (ServerVersionResult, error)` with `ServerVersionResult.Version`.

- [ ] **Step 1: Add the dependency**

```bash
cd studio && go get github.com/moby/moby/client@v0.6.1 && go mod tidy && grep moby go.mod
```

Expected: `github.com/moby/moby/client v0.6.1` under `require`. If `go get` reports a Go version requirement above 1.27.1, stop and report; do not bump the toolchain.

- [ ] **Step 2: Write `studio/docker.go`**

```go
// Docker Engine access. M1 only proves the socket works; M2 adds the
// container calls that run nodes.
package main

import (
	"context"

	"github.com/moby/moby/client"
)

// newDocker builds a client from DOCKER_HOST etc.; the default is the socket
// at /var/run/docker.sock, which compose mounts into the studio container.
func newDocker() (*client.Client, error) {
	return client.New(client.FromEnv)
}

// dockerVersion negotiates the API version and returns the engine version.
func dockerVersion(ctx context.Context, cli *client.Client) (string, error) {
	if _, err := cli.Ping(ctx, client.PingOptions{NegotiateAPIVersion: true}); err != nil {
		return "", err
	}
	v, err := cli.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return "", err
	}
	return v.Version, nil
}
```

- [ ] **Step 3: Wire it into `api.go` and `main.go`**

In `studio/api.go`, change the struct and the handler (add `"context"`, `"time"` and `"github.com/moby/moby/client"` to the imports):

```go
type server struct {
	store  Store
	docker *client.Client // nil until main wires it; health then answers 503
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
```

In `studio/main.go`, replace the `s := &server{...}` line:

```go
	cli, err := newDocker()
	if err != nil {
		log.Fatal(err)
	}
	s := &server{store: Store{dir: dir}, docker: cli}
```

- [ ] **Step 4: Run tests, `vet` and the real smoke against the host's Docker**

```bash
cd studio && go vet ./... && gofmt -l . && go test ./... 2>&1 | tail -3
STUDIO_DATA=$(mktemp -d) go run . &
sleep 1
curl -s localhost:8082/api/health; echo
kill %1
```

Expected: tests `ok`; health prints `{"docker":"29.8.2"}` (the host engine version; any version string is fine). `TestHealthWithoutDocker` still passes because the test server has a nil client.

- [ ] **Step 5: Commit**

```bash
git add studio/docker.go studio/api.go studio/main.go studio/go.mod studio/go.sum
git commit -m "studio: Docker smoke check on /api/health

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: UI scaffold, schema and API client

**Files:**
- Create: `studio/ui/` (Vite `react-ts` template, then trimmed)
- Create: `studio/ui/vite.config.ts` (replace the template's)
- Create: `studio/ui/src/flow/schema.ts`
- Create: `studio/ui/src/flow/api.ts`
- Create: `studio/ui/src/nodes/types.ts`
- Modify: `studio/ui/index.html`, `studio/ui/src/main.tsx`, `studio/ui/src/App.tsx`, `studio/ui/src/index.css`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: the API from Task 3 (`/api/flows`, `/api/health`).
- Produces: `NODE_TYPES`, `type NodeType`, `canConnect(source, target): boolean`, `FlowSchema` (Zod) and `type FlowFile`, `defaultData(type)`, `nextId(type, nodes)`; `api.{health,list,get,create,save,remove}`, `type FlowSummary`, `type Problem`, `class ApiError`; node data types `ProducerData`, `TopicData`, `ConsumerData` and node types `ProducerNode`, `TopicNode`, `ConsumerNode`, `StudioNode`. Tasks 6 and 7 import these by exactly these names.

No JS test framework (Ponytail: the edge table is five lines mirrored from Go, which has the test; the end-to-end check in Task 8 and `npm run build`'s type check cover the UI).

- [ ] **Step 1: Scaffold with create-vite and install the two runtime dependencies**

```bash
cd studio && npm create vite@9.2.1 ui -- --template react-ts
```

If it prompts, answer **No** to "Use rolldown-vite (Experimental)?" and **No** to "Install with npm and start now?". Then:

```bash
cd ui && npm install && npm install @xyflow/react@12.12.0 zod@4.6.5
rm -rf src/App.css src/assets public
grep -E '"(vite|@xyflow/react|zod|react)"' package.json
```

Expected: `"vite": "^8.3.3"` (or the 8.x the template wrote), `"@xyflow/react": "^12.12.0"`, `"zod": "^4.6.5"`. `package-lock.json` pins the exact versions; commit it.

- [ ] **Step 2: Replace `studio/ui/vite.config.ts`**

```ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `npm run dev` serves the UI on :5173 and forwards /api to the Go server on :8082.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': 'http://localhost:8082' } },
})
```

- [ ] **Step 3: Write `studio/ui/src/flow/schema.ts`**

```ts
import { z } from 'zod'

export const NODE_TYPES = ['producer', 'topic', 'consumer', 'transform'] as const
export type NodeType = (typeof NODE_TYPES)[number]

// Mirrors allowedEdges in ../../flow.go; the backend is the authority.
const ALLOWED: Record<NodeType, readonly NodeType[]> = {
  producer: ['topic'],
  topic: ['consumer'],
  consumer: ['topic', 'transform'],
  transform: ['topic'],
}

export function canConnect(source: string | undefined, target: string | undefined): boolean {
  return (ALLOWED[source as NodeType] ?? []).includes(target as NodeType)
}

// The file format is React Flow's own shape plus id and name (spec 4.1).
export const FlowSchema = z.object({
  id: z.string(),
  name: z.string(),
  nodes: z.array(
    z.object({
      id: z.string(),
      type: z.enum(NODE_TYPES),
      position: z.object({ x: z.number(), y: z.number() }),
      data: z.record(z.string(), z.unknown()),
    }),
  ),
  edges: z.array(z.object({ id: z.string(), source: z.string(), target: z.string() })),
  viewport: z.object({ x: z.number(), y: z.number(), zoom: z.number() }).optional(),
})
export type FlowFile = z.infer<typeof FlowSchema>

// What a node dragged from the palette starts with.
export function defaultData(type: NodeType): Record<string, unknown> {
  switch (type) {
    case 'producer':
      return { source: 'manual', key: '', value: '{"id": {{.Seq}}}' }
    case 'topic':
      return { name: '', partitions: 1, replication_factor: 1 }
    case 'consumer':
      return { group: '', auto_offset_reset: 'earliest', sink: { kind: 'log' } }
    case 'transform':
      return { expr: 'msg' }
  }
}

// Smallest unused "<type>-<n>"; ids must match the Go nodeIDRe.
export function nextId(type: NodeType, nodes: { id: string }[]): string {
  const used = new Set(nodes.map((n) => n.id))
  for (let n = 1; ; n++) {
    const id = `${type}-${n}`
    if (!used.has(id)) return id
  }
}
```

- [ ] **Step 4: Write `studio/ui/src/flow/api.ts`**

```ts
import { FlowSchema, type FlowFile } from './schema'

export type FlowSummary = { id: string; name: string; status: string }
export type Problem = { node?: string; edge?: string; message: string }

// No constructor parameter properties: the Vite template enables erasableSyntaxOnly.
export class ApiError extends Error {
  status: number
  problems: Problem[]
  constructor(status: number, message: string, problems: Problem[] = []) {
    super(message)
    this.status = status
    this.problems = problems
  }
}

async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...init })
  if (res.status === 204) return undefined as T
  const body = await res.json().catch(() => ({}))
  if (!res.ok) {
    const problems: Problem[] = body.errors ?? []
    const message =
      body.error ??
      (problems.length
        ? problems.map((p) => `${p.node ?? p.edge ?? 'flow'}: ${p.message}`).join('; ')
        : res.statusText)
    throw new ApiError(res.status, message, problems)
  }
  return body as T
}

export const api = {
  health: () => call<{ docker: string }>('/api/health'),
  list: () => call<FlowSummary[]>('/api/flows'),
  get: async (id: string) => FlowSchema.parse(await call<unknown>(`/api/flows/${id}`)),
  create: (flow: Omit<FlowFile, 'id'>) =>
    call<FlowFile>('/api/flows', { method: 'POST', body: JSON.stringify(flow) }),
  save: (flow: FlowFile) =>
    call<FlowFile>(`/api/flows/${flow.id}`, { method: 'PUT', body: JSON.stringify(flow) }),
  remove: (id: string) => call<void>(`/api/flows/${id}`, { method: 'DELETE' }),
}
```

- [ ] **Step 5: Write `studio/ui/src/nodes/types.ts`**

```ts
import type { Node } from '@xyflow/react'

// Node.data per type; field names match the Go structs in ../../flow.go.
export type ProducerData = { source: 'manual' | 'timer'; interval_ms?: number; key: string; value: string }
export type TopicData = { name: string; partitions: number; replication_factor: number }
export type ConsumerData = {
  group: string
  auto_offset_reset: 'earliest' | 'latest'
  instances?: number
  sink: { kind: 'log' | 'http'; url?: string }
}

export type ProducerNode = Node<ProducerData, 'producer'>
export type TopicNode = Node<TopicData, 'topic'>
export type ConsumerNode = Node<ConsumerData, 'consumer'>
export type StudioNode = ProducerNode | TopicNode | ConsumerNode
```

- [ ] **Step 6: Replace `index.html` title, `main.tsx`, `App.tsx` and `index.css`**

In `studio/ui/index.html` set `<title>Pipeline Studio</title>` and remove the `<link rel="icon" …>` line (the svg was deleted).

`studio/ui/src/main.tsx`:

```tsx
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@xyflow/react/dist/style.css'
import './index.css'
import App from './App'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
```

`studio/ui/src/App.tsx` (placeholder until Task 6):

```tsx
export default function App() {
  return <div className="studio">Pipeline Studio</div>
}
```

`studio/ui/src/index.css` (the whole file; later tasks only add class names already listed here):

```css
:root { font-family: system-ui, sans-serif; font-size: 14px; color: #222; }
* { box-sizing: border-box; }
html, body, #root { margin: 0; height: 100%; }
.studio {
  display: grid; height: 100%;
  grid-template-rows: 44px 1fr;
  grid-template-columns: 220px 1fr 320px;
  grid-template-areas: "top top top" "side canvas inspector";
}
.topbar { grid-area: top; display: flex; gap: 8px; align-items: center; padding: 0 12px; border-bottom: 1px solid #ddd; }
.topbar input { font: inherit; padding: 4px 6px; width: 240px; }
.topbar .error { color: #b00020; margin-left: auto; }
.side { grid-area: side; padding: 12px; border-right: 1px solid #ddd; overflow: auto; }
.side h2, .inspector h2 { font-size: 14px; margin: 0 0 8px; text-transform: capitalize; }
.canvas { grid-area: canvas; }
.inspector { grid-area: inspector; padding: 12px; border-left: 1px solid #ddd; overflow: auto; }
.inspector label { display: block; margin: 8px 0 2px; font-weight: 600; }
.inspector input, .inspector select, .inspector textarea { width: 100%; padding: 4px 6px; font: inherit; }
.inspector textarea { font-family: ui-monospace, monospace; min-height: 80px; }
.flows { list-style: none; padding: 0; margin: 0 0 16px; }
.flows li { display: flex; justify-content: space-between; padding: 4px 0; cursor: pointer; }
.flows li.current { font-weight: 600; }
.palette-item { padding: 8px; margin: 6px 0; border: 2px solid #999; border-radius: 4px; cursor: grab; background: #fff; }
.hint { color: #666; font-size: 12px; }
.node { min-width: 160px; padding: 6px 10px; border: 2px solid #999; border-radius: 6px; background: #fff; }
.node.selected { box-shadow: 0 0 0 2px #1976d2; }
.node-title { font-weight: 600; }
.node-summary { color: #555; font-size: 12px; }
.producer { border-color: #2e7d32; }
.topic { border-color: #ef6c00; }
.consumer { border-color: #1565c0; }
```

- [ ] **Step 7: Ignore build output, build, and serve the built page from Go**

Append to `.gitignore`:

```
# Studio build output and dependencies
/studio/studio
/studio/ui/node_modules/
/studio/ui/dist/*
!/studio/ui/dist/.gitkeep
```

```bash
cd studio/ui && npm run build && ls dist
cd .. && go test ./... 2>&1 | tail -1
STUDIO_DATA=$(mktemp -d) go run . &
sleep 1; curl -s localhost:8082/ | grep -o '<title>[^<]*</title>'; kill %1
```

Expected: `dist/` holds `index.html` and `assets/`; Go tests `ok`; curl prints `<title>Pipeline Studio</title>`. `npm run build` runs `tsc -b` first, so a type error anywhere fails here.

- [ ] **Step 8: Commit**

```bash
git add .gitignore studio/ui
git commit -m "studio: Vite + React Flow scaffold, flow schema and API client

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Canvas, palette, custom nodes and edge validation

**Files:**
- Create: `studio/ui/src/nodes/ProducerNode.tsx`, `studio/ui/src/nodes/TopicNode.tsx`, `studio/ui/src/nodes/ConsumerNode.tsx`
- Create: `studio/ui/src/Palette.tsx`
- Create: `studio/ui/src/Canvas.tsx`
- Modify: `studio/ui/src/App.tsx`

**Interfaces:**
- Consumes: `canConnect`, `defaultData`, `nextId`, `NodeType` (schema.ts); `StudioNode` and the per-type node types (nodes/types.ts). React Flow API verified with Context7 (`/websites/reactflow_dev`): `Node<Data, 'type'>`, `NodeProps<NodeType>`, `nodeTypes` prop, `Handle` + `Position`, `isValidConnection: (edge: Edge | Connection) => boolean`, `onConnect` + `addEdge`, `useNodesState`/`useEdgesState`, `useReactFlow().screenToFlowPosition`, `ReactFlowProvider`.
- Produces: `Canvas` with props `{ nodes, edges, onNodesChange, onEdgesChange, setNodes, setEdges, onSelect, onEdit }`, `Palette` (no props), `DRAG_TYPE` constant, the three node components. Task 7 keeps these props unchanged.

- [ ] **Step 1: Write the three node components**

`studio/ui/src/nodes/ProducerNode.tsx`:

```tsx
import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { ProducerNode as ProducerNodeType } from './types'

export default function ProducerNode({ data, selected }: NodeProps<ProducerNodeType>) {
  const summary = data.source === 'timer' ? `timer · every ${data.interval_ms ?? 1000} ms` : 'manual'
  return (
    <div className={`node producer${selected ? ' selected' : ''}`}>
      <div className="node-title">Producer</div>
      <div className="node-summary">{summary}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
```

`studio/ui/src/nodes/TopicNode.tsx`:

```tsx
import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { TopicNode as TopicNodeType } from './types'

export default function TopicNode({ data, selected }: NodeProps<TopicNodeType>) {
  return (
    <div className={`node topic${selected ? ' selected' : ''}`}>
      <div className="node-title">Topic</div>
      <div className="node-summary">
        {data.name || '(unnamed)'} · {data.partitions} partition{data.partitions === 1 ? '' : 's'}
      </div>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
```

`studio/ui/src/nodes/ConsumerNode.tsx`:

```tsx
import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { ConsumerNode as ConsumerNodeType } from './types'

export default function ConsumerNode({ data, selected }: NodeProps<ConsumerNodeType>) {
  return (
    <div className={`node consumer${selected ? ' selected' : ''}`}>
      <div className="node-title">Consumer</div>
      <div className="node-summary">
        {data.group || '(no group)'} · sink: {data.sink.kind}
      </div>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
```

- [ ] **Step 2: Write `studio/ui/src/Palette.tsx`**

Native HTML5 drag and drop (the classic React Flow sidebar pattern): the palette puts the node type on the `dataTransfer`, the canvas reads it on drop.

```tsx
import type { NodeType } from './flow/schema'

export const DRAG_TYPE = 'application/x-studio-node'

const ITEMS: { type: NodeType; label: string }[] = [
  { type: 'producer', label: 'Producer' },
  { type: 'topic', label: 'Topic' },
  { type: 'consumer', label: 'Consumer' },
]

export default function Palette() {
  return (
    <section>
      <h2>Nodes</h2>
      {ITEMS.map((it) => (
        <div
          key={it.type}
          className={`palette-item ${it.type}`}
          draggable
          onDragStart={(e) => {
            e.dataTransfer.setData(DRAG_TYPE, it.type)
            e.dataTransfer.effectAllowed = 'move'
          }}
        >
          {it.label}
        </div>
      ))}
      <p className="hint">Drag onto the canvas, then wire Producer → Topic → Consumer. Backspace deletes.</p>
    </section>
  )
}
```

- [ ] **Step 3: Write `studio/ui/src/Canvas.tsx`**

```tsx
import { useCallback, type DragEvent, type Dispatch, type SetStateAction } from 'react'
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  addEdge,
  useReactFlow,
  type Connection,
  type Edge,
  type OnEdgesChange,
  type OnNodesChange,
} from '@xyflow/react'
import { DRAG_TYPE } from './Palette'
import { canConnect, defaultData, nextId, type NodeType } from './flow/schema'
import ProducerNode from './nodes/ProducerNode'
import TopicNode from './nodes/TopicNode'
import ConsumerNode from './nodes/ConsumerNode'
import type { StudioNode } from './nodes/types'

const nodeTypes = { producer: ProducerNode, topic: TopicNode, consumer: ConsumerNode }

type Props = {
  nodes: StudioNode[]
  edges: Edge[]
  onNodesChange: OnNodesChange<StudioNode>
  onEdgesChange: OnEdgesChange
  setNodes: Dispatch<SetStateAction<StudioNode[]>>
  setEdges: Dispatch<SetStateAction<Edge[]>>
  onSelect: (id: string | null) => void
  onEdit: () => void // called for edits that bypass onNodesChange/onEdgesChange (drop, connect)
}

export default function Canvas({ nodes, edges, onNodesChange, onEdgesChange, setNodes, setEdges, onSelect, onEdit }: Props) {
  const { screenToFlowPosition } = useReactFlow()

  // Only the pairs flow.go allows; the backend re-checks on save. addEdge drops duplicates itself.
  const isValidConnection = useCallback(
    (c: Edge | Connection) => {
      const s = nodes.find((n) => n.id === c.source)
      const t = nodes.find((n) => n.id === c.target)
      return !!s && !!t && s.id !== t.id && canConnect(s.type, t.type)
    },
    [nodes],
  )

  const onConnect = useCallback(
    (c: Connection) => {
      setEdges((eds) => addEdge(c, eds))
      onEdit()
    },
    [setEdges, onEdit],
  )

  const onDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault()
      const type = e.dataTransfer.getData(DRAG_TYPE) as NodeType
      if (!type) return
      const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
      setNodes((nds) => [...nds, { id: nextId(type, nds), type, position, data: defaultData(type) } as StudioNode])
      onEdit()
    },
    [screenToFlowPosition, setNodes, onEdit],
  )

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      isValidConnection={isValidConnection}
      onDrop={onDrop}
      onDragOver={(e) => {
        e.preventDefault()
        e.dataTransfer.dropEffect = 'move'
      }}
      onSelectionChange={({ nodes: sel }) => onSelect(sel[0]?.id ?? null)}
      deleteKeyCode={['Backspace', 'Delete']}
      fitView
    >
      <Background />
      <Controls />
      <MiniMap />
    </ReactFlow>
  )
}
```

- [ ] **Step 4: Replace `studio/ui/src/App.tsx` with the editor shell (state only; Task 7 adds flows, save and the inspector)**

```tsx
import { useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, type Edge } from '@xyflow/react'
import Canvas from './Canvas'
import Palette from './Palette'
import type { StudioNode } from './nodes/types'

function Studio() {
  const [nodes, setNodes, onNodesChange] = useNodesState<StudioNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [selected, setSelected] = useState<string | null>(null)

  return (
    <div className="studio">
      <header className="topbar">
        <strong>Pipeline Studio</strong>
      </header>
      <aside className="side">
        <Palette />
      </aside>
      <main className="canvas">
        <Canvas
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          setNodes={setNodes}
          setEdges={setEdges}
          onSelect={setSelected}
          onEdit={() => {}}
        />
      </main>
      <aside className="inspector">{selected ? `Selected: ${selected}` : 'Select a node.'}</aside>
    </div>
  )
}

export default function App() {
  return (
    <ReactFlowProvider>
      <Studio />
    </ReactFlowProvider>
  )
}
```

- [ ] **Step 5: Type-check, build, and try it in a browser**

```bash
cd studio/ui && npm run build
npm run dev
```

Open http://localhost:5173 and check, in this order:

1. Drag Producer, Topic, Consumer from the palette onto the canvas; each lands under the cursor with ids `producer-1`, `topic-1`, `consumer-1`.
2. Drag from the Producer's right handle to the Topic's left handle: an edge appears. Topic → Consumer: an edge appears.
3. Drag from the Topic's right handle to the Producer: no edge (the handle shows the invalid cursor and nothing is added). Consumer → Consumer (drop a second consumer): no edge. Topic → Topic: no edge.
4. Drag Producer → Topic again: no second edge (`addEdge` dedupes).
5. Click a node: the right pane shows `Selected: <id>`. Backspace removes it and its edges.

If `npm run build` reports that a node component is not assignable to `nodeTypes`, change the constant to `const nodeTypes: NodeTypes = {...}` with `import type { NodeTypes } from '@xyflow/react'` and keep the components as written; do not loosen the component prop types.

Stop the dev server (Ctrl-C).

- [ ] **Step 6: Commit**

```bash
git add studio/ui/src
git commit -m "studio ui: canvas, palette, custom nodes and edge validation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Flow list, inspector, save and load

**Files:**
- Create: `studio/ui/src/FlowList.tsx`
- Create: `studio/ui/src/Inspector.tsx`
- Modify: `studio/ui/src/App.tsx` (full replacement below)

**Interfaces:**
- Consumes: `api`, `ApiError`, `FlowSummary` (api.ts); `FlowFile` (schema.ts); `Canvas` props from Task 6; `StudioNode`.
- Produces: `FlowList` props `{ flows, currentId, onOpen, onCreate, onDelete }`; `Inspector` props `{ node, onChange(id, patch) }`. M2 adds Deploy/Stop buttons to the top bar and M3 adds the tail drawer; neither changes these.

- [ ] **Step 1: Write `studio/ui/src/FlowList.tsx`**

```tsx
import type { FlowSummary } from './flow/api'

type Props = {
  flows: FlowSummary[]
  currentId: string | null
  onOpen: (id: string) => void
  onCreate: () => void
  onDelete: (id: string) => void
}

export default function FlowList({ flows, currentId, onOpen, onCreate, onDelete }: Props) {
  return (
    <section>
      <h2>
        Flows <button onClick={onCreate}>New</button>
      </h2>
      <ul className="flows">
        {flows.map((f) => (
          <li key={f.id} className={f.id === currentId ? 'current' : ''}>
            <span onClick={() => onOpen(f.id)}>
              {f.name} <small>({f.status})</small>
            </span>
            <button onClick={() => onDelete(f.id)} title="Delete flow">
              ×
            </button>
          </li>
        ))}
      </ul>
      {flows.length === 0 && <p className="hint">No flows yet. Click New.</p>}
    </section>
  )
}
```

- [ ] **Step 2: Write `studio/ui/src/Inspector.tsx`**

Each branch reads `node.data` after the `node.type` check so TypeScript narrows the union; `onChange` merges a partial patch into `data`.

```tsx
import type { StudioNode } from './nodes/types'

type Props = {
  node: StudioNode | null
  onChange: (id: string, patch: Record<string, unknown>) => void
}

export default function Inspector({ node, onChange }: Props) {
  if (!node) return <p className="hint">Select a node to edit it.</p>
  const set = (patch: Record<string, unknown>) => onChange(node.id, patch)

  return (
    <div>
      <h2>
        {node.type} <small>{node.id}</small>
      </h2>
      {node.type === 'producer' && (
        <>
          <label>Source</label>
          <select value={node.data.source} onChange={(e) => set({ source: e.target.value })}>
            <option value="manual">manual</option>
            <option value="timer">timer</option>
          </select>
          {node.data.source === 'timer' && (
            <>
              <label>Interval (ms)</label>
              <input
                type="number"
                min={10}
                value={node.data.interval_ms ?? 1000}
                onChange={(e) => set({ interval_ms: Number(e.target.value) })}
              />
            </>
          )}
          <label>Key template</label>
          <input
            value={node.data.key}
            placeholder="{{.Seq}} — empty means keyless"
            onChange={(e) => set({ key: e.target.value })}
          />
          <label>Value template (renders JSON)</label>
          <textarea value={node.data.value} onChange={(e) => set({ value: e.target.value })} />
          <p className="hint">Templates may use {'{{.Seq}}'}, {'{{.Now}}'} and {'{{.Rand}}'}.</p>
        </>
      )}
      {node.type === 'topic' && (
        <>
          <label>Name</label>
          <input value={node.data.name} onChange={(e) => set({ name: e.target.value })} />
          <label>Partitions</label>
          <input
            type="number"
            min={1}
            value={node.data.partitions}
            onChange={(e) => set({ partitions: Number(e.target.value) })}
          />
          <label>Replication factor</label>
          <input
            type="number"
            min={1}
            max={1}
            value={node.data.replication_factor}
            onChange={(e) => set({ replication_factor: Number(e.target.value) })}
          />
          <p className="hint">Single broker: replication factor stays 1.</p>
        </>
      )}
      {node.type === 'consumer' && (
        <>
          <label>Group id</label>
          <input value={node.data.group} onChange={(e) => set({ group: e.target.value })} />
          <label>auto.offset.reset</label>
          <select
            value={node.data.auto_offset_reset}
            onChange={(e) => set({ auto_offset_reset: e.target.value })}
          >
            <option value="earliest">earliest</option>
            <option value="latest">latest</option>
          </select>
          <label>Sink</label>
          <select value={node.data.sink.kind} disabled>
            <option value="log">log (tail panel)</option>
          </select>
        </>
      )}
    </div>
  )
}
```

- [ ] **Step 3: Replace `studio/ui/src/App.tsx`**

```tsx
import { useCallback, useEffect, useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, useReactFlow, type Edge } from '@xyflow/react'
import Canvas from './Canvas'
import FlowList from './FlowList'
import Inspector from './Inspector'
import Palette from './Palette'
import { api, ApiError, type FlowSummary } from './flow/api'
import type { FlowFile } from './flow/schema'
import type { StudioNode } from './nodes/types'

function describe(e: unknown): string {
  return e instanceof ApiError ? `${e.status}: ${e.message}` : String(e)
}

function Studio() {
  const [nodes, setNodes, onNodesChange] = useNodesState<StudioNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { getViewport, setViewport } = useReactFlow()
  const [flows, setFlows] = useState<FlowSummary[]>([])
  const [current, setCurrent] = useState<{ id: string; name: string } | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [dirty, setDirty] = useState(false)
  const [error, setError] = useState('')

  const refresh = useCallback(() => api.list().then(setFlows).catch((e) => setError(describe(e))), [])
  useEffect(() => {
    refresh()
  }, [refresh])

  const load = async (id: string) => {
    try {
      const f = await api.get(id)
      setNodes(f.nodes as unknown as StudioNode[]) // the backend validated the per-type data
      setEdges(f.edges)
      setViewport(f.viewport ?? { x: 0, y: 0, zoom: 1 })
      setCurrent({ id: f.id, name: f.name })
      setSelected(null)
      setDirty(false)
      setError('')
    } catch (e) {
      setError(describe(e))
    }
  }

  // React Flow's shape is the file format: only drop runtime-only fields.
  const toFile = (id: string, name: string): FlowFile => ({
    id,
    name,
    nodes: nodes.map(({ id, type, position, data }) => ({ id, type: type!, position, data })),
    edges: edges.map(({ id, source, target }) => ({ id, source, target })),
    viewport: getViewport(),
  })

  const save = async () => {
    if (!current) return
    try {
      await api.save(toFile(current.id, current.name))
      setDirty(false)
      setError('')
      refresh()
    } catch (e) {
      setError(describe(e))
    }
  }

  const create = async () => {
    const name = window.prompt('Flow name')?.trim()
    if (!name) return
    try {
      const f = await api.create({ name, nodes: [], edges: [] })
      await refresh()
      await load(f.id)
    } catch (e) {
      setError(describe(e))
    }
  }

  const remove = async (id: string) => {
    if (!window.confirm('Delete this flow?')) return
    try {
      await api.remove(id)
      if (current?.id === id) {
        setCurrent(null)
        setNodes([])
        setEdges([])
      }
      refresh()
    } catch (e) {
      setError(describe(e))
    }
  }

  // Selection and measurement changes are not edits.
  const touch = (changes: { type: string }[]) => {
    if (changes.some((c) => c.type !== 'select' && c.type !== 'dimensions')) setDirty(true)
  }

  const updateData = (id: string, patch: Record<string, unknown>) => {
    setNodes((nds) => nds.map((n) => (n.id === id ? ({ ...n, data: { ...n.data, ...patch } } as StudioNode) : n)))
    setDirty(true)
  }

  const node = nodes.find((n) => n.id === selected) ?? null

  return (
    <div className="studio">
      <header className="topbar">
        <strong>Pipeline Studio</strong>
        {current && (
          <>
            <input
              value={current.name}
              onChange={(e) => {
                setCurrent({ ...current, name: e.target.value })
                setDirty(true)
              }}
            />
            <button onClick={save} disabled={!dirty}>
              {dirty ? 'Save' : 'Saved'}
            </button>
          </>
        )}
        {error && <span className="error">{error}</span>}
      </header>
      <aside className="side">
        <FlowList flows={flows} currentId={current?.id ?? null} onOpen={load} onCreate={create} onDelete={remove} />
        <Palette />
      </aside>
      <main className="canvas">
        {current ? (
          <Canvas
            nodes={nodes}
            edges={edges}
            onNodesChange={(c) => {
              touch(c)
              onNodesChange(c)
            }}
            onEdgesChange={(c) => {
              touch(c)
              onEdgesChange(c)
            }}
            setNodes={setNodes}
            setEdges={setEdges}
            onSelect={setSelected}
            onEdit={() => setDirty(true)}
          />
        ) : (
          <p className="hint" style={{ padding: 16 }}>
            Open a flow on the left or click New.
          </p>
        )}
      </main>
      <aside className="inspector">
        <Inspector node={node} onChange={updateData} />
      </aside>
    </div>
  )
}

export default function App() {
  return (
    <ReactFlowProvider>
      <Studio />
    </ReactFlowProvider>
  )
}
```

- [ ] **Step 4: Build, then run the full M1 editor demo against the Go server**

```bash
cd studio/ui && npm run build
cd .. && STUDIO_DATA=$(mktemp -d) go run . &
cd ui && npm run dev
```

In http://localhost:5173:

1. Click **New**, name it `orders demo`: it appears in the list and opens (empty canvas, `Saved`).
2. Drag Producer, Topic, Consumer; wire Producer → Topic → Consumer; the button reads `Save`.
3. Select the Topic, set name `orders`, partitions `3`; select the Consumer, group `orders-workers`; the node summaries update as you type.
4. Click **Save**: button reads `Saved`. `curl -s localhost:8082/api/flows` lists it; `cat $STUDIO_DATA/<id>.json` (path printed by the Go server at start) shows nodes with `position` and `data`, edges, and `viewport`.
5. Reload the page, click the flow: nodes, edges, config and viewport come back; `Saved`.
6. Rename the flow in the top bar, Save, reload: the new name shows in the list.
7. Hand-edit the file to put an edge from `topic-1` to `producer-1`, reload, open the flow: it loads (save-level files are trusted), but clicking **Save** after any change shows `422: topic-1-producer-1: edge topic-1 → producer-1: topic → producer is not allowed` in the top bar and the button stays `Save`.
8. Delete the flow with ×: it disappears from the list and the canvas clears.

Stop both servers (Ctrl-C, then `kill %1`).

- [ ] **Step 5: Commit**

```bash
git add studio/ui/src
git commit -m "studio ui: flow list, inspector, save and load

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Image, compose service, Makefile check, docs

**Files:**
- Create: `studio/Dockerfile`, `studio/.dockerignore`
- Create: `flows/0a1b2c3d.json` (example flow)
- Modify: `docker-compose.yml` (add service `studio`)
- Modify: `Makefile` (`STUDIO_URL`, `up` message, `verify-studio`, `verify` prerequisites, `.PHONY`)
- Modify: `README.md`, `AGENTS.md`

**Interfaces:**
- Consumes: everything above; `studio -healthcheck` (Task 3) and `/api/health` (Task 4).
- Produces: compose service `studio` on `127.0.0.1:8082`, image `kafka-playground/studio:0.1.0`; `make verify-studio`; `make verify` now runs the studio check before the Kafka check. M2 adds `make nodes` and the `make down` container cleanup.

- [ ] **Step 1: Write `studio/Dockerfile` and `studio/.dockerignore`**

`studio/Dockerfile`:

```dockerfile
# UI: Vite build → /ui/dist
FROM node:24.21.0-alpine AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# Go: static binary with the UI embedded
FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui /ui/dist ./ui/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /studio .

FROM scratch
COPY --from=build /studio /studio
USER 65534
EXPOSE 8082
ENTRYPOINT ["/studio"]
```

`studio/.dockerignore` (keeps host build output out of `COPY . .`; the UI stage supplies `ui/dist`):

```
ui/node_modules
ui/dist
studio
```

- [ ] **Step 2: Add the example flow**

`flows/0a1b2c3d.json` (the spec's section 4.1 example; `instances` and the timer source are accepted at save level now and used by M3/M4):

```json
{
  "id": "0a1b2c3d",
  "name": "orders demo",
  "nodes": [
    {
      "id": "producer-1",
      "type": "producer",
      "position": { "x": 40, "y": 160 },
      "data": {
        "source": "timer",
        "interval_ms": 1000,
        "key": "{{.Seq}}",
        "value": "{\"id\": {{.Seq}}, \"at\": \"{{.Now}}\"}"
      }
    },
    {
      "id": "topic-1",
      "type": "topic",
      "position": { "x": 340, "y": 160 },
      "data": { "name": "orders", "partitions": 3, "replication_factor": 1 }
    },
    {
      "id": "consumer-1",
      "type": "consumer",
      "position": { "x": 640, "y": 160 },
      "data": {
        "group": "orders-workers",
        "auto_offset_reset": "earliest",
        "instances": 2,
        "sink": { "kind": "log" }
      }
    },
    {
      "id": "topic-2",
      "type": "topic",
      "position": { "x": 940, "y": 160 },
      "data": { "name": "orders-archive", "partitions": 1, "replication_factor": 1 }
    }
  ],
  "edges": [
    { "id": "e1", "source": "producer-1", "target": "topic-1" },
    { "id": "e2", "source": "topic-1", "target": "consumer-1" },
    { "id": "e3", "source": "consumer-1", "target": "topic-2" }
  ],
  "viewport": { "x": 0, "y": 0, "zoom": 1 }
}
```

- [ ] **Step 3: Add the compose service**

In `docker-compose.yml`, after the `producer` service and before `console`:

```yaml
  # Pipeline Studio: canvas editor for flows (http://localhost:8082). Flows are
  # files in ./flows. The Docker socket lets the studio run nodes as containers
  # from M2 on; /api/health proves it can reach the engine.
  studio:
    build: ./studio
    image: kafka-playground/studio:0.1.0
    pull_policy: build
    depends_on: *after-kafka
    ports:
      - "127.0.0.1:8082:8082"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./flows:/data
    environment:
      KAFKA_BROKERS: *bootstrap
    healthcheck:
      test: ["CMD", "/studio", "-healthcheck"]
      interval: 2s
      retries: 30
```

Then:

```bash
docker compose config --quiet && docker compose up -d --build studio && sleep 3 && curl -s localhost:8082/api/health; echo
```

Expected: `{"docker":"29.8.2"}` (any version string). If instead it prints `{"error":"docker: ... permission denied"}`: the socket inside the container is not readable by uid 65534. Add to the `studio` service, with this comment, and re-run the command:

```yaml
    # The engine socket is root-owned inside the container; the mount already
    # grants host-root to this service, so running it as root changes nothing.
    user: "0"
```

Record which branch happened in the commit message.

- [ ] **Step 4: Makefile**

Add after `CONSOLE_URL`:

```make
STUDIO_URL ?= http://localhost:8082
```

Add `verify-studio` to `.PHONY`. Change the `up` message line to:

```make
	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Studio: $(STUDIO_URL)   Broker from the host: localhost:9092"
```

Change the `verify` rule line to `verify: up verify-studio ## End-to-end check: studio API round trip, then one record seen once by the worker group and once by the audit group` (body unchanged). Add a new section before `# ── Verify ──`:

```make
# ── Studio ───────────────────────────────────────────────────────────────────

verify-studio: up ## End-to-end check of the studio API: health, create, reject a bad edge, round trip, delete
	@health=$$(curl -sS --fail-with-body $(STUDIO_URL)/api/health) || { echo "STUDIO FAILED: health: $$health"; exit 1; }; \
	echo "studio health: $$health"; \
	flow='{"name":"verify","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"manual","key":"","value":"{}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"verify","partitions":1,"replication_factor":1}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"}]}'; \
	created=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$flow") || { echo "STUDIO FAILED: create: $$created"; exit 1; }; \
	id=$$(echo "$$created" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	[ -f "flows/$$id.json" ] || { echo "STUDIO FAILED: flows/$$id.json not written ($$created)"; exit 1; }; \
	bad=$$(echo "$$flow" | sed 's/"source":"producer-1","target":"topic-1"/"source":"topic-1","target":"producer-1"/'); \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X PUT $(STUDIO_URL)/api/flows/$$id -H 'Content-Type: application/json' --data "$$bad"); \
	[ "$$code" = 422 ] || { echo "STUDIO FAILED: bad edge accepted ($$code)"; exit 1; }; \
	curl -sS --fail-with-body $(STUDIO_URL)/api/flows/$$id | grep -q '"name":"verify"' || { echo "STUDIO FAILED: round trip"; exit 1; }; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$id || { echo "STUDIO FAILED: delete"; exit 1; }; \
	[ ! -f "flows/$$id.json" ] || { echo "STUDIO FAILED: flows/$$id.json still exists"; exit 1; }; \
	echo "STUDIO OK ($$id)"
```

Recipe lines start with a real tab. Run:

```bash
make down && make verify
```

Expected: `studio health: {"docker":"…"}`, `STUDIO OK (<id>)`, then the existing Kafka check ending in `VERIFY OK`. Then `make down`.

- [ ] **Step 5: README and AGENTS**

`README.md`:

- Services table, new row after `producer`: `| \`studio\` | Pipeline Studio, built from \`studio/\` (Go + React Flow): draw flows, save them as JSON | http://localhost:8082 |`
- Quick start comment for `make verify`: `# studio API round trip, then one record seen exactly once per consumer group`
- New section after "Add a consumer":

```markdown
## Pipeline Studio

http://localhost:8082 is a canvas for building flows: drag Producer, Topic and Consumer nodes from the palette, wire them (only Producer → Topic → Consumer edges are accepted; the server rejects anything else on save), edit the selected node on the right, Save. Each flow is one file in `flows/` (`<id>.json`, React Flow's node/edge shape plus `name`); edit, copy or commit them like any other file — a file that does not parse is skipped and logged. `flows/0a1b2c3d.json` is an example.

Deploying a flow is the next milestone. `GET /api/health` reports the Docker Engine version the studio can reach through `/var/run/docker.sock`; deploy will run each node as a container.

UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the `studio` container. Go changes need `make up` (the image rebuilds).
```

`AGENTS.md`:

- Layout: add `- \`studio/\`: Pipeline Studio. Go control plane (\`main.go\`, \`api.go\`, \`flow.go\`, \`store.go\`, \`docker.go\`) with the React Flow UI from \`studio/ui/\` embedded at build (\`go:embed all:ui/dist\`; keep \`ui/dist/.gitkeep\`). Flows persist in \`flows/\` (bind mount).`
- Check your change, two new lines: `(cd studio && go vet ./... && gofmt -l . && go test ./...)   # after studio Go changes` and `(cd studio/ui && npm run build)                               # after UI changes; runs tsc`
- Rules, one new line: `- Node ids, node data fields and the allowed-edge table are defined in \`studio/flow.go\`; \`studio/ui/src/flow/schema.ts\` and \`studio/ui/src/nodes/types.ts\` mirror them. Change all three together.`

- [ ] **Step 6: Final check and commit**

```bash
docker compose config --quiet && (cd studio && go vet ./... && gofmt -l . && go test ./...) && (cd studio/ui && npm run build)
make down && make verify && make down
git add studio/Dockerfile studio/.dockerignore flows docker-compose.yml Makefile README.md AGENTS.md
git commit -m "Add Pipeline Studio service: image, compose, verify-studio, docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

Expected: `make verify` ends with `VERIFY OK`. M1 demo criterion (spec §7): open http://localhost:8082, build the example by hand or open `orders demo`, Save, `cat flows/<id>.json`, refresh the page, see it back.
