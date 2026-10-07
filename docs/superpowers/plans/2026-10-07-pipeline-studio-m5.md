# Pipeline Studio M5 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform: an `expr-lang/expr` expression between a consumer and the next topic reshapes or drops each record, with its own counts on the canvas.

**Architecture:** A transform has no container: `Resolve` hands its expression and node id to the upstream consumer's `NodeSpec`, and the consumer's forward goes to the transform's topic. The consumer runs each record through tail → sink → transform → forward; `transform.go` compiles the expression (on deploy, in `Validate`, and again when the consumer starts) and runs it over `msg`, the record's value decoded from JSON. The consumer's `/stats` reports the transform's counts under its node id (`steps`), and the snapshot puts them on the Transform node with the consumer's state.

**Tech Stack:** Go 1.27.1, `github.com/expr-lang/expr` v1.17.8 (new; `expr.Compile` with `expr.Env`, `expr.Run`, `vm.Program`), franz-go v1.22.1; UI: React 19 + `@xyflow/react` 12.12.0. No new npm dependency.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` — §3.4 "Inside a node container" (per-record order, transform is not a container), §3.6 (transform node's state and counters), §4.2 (`expr`, nil drops), §4.3 (consumer → transform → topic), §7 M5. M4 shipped on `main` at `4e50f52`.

## Global Constraints

- One new Go dependency, `github.com/expr-lang/expr` pinned at exactly `v1.17.8`; no new npm dependency; `go 1.27.1`, images, compose service and studio tag (`kafka-playground/studio:0.1.0`, `pull_policy: build`) unchanged.
- A transform is not a container: it runs inside its upstream consumer's containers (every instance), and the consumer forwards to the transform's topic.
- Per record, in order: tail (and the stdout log line), http sink when set, transform when set, forward when set. The sink still sees the original value; the forward carries the transform's result with the record's key.
- `msg` is the record's value decoded from JSON. The result, encoded as JSON, is the forwarded value; `nil` drops the record (not an error). A value that is not JSON, a runtime error, or a result JSON cannot encode is an error on the transform, and the record is not forwarded (it still commits: at-most-once, like a failed forward).
- Deploy compiles every transform's `expr` and answers 422 naming the node, the message `expr: <first line of the compile error>`; Save does not compile (a half-built flow saves).
- `/stats` of a consumer with a transform adds `steps: {<transform node id>: {total, errors, lastError}}`; `total` counts every record the transform got (dropped ones included).
- Snapshot: a Transform node gets its consumer node's `state` and the summed `total`/`errors` of its containers' `steps` (a `lastError` from an instance is prefixed `#<i>: `), and a `boot` joining theirs so `rate` behaves like any node's. No new JSON field: `NodeRuntime` in the UI needs no change.
- Everything that already holds keeps holding: every `/api` answer JSON (except the event stream), the cross-origin write guard, forward loops through a transform still refused, `make verify` ending in `STUDIO OK (...)` and `VERIFY OK`.
- Checks: `make test` (go vet, gofmt, go test, UI build, compose config). Before finishing a task that touches the Makefile or Go runtime code: `make down && make verify`, then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.
- Makefile: GNU make 3.81, BSD tools, recipe lines start with a real tab, shell `$` written `$$`.
- Validation rules live only in `studio/flow.go`. `NodeState` (`studio/engine.go`) and `NodeRuntime` (`studio/ui/src/flow/api.ts`) mirror each other.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Keep README.md, AGENTS.md and the spec in sync.

## Review Focus

1. An expression that does not compile (`msg.`, an unknown name) must be refused on deploy with a 422 naming the node and a one-line message — never a node container that exits. Pinned in Task 1, Step 1 (`TestCompileTransform`, `flow_test.go` cases) and Task 2 (`verify-studio` deploys one and expects 422).
2. A record the expression cannot handle (a missing field: `invalid operation: <nil> * <nil>`, a value that is not JSON) must count on the Transform node with its last error, not be forwarded, and leave the consumer running. Pinned in Task 1, Step 1 (`TestRunTransform`, `TestConsumerTransform`) and Task 2 (`verify-studio`).
3. A result JSON cannot hold (`msg.a / 0` is `+Inf`) must be a counted error, not a crash or a `null` forwarded. Pinned in Task 1, Step 1 (`TestRunTransform`).
4. `nil` used as a filter (`msg.qty > 0 ? msg : nil`) must drop the record silently: no error, nothing forwarded, the transform's total still counts it. Pinned in Task 1, Step 1 (`TestRunTransform`, `TestConsumerTransform`).
5. A transform behind a consumer with instances, some down: the Transform node must show the consumer's state and the sum of the running instances' counts. Pinned in Task 2, Step 1 (`TestApplySteps`).

## Decisions this plan makes (Task 3 writes them into the spec)

1. **`msg` is declared as a map.** `expr.Env(map[string]any{"msg": map[string]any{}})`: field access compiles (declaring it `nil` makes every `msg.x` a compile error), and at run time `msg` is whatever the value decodes to — an array or a string passes through `msg` unchanged. Checked while writing this plan with expr v1.17.8.
2. **Errors keep their first line.** expr appends the expression with a caret under the fault; a 422 message, a counter's last error and a tooltip show one line.
3. **Numbers decode as float64** (`encoding/json` into `any`): `{"id":1}` comes out `{"id":1}`, but integers above 2^53 lose precision. `json.Number` would keep them but break arithmetic in expressions.
4. **The transform's `total` counts every record it got**, dropped ones included; its `errors` count failures. Dropped records are visible as `total` minus what reaches the next topic.
5. **`notYetRunnable` goes.** Nothing is refused for its milestone any more.
6. **A Transform node's `boot` joins its consumer containers' boots**, so `withRates` gives no rate when one restarted, with no special case for transforms.

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `studio/go.mod`, `studio/go.sum` | `github.com/expr-lang/expr v1.17.8` | 1 |
| `studio/transform.go` (new) | `transformEnv`, `compileTransform`, `runTransform`, `firstLine` | 1 |
| `studio/flow.go` | Deploy-level compile check of every transform | 1 |
| `studio/resolve.go` | `NodeSpec.Transform`/`TransformNode`; `Resolve` forwards through a transform; `notYetRunnable` removed | 1 |
| `studio/engine.go` | `Deploy` stops calling `notYetRunnable`; `NodeState.steps`, `withStats` keeps them, `applySteps` | 1, 2 |
| `studio/node.go` | `stepStats`, `counters.step`, consumer runs the transform, `/stats` reports `steps` | 1 |
| `studio/*_test.go` | `transform_test.go` (new), `flow_test.go`, `resolve_test.go`, `node_test.go`, `engine_test.go` | 1, 2 |
| `Makefile` | `verify-studio`: a transform flow | 2 |
| `studio/ui/src/Palette.tsx`, `studio/ui/src/Inspector.tsx` | Transform in the palette; the expression hint | 3 |
| `README.md`, `AGENTS.md`, spec | docs | 3 |

---

### Task 1: Transforms run in their consumer

**Files:**
- Create: `studio/transform.go`, `studio/transform_test.go`
- Modify: `studio/go.mod`, `studio/go.sum`, `studio/flow.go`, `studio/resolve.go`, `studio/engine.go`, `studio/node.go`
- Test: `studio/flow_test.go`, `studio/resolve_test.go`, `studio/node_test.go`

**Interfaces:**
- Consumes: `Validate`'s transform case (`flow.go`, after the `level == Save` return); `NodeSpec`, `Resolve`, `notYetRunnable` (`resolve.go`); `Deploy` (`engine.go`); `consumer`, `handle`, `counters`, `nodeStats`, `runNode`, `errInvalidJSON` (`node.go`); test helpers `clone`, `good`, `node`, `edge`, `topic` (`flow_test.go`).
- Produces: `compileTransform(src string) (*vm.Program, error)`; `runTransform(p *vm.Program, value []byte) (out []byte, keep bool, err error)`; `NodeSpec` fields `Transform string` (JSON `transform,omitempty`) and `TransformNode string` (`transform_node,omitempty`); `type stepStats struct{ Total, Errors int64; LastError string }` (JSON `total`, `errors`, `lastError`); `nodeStats.Steps map[string]stepStats` (JSON `steps,omitempty`); `(*counters).step() stepStats`; `consumer` fields `transform *vm.Program`, `steps *counters`. Task 2 reads `nodeStats.Steps` and `NodeSpec.TransformNode`.

- [ ] **Step 1: Add the dependency and write the failing tests**

Run: `cd studio && go get github.com/expr-lang/expr@v1.17.8` (`go mod tidy` in Step 5 moves it to the direct requires).

Create `studio/transform_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func TestCompileTransform(t *testing.T) {
	if _, err := compileTransform(`{id: msg.id, total: msg.qty * msg.price}`); err != nil {
		t.Fatalf("the spec's example must compile: %v", err)
	}
	for _, src := range []string{`msg.`, `foo + 1`} {
		_, err := compileTransform(src)
		if err == nil || strings.Contains(err.Error(), "\n") {
			t.Fatalf("%q: want a one-line compile error, got %q", src, err)
		}
	}
}

func TestRunTransform(t *testing.T) {
	for _, c := range []struct {
		src, value string
		out        string // "" with keep false and errHas "" means dropped
		errHas     string
	}{
		{`{id: msg.id, total: msg.qty * msg.price}`, `{"id":1,"qty":2,"price":3}`, `{"id":1,"total":6}`, ""},
		{`{id: msg.id, total: msg.qty * msg.price}`, `{"id":2}`, "", "invalid operation"},
		{`msg.qty > 0 ? msg : nil`, `{"qty":0}`, "", ""},
		{`msg.qty > 0 ? msg : nil`, `{"qty":5}`, `{"qty":5}`, ""},
		{`msg`, `[1,2]`, `[1,2]`, ""},
		{`msg`, `{`, "", "not valid JSON"},
		{`msg.a / 0`, `{"a":1}`, "", "result:"},
	} {
		p, err := compileTransform(c.src)
		if err != nil {
			t.Fatalf("%q: %v", c.src, err)
		}
		out, keep, err := runTransform(p, []byte(c.value))
		switch {
		case c.errHas != "":
			if err == nil || !strings.Contains(err.Error(), c.errHas) || strings.Contains(err.Error(), "\n") {
				t.Errorf("%q on %s: want a one-line error containing %q, got %v", c.src, c.value, c.errHas, err)
			}
		case c.out == "":
			if err != nil || keep {
				t.Errorf("%q on %s: want dropped, got keep=%v %s %v", c.src, c.value, keep, out, err)
			}
		default:
			if err != nil || !keep || string(out) != c.out {
				t.Errorf("%q on %s: want %s, got keep=%v %s %v", c.src, c.value, c.out, keep, out, err)
			}
		}
	}
}
```

In `studio/flow_test.go`, insert before the `"transform empty expr"` case:

```go
		{"transform expr does not compile", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg."}`), node("topic-2", "topic", topic("x")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, "expr: unexpected end of expression"},
		{"transform expr does not compile saves", Save, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg."}`), node("topic-2", "topic", topic("x")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, ""},
```

In `studio/resolve_test.go`, replace the whole `TestNotYetRunnable` function with the one below, and drop `"strings"` from the imports (nothing else uses it):

```go
func TestResolveTransform(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes = append(f.Nodes,
		node("transform-1", "transform", `{"expr":"{id: msg.id}"}`),
		node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
	f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "orders-workers", Forward: "archive", Transform: "{id: msg.id}", TransformNode: "transform-1"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("a transform runs in its consumer, which forwards to the transform's topic:\n got %+v\nwant %+v", specs, want)
	}
	if len(topics) != 2 {
		t.Fatalf("want both topics created, got %+v", topics)
	}
}
```

Append to `studio/node_test.go`:

```go
func TestConsumerTransform(t *testing.T) {
	var forwarded []string
	p, err := compileTransform(`msg.qty > 0 ? {id: msg.id, total: msg.qty * msg.price} : nil`)
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{
		spec:      NodeSpec{Topic: "orders", Forward: "totals", TransformNode: "transform-1"},
		tail:      &tail{},
		counts:    &counters{},
		transform: p,
		steps:     &counters{},
		produce: func(_ context.Context, r *kgo.Record) error {
			forwarded = append(forwarded, string(r.Key)+"="+string(r.Value))
			return nil
		},
	}
	for _, v := range []string{`{"id":1,"qty":2,"price":3}`, `{"id":2,"qty":0}`, `{"id":3,"qty":1}`, `{`} {
		if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Key: []byte("k"), Value: []byte(v)}) {
			t.Fatalf("%s: a transform outcome never holds back the commit", v)
		}
	}
	// Only the first is forwarded, transformed and with its key: the second is
	// dropped (nil), the third fails (no price: 1 * nil), the fourth is not JSON.
	if len(forwarded) != 1 || forwarded[0] != `k={"id":1,"total":6}` {
		t.Fatalf("want only k={\"id\":1,\"total\":6} forwarded, got %q", forwarded)
	}
	step := c.steps.step()
	if step.Total != 4 || step.Errors != 2 || !strings.Contains(step.LastError, "not valid JSON") {
		t.Fatalf("the transform got 4 records and failed on 2, the last not JSON; got %+v", step)
	}
	if s := c.counts.stats("b", c.tail.last()); s.Total != 4 || s.Errors != 0 || s.TailSeq != 4 {
		t.Fatalf("the consumer still counts and tails every record, with no errors of its own; got %+v", s)
	}
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL to compile — `undefined: compileTransform`, `undefined: runTransform`, `unknown field Transform in struct literal`, `unknown field transform in struct literal`.

- [ ] **Step 3: `transform.go`**

Create `studio/transform.go`:

```go
// Transforms: an expr-lang program over msg, the record's value decoded from JSON.
// Its result, encoded as JSON, is the value the consumer forwards; nil drops the
// record. Validate compiles every transform on deploy; the consumer that runs one
// compiles it again when it starts.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// transformEnv declares msg as a map so that field access compiles; at run time
// msg is whatever the value decodes to (an object, an array, a string, a number).
var transformEnv = map[string]any{"msg": map[string]any{}}

// compileTransform compiles src for runTransform.
func compileTransform(src string) (*vm.Program, error) {
	p, err := expr.Compile(src, expr.Env(transformEnv))
	if err != nil {
		return nil, firstLine(err)
	}
	return p, nil
}

// runTransform runs p over a record's value. keep is false when the program
// returned nil: the record is dropped, which is not an error.
func runTransform(p *vm.Program, value []byte) (out []byte, keep bool, err error) {
	var msg any
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, false, errInvalidJSON
	}
	res, err := expr.Run(p, map[string]any{"msg": msg})
	if err != nil {
		return nil, false, firstLine(err)
	}
	if res == nil {
		return nil, false, nil
	}
	if out, err = json.Marshal(res); err != nil {
		return nil, false, fmt.Errorf("result: %w", err)
	}
	return out, true, nil
}

// firstLine keeps an expr error's first line; the rest repeats the expression
// with a caret under the fault, which a one-line counter or 422 cannot show.
func firstLine(err error) error {
	first, _, _ := strings.Cut(err.Error(), "\n")
	return errors.New(first)
}
```

In `studio/flow.go`'s `Validate`, transform case, extend the empty-expression check:

```go
			if strings.TrimSpace(d.Expr) == "" {
				add(n.ID, "", "expr is required")
			} else if _, err := compileTransform(d.Expr); err != nil {
				add(n.ID, "", "expr: %v", err)
			}
```

- [ ] **Step 4: `resolve.go`, `engine.go`, `node.go` — the consumer runs the transform**

`studio/resolve.go`: add the last two `NodeSpec` fields:

```go
	Transform       string `json:"transform,omitempty"`      // consumer: the expr its records pass through before the forward
	TransformNode   string `json:"transform_node,omitempty"` // consumer: that transform's node id, which its counts are reported under
```

Replace `Resolve`'s comment with:

```go
// Resolve assumes Validate(&f, Deploy) passed: every data field decodes and every
// producer has one edge to a topic, every consumer has one edge from a topic and
// at most one out (to a topic or a transform), and every transform one edge in
// from a consumer and one out to a topic. A consumer's transform runs in its own
// containers, so a transform has no spec of its own.
```

In `Resolve`, replace the `forward` map declaration with

```go
	out := map[string]string{}       // consumer or transform node id → the node its edge goes to
```

the loop that fills `forward` with

```go
	for _, e := range f.Edges {
		if t := byID[e.Source].Type; t == "consumer" || t == "transform" {
			out[e.Source] = e.Target
		}
	}
```

and, in the consumer case, the `spec := …` line and its sink lines with

```go
			spec := NodeSpec{Flow: f.ID, Node: dst.ID, Type: "consumer", Topic: topicName[src.ID], Group: d.Group, AutoOffsetReset: d.AutoOffsetReset}
			if d.Sink.Kind == "http" {
				spec.SinkURL = d.Sink.URL
			}
			next := out[dst.ID] // a topic, a transform, or "" when the consumer forwards nothing
			if t := byID[next]; t.Type == "transform" {
				var td TransformData
				json.Unmarshal(t.Data, &td)
				spec.Transform, spec.TransformNode = td.Expr, t.ID
				next = out[t.ID]
			}
			spec.Forward = topicName[next]
```

(the `for _, i := range instancesOf(dst)` loop after it stays). Delete `notYetRunnable` and its comment from the end of the file.

`studio/engine.go`: in `Deploy`, delete the three lines that call `notYetRunnable`.

`studio/node.go`: add `"github.com/expr-lang/expr/vm"` to the imports (before the franz-go import). Replace `nodeStats` and add `stepStats` after it:

```go
// nodeStats is what GET /stats answers; the control plane adds the container state.
type nodeStats struct {
	Boot      string               `json:"boot"`  // random per process: a restarted container starts its counters and tail over
	Total     int64                `json:"total"` // records produced (producers) or fetched (consumers)
	Errors    int64                `json:"errors"`
	LastError string               `json:"lastError"`
	TailSeq   int64                `json:"tailSeq"`         // seq of the newest tail record; the drawer fetches when it moves
	Steps     map[string]stepStats `json:"steps,omitempty"` // a consumer's transform, by its node id
}

// stepStats counts what a consumer's transform did: every record it got, the
// ones it failed on, and the last failure.
type stepStats struct {
	Total     int64  `json:"total"`
	Errors    int64  `json:"errors"`
	LastError string `json:"lastError"`
}
```

Add before `(*counters).stats`:

```go
func (c *counters) step() stepStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return stepStats{Total: c.total.Load(), Errors: c.errors.Load(), LastError: c.lastError}
}
```

Replace the `consumer` type and its comment with:

```go
// consumer takes each fetched record of one consumer node through the tail, the
// http sink (when set), the transform (when set) and the forward (when set). A
// failed sink, transform or forward is counted and logged, not retried:
// autocommit still moves past the record. The sink and the forward run under
// context.WithoutCancel: a stop (SIGTERM) must not fail them with "context
// canceled" before Close commits past this record.
type consumer struct {
	spec      NodeSpec
	tail      *tail
	counts    *counters
	transform *vm.Program                                              // nil without a transform
	steps     *counters                                                // the transform's own counts
	post      func(ctx context.Context, url string, body []byte) error // the http sink
	produce   func(context.Context, *kgo.Record) error                 // the forward
}
```

In `handle`, insert between the sink block and the forward block:

```go
	value := r.Value
	if c.transform != nil {
		c.steps.ok()
		out, keep, err := runTransform(c.transform, value)
		if err != nil {
			c.steps.fail(err)
			log.Printf("transform: %v", err)
		}
		if !keep {
			return true // failed or dropped (nil): nothing to forward
		}
		value = out
	}
```

and make the forward produce `Value: value` instead of `Value: r.Value`.

In `runNode`, right after `t, counts, boot := &tail{}, &counters{}, NewID()`, add

```go
	var steps *counters // a consumer's transform counts; nil without one
```

make the `GET /stats` handler

```go
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s := counts.stats(boot, t.last())
		if steps != nil {
			s.Steps = map[string]stepStats{spec.TransformNode: steps.step()}
		}
		reply(w, http.StatusOK, s)
	})
```

and, right after `c := &consumer{spec: spec, tail: t, counts: counts, post: postJSON, produce: produce}`, add

```go
		if spec.Transform != "" {
			p, err := compileTransform(spec.Transform)
			if err != nil {
				log.Fatal("transform: ", err) // Validate compiled the same source on deploy
			}
			steps = &counters{}
			c.transform, c.steps = p, steps
		}
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `cd studio && go mod tidy && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok  kafka-playground/studio`; `gofmt -l` prints nothing (run `gofmt -w` on edited files: the new struct fields shift alignment); `go.mod` lists `github.com/expr-lang/expr v1.17.8` among the direct requires.

- [ ] **Step 6: Run the end-to-end check**

The node runtime changed, so: `make down && make verify`, then `make down`.
Expected: `STUDIO OK (...)` and `VERIFY OK` (no transform in `verify-studio` yet; Task 2 adds it); `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 7: Commit**

```bash
git add studio/go.mod studio/go.sum studio/transform.go studio/transform_test.go studio/flow.go studio/flow_test.go studio/resolve.go studio/resolve_test.go studio/engine.go studio/node.go studio/node_test.go
git commit -m "studio: transforms compile on deploy and run in their consumer" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Transform nodes in the snapshot; `verify-studio` runs one

**Files:**
- Modify: `studio/engine.go`, `Makefile`
- Test: `studio/engine_test.go`

**Interfaces:**
- Consumes: Task 1's `nodeStats.Steps`, `stepStats`, `NodeSpec.TransformNode`; M4's `NodeState`, `containers()`, `withStats`, `Snapshot`, `withRates`.
- Produces: `NodeState.steps map[string]stepStats` (unexported, never in JSON); `applySteps(st *FlowState, specs []NodeSpec)`.

- [ ] **Step 1: Write the failing test**

Append to `studio/engine_test.go`:

```go
func TestApplySteps(t *testing.T) {
	specs := []NodeSpec{
		{Node: "consumer-1", Type: "consumer", Instance: 1, TransformNode: "transform-1"},
		{Node: "consumer-1", Type: "consumer", Instance: 2, TransformNode: "transform-1"},
		{Node: "consumer-2", Type: "consumer", TransformNode: "transform-2"},
		{Node: "consumer-3", Type: "consumer"},
	}
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"consumer-1": {State: "exited", Instances: []NodeState{
			{Instance: 1, State: "running", Boot: "a", steps: map[string]stepStats{"transform-1": {Total: 5, Errors: 1, LastError: "invalid operation"}}},
			{Instance: 2, State: "exited"},
		}},
		"consumer-2": {State: "running", Boot: "c", steps: map[string]stepStats{"transform-2": {Total: 3}}},
		"consumer-3": {State: "running"},
	}}
	applySteps(&st, specs)
	want := map[string]NodeState{
		"transform-1": {State: "exited", Total: 5, Errors: 1, LastError: "#1: invalid operation", Boot: "a"},
		"transform-2": {State: "running", Total: 3, Boot: "c"},
	}
	for id, w := range want {
		if got := st.Nodes[id]; !reflect.DeepEqual(got, w) {
			t.Errorf("%s:\n got %+v\nwant %+v", id, got, w)
		}
	}
	if len(st.Nodes) != 5 {
		t.Fatalf("only the two transform nodes are added, got %v", st.Nodes)
	}
}
```

- [ ] **Step 2: Run the test to see it fail**

Run: `cd studio && go test -run TestApplySteps ./...`
Expected: FAIL to compile — `unknown field steps in struct literal`, `undefined: applySteps`.

- [ ] **Step 3: Implement**

In `studio/engine.go`, add `"cmp"` and `"strings"` to the imports. Add a last, unexported field to `NodeState`, after `Instances`:

```go
	Instances  []NodeState        `json:"instances,omitempty"`

	steps map[string]stepStats // a consumer container's transform counts, by node id; applySteps moves them to the transform node
}
```

In `Snapshot`, move `specs, _ := Resolve(f)` up to right after the loop that asks every container for its stats, and call `applySteps` there, so the code reads:

```go
	for node, ns := range st.Nodes {
		for _, c := range ns.containers() {
			if c.State == "running" {
				*c = withStats(ctx, nodeRef{id, node, c.Instance}, *c)
			}
		}
		ns.sumInstances()
		st.Nodes[node] = ns
	}
	specs, _ := Resolve(f)
	applySteps(&st, specs)
```

(the `topics := …` block follows, unchanged; remove the old `specs, _ := Resolve(f)` line further down). In `withStats`, keep the steps:

```go
	ns.Total, ns.Errors, ns.LastError, ns.TailSeq, ns.Boot = s.Total, s.Errors, s.LastError, s.TailSeq, s.Boot
	ns.steps = s.Steps
	return ns
}
```

and add after `withStats`:

```go
// applySteps gives each transform node its consumer node's state and the counts
// the consumer's containers report for it, summed (a last error is the first
// container's that has one, prefixed with its instance). Its boot joins theirs,
// so a container that restarted gives the transform no rate rather than a wrong one.
func applySteps(st *FlowState, specs []NodeSpec) {
	done := map[string]bool{}
	for _, s := range specs {
		if s.TransformNode == "" || done[s.TransformNode] {
			continue
		}
		done[s.TransformNode] = true
		consumer := st.Nodes[s.Node]
		t := NodeState{State: cmp.Or(consumer.State, "missing")}
		var boots []string
		for _, c := range consumer.containers() {
			step, ok := c.steps[s.TransformNode]
			if !ok {
				continue
			}
			t.Total += step.Total
			t.Errors += step.Errors
			if t.LastError == "" && step.LastError != "" {
				t.LastError = step.LastError
				if c.Instance > 0 {
					t.LastError = fmt.Sprintf("#%d: %s", c.Instance, step.LastError)
				}
			}
			boots = append(boots, c.Boot)
		}
		t.Boot = strings.Join(boots, ",")
		st.Nodes[s.TransformNode] = t
	}
}
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok  kafka-playground/studio`; `gofmt -l` prints nothing.

- [ ] **Step 5: `verify-studio` runs a transform flow**

The flow: a manual producer → `studio-verify-t-in` → consumer-1 → transform-1 `{id: msg.id, total: msg.qty * msg.price}` → `studio-verify-t-out` → consumer-2. First saved with `msg.` instead of the expression, so its deploy must answer 422; then fixed and deployed. Two records: `t1-<n>` with `qty` 2 and `price` 3 must reach consumer-2 as `total` 6; `t2-<n>` has no `qty`, so the transform counts it as an error and does not forward it.

In `Makefile`, make the `verify-studio` help comment

```make
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, live ticks, chained flows, instances, transforms, stop, delete
```

and insert these lines right before the recipe's last line (`⇥echo "STUDIO OK ($$id)"`), after the instances block; every line starts with a real tab (`$$manual`, `create` and `flows` come from earlier in the recipe):

```make
	x='{"name":"verify-transform","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":'"$$manual"'},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify-t-in","partitions":1,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify-t","auto_offset_reset":"earliest","sink":{"kind":"log"}}},{"id":"transform-1","type":"transform","position":{"x":600,"y":0},"data":{"expr":"{id: msg.id, total: msg.qty * msg.price}"}},{"id":"topic-2","type":"topic","position":{"x":800,"y":0},"data":{"name":"studio-verify-t-out","partitions":1,"replication_factor":1}},{"id":"consumer-2","type":"consumer","position":{"x":1000,"y":0},"data":{"group":"studio-verify-t-out","auto_offset_reset":"earliest","sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"},{"id":"e3","source":"consumer-1","target":"transform-1"},{"id":"e4","source":"transform-1","target":"topic-2"},{"id":"e5","source":"topic-2","target":"consumer-2"}]}'; \
	xid=$$(create "$$(echo "$$x" | sed 's/msg.qty \* msg.price/msg./')" "the transform flow") || exit 1; flows="$$flows $$xid"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows/$$xid/deploy); \
	[ "$$code" = 422 ] || { echo "STUDIO FAILED: a transform that does not compile deployed ($$code)"; exit 1; }; \
	curl -sS --fail-with-body -X PUT $(STUDIO_URL)/api/flows/$$xid -H 'Content-Type: application/json' --data "$$x" >/dev/null || { echo "STUDIO FAILED: save the transform flow"; exit 1; }; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$xid/deploy >/dev/null || { echo "STUDIO FAILED: deploy the transform flow"; exit 1; }; \
	rec="$$(date +%s)"; \
	for v in "{\"id\":\"t1-$$rec\",\"qty\":2,\"price\":3}" "{\"id\":\"t2-$$rec\"}"; do \
		curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$xid/nodes/producer-1/send --data "$$v" >/dev/null || { echo "STUDIO FAILED: send to the transform flow"; exit 1; }; \
	done; \
	for i in $$(seq 30); do \
		curl -sS $(STUDIO_URL)/api/flows/$$xid/state | grep -q '"transform-1":{"state":"running","total":2,"errors":1,' && \
			curl -sS "$(STUDIO_URL)/api/flows/$$xid/nodes/consumer-2/tail?since=0" | grep -q "t1-$$rec" && break; \
		[ "$$i" = 30 ] && { echo "STUDIO FAILED: want 2 records and 1 error on the transform and t1-$$rec downstream: $$(curl -sS $(STUDIO_URL)/api/flows/$$xid/state)"; exit 1; }; sleep 1; \
	done; \
	out=$$(curl -sS "$(STUDIO_URL)/api/flows/$$xid/nodes/consumer-2/tail?since=0"); \
	echo "$$out" | grep -qF 'total\":6' && ! echo "$$out" | grep -q "t2-$$rec" || { echo "STUDIO FAILED: want t1-$$rec transformed (total 6) and t2-$$rec not forwarded: $$out"; exit 1; }; \
	echo "studio transform: $$(curl -sS $(STUDIO_URL)/api/flows/$$xid/state | grep -o '"transform-1":{[^}]*}')"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$xid || { echo "STUDIO FAILED: delete the transform flow"; exit 1; }; \
```

- [ ] **Step 6: Run the end-to-end check**

Run: `make test && make down && make verify`
Expected: a line `studio transform: "transform-1":{"state":"running","total":2,"errors":1,"lastError":"invalid operation: \u003cnil\u003e * \u003cnil\u003e (1:29)","boot":"…"}`, then `STUDIO OK (...)` and `VERIFY OK`. Then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 7: Commit**

```bash
git add studio/engine.go studio/engine_test.go Makefile
git commit -m "studio: transform nodes show their counts; verify runs a transform flow" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The palette offers Transform; docs

**Files:**
- Modify: `studio/ui/src/Palette.tsx`, `studio/ui/src/Inspector.tsx`, `README.md`, `AGENTS.md`, `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`

**Interfaces:**
- Consumes: Tasks 1–2 behaviour (nothing in the UI's types changes: a Transform node's live numbers come through the existing `NodeRuntime` fields and `runtimeLine`).

- [ ] **Step 1: Palette and Inspector**

In `studio/ui/src/Palette.tsx`:

```tsx
const ITEMS: NodeType[] = ['producer', 'topic', 'consumer', 'transform']
```

(the `// transform joins in M5` comment goes) and the hint:

```tsx
      <p className="hint">Drag onto the canvas, then wire Producer → Topic → Consumer, and on to a Topic, directly or through a Transform. Backspace deletes.</p>
```

In `studio/ui/src/Inspector.tsx`, the transform form's hint becomes:

```tsx
          <p className="hint">An expr-lang expression over msg, the record's value decoded from JSON, e.g. {'{id: msg.id, total: msg.qty * msg.price}'}. Its result is forwarded with the same key; nil drops the record. Deploy checks that it compiles.</p>
```

Run: `cd studio/ui && npm run build`. Expected: `✓ built`. A Transform node needs no other UI change: `StudioNodes`' `Shell` shows its state badge and, while its consumer runs, `runtimeLine`'s count, rate and errors (the last error as the tooltip).

- [ ] **Step 2: README**

In the "Flows" list, the first bullet becomes:

```markdown
- Edges: Producer → Topic, Topic → Consumer, then Consumer → Topic (a forward) or Consumer → Transform → Topic. The editor refuses other wires and the server rejects them on save.
```

In the "Nodes" list, insert after the **Consumer** bullet:

```markdown
- **Transform**: an [expr-lang](https://expr-lang.org) expression over `msg`, the record's value decoded from JSON, e.g. `{id: msg.id, total: msg.qty * msg.price}`. It runs in its consumer's containers, after the sink and before the forward: its result is forwarded with the record's key, and `nil` drops the record (a filter: `msg.qty > 0 ? msg : nil`). A deploy refuses an expression that does not compile (422, naming the node). A value that is not JSON, a failing expression (a missing field gives `invalid operation: <nil> * <nil>`) or a result JSON cannot hold is counted on the Transform node, and that record is not forwarded. The node shows its consumer's state and its own counts.
```

- [ ] **Step 3: AGENTS.md**

In "Layout", the `studio/` bullet's file list becomes `(main.go, api.go, flow.go, store.go, resolve.go, engine.go, docker.go, kafka.go, transform.go)`. Add to "Rules", after the `nodeRef` rule:

```markdown
- A transform has no container: `Resolve` gives its expression and node id to the upstream consumer's `NodeSpec`, the consumer reports its counts under that id in `/stats` `steps`, and `applySteps` puts them on the Transform node. Expressions compile in `Validate` (deploy) and again in the consumer, through `compileTransform` (`transform.go`).
```

- [ ] **Step 4: The spec**

In §7's M5 section, add after the demo bullet:

```markdown
- Built as decided in its plan: `expr-lang/expr` v1.17.8; `msg` is declared as a map so field access compiles, and is at run time whatever the value decodes to (numbers as float64); compile and run errors keep their first line; the transform's `total` counts every record it got, dropped ones included; a Transform node's `boot` joins its consumer containers', so its rate behaves like any node's; nothing is refused for its milestone any more.
```

- [ ] **Step 5: Check and commit**

Run: `make test`. Expected: passes.

```bash
git add studio/ui/src/Palette.tsx studio/ui/src/Inspector.tsx README.md AGENTS.md docs/superpowers/specs/2026-10-06-pipeline-studio-design.md
git commit -m "Transform in the palette; docs for Studio M5" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
