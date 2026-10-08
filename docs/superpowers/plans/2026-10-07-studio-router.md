# Pipeline Studio Router node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Router node that sends each record a consumer reads (after its Transform, if any) to the topic of the first rule whose condition holds, to a default topic, or nowhere, with per-branch counts on the canvas.

**Architecture:** Like a Transform, a Router has no container. `Resolve` folds its rules into the upstream consumer's `NodeSpec`. The consumer runs them per record after the transform, picking the forward topic. It reports the counts in `/stats` as `route`, and the snapshot's `applySteps` puts them on the Router node. Validation lives in `flow.go` and checks that the rules and the router's edges agree. The UI edits the rules in the Inspector and labels the router's edges with derived, never-saved text.

**Tech Stack:** Go 1.27 (expr-lang/expr v1.17.8, franz-go), React 19 + @xyflow/react 12.12.0 + TypeScript, Playwright 1.63.0, GNU make 3.81.

**Spec:** `docs/superpowers/specs/2026-10-07-studio-router-design.md`. The Studio spec (`docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`) stays the authority for everything the router spec does not change.

Every task below was applied, in order, to a scratch worktree of `0cd5eb8` and checked there. `make test` passed, and `make down && make verify` ended with `STUDIO OK`, `14 passed`, `UI OK` and `VERIFY OK`. Afterwards, `docker ps -aq -f label=studio.flow` and `git status --short flows` printed nothing. Each code block is exact: apply it as written, and every "replace" names text that occurs once in the file.

## Global Constraints

- No new dependencies and no version changes: expr-lang/expr stays v1.17.8, @xyflow/react 12.12.0, Playwright 1.63.0. Pin every image to an exact version; never `latest`.
- Local only: no auth, no TLS, ports on `127.0.0.1`.
- `studio/flow.go` (node types, data structs, `allowedEdges`), `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` change together. Every other validation rule lives only in `flow.go`.
- `NodeState` (`studio/engine.go`) and `NodeRuntime` (`studio/ui/src/flow/api.ts`) change together. The new fields `branches` and `unmatched` go just before `instances`, because `verify-studio` greps the JSON and relies on its field order.
- Rules are ordered and the first match wins. With no match and no default, the record is dropped and counted as `unmatched`, which is not an error. A failing condition is a router error: the record is not forwarded, not even to the default.
- Edges keep the shape `{id, source, target}`. The labels `#1`, `#2`, `default`, plus `· n` while running, are derived in the canvas and never saved.
- Makefile: GNU make 3.81 with BSD tools, recipes indented with real tabs, a shell `$` written as `$$`.
- The UI tests own the `studio-ui-` namespace: every flow, topic and group a test makes starts with it.
- Go code passes `gofmt -l` with no output.
- Keep `README.md`, `AGENTS.md` and the specs in sync with behaviour.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Where the plan departs from the spec

Task 7 writes each of these back into the router spec.

- **Save checks:** Save checks only the nodes and edges, as it does for every node type. The router's data is checked on deploy. The spec's §3 said Save decodes the router's data.
- **Router errors:** a router error also counts as its consumer's error (`router: …`), as a transform's failure does.
- **Empty topic:** a rule with no topic picked gets its own message, `rule <n>: pick a topic`.

## Review Focus

Inputs the spec implies but does not spell out. Each line names the test that pins it.

1. **A condition that is not true/false when it runs.** For example `msg.flag` on `{"flag": 1}`. It is a router error (`rule 1: …`), not a fall-through to the default. Pinned in Task 2, `TestRoute` (`notBoolean`).
2. **The record after a failed one.** It is routed normally: the reused VM survives a failing rule. Pinned in Task 2, `TestRoute` (the last row, `{"total":200}`).
3. **Several rules, and the default, sending to one topic.** It is valid, and Deploy accepts it. Pinned in Task 1, `TestValidate` ("router: several rules and the default to one topic").
4. **A record the transform drops (`nil`) or fails on.** It never reaches the router: it is neither counted there nor unmatched. Pinned in Task 2, `TestConsumerRouter` (the third and fourth records).
5. **A rule whose topic was never picked.** This is "Add rule" while no topic is wired. Deploy says `rule 1: pick a topic`, not that the topic `""` is not wired. Pinned in Task 1, `TestValidate` ("router rule with no topic picked").

---

### Task 1: Router data, validation and Resolve (Go)

The `router` node type: its data, the edge pairs, the deploy checks, and how `Resolve` folds it into its consumer's spec. `compileRule` starts `router.go`, which Task 2 extends.

**Files:**
- Create: `studio/router.go`
- Modify: `studio/flow.go`
- Modify: `studio/resolve.go`
- Test: `studio/flow_test.go`
- Test: `studio/resolve_test.go`

**Interfaces:**
- Consumes: `transformEnv` and `firstLine` (`studio/transform.go`, unchanged); the test helpers `node`, `edge`, `clone`, `good` (`studio/flow_test.go`).
- Produces:
  - `type RouterData struct { Rules []Rule; Default string }` and `type Rule struct { When, To string }` (`flow.go`, JSON `rules`, `default`, `when`, `to`).
  - `type Route struct { When, Topic string }` and the `NodeSpec` fields `Routes []Route`, `RouteDefault string`, `RouterNode string` (`resolve.go`).
  - `func compileRule(src string) (*vm.Program, error)` (`router.go`): a one-line error when it does not compile, or is known not to be a boolean.
  - The test helpers `addRouter(f *Flow, data string, targets ...string)` and `addRouterAfter(f *Flow, from, data string, targets ...string)` (`flow_test.go`), used again in `resolve_test.go`.

- [ ] **Step 1: Write the failing tests**

In `studio/flow_test.go`, replace:

```go
		{"forward to the topic it reads", Deploy, func(f *Flow) { f.Edges = append(f.Edges, edge("consumer-1", "topic-1")) }, `forwarding loops back to topic "orders"`},
```

with:

```go
		{"router valid", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"msg.total > 100","to":"topic-2"}],"default":"topic-3"}`, "topic-2", "topic-3")
		}, ""},
		{"router: several rules and the default to one topic", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"msg.a","to":"topic-2"},{"when":"msg.b","to":"topic-2"}],"default":"topic-2"}`, "topic-2")
		}, ""},
		{"router rule with no topic picked", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"true","to":""}]}`)
		}, "rule 1: pick a topic"},
		{"router after a transform", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"))
			addRouterAfter(f, "transform-1", `{"rules":[{"when":"true","to":"topic-2"}]}`, "topic-2")
		}, ""},
		{"router half-built saves", Save, func(f *Flow) { addRouter(f, `{"rules":[{"when":"","to":"topic-9"}]}`) }, ""},
		{"router without rules", Deploy, func(f *Flow) { addRouter(f, `{"rules":[]}`) }, "a router needs at least one rule"},
		{"router rule without a condition", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":" ","to":"topic-2"}]}`, "topic-2")
		}, "rule 1: when is required"},
		{"router rule that is not boolean", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"\"x\"","to":"topic-2"}]}`, "topic-2")
		}, "rule 1: when: expected bool, but got string"},
		{"router rule that does not compile", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"true","to":"topic-2"},{"when":"msg.","to":"topic-2"}]}`, "topic-2")
		}, "rule 2: when: unexpected end of expression"},
		{"router rule to an unwired topic", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"true","to":"topic-9"}]}`)
		}, `rule 1: topic "topic-9" is not wired to the router`},
		{"router default to an unwired topic", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"true","to":"topic-2"}],"default":"topic-9"}`, "topic-2")
		}, `default: topic "topic-9" is not wired to the router`},
		{"router edge without a rule", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"true","to":"topic-2"}]}`, "topic-2", "topic-3")
		}, "its edge to topic-3 has no rule: add one, or make it the default"},
		{"router fed by nothing", Deploy, func(f *Flow) {
			addRouter(f, `{"rules":[{"when":"true","to":"topic-2"}]}`, "topic-2")
			f.Edges = slices.DeleteFunc(f.Edges, func(e Edge) bool { return e.Target == "router-1" })
		}, "a router needs exactly one edge from a consumer or a transform"},
		{"router routes back to the topic it reads", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("router-1", "router", `{"rules":[{"when":"true","to":"topic-1"}]}`))
			f.Edges = append(f.Edges, edge("consumer-1", "router-1"), edge("router-1", "topic-1"))
		}, `forwarding loops back to topic "orders"`},
		{"forward to the topic it reads", Deploy, func(f *Flow) { f.Edges = append(f.Edges, edge("consumer-1", "topic-1")) }, `forwarding loops back to topic "orders"`},
```

In `studio/flow_test.go`, replace:

```go
import (
	"encoding/json"
```

with:

```go
import (
	"encoding/json"
	"slices"
```

Append to the end of `studio/flow_test.go`:

```go
// addRouter wires consumer-1 → router-1 with data, and router-1 to a new topic
// for each id in targets (named after it).
func addRouter(f *Flow, data string, targets ...string) {
	addRouterAfter(f, "consumer-1", data, targets...)
}

func addRouterAfter(f *Flow, from, data string, targets ...string) {
	f.Nodes = append(f.Nodes, node("router-1", "router", data))
	f.Edges = append(f.Edges, edge(from, "router-1"))
	for _, t := range targets {
		f.Nodes = append(f.Nodes, node(t, "topic", `{"name":"`+t+`","partitions":1,"replication_factor":1}`))
		f.Edges = append(f.Edges, edge("router-1", t))
	}
}
```

Append to the end of `studio/resolve_test.go`:

```go
func TestResolveRouter(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`))
	f.Edges = append(f.Edges, edge("consumer-1", "transform-1"))
	addRouterAfter(&f, "transform-1", `{"rules":[{"when":"msg.total > 100","to":"topic-2"},{"when":"true","to":"topic-3"}],"default":"topic-2"}`, "topic-2", "topic-3")
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "orders-workers",
		Transform: "msg", TransformNode: "transform-1",
		Routes: []Route{{When: "msg.total > 100", Topic: "topic-2"}, {When: "true", Topic: "topic-3"}}, RouteDefault: "topic-2", RouterNode: "router-1"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("a router runs in its consumer, after its transform, with its rules as topic names:\n got %+v\nwant %+v", specs, want)
	}
	if len(topics) != 3 {
		t.Fatalf("want all three topics created, got %+v", topics)
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL, a build failure naming `undefined: Route`.

- [ ] **Step 3: Implement**

In `studio/flow.go`, replace:

```go
type TransformData struct {
	Expr string `json:"expr"`
}
```

with:

```go
type TransformData struct {
	Expr string `json:"expr"`
}

// RouterData is a router's ordered rules: a record goes to the topic of the first
// rule whose condition holds, else to Default, else nowhere.
type RouterData struct {
	Rules   []Rule `json:"rules"`
	Default string `json:"default"` // a topic node id; "" for none
}

type Rule struct {
	When string `json:"when"` // an expr over msg that yields true or false
	To   string `json:"to"`   // a topic node id
}
```

In `studio/flow.go`, replace:

```go
	"consumer":  {"topic": true, "transform": true},
	"transform": {"topic": true},
}
```

with:

```go
	"consumer":  {"topic": true, "transform": true, "router": true},
	"transform": {"topic": true, "router": true},
	"router":    {"topic": true},
}
```

In `studio/flow.go`, replace:

```go
				add(n.ID, "", "a consumer may forward to at most one topic or transform")
```

with:

```go
				add(n.ID, "", "a consumer may forward to at most one topic, transform or router")
```

In `studio/flow.go`, replace:

```go
			if inDegree[n.ID] != 1 || outDegree[n.ID] != 1 {
				add(n.ID, "", "a transform needs one edge from a consumer and one edge to a topic")
			}
		}
	}
```

with:

```go
			if inDegree[n.ID] != 1 || outDegree[n.ID] != 1 {
				add(n.ID, "", "a transform needs one edge from a consumer and one edge to a topic or a router")
			}
		case "router":
			var d RouterData
			if !decodeData(n, &d, add) {
				continue
			}
			if inDegree[n.ID] != 1 {
				add(n.ID, "", "a router needs exactly one edge from a consumer or a transform")
			}
			if len(d.Rules) == 0 {
				add(n.ID, "", "a router needs at least one rule")
			}
			wired := map[string]bool{} // the topics the router has edges to
			for _, to := range next[n.ID] {
				wired[to] = true
			}
			used := map[string]bool{} // the topics a rule or the default names
			for i, r := range d.Rules {
				if strings.TrimSpace(r.When) == "" {
					add(n.ID, "", "rule %d: when is required", i+1)
				} else if _, err := compileRule(r.When); err != nil {
					add(n.ID, "", "rule %d: when: %v", i+1, err)
				}
				switch {
				case r.To == "":
					add(n.ID, "", "rule %d: pick a topic", i+1)
				case !wired[r.To]:
					add(n.ID, "", "rule %d: topic %q is not wired to the router", i+1, r.To)
				}
				used[r.To] = true
			}
			if d.Default != "" {
				if !wired[d.Default] {
					add(n.ID, "", "default: topic %q is not wired to the router", d.Default)
				}
				used[d.Default] = true
			}
			for _, to := range next[n.ID] {
				if !used[to] {
					add(n.ID, "", "its edge to %s has no rule: add one, or make it the default", to)
				}
			}
		}
	}
```

Create `studio/router.go`:

```go
// Routers: ordered rules over msg, the record's value (after its consumer's
// transform) decoded from JSON. A record goes to the topic of the first rule
// whose condition holds, else to the default topic, else nowhere. Validate
// compiles every rule on deploy; the consumer that runs a router compiles them
// again when it starts.
package main

import (
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// compileRule compiles a rule's condition in a transform's environment. It must
// yield true or false: a condition known not to (a string, a number) does not
// compile; with msg declared any, one of unknown type does, and a result that is
// not a boolean is an error when it runs.
func compileRule(src string) (*vm.Program, error) {
	p, err := expr.Compile(src, expr.Env(transformEnv{}), expr.AsBool())
	if err != nil {
		return nil, firstLine(err)
	}
	return p, nil
}
```

In `studio/resolve.go`, replace:

```go
type NodeSpec struct {
	Flow            string `json:"flow"`
	Node            string `json:"node"`
	Type            string `json:"type"`  // "producer" or "consumer"
	Topic           string `json:"topic"` // produced to, or consumed from
	Group           string `json:"group,omitempty"`
	AutoOffsetReset string `json:"auto_offset_reset,omitempty"`
	Key             string `json:"key,omitempty"`            // producer key template
	Value           string `json:"value,omitempty"`          // producer value template
	Source          string `json:"source,omitempty"`         // producer: "manual" or "timer"
	IntervalMS      int    `json:"interval_ms,omitempty"`    // producer: the timer's period
	Forward         string `json:"forward,omitempty"`        // consumer: the topic it forwards every record to
	SinkURL         string `json:"sink_url,omitempty"`       // consumer: the http sink's URL
	Instance        int    `json:"instance,omitempty"`       // consumer: 1..n when it runs n > 1 instances, else 0
	Transform       string `json:"transform,omitempty"`      // consumer: the expr its records pass through before the forward
	TransformNode   string `json:"transform_node,omitempty"` // consumer: that transform's node id, which its counts are reported under
}
```

with:

```go
type NodeSpec struct {
	Flow            string  `json:"flow"`
	Node            string  `json:"node"`
	Type            string  `json:"type"`  // "producer" or "consumer"
	Topic           string  `json:"topic"` // produced to, or consumed from
	Group           string  `json:"group,omitempty"`
	AutoOffsetReset string  `json:"auto_offset_reset,omitempty"`
	Key             string  `json:"key,omitempty"`            // producer key template
	Value           string  `json:"value,omitempty"`          // producer value template
	Source          string  `json:"source,omitempty"`         // producer: "manual" or "timer"
	IntervalMS      int     `json:"interval_ms,omitempty"`    // producer: the timer's period
	Forward         string  `json:"forward,omitempty"`        // consumer: the topic it forwards every record to
	SinkURL         string  `json:"sink_url,omitempty"`       // consumer: the http sink's URL
	Instance        int     `json:"instance,omitempty"`       // consumer: 1..n when it runs n > 1 instances, else 0
	Transform       string  `json:"transform,omitempty"`      // consumer: the expr its records pass through before the forward
	TransformNode   string  `json:"transform_node,omitempty"` // consumer: that transform's node id, which its counts are reported under
	Routes          []Route `json:"routes,omitempty"`         // consumer: its router's rules, in order; set, they choose the forward
	RouteDefault    string  `json:"route_default,omitempty"`  // consumer: the router's default topic; "" drops what no rule matches
	RouterNode      string  `json:"router_node,omitempty"`    // consumer: the router's node id, which its counts are reported under
}

// Route is one router rule as a consumer runs it: a condition and a topic name.
type Route struct {
	When  string `json:"when"`
	Topic string `json:"topic"`
}
```

In `studio/resolve.go`, replace:

```go
			next := out[dst.ID] // a topic, a transform, or "" when the consumer forwards nothing
			if t := byID[next]; t.Type == "transform" {
				var td TransformData
				json.Unmarshal(t.Data, &td)
				spec.Transform, spec.TransformNode = td.Expr, t.ID
				next = out[t.ID]
			}
			spec.Forward = topicName[next]
```

with:

```go
			next := out[dst.ID] // a topic, a transform, a router, or "" when the consumer forwards nothing
			if t := byID[next]; t.Type == "transform" {
				var td TransformData
				json.Unmarshal(t.Data, &td)
				spec.Transform, spec.TransformNode = td.Expr, t.ID
				next = out[t.ID]
			}
			if r := byID[next]; r.Type == "router" {
				var rd RouterData
				json.Unmarshal(r.Data, &rd)
				for _, rule := range rd.Rules {
					spec.Routes = append(spec.Routes, Route{When: rule.When, Topic: topicName[rule.To]})
				}
				spec.RouteDefault, spec.RouterNode = topicName[rd.Default], r.ID
				next = "" // the router chooses the forward
			}
			spec.Forward = topicName[next]
```

- [ ] **Step 4: Run the checks**

Run: `cd studio && test -z "$(gofmt -l .)" && go vet ./... && go test ./...`
Expected: `ok  	kafka-playground/studio`, and gofmt lists nothing.

- [ ] **Step 5: Commit**

```bash
git add studio/flow_test.go studio/resolve_test.go studio/flow.go studio/router.go studio/resolve.go
git commit -m "studio: router node data, deploy checks and Resolve — rules on the node, first match wins, rules and edges must agree" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The router at run time (Go)

The consumer runs its router after its transform: the first rule that holds picks the forward topic, else the default, else the record is dropped as unmatched. A failing condition is a router error and counts on the consumer too, like a transform's. `/stats` reports the counts as `route`. The JSON decoding a transform does moves into `decodeMsg`, which both use.

**Files:**
- Create: `studio/router_test.go`
- Modify: `studio/node.go`
- Modify: `studio/router.go`
- Modify: `studio/transform.go`
- Test: `studio/node_test.go`

**Interfaces:**
- Consumes: `compileRule`, `Route`, `NodeSpec.Routes` and `NodeSpec.RouteDefault` (Task 1); `counters`, `tally` and `errInvalidJSON` (`node.go`); `numbers` and `firstLine` (`transform.go`).
- Produces:
  - `func decodeMsg(value []byte) (any, error)` (`transform.go`).
  - `func newRouter(routes []Route, def string) (*router, error)`, `func (r *router) route(value []byte) (string, error)` and `func (r *router) read() routeTally` (`router.go`). `route` returns `""` with no error for an unmatched record.
  - `type routeTally struct { tally; Branches []int64; Unmatched int64 }`, whose JSON is `{total, errors, lastError, branches, unmatched}`. `Branches` has one entry per rule, then one for the default.
  - `nodeStats.Route *routeTally` (JSON `route`) and `consumer.router *router` (`node.go`).

- [ ] **Step 1: Write the failing tests**

Create `studio/router_test.go`:

```go
package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestCompileRule(t *testing.T) {
	for _, src := range []string{`msg.total > 100`, `msg.region == "EU"`, `msg.paid`} {
		if _, err := compileRule(src); err != nil {
			t.Errorf("%q: want it to compile, got %v", src, err)
		}
	}
	for src, want := range map[string]string{`"x"`: "expected bool, but got string", `1 + 2`: "expected bool, but got int", `msg.`: "unexpected end of expression"} {
		_, err := compileRule(src)
		if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "\n") {
			t.Errorf("%q: want a one-line error containing %q, got %v", src, want, err)
		}
	}
}

func TestRoute(t *testing.T) {
	routes := []Route{{When: "msg.total > 100", Topic: "big"}, {When: `msg.region == "EU"`, Topic: "eu"}}
	withDefault, err := newRouter(routes, "other")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		value, topic, errHas string
	}{
		{`{"total":150,"region":"EU"}`, "big", ""}, // both hold: the first rule wins
		{`{"total":5,"region":"EU"}`, "eu", ""},
		{`{"total":5}`, "other", ""},
		{`{"total":9007199254740993}`, "big", ""},            // integers exact, as in a transform
		{`{"region":"EU"}`, "", "rule 1: invalid operation"}, // nil > 100: the record is not routed, rule 2 is not tried
		{`{"total":5,"region":"EU"} x`, "", "not valid JSON"},
		{`{"total":200}`, "big", ""}, // a failed record leaves the VM fit for the next
	} {
		topic, err := withDefault.route([]byte(c.value))
		if c.errHas != "" {
			if err == nil || !strings.Contains(err.Error(), c.errHas) || strings.Contains(err.Error(), "\n") || topic != "" {
				t.Errorf("%s: want no topic and a one-line error containing %q, got %q %v", c.value, c.errHas, topic, err)
			}
		} else if err != nil || topic != c.topic {
			t.Errorf("%s: want %q, got %q %v", c.value, c.topic, topic, err)
		}
	}
	want := routeTally{tally: tally{Total: 7, Errors: 2, LastError: "value is not valid JSON"}, Branches: []int64{3, 1, 1}}
	if got := withDefault.read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("per rule, then the default:\n got %+v\nwant %+v", got, want)
	}

	notBoolean, err := newRouter([]Route{{When: "msg.flag", Topic: "flagged"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	if topic, err := notBoolean.route([]byte(`{"flag":1}`)); topic != "" || err == nil || !strings.HasPrefix(err.Error(), "rule 1: ") {
		t.Fatalf("a condition that is not a boolean when it runs is an error, not the default; got %q %v", topic, err)
	}

	noDefault, err := newRouter(routes, "")
	if err != nil {
		t.Fatal(err)
	}
	if topic, err := noDefault.route([]byte(`{"total":5}`)); topic != "" || err != nil {
		t.Fatalf("no rule matches and no default: dropped, not an error; got %q %v", topic, err)
	}
	want = routeTally{tally: tally{Total: 1}, Branches: []int64{0, 0, 0}, Unmatched: 1}
	if got := noDefault.read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("an unmatched record counts as unmatched:\n got %+v\nwant %+v", got, want)
	}
}
```

Append to the end of `studio/node_test.go`:

```go
func TestConsumerRouter(t *testing.T) {
	var forwarded []string
	tr, err := newTransform(`msg.qty > 0 ? {id: msg.id, total: msg.qty * msg.price} : nil`)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{
		spec:      NodeSpec{Topic: "orders"},
		tail:      &tail{},
		counts:    &counters{},
		transform: tr,
		router:    rt,
		produce: func(_ context.Context, r *kgo.Record) error {
			forwarded = append(forwarded, r.Topic+":"+string(r.Key)+"="+string(r.Value))
			return nil
		},
	}
	for _, v := range []string{`{"id":1,"qty":2,"price":100}`, `{"id":2,"qty":1,"price":5}`, `{"id":3,"qty":1,"price":"x"}`, `{"id":4,"qty":0}`} {
		if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Key: []byte("k"), Value: []byte(v)}) {
			t.Fatalf("%s: a router outcome never holds back the commit", v)
		}
	}
	// The first is routed transformed and with its key; the second matches no rule
	// and has no default: dropped. The third fails in the transform (1 * "x") and
	// the fourth is dropped by it (nil): neither reaches the router.
	if len(forwarded) != 1 || forwarded[0] != `big:k={"id":1,"total":200}` {
		t.Fatalf(`want only big:k={"id":1,"total":200} forwarded, got %q`, forwarded)
	}
	if r := rt.read(); r.Total != 2 || r.Unmatched != 1 || r.Errors != 0 || r.Branches[0] != 1 {
		t.Fatalf("the router got 2 records: 1 routed by rule 1, 1 unmatched; got %+v", r)
	}

	failing, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	c.transform, c.router, forwarded = nil, failing, nil
	if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"id":4}`)}) {
		t.Fatal("a router error never holds back the commit")
	}
	if len(forwarded) != 0 {
		t.Fatalf("a failing rule routes the record nowhere, not to the default; got %q", forwarded)
	}
	if s := c.counts.read(); !strings.HasPrefix(s.LastError, "router: rule 1: ") {
		t.Fatalf("the consumer counts the router's failure as its own; got %+v", s)
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL, a build failure naming `undefined: newRouter`.

- [ ] **Step 3: Implement**

In `studio/transform.go`, replace:

```go
func (t *transform) eval(value []byte) (out []byte, err error) {
	d := json.NewDecoder(bytes.NewReader(value))
	d.UseNumber()
	var msg any
	if err := d.Decode(&msg); err != nil {
		return nil, errInvalidJSON
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errInvalidJSON // trailing data after the value
	}
	res, err := t.vm.Run(t.program, transformEnv{Msg: numbers(msg)})
```

with:

```go
func (t *transform) eval(value []byte) (out []byte, err error) {
	msg, err := decodeMsg(value)
	if err != nil {
		return nil, err
	}
	res, err := t.vm.Run(t.program, transformEnv{Msg: msg})
```

In `studio/transform.go`, replace:

```go
// numbers turns every JSON number
```

with:

```go
// decodeMsg decodes a record's value into msg, as transforms and routers see it.
func decodeMsg(value []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(value))
	d.UseNumber()
	var msg any
	if err := d.Decode(&msg); err != nil {
		return nil, errInvalidJSON
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errInvalidJSON // trailing data after the value
	}
	return numbers(msg), nil
}

// numbers turns every JSON number
```

In `studio/router.go`, replace:

```go
import (
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)
```

with:

```go
import (
	"fmt"
	"sync/atomic"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)
```

Append to the end of `studio/router.go`:

```go
// router is one consumer's compiled rules, the VM they run on (a consumer
// handles one record at a time) and its own counts.
type router struct {
	rules     []*vm.Program
	topics    []string // rule i's topic
	def       string   // the default topic; "" drops what no rule matches
	vm        vm.VM
	counts    counters
	branches  []atomic.Int64 // records per rule, then the default's
	unmatched atomic.Int64   // records dropped: no rule matched, no default
}

// routeTally is what /stats reports for a consumer's router.
type routeTally struct {
	tally
	Branches  []int64 `json:"branches"`  // per rule, then the default (0 without one)
	Unmatched int64   `json:"unmatched"` // dropped: no rule matched, no default
}

// newRouter compiles routes for one consumer to run.
func newRouter(routes []Route, def string) (*router, error) {
	r := &router{def: def, branches: make([]atomic.Int64, len(routes)+1)}
	for i, rt := range routes {
		p, err := compileRule(rt.When)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
		r.rules = append(r.rules, p)
		r.topics = append(r.topics, rt.Topic)
	}
	return r, nil
}

// route picks a value's topic and counts the outcome. "" with no error drops the
// record (no rule matched, no default), which is not an error.
func (r *router) route(value []byte) (string, error) {
	r.counts.ok()
	topic, err := r.pick(value)
	switch {
	case err != nil:
		r.counts.fail(err)
	case topic == "":
		r.unmatched.Add(1)
	}
	return topic, err
}

// pick is route without the outcome's counting: the first rule that holds, else
// the default. A rule that fails stops it, so a broken rule never falls through.
func (r *router) pick(value []byte) (string, error) {
	msg, err := decodeMsg(value)
	if err != nil {
		return "", err
	}
	for i, p := range r.rules {
		res, err := r.vm.Run(p, transformEnv{Msg: msg})
		if err != nil {
			return "", fmt.Errorf("rule %d: %w", i+1, firstLine(err))
		}
		if res == true {
			r.branches[i].Add(1)
			return r.topics[i], nil
		}
	}
	if r.def != "" {
		r.branches[len(r.rules)].Add(1)
	}
	return r.def, nil
}

func (r *router) read() routeTally {
	t := routeTally{tally: r.counts.read(), Branches: make([]int64, len(r.branches)), Unmatched: r.unmatched.Load()}
	for i := range r.branches {
		t.Branches[i] = r.branches[i].Load()
	}
	return t
}
```

In `studio/node.go`, replace:

```go
// network: POST /send (producers), GET /tail?since=N and GET /stats. A consumer
// also posts each record to its http sink and forwards it to its next topic.
```

with:

```go
// network: POST /send (producers), GET /tail?since=N and GET /stats. A consumer
// also posts each record to its http sink and forwards it to its next topic, or
// to the one its router picks.
```

In `studio/node.go`, replace:

```go
	Boot    string `json:"boot"` // random per process: a restarted container starts its counters and tail over
	tally          // records produced (producers) or fetched (consumers), the failures, the last one
	TailSeq int64  `json:"tailSeq"`        // seq of the newest tail record; the drawer fetches when it moves
	Step    *tally `json:"step,omitempty"` // a consumer's transform, when it runs one
}
```

with:

```go
	Boot    string      `json:"boot"` // random per process: a restarted container starts its counters and tail over
	tally               // records produced (producers) or fetched (consumers), the failures, the last one
	TailSeq int64       `json:"tailSeq"`         // seq of the newest tail record; the drawer fetches when it moves
	Step    *tally      `json:"step,omitempty"`  // a consumer's transform, when it runs one
	Route   *routeTally `json:"route,omitempty"` // a consumer's router, when it runs one
}
```

In `studio/node.go`, replace:

```go
	transform *transform                                               // nil without one

```

with:

```go
	transform *transform                                               // nil without one
	router    *router                                                  // nil without one

```

In `studio/node.go`, replace:

```go
	if c.spec.Forward != "" {
		pctx, cancel := context.WithTimeout(work, 10*time.Second)
		err := c.produce(pctx, &kgo.Record{Topic: c.spec.Forward, Key: r.Key, Value: value})
		cancel()
		if err != nil {
			c.counts.fail(fmt.Errorf("forward: %w", err))
			log.Printf("forward to %s: %v", c.spec.Forward, err)
```

with:

```go
	forward := c.spec.Forward
	if c.router != nil {
		topic, err := c.router.route(value)
		if err != nil {
			c.counts.fail(fmt.Errorf("router: %w", err))
			log.Printf("router: %v", err)
		}
		forward = topic // "": failed or unmatched, nothing to forward
	}
	if forward != "" {
		pctx, cancel := context.WithTimeout(work, 10*time.Second)
		err := c.produce(pctx, &kgo.Record{Topic: forward, Key: r.Key, Value: value})
		cancel()
		if err != nil {
			c.counts.fail(fmt.Errorf("forward: %w", err))
			log.Printf("forward to %s: %v", forward, err)
```

In `studio/node.go`, replace:

```go
	var tr *transform           // a consumer's transform; nil without one

```

with:

```go
	var tr *transform           // a consumer's transform; nil without one
	var rt *router              // a consumer's router; nil without one

```

In `studio/node.go`, replace:

```go
			s.Step = &step
		}
		reply(w, http.StatusOK, s)
```

with:

```go
			s.Step = &step
		}
		if rt != nil {
			route := rt.read()
			s.Route = &route
		}
		reply(w, http.StatusOK, s)
```

In `studio/node.go`, replace:

```go
				log.Fatal("transform: ", err) // Validate compiled the same source on deploy
			}
		}
		c := &consumer{spec: spec, tail: t, counts: counts, transform: tr, post: postJSON, produce: produce}
```

with:

```go
				log.Fatal("transform: ", err) // Validate compiled the same source on deploy
			}
		}
		if len(spec.Routes) > 0 {
			if rt, err = newRouter(spec.Routes, spec.RouteDefault); err != nil {
				log.Fatal("router: ", err) // Validate compiled the same rules on deploy
			}
		}
		c := &consumer{spec: spec, tail: t, counts: counts, transform: tr, router: rt, post: postJSON, produce: produce}
```

- [ ] **Step 4: Run the checks**

Run: `cd studio && test -z "$(gofmt -l .)" && go vet ./... && go test ./...`
Expected: `ok  	kafka-playground/studio`, and gofmt lists nothing.

- [ ] **Step 5: Commit**

```bash
git add studio/router_test.go studio/node_test.go studio/transform.go studio/router.go studio/node.go
git commit -m "studio: routers run in their consumer after the transform — first match, default or unmatched; /stats route with per-branch counts" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Router counts in the snapshot (Go, and the TypeScript mirror)

`applySteps` gives the Router node its consumer's state and the `route` counts, as it does for a Transform, through one shared `stepState`. Branch counts are summed element-wise over instances. `NodeRuntime` mirrors the two new fields.

**Files:**
- Modify: `studio/engine.go`
- Modify: `studio/ui/src/flow/api.ts`
- Test: `studio/engine_test.go`

**Interfaces:**
- Consumes: `routeTally` and `nodeStats.Route` (Task 2); `NodeSpec.RouterNode` (Task 1).
- Produces:
  - `NodeState.Branches []int64` (JSON `branches`) and `NodeState.Unmatched int64` (JSON `unmatched`), both just before `instances`. Also the unexported `NodeState.route *routeTally` and `func stepState(consumer NodeState, reported func(*NodeState) *routeTally) NodeState` (`engine.go`).
  - `NodeRuntime.branches?: number[]` and `NodeRuntime.unmatched?: number` (`studio/ui/src/flow/api.ts`), used by Task 4.

- [ ] **Step 1: Write the failing tests**

In `studio/engine_test.go`, replace:

```go
func TestStreamTicksEnds(t *testing.T) {
```

with:

```go
func TestApplyStepsRouter(t *testing.T) {
	specs := []NodeSpec{
		{Node: "consumer-1", Type: "consumer", Instance: 1, TransformNode: "transform-1", RouterNode: "router-1"},
		{Node: "consumer-1", Type: "consumer", Instance: 2, TransformNode: "transform-1", RouterNode: "router-1"},
		{Node: "consumer-2", Type: "consumer", RouterNode: "router-2"},
	}
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"consumer-1": {State: "running", Instances: []NodeState{
			{Instance: 1, State: "running", Boot: "a", step: &tally{Total: 4}, route: &routeTally{tally: tally{Total: 4}, Branches: []int64{2, 1, 0}, Unmatched: 1}},
			{Instance: 2, State: "running", Boot: "b", step: &tally{Total: 2}, route: &routeTally{tally: tally{Total: 2, Errors: 1, LastError: "rule 1: invalid operation"}, Branches: []int64{1, 0, 0}}},
		}},
		"consumer-2": {State: "running", Boot: "c", answered: true}, // answers, but reports no router (added after deploy)
	}}
	applySteps(&st, specs)
	want := map[string]NodeState{
		"transform-1": {State: "running", Total: 6, Boot: "a,b"},
		"router-1":    {State: "running", Total: 6, Errors: 1, LastError: "#2: rule 1: invalid operation", Boot: "a,b", Branches: []int64{3, 1, 0}, Unmatched: 1},
		"router-2":    {State: "missing"},
	}
	for id, w := range want {
		if got := st.Nodes[id]; !reflect.DeepEqual(got, w) {
			t.Errorf("%s:\n got %+v\nwant %+v", id, got, w)
		}
	}
}

func TestStreamTicksEnds(t *testing.T) {
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL, a build failure naming `unknown field route in struct literal of type NodeState`.

- [ ] **Step 3: Implement**

In `studio/engine.go`, replace:

```go
	Warning    string             `json:"warning,omitempty"`
	Instances  []NodeState        `json:"instances,omitempty"`

	step     *tally // a consumer container's transform counts; applySteps puts them on the transform node
	answered bool   // its container's /stats answered in this snapshot
}
```

with:

```go
	Warning    string             `json:"warning,omitempty"`
	Branches   []int64            `json:"branches,omitempty"`  // routers: records per rule, then the default's
	Unmatched  int64              `json:"unmatched,omitempty"` // routers: records dropped, no rule matched and no default
	Instances  []NodeState        `json:"instances,omitempty"`

	step     *tally      // a consumer container's transform counts; applySteps puts them on the transform node
	route    *routeTally // and its router's, put on the router node
	answered bool        // its container's /stats answered in this snapshot
}
```

In `studio/engine.go`, replace:

```go
	ns.TailSeq, ns.Boot, ns.step, ns.answered = s.TailSeq, s.Boot, s.Step, true
```

with:

```go
	ns.TailSeq, ns.Boot, ns.step, ns.route, ns.answered = s.TailSeq, s.Boot, s.Step, s.Route, true
```

In `studio/engine.go`, replace:

```go
// applySteps gives each transform node its consumer node's state and the counts
// the consumer's containers report for it, summed as sumCounts sums instances. Its
// boot joins theirs, so a container that restarted gives the transform no rate
// rather than a wrong one. A transform whose consumer's containers answer but
// none reports it (added after deploy) is "missing" until one does; one whose
// consumer did not answer keeps the consumer's state, with no numbers.
func applySteps(st *FlowState, specs []NodeSpec) {
	for _, s := range specs {
		if s.TransformNode == "" || s.Instance > 1 {
			continue // a consumer's specs repeat per instance; its first does for all
		}
		consumer := st.Nodes[s.Node]
		t := NodeState{State: consumer.State}
		var steps []NodeState
		var boots []string
		answered := false
		for _, c := range consumer.containers() {
			answered = answered || c.answered
			if c.step != nil {
				step := NodeState{Instance: c.Instance}
				step.setCounts(*c.step)
				steps = append(steps, step)
				boots = append(boots, c.Boot)
			}
		}
		t.sumCounts(steps)
		t.Boot = strings.Join(boots, ",")
		if len(boots) == 0 && (t.State == "" || t.State == "running" && answered) {
			t.State = "missing"
		}
		st.Nodes[s.TransformNode] = t
	}
}
```

with:

```go
// applySteps gives each transform and router node its consumer node's state and
// the counts the consumer's containers report for it.
func applySteps(st *FlowState, specs []NodeSpec) {
	for _, s := range specs {
		if s.Instance > 1 {
			continue // a consumer's specs repeat per instance; its first does for all
		}
		consumer := st.Nodes[s.Node]
		if s.TransformNode != "" {
			st.Nodes[s.TransformNode] = stepState(consumer, func(c *NodeState) *routeTally {
				if c.step == nil {
					return nil
				}
				return &routeTally{tally: *c.step}
			})
		}
		if s.RouterNode != "" {
			st.Nodes[s.RouterNode] = stepState(consumer, func(c *NodeState) *routeTally { return c.route })
		}
	}
}

// stepState is the state of a step (a transform or a router) that runs in
// consumer: the consumer's state, and the counts its containers report for the
// step (reported picks them), summed as sumCounts sums instances. Its boot joins
// theirs, so a container that restarted gives the step no rate rather than a
// wrong one. A step whose consumer's containers answer but none reports it
// (added after deploy) is "missing" until one does; one whose consumer did not
// answer keeps the consumer's state, with no numbers.
func stepState(consumer NodeState, reported func(*NodeState) *routeTally) NodeState {
	t := NodeState{State: consumer.State}
	var steps []NodeState
	var boots []string
	answered := false
	for _, c := range consumer.containers() {
		answered = answered || c.answered
		if r := reported(c); r != nil {
			step := NodeState{Instance: c.Instance, Branches: r.Branches, Unmatched: r.Unmatched}
			step.setCounts(r.tally)
			steps = append(steps, step)
			boots = append(boots, c.Boot)
		}
	}
	t.sumCounts(steps)
	t.Boot = strings.Join(boots, ",")
	if len(boots) == 0 && (t.State == "" || t.State == "running" && answered) {
		t.State = "missing"
	}
	return t
}
```

In `studio/engine.go`, replace:

```go
	ns.Total, ns.Errors, ns.Rate, ns.LastError = 0, 0, 0, ""
	for _, in := range cs {
		ns.Total += in.Total
		ns.Errors += in.Errors
		ns.Rate += in.Rate
```

with:

```go
	ns.Total, ns.Errors, ns.Rate, ns.LastError, ns.Branches, ns.Unmatched = 0, 0, 0, "", nil, 0
	for _, in := range cs {
		ns.Total += in.Total
		ns.Errors += in.Errors
		ns.Rate += in.Rate
		ns.Unmatched += in.Unmatched
		for i, b := range in.Branches {
			if i == len(ns.Branches) {
				ns.Branches = append(ns.Branches, 0)
			}
			ns.Branches[i] += b
		}
```

In `studio/ui/src/flow/api.ts`, replace:

```ts
  warning?: string
  instances?: NodeRuntime[]
}
```

with:

```ts
  warning?: string
  branches?: number[] // routers: records per rule, then the default's
  unmatched?: number // routers: records dropped, no rule matched and no default
  instances?: NodeRuntime[]
}
```

- [ ] **Step 4: Run the checks**

Run: `cd studio && test -z "$(gofmt -l .)" && go vet ./... && go test ./...`
Expected: `ok  	kafka-playground/studio`, and gofmt lists nothing.
Then run `cd studio/ui && npx tsc -b . e2e`. Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add studio/engine_test.go studio/engine.go studio/ui/src/flow/api.ts
git commit -m "studio: the snapshot puts a router's counts, branches and unmatched on its node, summed over instances; NodeRuntime mirrors them" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The Router in the UI

Five pieces:

- the palette item and the schema mirror;
- the node's summary (`2 rules · default <topic name>`);
- `unmatched` on its runtime line;
- a rule added when a router is wired to a topic;
- the derived edge labels, and the Inspector's rule editor (`RouterRules.tsx`).

There is no UI unit-test runner. `make test` type-checks and builds the UI, and Task 6 drives all of this in a browser.

**Files:**
- Create: `studio/ui/src/RouterRules.tsx`
- Modify: `studio/ui/src/App.tsx`
- Modify: `studio/ui/src/Canvas.tsx`
- Modify: `studio/ui/src/Inspector.tsx`
- Modify: `studio/ui/src/Palette.tsx`
- Modify: `studio/ui/src/flow/schema.ts`
- Modify: `studio/ui/src/index.css`
- Modify: `studio/ui/src/nodes/StudioNodes.tsx`
- Modify: `studio/ui/src/nodes/types.ts`

**Interfaces:**
- Consumes: `NodeRuntime.branches` and `NodeRuntime.unmatched` (Task 3), and the server rules from Task 1.
- Produces, used by Task 6's tests:
  - The palette item `.palette-item.router`, and node ids `router-<n>`.
  - In the Inspector, a `<fieldset>` per rule with the legend `Rule <n>` (role `group`), holding controls labelled `When` and `Topic` and buttons `Up`, `Down` and `Remove`. Below the rules come a button `Add rule` and a select labelled `Default`, whose `none: drop the record` option has the value `''`.
  - React Flow names each edge `Edge from <source> to <target>` (its `aria-label`). A router's edges carry the label text `#<n>`, `default`, joined with `, `, plus ` · <count>` once the flow reports `branches`.
  - The node summary `<n> rule(s) · default <topic name>` or `· no default`.
  - Inspector's new prop: `topics: { id: string; name: string }[]`.

- [ ] **Step 1: Mirror the node type (schema, types, palette)**

In `studio/ui/src/flow/schema.ts`, replace:

```ts
export const NODE_TYPES = ['producer', 'topic', 'consumer', 'transform'] as const
```

with:

```ts
export const NODE_TYPES = ['producer', 'topic', 'consumer', 'transform', 'router'] as const
```

In `studio/ui/src/flow/schema.ts`, replace:

```ts
  consumer: ['topic', 'transform'],
  transform: ['topic'],
}
```

with:

```ts
  consumer: ['topic', 'transform', 'router'],
  transform: ['topic', 'router'],
  router: ['topic'],
}
```

In `studio/ui/src/flow/schema.ts`, replace:

```ts
    case 'transform':
      return { expr: 'msg' }
  }
```

with:

```ts
    case 'transform':
      return { expr: 'msg' }
    case 'router':
      return { rules: [], default: '' }
  }
```

In `studio/ui/src/nodes/types.ts`, replace:

```ts
export type TransformData = { expr: string }

```

with:

```ts
export type TransformData = { expr: string }
export type Rule = { when: string; to: string } // to: a topic node's id
export type RouterData = { rules: Rule[]; default: string } // default: a topic node's id, '' for none

```

In `studio/ui/src/nodes/types.ts`, replace:

```ts
export type TransformNode = Node<TransformData, 'transform'>
export type StudioNode = ProducerNode | TopicNode | ConsumerNode | TransformNode
```

with:

```ts
export type TransformNode = Node<TransformData, 'transform'>
export type RouterNode = Node<RouterData, 'router'>
export type StudioNode = ProducerNode | TopicNode | ConsumerNode | TransformNode | RouterNode
```

In `studio/ui/src/Palette.tsx`, replace:

```tsx
const ITEMS: NodeType[] = ['producer', 'topic', 'consumer', 'transform']
```

with:

```tsx
const ITEMS: NodeType[] = ['producer', 'topic', 'consumer', 'transform', 'router']
```

In `studio/ui/src/Palette.tsx`, replace:

```tsx
Drag onto the canvas, then wire Producer → Topic → Consumer, and on to a Topic, directly or through a Transform. Backspace deletes.
```

with:

```tsx
Drag onto the canvas, then wire Producer → Topic → Consumer, and on to a Topic, directly or through a Transform, a Router, or both. Backspace deletes.
```

- [ ] **Step 2: The node: its summary and unmatched on its runtime line**

In `studio/ui/src/nodes/StudioNodes.tsx`, replace:

```tsx
import { Handle, Position, type NodeProps } from '@xyflow/react'
```

with:

```tsx
import { Handle, Position, useNodesData, type NodeProps } from '@xyflow/react'
```

In `studio/ui/src/nodes/StudioNodes.tsx`, replace:

```tsx
import type { ConsumerNode, ProducerNode, TopicNode, TransformNode } from './types'
```

with:

```tsx
import type { ConsumerNode, ProducerNode, RouterNode, TopicNode, TransformNode } from './types'
```

In `studio/ui/src/nodes/StudioNodes.tsx`, replace:

```tsx
  if (rt.errors) parts.push(`${rt.errors} errors`)

```

with:

```tsx
  if (rt.errors) parts.push(`${rt.errors} errors`)
  if (rt.unmatched) parts.push(`${rt.unmatched} unmatched`)

```

In `studio/ui/src/nodes/StudioNodes.tsx`, replace:

```tsx
  transform: ({ id, data, selected }: NodeProps<TransformNode>) => (
    <Shell id={id} type="transform" selected={selected}>
      {data.expr || '(no expression)'}
    </Shell>
  ),
}
```

with:

```tsx
  transform: ({ id, data, selected }: NodeProps<TransformNode>) => (
    <Shell id={id} type="transform" selected={selected}>
      {data.expr || '(no expression)'}
    </Shell>
  ),
  router: ({ id, data, selected }: NodeProps<RouterNode>) => {
    const fallback = useNodesData<TopicNode>(data.default) // follows the default topic's renames
    const n = data.rules.length
    return (
      <Shell id={id} type="router" selected={selected}>
        {n} rule{n === 1 ? '' : 's'} · {data.default ? `default ${fallback?.data.name || data.default}` : 'no default'}
      </Shell>
    )
  },
}
```

- [ ] **Step 3: The canvas: wiring adds a rule; edges get their labels**

In `studio/ui/src/Canvas.tsx`, replace:

```tsx
import { useCallback, useState, type DragEvent, type Dispatch, type SetStateAction } from 'react'
```

with:

```tsx
import { useCallback, useContext, useMemo, useState, type DragEvent, type Dispatch, type SetStateAction } from 'react'
```

In `studio/ui/src/Canvas.tsx`, replace:

```tsx
import { nodeTypes } from './nodes/StudioNodes'
import type { StudioNode } from './nodes/types'
```

with:

```tsx
import { RuntimeContext, nodeTypes } from './nodes/StudioNodes'
import type { RouterData, StudioNode } from './nodes/types'
```

In `studio/ui/src/Canvas.tsx`, replace:

```tsx
  defaultViewport?: Viewport // initial viewport (read on mount); fits the view when absent and the flow has nodes
}

```

with:

```tsx
  defaultViewport?: Viewport // initial viewport (read on mount); fits the view when absent and the flow has nodes
}

// The label on a router's edge to target: the rules that send there (#1, #2) and
// the default, each with its count while the flow runs (#1 · 80).
function routeLabel(data: RouterData, target: string, branches?: number[]): string {
  const tos = [...data.rules.map((r) => r.to), data.default] // in the order of branches: the rules', then the default's
  return tos
    .flatMap((to, i) => {
      if (to !== target) return []
      const name = i < data.rules.length ? `#${i + 1}` : 'default'
      return [branches ? `${name} · ${branches[i] ?? 0}` : name]
    })
    .join(', ')
}

```

In `studio/ui/src/Canvas.tsx`, replace:

```tsx
  const { screenToFlowPosition, getNode } = useReactFlow()
```

with:

```tsx
  const { screenToFlowPosition, getNode, getEdges } = useReactFlow()
  const runtime = useContext(RuntimeContext)
```

In `studio/ui/src/Canvas.tsx`, replace:

```tsx
  const onConnect = useCallback((c: Connection) => setEdges((eds) => addEdge(c, eds)), [setEdges])

```

with:

```tsx
  // A new edge from a router adds a rule for its topic, with a condition to fill in
  // (Deploy refuses an empty one). A duplicate edge, which addEdge drops, adds none.
  const onConnect = useCallback(
    (c: Connection) => {
      const isNew = !getEdges().some((e) => e.source === c.source && e.target === c.target)
      setEdges((eds) => addEdge(c, eds))
      if (!isNew || getNode(c.source)?.type !== 'router') return
      setNodes((nds) =>
        nds.map((n) => (n.id === c.source && n.type === 'router' ? { ...n, data: { ...n.data, rules: [...n.data.rules, { when: '', to: c.target }] } } : n)),
      )
    },
    [getEdges, getNode, setEdges, setNodes],
  )

  // A router's edges carry labels worked out from its rules, never saved.
  const shownEdges = useMemo(
    () =>
      edges.map((e) => {
        const router = nodes.find((n) => n.id === e.source)
        return router?.type === 'router' ? { ...e, label: routeLabel(router.data, e.target, runtime[router.id]?.branches) } : e
      }),
    [edges, nodes, runtime],
  )

```

In `studio/ui/src/Canvas.tsx`, replace:

```tsx
      edges={edges}
      nodeTypes={nodeTypes}
```

with:

```tsx
      edges={shownEdges}
      nodeTypes={nodeTypes}
```

- [ ] **Step 4: The Inspector: the rules editor**

Create `studio/ui/src/RouterRules.tsx`:

```tsx
import type { RouterData, Rule } from './nodes/types'

type Props = {
  data: RouterData
  topics: { id: string; name: string }[] // the topics the router has edges to
  set: (patch: Partial<RouterData>) => void
}

// A router's rules in order (first match wins), each a condition and one of the
// router's wired topics, and its default. A rule or default naming a topic the
// router is not wired to stays, marked, until it is changed: Deploy refuses it.
export default function RouterRules({ data, topics, set }: Props) {
  const wired = new Set(topics.map((t) => t.id))
  const setRules = (rules: Rule[]) => set({ rules })
  const update = (i: number, patch: Partial<Rule>) => setRules(data.rules.map((r, j) => (j === i ? { ...r, ...patch } : r)))
  const move = (i: number, by: number) => {
    const rules = [...data.rules]
    ;[rules[i], rules[i + by]] = [rules[i + by], rules[i]]
    setRules(rules)
  }
  // A select's options: the wired topics, plus the current value when it is not one of them.
  const options = (current: string) => (
    <>
      {current !== '' && !wired.has(current) && <option value={current}>{current} (not wired)</option>}
      {topics.map((t) => (
        <option key={t.id} value={t.id}>
          {t.name}
        </option>
      ))}
    </>
  )

  return (
    <>
      {data.rules.map((r, i) => (
        <fieldset key={i} className="rule">
          <legend>Rule {i + 1}</legend>
          <label htmlFor={`inspector-rule-${i}-when`}>When</label>
          <textarea id={`inspector-rule-${i}-when`} value={r.when} placeholder="msg.total > 100" onChange={(e) => update(i, { when: e.target.value })} />
          <label htmlFor={`inspector-rule-${i}-topic`}>Topic</label>
          <select id={`inspector-rule-${i}-topic`} value={r.to} onChange={(e) => update(i, { to: e.target.value })}>
            {r.to === '' && <option value="">(pick a topic)</option>}
            {options(r.to)}
          </select>
          {!wired.has(r.to) && <p className="hint">Not wired: draw an edge from the router to its topic.</p>}
          <div className="buttons">
            <button disabled={i === 0} onClick={() => move(i, -1)}>
              Up
            </button>
            <button disabled={i === data.rules.length - 1} onClick={() => move(i, 1)}>
              Down
            </button>
            <button onClick={() => setRules(data.rules.filter((_, j) => j !== i))}>Remove</button>
          </div>
        </fieldset>
      ))}
      <button onClick={() => setRules([...data.rules, { when: '', to: topics[0]?.id ?? '' }])}>Add rule</button>
      <label htmlFor="inspector-default">Default</label>
      <select id="inspector-default" value={data.default} onChange={(e) => set({ default: e.target.value })}>
        <option value="">none: drop the record</option>
        {options(data.default)}
      </select>
      <p className="hint">
        First match wins. Without a default, records no rule matches are dropped and counted. Each condition is an expr-lang
        expression over msg (the value after the transform, if any) that yields true or false, e.g. {'msg.total > 100'}.
        Wiring the router to a topic adds a rule for it.
      </p>
    </>
  )
}
```

In `studio/ui/src/Inspector.tsx`, replace:

```tsx
import RewindGroup from './RewindGroup'
```

with:

```tsx
import RewindGroup from './RewindGroup'
import RouterRules from './RouterRules'
```

In `studio/ui/src/Inspector.tsx`, replace:

```tsx
  dirty: boolean // unsaved edits: a rewind uses the saved group and topic
  onChange: (id: string, patch: Record<string, unknown>) => void
}

export default function Inspector({ node, flowId, running, dirty, onChange }: Props) {
```

with:

```tsx
  dirty: boolean // unsaved edits: a rewind uses the saved group and topic
  topics: { id: string; name: string }[] // the topics the node has edges to, for a router's rules
  onChange: (id: string, patch: Record<string, unknown>) => void
}

export default function Inspector({ node, flowId, running, dirty, topics, onChange }: Props) {
```

In `studio/ui/src/Inspector.tsx`, replace:

```tsx
          <p className="hint">Wire it to a topic to forward every record there with the same key, or through a Transform to reshape or drop records first.</p>
```

with:

```tsx
          <p className="hint">Wire it to a topic to forward every record there with the same key, through a Transform to reshape or drop records first, or through a Router to pick each record's topic.</p>
```

In `studio/ui/src/Inspector.tsx`, replace:

```tsx
Its result is forwarded with the same key; nil drops the record. Deploy checks that it compiles.</p>
        </>
      )}
```

with:

```tsx
Its result is forwarded with the same key; nil drops the record. Deploy checks that it compiles.</p>
        </>
      )}
      {node.type === 'router' && <RouterRules data={node.data} topics={topics} set={set} />}
```

In `studio/ui/src/App.tsx`, replace:

```tsx
  const picked = nodes.filter((n) => n.selected)
  const node = picked.length === 1 ? picked[0] : null

```

with:

```tsx
  const picked = nodes.filter((n) => n.selected)
  const node = picked.length === 1 ? picked[0] : null
  const topics = node
    ? edges.flatMap((e) => {
        const t = e.source === node.id ? nodes.find((n) => n.id === e.target) : undefined
        return t?.type === 'topic' ? [{ id: t.id, name: t.data.name || t.id }] : []
      })
    : []

```

In `studio/ui/src/App.tsx`, replace:

```tsx
<Inspector node={node} flowId={current?.id} running={running} dirty={dirty} onChange={updateData} />
```

with:

```tsx
<Inspector node={node} flowId={current?.id} running={running} dirty={dirty} topics={topics} onChange={updateData} />
```

In `studio/ui/src/index.css`, replace:

```css
.transform { border-color: #6a1b9a; }
```

with:

```css
.transform { border-color: #6a1b9a; }
.router { border-color: #00838f; }
```

In `studio/ui/src/index.css`, replace:

```css
.inspector .buttons { display: flex; gap: 6px; }
```

with:

```css
.inspector .buttons { display: flex; gap: 6px; }
.inspector fieldset.rule { margin: 8px 0; padding: 4px 8px 8px; border: 1px solid #ddd; border-radius: 4px; }
.inspector fieldset.rule .buttons { margin-top: 6px; }
```

- [ ] **Step 5: Run the checks**

Run: `make test`
Expected: it ends with `docker compose config --quiet` and no error. The UI build runs `tsc -b . e2e` and then `vite build`; its "Some chunks are larger than 500 kB" warning was there before this task.

- [ ] **Step 6: Commit**

```bash
git add studio/ui/src/flow/schema.ts studio/ui/src/nodes/types.ts studio/ui/src/Palette.tsx studio/ui/src/nodes/StudioNodes.tsx studio/ui/src/Canvas.tsx studio/ui/src/RouterRules.tsx studio/ui/src/Inspector.tsx studio/ui/src/App.tsx studio/ui/src/index.css
git commit -m "ui: the Router node — palette, summary, rules in the Inspector, a rule per new edge, edge labels with branch counts" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: verify-studio: a router flow end to end

A router flow is checked live:

- a rule that does not compile is refused with a 422;
- once fixed, the flow deploys;
- a big record and a small one each reach their own topic's consumer;
- the Router node shows `"total":2` and `"branches":[1,1]`.

The flow is removed afterwards, and its topics are added to the cleanup list.

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: the whole backend (Tasks 1–3), through the HTTP API.
- Produces: nothing other tasks use.

- [ ] **Step 1: Add the router checks**

In `Makefile`, replace:

```makefile
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, rewind, live ticks, chained flows, instances, transforms, stop, delete; removes its flows and topics
```

with:

```makefile
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, rewind, live ticks, chained flows, instances, transforms, routers, stop, delete; removes its flows and topics
```

In `Makefile`, replace:

```makefile
--topic "studio-verify|studio-verify-(timer|a|a-out|b|instances|t-in|t-out)"
```

with:

```makefile
--topic "studio-verify|studio-verify-(timer|a|a-out|b|instances|t-in|t-out|r-in|r-big|r-other)"
```

In `Makefile`, replace:

```makefile
	[ "$$code" = 404 ] || { echo "STUDIO FAILED: the events of a deleted flow answered $$code, want 404"; exit 1; }; \
	echo "STUDIO OK ($$id)"
```

with:

```makefile
	[ "$$code" = 404 ] || { echo "STUDIO FAILED: the events of a deleted flow answered $$code, want 404"; exit 1; }; \
	r=$$(flow3 verify-router "$$manual" studio-verify-r-in 1 '{"group":"studio-verify-r","auto_offset_reset":"earliest","sink":{"kind":"log"}}' \
		',{"id":"router-1","type":"router","position":{"x":600,"y":0},"data":{"rules":[{"when":"msg.total > 100","to":"topic-2"}],"default":"topic-3"}},{"id":"topic-2","type":"topic","position":{"x":800,"y":-100},"data":{"name":"studio-verify-r-big","partitions":1,"replication_factor":1}},{"id":"topic-3","type":"topic","position":{"x":800,"y":100},"data":{"name":"studio-verify-r-other","partitions":1,"replication_factor":1}},{"id":"consumer-2","type":"consumer","position":{"x":1000,"y":-100},"data":{"group":"studio-verify-r-big","auto_offset_reset":"earliest","sink":{"kind":"log"}}},{"id":"consumer-3","type":"consumer","position":{"x":1000,"y":100},"data":{"group":"studio-verify-r-other","auto_offset_reset":"earliest","sink":{"kind":"log"}}}' \
		',{"id":"e3","source":"consumer-1","target":"router-1"},{"id":"e4","source":"router-1","target":"topic-2"},{"id":"e5","source":"router-1","target":"topic-3"},{"id":"e6","source":"topic-2","target":"consumer-2"},{"id":"e7","source":"topic-3","target":"consumer-3"}'); \
	broken=$$(echo "$$r" | sed 's/msg\.total > 100/msg./'); \
	rid=$$(create "$$broken" "the router flow") || exit 1; flows="$$flows $$rid"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows/$$rid/deploy); \
	[ "$$code" = 422 ] || { echo "STUDIO FAILED: a router rule that does not compile deployed ($$code)"; exit 1; }; \
	curl -sS --fail-with-body -X PUT $(STUDIO_URL)/api/flows/$$rid -H 'Content-Type: application/json' --data "$$r" >/dev/null || { echo "STUDIO FAILED: save the router flow"; exit 1; }; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$rid/deploy >/dev/null || { echo "STUDIO FAILED: deploy the router flow"; exit 1; }; \
	rec="$$(date +%s)"; \
	for v in "{\"id\":\"r1-$$rec\",\"total\":150}" "{\"id\":\"r2-$$rec\",\"total\":5}"; do \
		curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$rid/nodes/producer-1/send --data "$$v" >/dev/null || { echo "STUDIO FAILED: send to the router flow"; exit 1; }; \
	done; \
	for i in $$(seq 30); do \
		curl -sS $(STUDIO_URL)/api/flows/$$rid/state | grep -q '"router-1":{"state":"running","total":2,[^}]*"branches":\[1,1\]' && \
			curl -sS "$(STUDIO_URL)/api/flows/$$rid/nodes/consumer-2/tail?since=0" | grep -q "r1-$$rec" && \
			curl -sS "$(STUDIO_URL)/api/flows/$$rid/nodes/consumer-3/tail?since=0" | grep -q "r2-$$rec" && break; \
		[ "$$i" = 30 ] && { echo "STUDIO FAILED: want 2 records on the router, branches [1,1], r1-$$rec on the big topic and r2-$$rec on the other: $$(curl -sS $(STUDIO_URL)/api/flows/$$rid/state)"; exit 1; }; sleep 1; \
	done; \
	curl -sS "$(STUDIO_URL)/api/flows/$$rid/nodes/consumer-2/tail?since=0" | grep -q "r2-$$rec" && { echo "STUDIO FAILED: r2-$$rec (total 5) reached the big topic"; exit 1; }; \
	echo "studio router: $$(curl -sS $(STUDIO_URL)/api/flows/$$rid/state | grep -o '"router-1":{[^}]*}')"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$rid || { echo "STUDIO FAILED: delete the router flow"; exit 1; }; \
	echo "STUDIO OK ($$id)"
```

- [ ] **Step 2: Run the checks**

Run: `make down && make verify-studio`
Expected: the output includes a line like `studio router: "router-1":{"state":"running","total":2,"boot":"…","branches":[1,1]}`, then `STUDIO OK (<id>)`.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "make: verify-studio checks a router flow — a bad rule refused, one record per branch, branches [1,1]" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: UI tests: a router built in the editor, and its edge counts

Two Playwright tests.

- **In the editor:** an API-created flow is shown unfitted, in one column, so the dropped router lands in a known gap. Wiring the router adds rules, and Deploy refuses their empty conditions. Then a condition is filled in, a rule is removed and its topic made the default, and the flow runs.
- **On a deployed router flow:** two sends from the producer's drawer give the edges `#1 · 1` and `default · 1`.

The long `studio-ui-…` names widen the nodes, which is why the editor test stacks the nodes vertically.

**Files:**
- Test: `studio/ui/e2e/editor.spec.ts`
- Test: `studio/ui/e2e/flows.ts`
- Test: `studio/ui/e2e/locators.ts`
- Test: `studio/ui/e2e/nodes.spec.ts`

**Interfaces:**
- Consumes: Task 4's labels and names (see its Interfaces); the existing fixtures `studio.create`, `studio.open`, `deployFlow`, and the locators `connect`, `field`, `nodeOf`, `runtimeOf`, `tail`, `topBar`, `paletteItem`.
- Produces:
  - `edgeOf(page, from, to)` and `ruleOf(page, n)` (`e2e/locators.ts`).
  - An optional `y` on `node(id, type, x, data, y = 0)` (`e2e/flows.ts`).

- [ ] **Step 1: The helpers**

In `studio/ui/e2e/flows.ts`, replace:

```ts
// A flow's parts, laid out left to right.
export const node = (id: string, type: NodeType, x: number, data: Data): FlowNode => ({ id, type, position: { x, y: 0 }, data })
```

with:

```ts
// A flow's parts, laid out left to right (or where y puts them).
export const node = (id: string, type: NodeType, x: number, data: Data, y = 0): FlowNode => ({ id, type, position: { x, y }, data })
```

In `studio/ui/e2e/locators.ts`, replace:

```ts
export const runtimeOf = (page: Page, id: string) => page.getByTestId(`runtime-${id}`)
```

with:

```ts
export const runtimeOf = (page: Page, id: string) => page.getByTestId(`runtime-${id}`)
export const edgeOf = (page: Page, from: string, to: string) => page.getByLabel(`Edge from ${from} to ${to}`, { exact: true })
export const ruleOf = (page: Page, n: number) => page.locator('.inspector').getByRole('group', { name: `Rule ${n}`, exact: true })
```

- [ ] **Step 2: The editor test**

In `studio/ui/e2e/editor.spec.ts`, replace:

```ts
import {
  connect,
  deployFlow,
```

with:

```ts
import {
  chain,
  connect,
  consumer,
  deployFlow,
  edgeOf,
```

In `studio/ui/e2e/editor.spec.ts`, replace:

```ts
  flowRow,
  messageCount,
  nodeOf,
  paletteItem,
  runtimeOf,
  simple,
  tail,
  test,
  topBar,
} from './studio'
```

with:

```ts
  flowRow,
  manual,
  messageCount,
  node,
  nodeOf,
  paletteItem,
  ruleOf,
  runtimeOf,
  simple,
  tail,
  test,
  topBar,
  topic,
} from './studio'
```

In `studio/ui/e2e/editor.spec.ts`, replace:

```ts
test('a wire the edge table refuses is not drawn',
```

with:

```ts
test('a router built in the editor: wiring adds rules, Deploy wants their conditions', async ({ page, studio }) => {
  const name = studio.unique('router-editor')
  const flow = chain(name, manual, topic(`${name}-in`), consumer(`${name}-in`))
  flow.nodes = [
    node('producer-1', 'producer', 60, manual, 20),
    node('topic-1', 'topic', 60, topic(`${name}-in`), 100),
    node('consumer-1', 'consumer', 60, consumer(`${name}-in`), 180),
    node('topic-2', 'topic', 60, topic(`${name}-big`), 360),
    node('topic-3', 'topic', 60, topic(`${name}-other`), 440),
  ]
  // One column, unfitted: the long names widen the nodes, and the router goes in the gap at y 270.
  flow.viewport = { x: 0, y: 0, zoom: 1 }
  await studio.create(flow)
  await studio.open(page, name)

  await paletteItem(page, 'router').dragTo(page.locator('.react-flow__pane'), { targetPosition: { x: 60, y: 270 } })
  await connect(page, 'consumer-1', 'router-1')
  await connect(page, 'router-1', 'topic-2')
  await connect(page, 'router-1', 'topic-3')
  await nodeOf(page, 'router-1').click()
  await expect(ruleOf(page, 1).getByLabel('Topic')).toHaveValue('topic-2')
  await expect(ruleOf(page, 2).getByLabel('Topic')).toHaveValue('topic-3')
  await expect(nodeOf(page, 'router-1')).toContainText('2 rules · no default')
  await expect(edgeOf(page, 'router-1', 'topic-3')).toContainText('#2')

  await page.getByRole('button', { name: 'Save' }).click()
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: router-1: rule 1: when is required')

  await ruleOf(page, 1).getByLabel('When').fill('msg.total > 100')
  await ruleOf(page, 2).getByRole('button', { name: 'Remove' }).click()
  await field(page, 'Default').selectOption('topic-3')
  await expect(nodeOf(page, 'router-1')).toContainText(`1 rule · default ${name}-other`)
  await expect(edgeOf(page, 'router-1', 'topic-2')).toContainText('#1')
  await expect(edgeOf(page, 'router-1', 'topic-3')).toContainText('default')
  await page.getByRole('button', { name: 'Save' }).click()
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page).getByText('running', { exact: true })).toBeVisible({ timeout: 15_000 })
})

test('a wire the edge table refuses is not drawn',
```

- [ ] **Step 3: The edge-count test**

In `studio/ui/e2e/nodes.spec.ts`, replace:

```ts
import { chain, consumer, deployFlow, edge, expect, node, nodeOf, runtimeOf, simple, tail, test, timer, topBar, topic } from './studio'
```

with:

```ts
import { chain, consumer, deployFlow, edge, edgeOf, expect, node, nodeOf, runtimeOf, simple, tail, test, timer, topBar, topic } from './studio'
```

In `studio/ui/e2e/nodes.spec.ts`, replace:

```ts
test('a consumer with instances shows each one and tails the one picked',
```

with:

```ts
test("a router's edges count what each rule sends", async ({ page, studio }) => {
  const name = studio.unique('router')
  const big = { source: 'manual', key: '', value: '{{if eq .Seq 1}}{"total": 150}{{else}}{"total": 5}{{end}}' }
  const flow = chain(name, big, topic(`${name}-in`), consumer(`${name}-in`))
  flow.nodes.push(
    node('router-1', 'router', 750, { rules: [{ when: 'msg.total > 100', to: 'topic-2' }], default: 'topic-3' }),
    node('topic-2', 'topic', 1000, topic(`${name}-big`), -100),
    node('topic-3', 'topic', 1000, topic(`${name}-other`), 100),
  )
  flow.edges.push(edge('consumer-1', 'router-1'), edge('router-1', 'topic-2'), edge('router-1', 'topic-3'))
  await deployFlow(await studio.create(flow))
  await studio.open(page, name)

  await nodeOf(page, 'producer-1').click()
  for (const offset of [0, 1]) {
    await tail(page).getByRole('button', { name: 'Send' }).click()
    await expect(tail(page)).toContainText(`at offset ${offset}`)
  }
  await expect(runtimeOf(page, 'router-1')).toContainText(/(?<!\d)2 msgs/, { timeout: 15_000 })
  await expect(edgeOf(page, 'router-1', 'topic-2')).toContainText('#1 · 1')
  await expect(edgeOf(page, 'router-1', 'topic-3')).toContainText('default · 1')
})

test('a consumer with instances shows each one and tails the one picked',
```

- [ ] **Step 4: Run the checks**

Run: `make test && make down && make verify-ui`
Expected: `14 passed`, then `UI OK`. To run only the new tests against a running stack: `cd studio/ui && STUDIO_URL=http://localhost:8082 KAFKA_BOOTSTRAP=localhost:19092 npx playwright test -g router` (`2 passed`).

- [ ] **Step 5: Commit**

```bash
git add studio/ui/e2e/flows.ts studio/ui/e2e/locators.ts studio/ui/e2e/editor.spec.ts studio/ui/e2e/nodes.spec.ts
git commit -m "ui tests: a router built in the editor (edges add rules, Deploy wants their conditions) and its edges counting each branch" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Docs

The changes:

- `README.md`: the Router bullet and the edges;
- `AGENTS.md`: the router runs inside its consumer;
- the Studio spec: §3.4, §3.6, §4.2, §4.3, a "Router." milestone in §7, and §9;
- the router spec: its status, the Save rule as built, and router errors counting on the consumer;
- the UI-tests spec: the three tests added since it was written, the rewind one included.

**Files:**
- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`
- Modify: `docs/superpowers/specs/2026-10-07-studio-router-design.md`
- Modify: `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`

**Interfaces:**
- Consumes: what Tasks 1–6 built.
- Produces: nothing other tasks use.

- [ ] **Step 1: Update the docs**

In `README.md`, replace:

```markdown
- Edges: Producer → Topic, Topic → Consumer, then Consumer → Topic (a forward) or Consumer → Transform → Topic. The editor refuses other wires and the server rejects them on save.
```

with:

```markdown
- Edges: Producer → Topic, Topic → Consumer, then Consumer → Topic (a forward), Consumer → Transform → Topic, Consumer → Router → Topics, or Consumer → Transform → Router → Topics. The editor refuses other wires and the server rejects them on save.
```

In `README.md`, replace:

```markdown
and its own counts.
- Flows feed each other
```

with:

```markdown
and its own counts.
- **Router**: ordered rules, each a condition over `msg` (an expr-lang expression that yields true or false, e.g. `msg.total > 100`) and one of the topics the router is wired to, plus an optional default topic. It runs in its consumer's containers, after the Transform if there is one: a record goes, with its key, to the topic of the first rule that holds, else to the default; with no default it is dropped and counted as `unmatched`. Wiring the router to a topic adds a rule for it, to fill in. A deploy refuses a condition that is empty, does not compile or cannot be a boolean, a rule or default whose topic the router has no edge to, and an edge no rule or default uses (422, naming the node and the rule). A value that is not JSON or a condition that fails (`rule 1: invalid operation: <nil> > int`) is counted on the Router node and as an error of its consumer (`router: …`), and that record is not forwarded, not even to the default. Its edges are labelled `#1`, `#2`, `default`, with each one's count while the flow runs (`#1 · 80`).
- Flows feed each other
```

In `AGENTS.md`, replace:

```markdown
`docker.go`, `kafka.go`, `transform.go`)
```

with:

```markdown
`docker.go`, `kafka.go`, `transform.go`, `router.go`)
```

In `AGENTS.md`, replace:

```markdown
Expressions compile in `Validate` (deploy) and again in the consumer, through `compileTransform` (`transform.go`).
```

with:

```markdown
Expressions compile in `Validate` (deploy) and again in the consumer, through `compileTransform` (`transform.go`). A router is the same: its rules go to the consumer's `NodeSpec` (`Routes`, `RouteDefault`, `RouterNode`), its counts arrive in `/stats` `route`, and `applySteps` puts them on the Router node; conditions compile through `compileRule` (`router.go`).
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
  answer but 2xx counts as an error) — then the transform if any (M5), and if the
  node forwards, `ProduceSync` to that topic with the same key (through the same
  client, 10 s timeout).
```

with:

```markdown
  answer but 2xx counts as an error) — then the transform if any (M5), then the
  router if any (Router), and if the node forwards (to its next topic, or to the
  one its router picks), `ProduceSync` to that topic with the same key (through
  the same client, 10 s timeout).
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
(from M5 also `step: {total, errors, lastError}` on a consumer that runs a transform)
```

with:

```markdown
(from M5 also `step: {total, errors, lastError}` on a consumer that runs a transform, and `route: {total, errors, lastError, branches, unmatched}` on one that runs a router)
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
From M5 a transform node carries its consumer's state (`missing` while no container reports it) and the counters the consumer reports for it under `step`.
```

with:

```markdown
From M5 a transform node carries its consumer's state (`missing` while no container reports it) and the counters the consumer reports for it under `step`; a router node likewise, from `route`, plus `branches` (records per rule, then the default's) and `unmatched`, summed over instances.
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
| transform | `expr` | M5 | an `expr-lang/expr` program over `msg` (the decoded JSON value) returning the new value, or `nil` to drop the record; must compile on deploy |
```

with:

```markdown
| transform | `expr` | M5 | an `expr-lang/expr` program over `msg` (the decoded JSON value) returning the new value, or `nil` to drop the record; must compile on deploy |
| router | `rules` | Router | ordered `[{when, to}]`: `when` an `expr-lang/expr` condition over `msg` that must compile as a boolean on deploy, `to` the id of a topic the router has an edge to |
| | `default` | Router | the id of a topic the router has an edge to, or empty: no rule matching drops the record |
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
  X -- "exactly 1" --> T3[Topic]
```
```

with:

```markdown
  X -- "exactly 1" --> T3[Topic]
  C -- "0..1 (Router)" --> R[Router]
  X -- "0..1 (Router)" --> R
  R -- "1..n, one per rule or default" --> T4[Topic]
```
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
| Allowed pairs: producer→topic, topic→consumer, consumer→topic, consumer→transform, transform→topic. Anything else
```

with:

```markdown
| Allowed pairs: producer→topic, topic→consumer, consumer→topic, consumer→transform, transform→topic, consumer→router, transform→router, router→topic. Anything else
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
| consumer: exactly one incoming edge; at most one outgoing edge in total, to either a topic or a transform | Go, deploy |
| transform: exactly one incoming (from a consumer) and one outgoing (to a topic) | Go, deploy |
```

with:

```markdown
| consumer: exactly one incoming edge; at most one outgoing edge in total, to a topic, a transform or a router | Go, deploy |
| transform: exactly one incoming (from a consumer) and one outgoing (to a topic or a router) | Go, deploy |
| router: exactly one incoming (from a consumer or a transform); every outgoing edge is a rule's or the default's topic, and every rule's and the default's topic has an edge | Go, deploy |
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
### Not planned
```

with:

```markdown
### Router.

Designed in `docs/superpowers/specs/2026-10-07-studio-router-design.md`.

- A Router node after a consumer or its transform: ordered rules (an
  `expr-lang/expr` condition and a wired topic each), first match wins, an
  optional default; no match and no default drops the record, counted as
  `unmatched`.
- Runs inside the upstream consumer: tail → sink → transform → router →
  forward. A value that is not JSON or a failing condition is a router error
  (and its consumer's): the record is not forwarded and later rules are not
  tried.
- **Demo:** `msg.total > 100` to one topic, default to another; a big and a
  small record each arrive on their own topic; the router's edges read
  `#1 · 1` and `default · 1`.
- Built as decided in its plan: conditions compile with `expr.AsBool()` in a
  transform's environment, so one known not to be a boolean (`"x"`, `1 + 2`)
  is refused on deploy and one of unknown type is checked when it runs; the
  router node reuses the transform's snapshot path (`applySteps`), its
  `branches` summed element-wise over instances; the edge labels are worked
  out in the canvas and never saved.

### Not planned
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace:

```markdown
in M5 with a transform
  (one record through, one error),
```

with:

```markdown
in M5 with a transform
  (one record through, one error), with a router (one record per branch, a rule
  that does not compile refused),
```

In `docs/superpowers/specs/2026-10-07-studio-router-design.md`, replace:

```markdown
Status: approved in brainstorming on 2026-10-07; implementation plan to follow
in `docs/superpowers/plans/`.
```

with:

```markdown
Status: approved in brainstorming on 2026-10-07; built from
`docs/superpowers/plans/2026-10-07-studio-router.md`.
```

In `docs/superpowers/specs/2026-10-07-studio-router-design.md`, replace:

```markdown
- **Save:** the router's data decodes (an object with a `rules` array of
  `{when, to}` and an optional string `default`). A half-built router saves.
```

with:

```markdown
- **Save:** like every node's, only the node and edge checks (ids, type,
  allowed pairs); a half-built router saves, and its data is checked on deploy.
```

In `docs/superpowers/specs/2026-10-07-studio-router-design.md`, replace:

```markdown
- A value that is not JSON, or a runtime error in a condition (`nil > 100`), is a
  router error: the record is not forwarded, later rules are not tried, and the
  record still commits (at-most-once, like a failed transform or forward).
```

with:

```markdown
- A value that is not JSON, or a runtime error in a condition (`nil > 100`), is a
  router error, counted on the Router node and as its consumer's error
  (`router: …`, as a transform's failure is): the record is not forwarded, later
  rules are not tried, and the record still commits (at-most-once, like a failed
  transform or forward).
```

In `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`, replace:

```markdown
Eleven tests, run one at a time;
```

with:

```markdown
Fourteen tests (the last three added since, below), run one at a time;
```

In `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`, replace:

```markdown
    the bottom, the newest record comes into view.
```

with:

```markdown
    the bottom, the newest record comes into view.

### Added since

12. **Rewind** (`nodes.spec.ts`). On a deployed flow the consumer's "to earliest"
    is disabled and says to stop the flow; after Stop it rewinds and shows
    `<group> on <topic>: 1 partition rewound to earliest`.
13. **Router in the editor** (`editor.spec.ts`). On an API-created flow shown
    unfitted (one column, room for the router), drop a Router from the palette
    and wire consumer → router → two topics: the Inspector shows two rules
    with those topics, the node reads `2 rules · no default`. Save and Deploy:
    the top bar shows `422: router-1: rule 1: when is required`. Fill rule 1,
    remove rule 2, make its topic the default: the edges read `#1` and
    `default`; Save and Deploy run the flow.
14. **Router counts** (`nodes.spec.ts`). An API-created router flow
    (`msg.total > 100` to one topic, default to another) deployed; two sends
    from the producer's drawer, one big and one small: the router shows
    `2 msgs` and its edges `#1 · 1` and `default · 1`.
```

- [ ] **Step 2: Run the checks**

Run: `make test && make down && make verify`
Expected:
- `STUDIO OK`, `14 passed`, `UI OK` and `VERIFY OK`.
- Then `make down`, after which `docker ps -aq -f label=studio.flow` and `git status --short flows` both print nothing.

- [ ] **Step 3: Commit**

```bash
git add README.md AGENTS.md docs/superpowers/specs/2026-10-06-pipeline-studio-design.md docs/superpowers/specs/2026-10-07-studio-router-design.md docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md
git commit -m "docs: the Router node — README, AGENTS, the Studio spec (record path, snapshot, data, edges, milestone), the router and UI-tests specs" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
