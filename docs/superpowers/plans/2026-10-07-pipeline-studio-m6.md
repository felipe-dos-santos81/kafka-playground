# Pipeline Studio M6 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hardening. The live view says when its numbers stop being live and why, and the numbers it does show are right. The tail and the timer behave under load and on Stop, and `verify-studio` cleans up after itself.

**Architecture:**
- **Snapshot:** `Snapshot` asks every running container for `/stats` in parallel, under one 800 ms budget, while the broker calls run. A container that does not answer keeps no numbers, and gets a `warning` (not `lastError`) once it is 5 s old.
- **Lag** counts only partitions the group has committed.
- **Stream:** `streamTicks` measures Δt between snapshot starts. It ends when the flow's file is gone, so the browser's reconnect gets `/events`' 404. `/events` flushes its headers before the first snapshot.
- **UI:** `watch` reports paused or gone; the top bar says so and the numbers grey out. The tail drawer keeps one fetch in flight and follows only when scrolled to the bottom.
- **Producer:** template failures are the producer's error (counted, logged by a timer, 500 on `send`), and a send cut by Stop is not.
- **`verify-studio`:** deletes its topics, tolerates braces in error text, and checks the 404.

**Tech Stack:** Go 1.27.1, franz-go `kadm` v1.19.0 (`GroupMemberLag.Commit.At` is -1 for a partition with no commit, and its `Lag` then runs from the partition's start; checked through Context7 `/twmb/franz-go` and the module source at `groups.go`), `sync.WaitGroup.Go`; React 19 with the browser's `EventSource`. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`: §7 M6 (every bullet), §9 (M6's verification), and §3.6 (the snapshot loop). M5 shipped on `main`; this plan starts from `309688d`.

## Global Constraints

- No new Go or npm dependency; `go 1.27.1`, images, compose services and the studio tag (`kafka-playground/studio:0.1.0`, `pull_policy: build`) unchanged.
- The spec's numbers, verbatim: `/stats` calls run "in parallel under one 800 ms budget, alongside the Kafka calls"; "in the first 5 s after its container starts, nothing is shown"; "Δt for rates is measured between snapshot starts"; the top bar says "live numbers paused: <reason>"; "`/events` answers 404 JSON for an unknown flow".
- A node that does not answer `/stats` has "its numbers left out and the reason in its `warning` (not `lastError`)".
- `NodeState` (`studio/engine.go`) and `NodeRuntime` (`studio/ui/src/flow/api.ts`) mirror each other. This plan adds no JSON field: `warning` exists on both.
- `verify-studio` greps the snapshot JSON and relies on its field order (`lag` before `assigned`, `instances` last, node ids sorted). Keep it.
- Every `/api` answer stays JSON (except the event stream); the cross-origin write guard and `make verify` ending in `STUDIO OK (...)` and `VERIFY OK` keep holding.
- Checks: `make test` (go vet, gofmt, go test, UI build, compose config). Before finishing a task that touches the Makefile or Go runtime code: `make down && make verify`, then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.
- Makefile: GNU make 3.81, BSD tools, recipe lines start with a real tab, shell `$` written `$$`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Keep README.md, AGENTS.md and the spec in sync.

## Review Focus

1. **The studio restarts while a flow's page is open** (`docker compose stop studio`, then `start`). The page must grey its numbers and say why within a second or two, then come back by itself with no reload. Pinned in Task 3, Step 3 (the UI has no unit tests; a live check).
2. **A node container stops answering `/stats` while Docker still says `running`** (SIGSTOP). That node must show the reason as a warning, every other node keeps its numbers, and ticks keep arriving about once a second. Pinned in Task 1, Step 1 (`TestWithStats`) and Step 5 (a live `docker kill -s STOP`).
3. **A `latest` consumer before its first commit** must show no lag, not the partition's whole length. Pinned in Task 1, Step 1 (`TestApplyKafka`) and Step 5 (live, right after deploy).
4. **A flow deleted while its page is open:** the stream must end, `/events` answers 404, and the page closes the flow saying so. Pinned in Task 1, Step 1 (`TestStreamTicksEnds`, `TestEventsOfAnUnknownFlow`), Task 3, Step 3 (live) and Task 4 (`verify-studio`).
5. **Scrolling up in the tail drawer while a timer produces** must keep the view where it is; scrolling back to the bottom follows again. Pinned in Task 3, Step 3 (live).

## Decisions this plan makes (Task 4 writes them into the spec)

1. **The 5 s grace counts from the container's creation time** (`ContainerList`'s `Created`, kept on `NodeState` as the unexported `created`). Node containers have no restart policy, so creation is their start; a container restarted by hand keeps its creation time and gets no grace.
2. **Only containers Docker reports `running` are asked.** A `docker pause`d one shows `paused` and no numbers, so it needs no warning.
3. **A partition counts toward lag only when kadm reports a commit for it** (`Commit.At >= 0`). For the others kadm measures from the partition's start, which would show a `latest` consumer as far behind.
4. **A node with instances carries its first instance's warning, prefixed `#<i>: `**, like its `lastError`.
5. **The stream ends when its flow's file is gone** (`ErrNotFound` from the snapshot). The 404 comes from the browser's reconnect.
6. **The UI reopens a refused stream every 2 s unless the flow answers 404.** Vite's dev proxy answers 502 while the studio is down; the compose'd UI sees a dropped connection, which `EventSource` retries by itself.
7. **The tail drawer retries a failed fetch on the next tick** through a tick counter that `App` passes down.
8. **A template `send` that renders no JSON is a 500** (the node's error, counted); a body that is not JSON stays a 400 (the caller's error).
9. **`verify-studio` deletes `studio-verify.*` topics** (a `kafka-topics.sh --delete` regex) in its exit trap, after deleting its flows, so a failed run cleans up too.

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `studio/engine.go` | `statsBudget`, `statsGrace`, `NodeState.created`, `nodeStates`/`nodeOf` keep each container's summary, parallel `/stats` in `Snapshot`, `withStats` and `nodeStatsOf` take a URL and warn, committed-only lag, instance warnings, `streamTicks` Δt and end | 1 |
| `studio/api.go` | `/events` flushes its headers first | 1 |
| `studio/engine_test.go`, `studio/api_test.go` | `TestApplyKafka` lag cases, `TestWithRates` Δt 0, `TestStreamTicksEnds`, `TestWithStats`, `TestEventsOfAnUnknownFlow` | 1 |
| `studio/node.go`, `studio/node_test.go` | template errors, a send cut by Stop, the timer test that cannot block | 2 |
| `studio/ui/src/flow/api.ts`, `App.tsx`, `TailDrawer.tsx`, `nodes/StudioNodes.tsx`, `index.css` | paused and gone, warnings on nodes, the tail drawer | 3 |
| `Makefile`, `README.md`, spec | `verify-studio` cleanup and checks; docs | 4 |

---

### Task 1: Honest, parallel snapshots; the stream ends with its flow

**Files:**
- Modify: `studio/engine.go`, `studio/api.go`
- Test: `studio/engine_test.go`, `studio/api_test.go`

**Interfaces:**
- Consumes: `Snapshot`, `stateOf`, `nodeStates`, `nodeOf`, `withStats`, `nodeStatsOf`, `applyKafka`, `sumInstances`, `sumCounts`, `instanceError`, `streamTicks`, `withRates` (`engine.go`); `nodeRef.url` (`resolve.go`); `ErrNotFound` (`store.go`); `events` (`api.go`); test helpers `newTestServer`, `call` (`api_test.go`).
- Produces: `const statsBudget = 800 * time.Millisecond`, `const statsGrace = 5 * time.Second`; `NodeState.created int64` (unexported); `nodeOf(insts map[int]container.Summary) NodeState`; `withStats(ctx context.Context, url string, ns NodeState) NodeState`; `nodeStatsOf(ctx context.Context, url string) (nodeStats, error)`. Task 3 shows `warning` on producer and consumer nodes.

- [ ] **Step 1: Write the failing tests**

In `studio/engine_test.go`, replace

```go
	// A non-nil but empty answer means the broker answered and knows no such topic
```

with

```go
	// Lag counts only the partitions the group has committed (kadm says At -1 for
	// the others, with a lag from the partition's start); none committed, no lag.
	uncommitted := kadm.GroupMemberLag{Topic: "orders", Partition: 0, Lag: 7, Commit: kadm.Offset{At: -1}}
	committed := kadm.GroupMemberLag{Topic: "orders", Partition: 1, Lag: 2, Commit: kadm.Offset{At: 3}}
	for _, c := range []struct {
		lag  map[int32]kadm.GroupMemberLag
		want *int64
	}{
		{map[int32]kadm.GroupMemberLag{0: uncommitted}, nil},
		{map[int32]kadm.GroupMemberLag{0: uncommitted, 1: committed}, &committed.Lag},
	} {
		fresh := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
		applyKafka(&fresh, "f", specs[1:2], nil, kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": c.lag}}}, nil)
		if got := fresh.Nodes["consumer-1"].Lag; (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
			t.Fatalf("lag over %v: got %v, want %v", c.lag, got, c.want)
		}
	}

	// A non-nil but empty answer means the broker answered and knows no such topic
```

In `studio/engine_test.go`, replace

```go
	withRates(&cur, prev, 2)
	for node, want := range
```

with

```go
	if withRates(&cur, prev, 0); cur.Nodes["steady"].Rate != 0 {
		t.Fatalf("Δt 0 (a clock step): want no rate, got %v", cur.Nodes["steady"].Rate)
	}
	withRates(&cur, prev, 2)
	for node, want := range
```

In `studio/engine_test.go`, replace

```go
	"errors"
	"reflect"
```

with

```go
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
```

Append to `studio/engine_test.go`:

```go
func TestStreamTicksEnds(t *testing.T) {
	for _, c := range []struct {
		name  string
		snap  func(context.Context) (FlowState, error)
		flush func() error
		want  string // what was written before the stream ended
	}{
		{"a failed flush", func(context.Context) (FlowState, error) { return FlowState{Status: "stopped"}, nil },
			func() error { return errors.New("broken pipe") }, "event: tick\ndata: {\"status\":\"stopped\",\"nodes\":null}\n\n"},
		{"the flow deleted", func(context.Context) (FlowState, error) { return FlowState{}, ErrNotFound },
			func() error { return nil }, ""},
	} {
		var out bytes.Buffer
		done := make(chan struct{})
		go func() {
			streamTicks(context.Background(), &out, c.flush, c.snap, time.Millisecond)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: the stream did not end", c.name)
		}
		if out.String() != c.want {
			t.Fatalf("%s: wrote %q, want %q", c.name, out.String(), c.want)
		}
	}
}

func TestWithStats(t *testing.T) {
	status := http.StatusOK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, `{"boot":"b","total":7,"errors":1,"lastError":"x","tailSeq":7}`)
	}))
	defer ts.Close()
	old := time.Now().Add(-time.Minute).Unix()

	got := withStats(context.Background(), ts.URL, NodeState{State: "running", created: old})
	if got.Total != 7 || got.Errors != 1 || got.LastError != "x" || got.TailSeq != 7 || got.Boot != "b" || got.Warning != "" {
		t.Fatalf("a 200: want its counters, got %+v", got)
	}
	status = http.StatusInternalServerError
	got = withStats(context.Background(), ts.URL, NodeState{State: "running", created: old})
	if got.Total != 0 || got.LastError != "" || got.Warning != "stats: 500 Internal Server Error" {
		t.Fatalf("a 500: want no numbers and the reason as a warning, got %+v", got)
	}
	if got = withStats(context.Background(), ts.URL, NodeState{State: "running", created: time.Now().Unix()}); got.Warning != "" {
		t.Fatalf("a container younger than statsGrace: want nothing said yet, got %+v", got)
	}

	ns := NodeState{Instances: []NodeState{{Instance: 1, Total: 2}, {Instance: 2, Warning: "stats: timeout"}}}
	if ns.sumInstances(); ns.Total != 2 || ns.Warning != "#2: stats: timeout" {
		t.Fatalf("instances: want their sum and the first warning with its instance, got %+v", ns)
	}
}
```

Append to `studio/api_test.go`:

```go
func TestEventsOfAnUnknownFlow(t *testing.T) {
	ts := newTestServer(t)
	code, body := call(t, ts, "GET", "/api/flows/deadbeef/events", nil)
	var e struct{ Error string }
	if code != 404 || json.Unmarshal(body, &e) != nil || e.Error == "" {
		t.Fatalf("events of an unknown flow: want 404 with a JSON error, got %d %s", code, body)
	}
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL to compile: `cannot use ts.URL (variable of type string) as nodeRef value in argument to withStats` and `unknown field created in struct literal of type NodeState`.

- [ ] **Step 3: Implement**

In `studio/engine.go`, replace

```go
	step *stepStats // a consumer container's transform counts; applySteps puts them on the transform node
}
```

with

```go
	step    *stepStats // a consumer container's transform counts; applySteps puts them on the transform node
	created int64      // when the container was created (Unix seconds): withStats gives it statsGrace
}

// A snapshot asks every running container for /stats in parallel, all within
// statsBudget. A container that does not answer gets a warning, unless it was
// created less than statsGrace ago: it is still starting.
const (
	statsBudget = 800 * time.Millisecond
	statsGrace  = 5 * time.Second
)
```

In `studio/engine.go`, replace

```go
	slots := map[string]map[int]string{} // node id → instance → container state
	for _, n := range f.Nodes {
		if n.Type == "producer" || n.Type == "consumer" {
			slots[n.ID] = map[int]string{}
			for _, i := range instancesOf(n) {
				slots[n.ID][i] = "missing"
			}
		}
	}
```

with

```go
	slots := map[string]map[int]container.Summary{} // node id → instance → its container
	for _, n := range f.Nodes {
		if n.Type == "producer" || n.Type == "consumer" {
			slots[n.ID] = map[int]container.Summary{}
			for _, i := range instancesOf(n) {
				slots[n.ID][i] = container.Summary{State: "missing"}
			}
		}
	}
```

In `studio/engine.go`, replace

```go
		if slots[node] == nil {
			slots[node] = map[int]string{}
		}
		slots[node][i] = string(c.State)
```

with

```go
		if slots[node] == nil {
			slots[node] = map[int]container.Summary{}
		}
		slots[node][i] = c
```

In `studio/engine.go`, replace

```go
			for i, state := range insts {
				isInstance := i > 0
				if state == "missing" && isInstance != asInstances {
```

with

```go
			for i, c := range insts {
				isInstance := i > 0
				if c.State == "missing" && isInstance != asInstances {
```

In `studio/engine.go`, replace

```go
// nodeOf folds a node's container states into one NodeState: a lone instance 0
// is the node itself; otherwise every instance is an entry of Instances, in
// order, and the node runs only when all of them run (else it takes the first
// other state).
func nodeOf(insts map[int]string) NodeState {
	if s, ok := insts[0]; ok && len(insts) == 1 {
		return NodeState{State: s}
	}
	ns := NodeState{State: "running"}
	for _, i := range slices.Sorted(maps.Keys(insts)) {
		ns.Instances = append(ns.Instances, NodeState{Instance: i, State: insts[i]})
		if ns.State == "running" && insts[i] != "running" {
			ns.State = insts[i]
		}
	}
	return ns
}
```

with

```go
// nodeOf folds a node's containers into one NodeState: a lone instance 0 is the
// node itself; otherwise every instance is an entry of Instances, in order, and
// the node runs only when all of them run (else it takes the first other state).
func nodeOf(insts map[int]container.Summary) NodeState {
	if c, ok := insts[0]; ok && len(insts) == 1 {
		return NodeState{State: string(c.State), created: c.Created}
	}
	ns := NodeState{State: "running"}
	for _, i := range slices.Sorted(maps.Keys(insts)) {
		c := insts[i]
		ns.Instances = append(ns.Instances, NodeState{Instance: i, State: string(c.State), created: c.Created})
		if ns.State == "running" && c.State != "running" {
			ns.State = string(c.State)
		}
	}
	return ns
}
```

In `studio/engine.go`, replace

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
	topics
```

with

```go
	// Every running container's /stats at once, under one budget, while the broker
	// calls below run; each goroutine writes only its own container's state.
	sctx, cancelStats := context.WithTimeout(ctx, statsBudget)
	defer cancelStats()
	var wg sync.WaitGroup
	nodes := map[string]*NodeState{}
	for node, ns := range st.Nodes {
		nodes[node] = &ns
		for _, c := range ns.containers() {
			if c.State == "running" {
				wg.Go(func() { *c = withStats(sctx, nodeRef{id, node, c.Instance}.url("/stats"), *c) })
			}
		}
	}
	specs, _ := Resolve(f)
	topics
```

In `studio/engine.go`, replace

```go
		if got, err := e.adm.ListEndOffsets(kctx, names...); err == nil {
			ends = got
		}
	}
	applyKafka(&st, id, specs, topics, lags, ends)
```

with

```go
		if got, err := e.adm.ListEndOffsets(kctx, names...); err == nil {
			ends = got
		}
	}
	wg.Wait()
	for node, ns := range nodes {
		ns.sumInstances()
		st.Nodes[node] = *ns
	}
	applySteps(&st, specs)
	applyKafka(&st, id, specs, topics, lags, ends)
```

In `studio/engine.go`, replace

```go
// withStats is ns (a node, or one of its instances) with the counters its
// container r reports on /stats.
func withStats(ctx context.Context, r nodeRef, ns NodeState) NodeState {
	s, err := nodeStatsOf(ctx, r)
	if err != nil {
		ns.LastError = "stats: " + err.Error()
		return ns
	}
```

with

```go
// withStats is ns (a node, or one of its instances) with the counters its
// container reports at url. A container that does not answer keeps no numbers,
// and its warning says why once it is older than statsGrace.
func withStats(ctx context.Context, url string, ns NodeState) NodeState {
	s, err := nodeStatsOf(ctx, url)
	if err != nil {
		if time.Since(time.Unix(ns.created, 0)) >= statsGrace {
			ns.Warning = "stats: " + err.Error()
		}
		return ns
	}
```

In `studio/engine.go`, replace

```go
// nodeStatsOf asks a running node container for its counters (1 s budget).
func nodeStatsOf(ctx context.Context, r nodeRef) (nodeStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url("/stats"), nil)
```

with

```go
// nodeStatsOf asks a node container's /stats at url for its counters, within ctx;
// any answer but 200 is an error.
func nodeStatsOf(ctx context.Context, url string) (nodeStats, error) {
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
```

In `studio/engine.go`, replace

```go
// for each consumer node its group's lag on its
// topic and the partitions
```

with

```go
// for each consumer node its group's lag on its topic (over the partitions the
// group has committed; none committed yet, no lag) and the partitions
```

In `studio/engine.go`, replace

```go
		var lag int64
		held := map[*NodeState][]int32{} // container → partitions its client holds
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Err == nil && ml.Lag > 0 {
				lag += ml.Lag
			}
```

with

```go
		var lag int64
		committed := false
		held := map[*NodeState][]int32{} // container → partitions its client holds
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Err == nil && ml.Commit.At >= 0 { // At -1: no commit, so kadm's lag runs from the start
				committed = true
				lag += max(ml.Lag, 0)
			}
```

In `studio/engine.go`, replace

```go
		ns.Lag = &lag
		for c, ps := range held {
```

with

```go
		if committed {
			ns.Lag = &lag
		}
		for c, ps := range held {
```

In `studio/engine.go`, replace

```go
// sumInstances gives a node with instances the sums of their counters and rates;
// a node with one container is its container already.
func (ns *NodeState) sumInstances() {
	if len(ns.Instances) > 0 {
		ns.sumCounts(ns.Instances)
	}
}
```

with

```go
// sumInstances gives a node with instances the sums of their counters and rates,
// and the first of their warnings, prefixed with its instance; a node with one
// container is its container already.
func (ns *NodeState) sumInstances() {
	if len(ns.Instances) == 0 {
		return
	}
	ns.sumCounts(ns.Instances)
	ns.Warning = ""
	for _, in := range ns.Instances {
		if in.Warning != "" {
			ns.Warning = instanceError(in.Instance, in.Warning)
			break
		}
	}
}
```

In `studio/engine.go`, replace

```go
// streamTicks is the body of GET /api/flows/{id}/events: every period it writes
// the flow's snapshot as an SSE `tick` event with rates against the previous
// tick; a failed snapshot is a `problem` event and the stream goes on. It returns
// when ctx ends (the browser went away) or a flush fails. Each open stream polls
// on its own: one tab, one loop.
```

with

```go
// streamTicks is the body of GET /api/flows/{id}/events: every period it writes
// the flow's snapshot as an SSE `tick` event with rates against the previous
// tick (Δt between the two snapshots' starts); a failed snapshot is a `problem`
// event and the stream goes on. It returns when ctx ends (the browser went
// away), a flush fails, or the flow is gone: the browser's reconnect then gets
// a 404. Each open stream polls on its own: one tab, one loop.
```

In `studio/engine.go`, replace

```go
	for {
		st, err := snap(ctx)
		if ctx.Err() != nil {
			return
		}
```

with

```go
	for {
		start := time.Now()
		st, err := snap(ctx)
		if ctx.Err() != nil || errors.Is(err, ErrNotFound) {
			return
		}
```

In `studio/engine.go`, replace

```go
			now := time.Now()
			withRates(&st, prev, now.Sub(last).Seconds())
			prev, last = st, now
```

with

```go
			withRates(&st, prev, start.Sub(last).Seconds())
			prev, last = st, start
```

In `studio/api.go`, replace

```go
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	streamTicks(
```

with

```go
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	if rc.Flush() != nil { // the headers, now: the browser's EventSource opens before the first snapshot
		return
	}
	streamTicks(
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd studio && go vet ./... && gofmt -l . && go test -race -count=1 ./...`
Expected: `ok  kafka-playground/studio`; `gofmt -l` prints nothing. `-race` matters here: the `/stats` goroutines write into the snapshot.

- [ ] **Step 5: Check it live**

Run `make down && make verify`. Expected: `STUDIO OK (...)` and `VERIFY OK`. Then, with the stack still up, run this from the repo root. It deploys a timer into a 3-partition topic read by a 3-instance `latest` consumer:

```sh
U=http://localhost:8082
F='{"name":"m6-live","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"timer","interval_ms":100,"key":"{{.Seq}}","value":"{\"n\": {{.Seq}}}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"m6-live","partitions":3,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"m6-live","auto_offset_reset":"latest","instances":3,"sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"}]}'
id=$(curl -sS -X POST $U/api/flows -H 'Content-Type: application/json' --data "$F" | sed 's/^{"id":"\([0-9a-f]*\)".*/\1/')
curl -sS -X POST $U/api/flows/$id/deploy; echo
curl -sS $U/api/flows/$id/state; echo                      # right after deploy
sleep 8; docker kill -s STOP studio-$id-consumer-1-2 >/dev/null
time curl -sS $U/api/flows/$id/state; echo                 # instance 2 does not answer
curl -sS -N --max-time 3.5 $U/api/flows/$id/events | grep '^event:'
docker kill -s CONT studio-$id-consumer-1-2 >/dev/null
curl -sS -N -D - --max-time 2 $U/api/flows/$id/events -o /dev/null | head -1
(curl -sS -N --max-time 10 $U/api/flows/$id/events -o /dev/null -w 'stream ended after %{time_total}s\n' & sleep 2; curl -sS -X DELETE $U/api/flows/$id; wait)
curl -sS -o /dev/null -w '%{http_code}\n' $U/api/flows/$id/events
make down
```

Expected, in order:
- Right after deploy, `consumer-1` has no `lag` (no commit yet) and no `warning`, even though instances 2 and 3 have no numbers yet.
- With instance 2 stopped, the `state` call takes about 0.8 s. `consumer-1` then carries `"warning":"#2: stats: Get \"http://studio-<id>-consumer-1-2:9000/stats\": context deadline exceeded"`, instance 2 has the same warning without the prefix and no `total`, and instances 1 and 3 keep their numbers.
- Three `event: tick` lines arrive in 3.5 s.
- The headers line is `HTTP/1.1 200 OK`, printed before any tick.
- `stream ended after 3.0…s`: the stream ends within a second of the delete.
- The last call prints `404`.

After `make down`, `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing. The `m6-live` topic stays in the broker's volume until `make down` removes the volume, as every topic does.

- [ ] **Step 6: Commit**

```bash
git add studio/engine.go studio/api.go studio/engine_test.go studio/api_test.go
git commit -m "studio: snapshots ask /stats in parallel under one budget and warn about a silent container; lag counts committed partitions; the stream ends with its flow" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Timer and template errors; a send cut by Stop is no error

**Files:**
- Modify: `studio/node.go`
- Test: `studio/node_test.go`

**Interfaces:**
- Consumes: `producer`, `next`, `produceOne`, `send`, `run`, `counters.step`, `errInvalidJSON` (`node.go`).
- Produces: nothing new; `next` returns a non-`errInvalidJSON` error ("value template rendered %.60q, which is not JSON") when a template renders no JSON.

- [ ] **Step 1: Write the failing tests**

In `studio/node_test.go`, replace

```go
	if got := p.tail.since(0); len(got) != 3 {
		t.Fatalf("want the 3 produced records in the tail, got %d", len(got))
	}
```

with

```go
	if got := p.tail.since(0); len(got) != 3 {
		t.Fatalf("want the 3 produced records in the tail, got %d", len(got))
	}
	// Templates that render no JSON are the node's error: 500, counted, nothing produced.
	bad := &producer{spec: NodeSpec{Topic: "orders", Value: `{{.Seq}}x`}, tail: &tail{}, counts: &counters{}, produce: p.produce}
	w := httptest.NewRecorder()
	bad.send(w, httptest.NewRequest(http.MethodPost, "/send", nil))
	if s := bad.counts.step(); w.Code != http.StatusInternalServerError || s.Errors != 1 || !strings.Contains(s.LastError, `rendered "1x"`) || len(sent) != 3 {
		t.Fatalf("a template rendering 1x: want 500 and one error counted, got %d %s and %+v", w.Code, w.Body, s)
	}
```

In `studio/node_test.go`, replace

```go
	got := make(chan *kgo.Record, 16)
	p := &producer{
		spec:   NodeSpec{Topic: "orders", Value: `{"n": {{.Seq}}}`},
		tail:   &tail{},
		counts: &counters{},
		produce: func(_ context.Context, r *kgo.Record) error {
			got <- r
			return nil
		},
	}
```

with

```go
	got := make(chan *kgo.Record, 16)
	p := &producer{
		spec:   NodeSpec{Topic: "orders", Value: `{"n": {{.Seq}}}`},
		tail:   &tail{},
		counts: &counters{},
		// Like ProduceSync: a full channel (the test stopped reading) blocks until
		// the context ends, and then fails with the context's error.
		produce: func(ctx context.Context, r *kgo.Record) error {
			select {
			case got <- r:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
```

In `studio/node_test.go`, replace

```go
	if p.counts.total.Load() < 3 || p.tail.last() < 3 {
		t.Fatalf("want at least 3 counted and tailed, got %d and %d", p.counts.total.Load(), p.tail.last())
	}
```

with

```go
	if s := p.counts.step(); s.Total < 3 || p.tail.last() < 3 || s.Errors != 0 {
		t.Fatalf("want at least 3 counted and tailed, and a send cut by the stop not counted as an error; got %+v, tail %d", s, p.tail.last())
	}

	// A value that renders no JSON is the timer's error, tick after tick; nothing is produced.
	bad := &producer{spec: NodeSpec{Topic: "orders", Value: `{{.Seq}}x`}, tail: &tail{}, counts: &counters{}, produce: p.produce}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go bad.run(ctx, 5*time.Millisecond)
	for deadline := time.Now().Add(2 * time.Second); bad.counts.errors.Load() < 2 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	if s := bad.counts.step(); s.Errors < 2 || !strings.Contains(s.LastError, "not JSON") || bad.tail.last() != 0 {
		t.Fatalf("want the timer's errors counted and nothing produced, got %+v", s)
	}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd studio && go test -run 'TestProducer' ./...`
Expected: FAIL. `TestProducerSend`: `a template rendering 1x: want 500 and one error counted, got 400 {"error":"value is not valid JSON"}`. `TestProducerTimer`: `want the timer's errors counted and nothing produced, got {Total:0 Errors:2 LastError:value is not valid JSON}`. Before that it may also report a send cut by the stop counted as an error.

- [ ] **Step 3: Implement**

In `studio/node.go`, replace

```go
		if key, err = render(p.spec.Key, d); err != nil {
			return nil, fmt.Errorf("key template: %w", err)
		}
		body = []byte(v)
	}
	if !json.Valid(body) {
		return nil, errInvalidJSON
	}
```

with

```go
		if key, err = render(p.spec.Key, d); err != nil {
			return nil, fmt.Errorf("key template: %w", err)
		}
		if !json.Valid([]byte(v)) {
			return nil, fmt.Errorf("value template rendered %.60q, which is not JSON", v)
		}
		body = []byte(v)
	} else if !json.Valid(body) {
		return nil, errInvalidJSON
	}
```

In `studio/node.go`, replace

```go
// produceOne produces rec and counts the outcome; the tail gets every record that made it.
func (p *producer) produceOne(ctx context.Context, rec *kgo.Record) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.produce(ctx, rec); err != nil {
		p.counts.fail(err)
		return err
	}
```

with

```go
// produceOne produces rec and counts the outcome; the tail gets every record that
// made it. A send cut short because ctx ended (Stop, or the caller went away) is
// no error of the node's and is not counted.
func (p *producer) produceOne(ctx context.Context, rec *kgo.Record) error {
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.produce(pctx, rec); err != nil {
		if ctx.Err() == nil {
			p.counts.fail(err)
		}
		return err
	}
```

In `studio/node.go`, replace

```go
// send produces one record. A body is the value as is (curl, webhooks), keyed by
// ?key=; an empty body renders the node's own templates (the UI's Send button).
// It answers {partition, offset}.
```

with

```go
// send produces one record. A body is the value as is (curl, webhooks), keyed by
// ?key=; an empty body renders the node's own templates (the UI's Send button).
// It answers {partition, offset}: 400 for a body that is not JSON, 500 (and an
// error counted) for templates that fail or render no JSON.
```

In `studio/node.go`, replace

```go
		rec, err := p.next("", nil)
		if err != nil {
			p.counts.fail(err)
			continue
		}
		if err := p.produceOne(ctx, rec); err != nil {
			log.Printf("produce: %v", err)
		}
```

with

```go
		rec, err := p.next("", nil)
		if err != nil {
			p.counts.fail(err)
			log.Printf("timer: %v", err)
			continue
		}
		if err := p.produceOne(ctx, rec); err != nil && ctx.Err() == nil {
			log.Printf("produce: %v", err)
		}
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd studio && go vet ./... && gofmt -l . && go test -race -count=3 -run 'TestProducer' ./... && go test ./...`
Expected: `ok  kafka-playground/studio` twice; `gofmt -l` prints nothing.

- [ ] **Step 5: Run the end-to-end check**

Run: `make down && make verify`, then `make down`.
Expected: `STUDIO OK (...)` and `VERIFY OK`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 6: Commit**

```bash
git add studio/node.go studio/node_test.go
git commit -m "studio: template failures are the producer's error, logged by a timer; a send cut by Stop is not counted; the timer test cannot block" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The UI says when numbers are not live; the tail drawer behaves

**Files:**
- Modify: `studio/ui/src/flow/api.ts`, `studio/ui/src/App.tsx`, `studio/ui/src/TailDrawer.tsx`, `studio/ui/src/nodes/StudioNodes.tsx`, `studio/ui/src/index.css`

**Interfaces:**
- Consumes: Task 1's behaviour: `problem` events, a stream that ends when its flow is deleted, `/events` 404, and `warning` on producer and consumer nodes (instances roll theirs up as `#<i>: …`). Also `api.get`, `ApiError` and `describe` (`api.ts`).
- Produces: `type LiveStatus = { paused?: string; gone?: boolean }`; `watch(id, onTick, onStatus: (s: LiveStatus) => void)`; `TailDrawer` prop `tick: number`.

- [ ] **Step 1: Implement**

In `studio/ui/src/flow/api.ts`, replace

```ts
// watch opens the flow's event stream: onTick gets a snapshot once a second.
// EventSource reconnects by itself; the returned function closes the stream.
export function watch(id: string, onTick: (s: FlowState) => void): () => void {
  const es = new EventSource(`/api/flows/${id}/events`)
  es.addEventListener('tick', (e) => onTick(JSON.parse((e as MessageEvent<string>).data)))
  return () => es.close()
}
```

with

```ts
// What the live view says besides its ticks: paused (and why) until the next
// tick, or gone when the flow was deleted.
export type LiveStatus = { paused?: string; gone?: boolean }

// watch opens the flow's event stream: onTick gets a snapshot once a second, and
// onStatus hears whenever the numbers stop being live. A `problem` event or a
// dropped connection pauses them until the next tick (EventSource reconnects by
// itself). A stream the server refused is either gone (the flow answers 404) or
// reopened every 2 s (Vite's proxy answers 502 while studio is down). The
// returned function closes the stream.
export function watch(id: string, onTick: (s: FlowState) => void, onStatus: (s: LiveStatus) => void): () => void {
  let es: EventSource
  let retry: ReturnType<typeof setTimeout> | undefined
  let closed = false
  const reopen = (why: string) => {
    onStatus({ paused: why })
    if (!closed) retry = setTimeout(open, 2000)
  }
  const open = () => {
    es = new EventSource(`/api/flows/${id}/events`)
    es.addEventListener('tick', (e) => {
      onStatus({})
      onTick(JSON.parse((e as MessageEvent<string>).data))
    })
    es.addEventListener('problem', (e) => onStatus({ paused: JSON.parse((e as MessageEvent<string>).data).error }))
    es.onerror = () => {
      if (es.readyState !== EventSource.CLOSED) return onStatus({ paused: 'connection lost, reconnecting' })
      api.get(id).then(
        () => reopen('the stream was refused, retrying'),
        (e) => (e instanceof ApiError && e.status === 404 ? onStatus({ gone: true }) : reopen(describe(e))),
      )
    }
  }
  open()
  return () => {
    closed = true
    clearTimeout(retry)
    es.close()
  }
}
```

In `studio/ui/src/App.tsx`, replace

```tsx
import { api, containersOf, describe, watch, type FlowState, type FlowSummary, type NodeRuntime } from './flow/api'
```

with

```tsx
import { api, containersOf, describe, watch, type FlowState, type FlowSummary, type LiveStatus, type NodeRuntime } from './flow/api'
```

In `studio/ui/src/App.tsx`, replace

```tsx
  const [flowState, setFlowState] = useState<FlowState | null>(null)
```

with

```tsx
  const [flowState, setFlowState] = useState<FlowState | null>(null)
  const [tick, setTick] = useState(0) // counts ticks: the tail drawer retries a failed fetch on the next one
  const [paused, setPaused] = useState('') // why the live numbers stopped, until the next tick
```

In `studio/ui/src/App.tsx`, replace

```tsx
  // The open flow's live snapshot: one `tick` a second over SSE (spec 3.6).
  const flowId = current?.id
  useEffect(() => {
    setFlowState(null)
    if (!flowId) return
    return watch(flowId, setFlowState)
  }, [flowId])
```

with

```tsx
  // The open flow's live snapshot: one `tick` a second over SSE (spec 3.6). When
  // the flow is deleted elsewhere (another tab, curl), it closes and says so.
  const flowId = current?.id
  useEffect(() => {
    setFlowState(null)
    setPaused('')
    if (!flowId) return
    const onTick = (s: FlowState) => {
      setFlowState(s)
      setTick((n) => n + 1)
    }
    const onStatus = (s: LiveStatus) => {
      setPaused(s.paused ?? '')
      if (!s.gone) return
      setError('the open flow was deleted')
      setCurrent(null)
      setNodes([])
      setEdges([])
      refresh()
    }
    return watch(flowId, onTick, onStatus)
  }, [flowId, refresh, setNodes, setEdges])
```

In `studio/ui/src/App.tsx`, replace

```tsx
            <span className="status">{running ? 'running' : 'stopped'}</span>
```

with

```tsx
            <span className="status">{running ? 'running' : 'stopped'}</span>
            {paused && <span className="paused">live numbers paused: {paused}</span>}
```

In `studio/ui/src/App.tsx`, replace

```tsx
      <main className="canvas">
```

with

```tsx
      <main className={paused ? 'canvas paused' : 'canvas'}>
```

In `studio/ui/src/App.tsx`, replace

```tsx
          tailSeq={tailed?.tailSeq}
          boot={tailed?.boot}
```

with

```tsx
          tailSeq={tailed?.tailSeq}
          boot={tailed?.boot}
          tick={tick}
```

In `studio/ui/src/nodes/StudioNodes.tsx`, replace

```tsx
  const parts = [`${rt.total ?? 0} msgs`, `${(rt.rate ?? 0).toFixed(1)}/s`]
```

with

```tsx
  if (rt.warning && rt.total === undefined) return rt.warning // no numbers: say why
  const parts = [`${rt.total ?? 0} msgs`, `${(rt.rate ?? 0).toFixed(1)}/s`]
```

In `studio/ui/src/nodes/StudioNodes.tsx`, replace

```tsx
    if (held.length > 0) parts.push(`${c.instance ? `#${c.instance} ` : ''}p${held.join(',')}`)
  }
  return parts.join(' · ')
```

with

```tsx
    if (held.length > 0) parts.push(`${c.instance ? `#${c.instance} ` : ''}p${held.join(',')}`)
  }
  if (rt.warning) parts.push(rt.warning)
  return parts.join(' · ')
```

In `studio/ui/src/TailDrawer.tsx`, replace

```tsx
  tailSeq?: number
  boot?: string
}
```

with

```tsx
  tailSeq?: number
  boot?: string
  tick: number // counts the flow's ticks
}
```

In `studio/ui/src/TailDrawer.tsx`, replace

```tsx
// The selected node's last records. It fetches only when the node's tailSeq (from
// the SSE tick) moves past what it has, and starts over when boot changes: the
// container restarted and numbers its records from 1 again. A producer's drawer
// also has Send, which renders the node's own key and value templates. A consumer
// with instances tails one of them, picked in the header.
export default function TailDrawer({ flowId, node, instance, instances, onInstance, tailSeq = 0, boot = '' }: Props) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [sent, setSent] = useState('')
  const since = useRef(0)
  const box = useRef<HTMLElement>(null)
```

with

```tsx
// The selected node's last records. It fetches only when the node's tailSeq (from
// the SSE tick) moves past what it has, one fetch at a time (when one ends, it
// fetches again if tailSeq moved meanwhile; a failed one is retried on the next
// tick), and starts over when boot changes: the container restarted and numbers
// its records from 1 again. It follows the newest record only while scrolled to
// the bottom. A producer's drawer also has Send, which renders the node's own key
// and value templates. A consumer with instances tails one of them, picked in the
// header.
export default function TailDrawer({ flowId, node, instance, instances, onInstance, tailSeq = 0, boot = '', tick }: Props) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [sent, setSent] = useState('')
  const [fetched, setFetched] = useState(0) // counts fetches that brought records, to chase tailSeq
  const since = useRef(0)
  const fetching = useRef(false)
  const generation = useRef(0) // bumped on a restart: a fetch from before it is dropped
  const atBottom = useRef(true)
  const box = useRef<HTMLElement>(null)
```

In `studio/ui/src/TailDrawer.tsx`, replace

```tsx
    lastBoot.current = boot
    since.current = 0
    setEntries([])
  }, [boot])

  useEffect(() => {
    if (tailSeq <= since.current) return
    let live = true
    api.tail(flowId, node.id, since.current, instance).then(
      (got) => {
        if (!live) return
        setError('')
        if (got.length === 0) return
        since.current = got[got.length - 1].seq
        setEntries((es) => [...es, ...got].slice(-100))
      },
      (e) => {
        if (live) setError(describe(e))
      },
    )
    return () => {
      live = false
    }
  }, [flowId, node.id, instance, boot, tailSeq])

  // Keep the newest record in view.
  useEffect(() => {
    box.current?.scrollTo({ top: box.current.scrollHeight })
  }, [entries])
```

with

```tsx
    lastBoot.current = boot
    generation.current++
    since.current = 0
    setEntries([])
  }, [boot])

  useEffect(() => {
    if (fetching.current || tailSeq <= since.current) return
    fetching.current = true
    const gen = generation.current
    api.tail(flowId, node.id, since.current, instance).then(
      (got) => {
        fetching.current = false
        setError('')
        if (gen !== generation.current || got.length === 0) return
        since.current = got[got.length - 1].seq
        setEntries((es) => [...es, ...got].slice(-100))
        setFetched((n) => n + 1)
      },
      (e) => {
        fetching.current = false
        setError(describe(e))
      },
    )
  }, [flowId, node.id, instance, boot, tailSeq, tick, fetched])

  // Follow the newest record, unless scrolled up to read an older one.
  useEffect(() => {
    if (atBottom.current) box.current?.scrollTo({ top: box.current.scrollHeight })
  }, [entries])
  const onScroll = (e: UIEvent<HTMLElement>) => {
    const el = e.currentTarget
    atBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 8
  }
```

In `studio/ui/src/TailDrawer.tsx`, replace

```tsx
import { useEffect, useRef, useState } from 'react'
```

with

```tsx
import { useEffect, useRef, useState, type UIEvent } from 'react'
```

In `studio/ui/src/TailDrawer.tsx`, replace

```tsx
    <section className="drawer" ref={box}>
```

with

```tsx
    <section className="drawer" ref={box} onScroll={onScroll}>
```

Append to `studio/ui/src/index.css`:

```css
.topbar .paused { color: #8a6d00; }
.canvas.paused .node-runtime { opacity: 0.4; }
```

- [ ] **Step 2: Build**

Run: `cd studio/ui && npm run build`
Expected: `✓ built` and no TypeScript error.

- [ ] **Step 3: Check it live** (the UI has no unit tests)

Run: `make up`. Open http://localhost:8082, create a flow: a timer producer (`interval_ms` 200) → a topic → a consumer. Save and Deploy, then select the consumer.
- **Tail drawer:** records stream in. Scroll the drawer up and the view stays put while records arrive; scroll back to the bottom and it follows again.
- **Studio restart:** `docker compose stop studio`. Within a couple of seconds the numbers grey out and the top bar says `live numbers paused: connection lost, reconnecting`. `docker compose start studio`: the numbers come back with no reload.
- **Gone:** with the page open, `curl -X DELETE localhost:8082/api/flows/<id>`. Within a few seconds the canvas closes, the top bar says `the open flow was deleted`, and the list no longer shows the flow.
- **Warning:** deploy another flow, select its consumer, and `docker kill -s STOP studio-<id>-consumer-1`. Its runtime line shows `stats: … context deadline exceeded`. Then `docker kill -s CONT studio-<id>-consumer-1`.

Then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 4: Commit**

```bash
git add studio/ui/src/flow/api.ts studio/ui/src/App.tsx studio/ui/src/TailDrawer.tsx studio/ui/src/nodes/StudioNodes.tsx studio/ui/src/index.css
git commit -m "ui: live numbers pause and say why, a deleted flow closes, nodes show warnings; the tail keeps one fetch in flight and follows only at the bottom" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `verify-studio` cleans up and checks the 404; docs

**Files:**
- Modify: `Makefile`, `README.md`, `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`

**Interfaces:**
- Consumes: Tasks 1–3 behaviour; `verify-studio`'s `flows`, `id`, `xid` and its exit trap; `KAFKA_BIN`, `BOOTSTRAP`.

- [ ] **Step 1: Makefile and docs**

In `Makefile`, replace

```make
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, live ticks, chained flows, instances, transforms, stop, delete
```

with

```make
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, live ticks, chained flows, instances, transforms, stop, delete; removes its flows and topics
```

In `Makefile`, replace

```make
	flows=; trap 'for f in $$flows; do curl -sS -X DELETE $(STUDIO_URL)/api/flows/$$f >/dev/null 2>&1; done; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1' EXIT; \
```

with

```make
	flows=; trap 'for f in $$flows; do curl -sS -X DELETE $(STUDIO_URL)/api/flows/$$f >/dev/null 2>&1; done; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1; $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --delete --topic "studio-verify.*" >/dev/null 2>&1' EXIT; \
```

In `Makefile`, replace

```make
		curl -sS "$(STUDIO_URL)/api/flows/$$id/state" | grep -q '"consumer-1":{[^}]*"lag":0[,}]' && break; \
```

with

```make
		curl -sS "$(STUDIO_URL)/api/flows/$$id/state" | sed -E -e 's/\\"//g' -e 's/"(lastError|warning)":"[^"]*"//g' | grep -q '"consumer-1":{[^}]*"lag":0[,}]' && break; \
```

In `Makefile`, replace

```make
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$xid || { echo "STUDIO FAILED: delete the transform flow"; exit 1; }; \
```

with

```make
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$xid || { echo "STUDIO FAILED: delete the transform flow"; exit 1; }; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' $(STUDIO_URL)/api/flows/$$xid/events); \
	[ "$$code" = 404 ] || { echo "STUDIO FAILED: the events of a deleted flow answered $$code, want 404"; exit 1; }; \
```

In `README.md`, replace

```markdown
`curl -X POST 'localhost:8082/api/flows/<id>/nodes/producer-1/send?key=k1' --data '{"id": 1}'` sends that body as the value.
```

with

```markdown
`curl -X POST 'localhost:8082/api/flows/<id>/nodes/producer-1/send?key=k1' --data '{"id": 1}'` sends that body as the value. Templates that fail or render no JSON are the producer's error: counted, logged by a timer, and a 500 for a template `send`.
```

In `README.md`, replace

```markdown
`GET /api/flows/<id>/state` returns the same snapshot without rates. The flow list refreshes every 5 s.
```

with

```markdown
`GET /api/flows/<id>/state` returns the same snapshot without rates. The flow list refreshes every 5 s.
- When the stream reports a problem or drops (the studio restarting), the numbers grey out and the top bar says `live numbers paused: <reason>` until the next tick. A flow deleted elsewhere closes, saying so. A node whose container does not answer `/stats` shows why instead of its numbers, once the container is 5 s old. A consumer's lag counts only partitions its group has committed: none yet (a `latest` consumer before its first record), no lag.
```

In `README.md`, replace

```markdown
- The tail shows a node's last 100 records, fetched when its count moves and started over when its container restarts;
```

with

```markdown
- The tail shows a node's last 100 records: it fetches when a newer one arrives (the snapshot's `tailSeq` moves), keeps the newest in view only while scrolled to the bottom, and starts over when the container restarts;
```

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`, replace

```markdown
  consumer's tick still arrives about once a second.
```

with

```markdown
  consumer's tick still arrives about once a second.
- Built as decided in its plan: the `/stats` calls run in parallel under one 800 ms budget (`statsBudget`) while the broker calls run; a container created less than 5 s ago (`statsGrace`) that does not answer says nothing; a node with instances carries its first instance's warning, prefixed `#<i>: `; a partition counts toward lag only when kadm reports a commit for it (`Commit.At` ≥ 0; for the others kadm measures from the partition's start); the stream ends when its flow's file is gone, so the browser's reconnect gets the 404; the UI reopens a refused stream every 2 s unless the flow answers 404; the tail drawer retries a failed fetch on the next tick; `verify-studio` deletes the `studio-verify.*` topics when it exits.
```

The new `sed` drops escaped quotes, then the `lastError` and `warning` strings, before the `[^}]*` grep, so a `}` inside error text cannot end the match early. Every recipe line keeps its leading real tab.

- [ ] **Step 2: Check and run the end-to-end check**

Run: `make test && make down && make verify`, then:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
make down
```

Expected:
- `STUDIO OK (...)` and `VERIFY OK`.
- The topic list shows no `studio-verify…` topic, only `__consumer_offsets`, `events` and `orders`.
- After `make down`, `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 3: Commit**

```bash
git add Makefile README.md docs/superpowers/specs/2026-10-06-pipeline-studio-design.md
git commit -m "verify-studio removes its topics, checks a deleted flow's events answer 404 and reads lag past braces in error text; docs for Studio M6" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
