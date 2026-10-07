# Pipeline Studio M3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Live status: every deployed node shows its record count, rate, errors, lag and partitions, streamed once a second over SSE; topics show their partitions and end offset; the tail drawer fetches only when a node's records move; timer producers run.

**Architecture:** Node containers count records where they pass and report `{boot, total, errors, lastError, tailSeq}` on `GET /stats`; the timer source is a ticker in the producer node. The control plane's `Engine.Snapshot` joins the M2 container states with each running node's `/stats` and the broker's view from `kadm` (`Lag`, which also names each partition's group member, and `ListEndOffsets`). `GET /api/flows/{id}/events` runs one loop per open stream that sends the snapshot as an SSE `tick` every second with rates computed from the previous tick; `GET /api/flows/{id}/state` returns the same snapshot without rates. The UI swaps its 1 s `/state` poll for an `EventSource`, renders the numbers on the nodes, and drives the tail drawer from each node's `tailSeq` and `boot`.

**Tech Stack:** Go 1.27.1 standard library (`net/http` `ResponseController.Flush` for SSE), `github.com/twmb/franz-go` v1.22.1 (`kgo`), `github.com/twmb/franz-go/pkg/kadm` v1.19.0 (`Lag`, `ListEndOffsets`), moby client v0.6.1; UI: browser `EventSource`, existing React 19 + `@xyflow/react` 12.12.0. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` — §3.3 (API), §3.4 "Inside a node container" (producer timer, stats), §3.6 (SSE, tick shape, tail fetched on `tailSeq`), §7 M3, §8 risks 7 and 10, §9. M2 shipped on `main` at `ba287ee`.

## Global Constraints

- No new Go or npm dependency; `go 1.27.1` stays. Images, compose service and studio image tag (`kafka-playground/studio:0.1.0`, `pull_policy: build`) do not change.
- Node API on `:9000`: `POST /send`, `GET /tail?since=N`, and new `GET /stats` → `{"boot","total","errors","lastError","tailSeq"}`. `boot` is a fresh `NewID()` per node process.
- Timer source: one record every `interval_ms` (≥ 10, already validated) rendered from the node's key/value templates with `.Seq` (1-based), `.Now` (RFC 3339) and `.Rand` (0–999); `/send` keeps working in both modes.
- Snapshot node fields (JSON names exactly): `state`, `total`, `rate`, `errors`, `lastError`, `tailSeq`, `boot`, `lag`, `assigned`, `partitions`, `endOffset`, `warning`; zero values are omitted, except `lag`, which is present (even `0`) whenever the broker answered and absent when it did not. Topic nodes: `state` `ready` or `missing`, `partitions`, `endOffset` (sum over partitions), `warning` when the actual partition count differs from the flow's.
- SSE: `GET /api/flows/{id}/events`, `Content-Type: text/event-stream`, an `event: tick` with the JSON snapshot every 1 s; a failed snapshot is an `event: problem` with `{"error": …}` and the stream continues; the loop ends when the browser disconnects. Unknown flow → 404 JSON before streaming.
- `rate` is records per second since the previous tick, rounded to 0.1; no rate when the node's `boot` changed or its `total` went down.
- Everything that already holds keeps holding: every `/api` answer JSON (except the event stream), cross-origin write guard, M2's 409/422/502 rules, `make verify` ending in `STUDIO OK (...)` and `VERIFY OK`.
- After Go changes: `(cd studio && go vet ./... && gofmt -l . && go test ./...)`, `gofmt -l` prints nothing. After UI changes: `(cd studio/ui && npm run build)`. Before finishing a task that touches the Makefile: `docker compose config --quiet && make down && make verify`, then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.
- Makefile: GNU make 3.81, BSD tools, recipe lines start with a real tab, shell `$` written `$$`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Keep README.md in sync; `studio/flow.go`, `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` change together.

## Review Focus

1. A consumer whose group has never committed (fresh deploy with `earliest`) must show its lag as the records waiting, then drain to `0`; lag must never be negative or summed from failed partitions. Pinned in Task 2, Step 1 (`TestApplyKafka`: an errored partition with lag `-1` is skipped) and Step 5 (`verify-studio` waits for `"lag":0`).
2. A topic that already existed with a different partition count must show a warning naming both numbers (spec risk 7). Pinned in Task 2, Step 1 (`TestApplyKafka`, `topic-1`).
3. A node container that restarts (`docker restart`) starts its counters and tail at zero: the rate must not go negative or spike, and the tail drawer must start over instead of missing records. Pinned in Task 3, Step 1 (`TestWithRates`: changed `boot` and a falling `total` give no rate) and Task 4 (drawer resets on `boot`).
4. Closing the browser tab must end that stream's loop — no goroutine left polling Docker and Kafka. Pinned in Task 3, Step 1 (`TestStreamTicks` returns when its context ends).
5. The broker briefly unreachable must still produce ticks with container states; lag and topic fields are just absent. Pinned in Task 2, Step 1 (`TestApplyKafka` with no Kafka answers) and Task 3, Step 1 (`TestStreamTicks`: a failed snapshot is a `problem` event and the next tick still arrives).

## Decisions this plan makes (Task 5 writes them into the spec)

1. **One loop per stream, not a shared poller.** Each open `events` stream takes its own snapshot every second; there is no per-run poller to start, stop and reconcile. One tab is one loop; a shared poller is an optimisation for many viewers (spec §3.6 described a per-run poller).
2. **Kafka every tick.** `Lag` and `ListEndOffsets` run on every snapshot, not every second tick — two small requests to a local broker.
3. **Counters where records pass.** Producers count in their produce path, consumers in their fetch loop; no `WithHooks` (spec §3.4 named hooks).
4. **Assignment from the group description.** A consumer node's `assigned` partitions come from `kadm.Lag`, whose per-partition member carries the node's `ClientID` (its container name); the node does not override `OnPartitionsRevoked`, which would replace franz-go's commit-on-revoke. `assigned` sits on the node; the spec's `instances` array arrives with M4's `instances`.
5. **`/state` is the snapshot.** `GET /api/flows/{id}/state` returns what a tick carries, minus rates — for curl and `verify-studio`; the UI only uses the stream.
6. **`boot` per node process.** `/stats` and the tick carry a random id per node process; the drawer starts over when it changes. This replaces M2's parked restart check, which could never fire.
7. **Lag is the group's lag on the node's topic.** Two consumer nodes in one group show the same lag and different `assigned` partitions.

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `studio/node.go` | counters, `/stats`, `boot`, producer `next`/`produceOne`/`run` (timer), consumer counting | 1 |
| `studio/resolve.go` | `NodeSpec` gains `Source`, `IntervalMS`; the timer limit goes; `nodeURL` | 1, 2 |
| `studio/engine.go` | `NodeState` fields, `stateOf`, `Snapshot`, `nodeStatsOf`, `applyKafka`, `withRates`, `streamTicks` | 2, 3 |
| `studio/api.go` | `/state` returns the snapshot; `GET …/events`; proxy uses `nodeURL` | 2, 3 |
| `studio/*_test.go` | `node_test.go`, `resolve_test.go`, `engine_test.go` | 1–3 |
| `Makefile` | `verify-studio`: lag drains to 0; a timer flow's tick has rates | 2, 3 |
| `studio/ui/src/flow/api.ts` | `NodeRuntime`, `FlowState`, `watch()` | 4 |
| `studio/ui/src/App.tsx`, `nodes/StudioNodes.tsx`, `TailDrawer.tsx`, `index.css` | stream, live numbers on nodes, drawer on `tailSeq`/`boot` | 4 |
| `README.md`, spec | docs | 5 |

---

### Task 1: Node counters, `/stats` and the timer source

**Files:**
- Modify: `studio/node.go` (whole file below), `studio/resolve.go`, `studio/node_test.go`, `studio/resolve_test.go`

**Interfaces:**
- Consumes: `NodeSpec`, `containerName`, `Resolve`, `notYetRunnable` (`resolve.go`); `render`, `templateData` (`flow.go`); `NewID` (`store.go`); `reply`, `fail` (`api.go`).
- Produces: `NodeSpec` fields `Source string` (JSON `source,omitempty`) and `IntervalMS int` (`interval_ms,omitempty`); `type nodeStats struct{ Boot string; Total, Errors int64; LastError string; TailSeq int64 }` (JSON `boot`, `total`, `errors`, `lastError`, `tailSeq`) — Task 2 decodes it; `type counters` with `ok()`, `fail(error)`, `stats(boot string, tailSeq int64) nodeStats`; `(*tail).last() int64`; producer methods `next`, `produceOne`, `run`; node route `GET /stats`.

- [ ] **Step 1: Write the failing tests**

In `studio/node_test.go`, add `"fmt"` and `"time"` to the imports; in `TestProducerSend` give the producer literal the field `counts: &counters{},` and append at the end of the test:

```go
	if s := p.counts.stats("b", p.tail.last()); s.Total != 3 || s.Errors != 1 || s.LastError != "broker down" || s.TailSeq != 3 || s.Boot != "b" {
		t.Fatalf("want 3 produced, 1 error, tailSeq 3; got %+v", s)
	}
```

Append:

```go
func TestProducerTimer(t *testing.T) {
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.run(ctx, 5*time.Millisecond)
		close(done)
	}()
	for i := 1; i <= 3; i++ {
		select {
		case r := <-got:
			if want := fmt.Sprintf(`{"n": %d}`, i); string(r.Value) != want || r.Topic != "orders" {
				t.Fatalf("record %d: got %s %s, want %s", i, r.Topic, r.Value, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("the timer produced only %d records", i-1)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the timer did not stop when its context ended")
	}
	if p.counts.total.Load() < 3 || p.tail.last() < 3 {
		t.Fatalf("want at least 3 counted and tailed, got %d and %d", p.counts.total.Load(), p.tail.last())
	}
}
```

In `studio/resolve_test.go`: in `TestResolve`, add `Source: "manual",` to the producer's expected `NodeSpec` (the fixture's producer is manual); in `TestNotYetRunnable`, rename the `"timer producer"` case to `"timer producer runs"` and change its expected node to `""` (runnable).

Run: `cd studio && go test ./...`
Expected: FAIL — `unknown field counts`, `p.run undefined`, `p.tail.last undefined`.

- [ ] **Step 2: `resolve.go` — the timer reaches the node, and is no longer refused**

In `NodeSpec`, add after `Topic`:

```go
	Source          string `json:"source,omitempty"`      // producer: "manual" or "timer"
	IntervalMS      int    `json:"interval_ms,omitempty"` // producer: the timer's period
```

In `Resolve`'s producer case, build the spec as `NodeSpec{Flow: f.ID, Node: src.ID, Type: "producer", Topic: topicName[dst.ID], Source: d.Source, IntervalMS: d.IntervalMS, Key: d.Key, Value: d.Value}`.

In `notYetRunnable`, delete the whole `case "producer":` branch (the timer check); the `case "consumer":` branch and the edge loop stay.

- [ ] **Step 3: Replace `studio/node.go`**

```go
// `studio node`: one producer or consumer of a deployed flow, in its own
// container. Its whole configuration is env STUDIO_NODE (a NodeSpec) plus
// KAFKA_BROKERS. It serves the control plane on :9000 inside the compose
// network: POST /send (producers), GET /tail?since=N and GET /stats.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

var errInvalidJSON = errors.New("value is not valid JSON")

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

// last is the seq of the newest record, 0 before the first.
func (t *tail) last() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seq
}

// nodeStats is what GET /stats answers; the control plane adds the container state.
type nodeStats struct {
	Boot      string `json:"boot"`  // random per process: a restarted container starts its counters and tail over
	Total     int64  `json:"total"` // records produced (producers) or fetched (consumers)
	Errors    int64  `json:"errors"`
	LastError string `json:"lastError"`
	TailSeq   int64  `json:"tailSeq"` // seq of the newest tail record; the drawer fetches when it moves
}

// counters count records where they pass: produced by producers, fetched by consumers.
type counters struct {
	total, errors atomic.Int64
	mu            sync.Mutex
	lastError     string
}

func (c *counters) ok() { c.total.Add(1) }

func (c *counters) fail(err error) {
	c.errors.Add(1)
	c.mu.Lock()
	c.lastError = err.Error()
	c.mu.Unlock()
}

func (c *counters) stats(boot string, tailSeq int64) nodeStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nodeStats{Boot: boot, Total: c.total.Load(), Errors: c.errors.Load(), LastError: c.lastError, TailSeq: tailSeq}
}

// producer serves /send and runs the timer for one producer node.
type producer struct {
	spec    NodeSpec
	tail    *tail
	counts  *counters
	seq     atomic.Int64 // .Seq of the last rendered record
	produce func(context.Context, *kgo.Record) error
}

// next builds the record to produce: body as the value with key as given, or,
// with an empty body, the node's own key and value templates rendered with the
// next .Seq.
func (p *producer) next(key string, body []byte) (*kgo.Record, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		d := templateData{Seq: int(p.seq.Add(1)), Now: time.Now().UTC().Format(time.RFC3339), Rand: rand.IntN(1000)}
		v, err := render(p.spec.Value, d)
		if err != nil {
			return nil, fmt.Errorf("value template: %w", err)
		}
		if key, err = render(p.spec.Key, d); err != nil {
			return nil, fmt.Errorf("key template: %w", err)
		}
		body = []byte(v)
	}
	if !json.Valid(body) {
		return nil, errInvalidJSON
	}
	rec := &kgo.Record{Topic: p.spec.Topic, Value: body}
	if key != "" {
		rec.Key = []byte(key)
	}
	return rec, nil
}

// produceOne produces rec and counts the outcome; the tail gets every record that made it.
func (p *producer) produceOne(ctx context.Context, rec *kgo.Record) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.produce(ctx, rec); err != nil {
		p.counts.fail(err)
		return err
	}
	p.counts.ok()
	p.tail.push(rec)
	return nil
}

// send produces one record. A body is the value as is (curl, webhooks), keyed by
// ?key=; an empty body renders the node's own templates (the UI's Send button).
// It answers {partition, offset}.
func (p *producer) send(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		fail(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	rec, err := p.next(r.URL.Query().Get("key"), body)
	switch {
	case errors.Is(err, errInvalidJSON):
		fail(w, http.StatusBadRequest, err.Error())
		return
	case err != nil:
		p.counts.fail(err)
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := p.produceOne(r.Context(), rec); err != nil {
		fail(w, http.StatusBadGateway, err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{"partition": rec.Partition, "offset": rec.Offset})
}

// run is the timer source: a rendered record every period until ctx ends. A slow
// broker makes the ticker drop ticks rather than queue them.
func (p *producer) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rec, err := p.next("", nil)
		if err != nil {
			p.counts.fail(err)
			continue
		}
		if err := p.produceOne(ctx, rec); err != nil {
			log.Printf("produce: %v", err)
		}
	}
}

// consume polls the group until ctx ends, counting and tailing every record.
func consume(ctx context.Context, cl *kgo.Client, t *tail, c *counters) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(topic string, partition int32, err error) {
			c.fail(err)
			log.Printf("fetch %s[%d]: %v", topic, partition, err)
		})
		fs.EachRecord(func(r *kgo.Record) {
			c.ok()
			t.push(r)
			log.Printf("%s[%d]@%d key=%s %s", r.Topic, r.Partition, r.Offset, r.Key, r.Value)
		})
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

	t, counts, boot := &tail{}, &counters{}, NewID()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /tail", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		reply(w, http.StatusOK, t.since(since))
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, counts.stats(boot, t.last()))
	})
	if spec.Type == "producer" {
		p := &producer{spec: spec, tail: t, counts: counts, produce: func(ctx context.Context, rec *kgo.Record) error {
			return cl.ProduceSync(ctx, rec).FirstErr()
		}}
		mux.HandleFunc("POST /send", p.send)
		if spec.Source == "timer" {
			go p.run(ctx, time.Duration(spec.IntervalMS)*time.Millisecond)
		}
	} else {
		mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
			fail(w, http.StatusConflict, "only producer nodes send")
		})
		go consume(ctx, cl, t, counts)
	}
	srv := &http.Server{Addr: nodeAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("node %s (%s) on topic %s, boot %s", spec.Node, spec.Type, spec.Topic, boot)

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	cl.Close()
}
```

- [ ] **Step 4: Run the tests**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing. The timer runs against a real broker in Task 3's `verify-studio`.

- [ ] **Step 5: Commit**

```bash
git add studio/node.go studio/node_test.go studio/resolve.go studio/resolve_test.go
git commit -m "studio: node counters, /stats with a boot id, and the timer source

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The snapshot — node counters and the broker's view; `/state` returns it

**Files:**
- Modify: `studio/engine.go`, `studio/resolve.go` (`nodeURL`), `studio/api.go` (`flowState`, `nodeProxy`), `studio/engine_test.go`, `Makefile` (`verify-studio`)

**Interfaces:**
- Consumes: `nodeStats`, `nodeAddr` (Task 1, `node.go`); `Engine`, `FlowState`, `State`, `flowContainers`, `labelNode` (M2); `Resolve`, `NodeSpec`, `containerName` (`resolve.go`); `TopicData`.
- Produces:
  - `NodeState` with the fields below (Task 3 sets `Rate`; Task 4's `NodeRuntime` mirrors them).
  - `func nodeURL(flow, node, path string) string` in `resolve.go`.
  - `func (e *Engine) stateOf(ctx context.Context, f Flow) (FlowState, error)`; `State` now calls it.
  - `func (e *Engine) Snapshot(ctx context.Context, id string) (FlowState, error)` — Task 3 streams it.
  - `func nodeStatsOf(ctx context.Context, flow, node string) (nodeStats, error)`.
  - `func applyKafka(st *FlowState, flow string, specs []NodeSpec, topics map[string]TopicData, lags kadm.DescribedGroupLags, ends kadm.ListedOffsets)` (pure).
  - `GET /api/flows/{id}/state` answers the snapshot.

`kadm` shapes confirmed with `go doc` on v1.19.0: `(*Client).Lag(ctx, groups ...string) (DescribedGroupLags, error)`; `DescribedGroupLags map[string]DescribedGroupLag`; `DescribedGroupLag{Group string; Lag GroupLag; …}` with method `(*DescribedGroupLag).Error() error`; `GroupLag map[string]map[int32]GroupMemberLag`; `GroupMemberLag{Member *DescribedGroupMember; Topic string; Partition int32; Lag int64 (−1 on error); Err error}` (`Member` is nil while the group is Empty — lag is then computed for every partition); `DescribedGroupMember{ClientID string; …}`; `(*Client).ListEndOffsets(ctx, topics ...string) (ListedOffsets, error)`; `ListedOffsets map[string]map[int32]ListedOffset`, `ListedOffset{Partition int32; Offset int64; Err error}` — a missing topic appears as partition −1 with `kerr.UnknownTopicOrPartition`. Called with no groups or no topics, both list *everything*, so the snapshot only calls them with at least one name.

- [ ] **Step 1: Write the failing test**

Append to `studio/engine_test.go` (add `"errors"`, `"reflect"`, `"strings"`, `"github.com/twmb/franz-go/pkg/kadm"` and `"github.com/twmb/franz-go/pkg/kerr"` to its imports, keeping the ones it has):

```go
func TestApplyKafka(t *testing.T) {
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"producer-1": {State: "running", Total: 9},
		"consumer-1": {State: "running", Total: 4},
		"consumer-2": {State: "running"},
	}}
	specs := []NodeSpec{
		{Node: "producer-1", Type: "producer", Topic: "orders"},
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g"},
		{Node: "consumer-2", Type: "consumer", Topic: "orders", Group: "g"},
	}
	topics := map[string]TopicData{"topic-1": {Name: "orders", Partitions: 2}, "topic-2": {Name: "gone", Partitions: 1}}
	one := &kadm.DescribedGroupMember{ClientID: containerName("f", "consumer-1")}
	two := &kadm.DescribedGroupMember{ClientID: containerName("f", "consumer-2")}
	lags := kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": {
		0: {Topic: "orders", Partition: 0, Lag: 2, Member: one},
		1: {Topic: "orders", Partition: 1, Lag: 3, Member: two},
		2: {Topic: "orders", Partition: 2, Lag: -1, Member: one, Err: errors.New("no end offset")},
	}}}}
	ends := kadm.ListedOffsets{
		"orders": {0: {Partition: 0, Offset: 10}, 1: {Partition: 1, Offset: 5}, 2: {Partition: 2, Offset: 7}},
		"gone":   {-1: {Partition: -1, Offset: -1, Err: kerr.UnknownTopicOrPartition}},
	}
	applyKafka(&st, "f", specs, topics, lags, ends)

	c1, c2 := st.Nodes["consumer-1"], st.Nodes["consumer-2"]
	if c1.Total != 4 || c1.Lag == nil || *c1.Lag != 5 || !reflect.DeepEqual(c1.Assigned, map[string][]int32{"orders": {0, 2}}) {
		t.Fatalf("consumer-1: want total 4, lag 5 (errored partition skipped), assigned [0 2]; got %+v lag %v", c1, c1.Lag)
	}
	if c2.Lag == nil || *c2.Lag != 5 || !reflect.DeepEqual(c2.Assigned, map[string][]int32{"orders": {1}}) {
		t.Fatalf("consumer-2: want the group's lag 5 and assigned [1]; got %+v", c2)
	}
	if p := st.Nodes["producer-1"]; p.Total != 9 || p.Lag != nil {
		t.Fatalf("producer-1 must be left as it was, got %+v", p)
	}
	t1 := st.Nodes["topic-1"]
	if t1.State != "ready" || t1.Partitions != 3 || t1.EndOffset != 22 || !strings.Contains(t1.Warning, "3 partitions") || !strings.Contains(t1.Warning, "asks for 2") {
		t.Fatalf("topic-1: want ready, 3 partitions, end 22 and a warning naming 3 and 2; got %+v", t1)
	}
	if t2 := st.Nodes["topic-2"]; t2.State != "missing" || t2.Partitions != 0 {
		t.Fatalf("topic-2: want missing, got %+v", t2)
	}

	// The broker did not answer: container states stay, nothing is invented.
	bare := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
	applyKafka(&bare, "f", specs, topics, nil, nil)
	if len(bare.Nodes) != 1 || bare.Nodes["consumer-1"].Lag != nil {
		t.Fatalf("no Kafka answers: want the snapshot unchanged, got %+v", bare.Nodes)
	}
}
```

Run: `cd studio && go test -run TestApplyKafka ./...`
Expected: FAIL — `undefined: applyKafka`, unknown fields `Total`, `Lag`, `Assigned`, `Partitions`, `EndOffset`, `Warning`.

- [ ] **Step 2: `nodeURL` in `studio/resolve.go`**

Add after `containerName`:

```go
// nodeURL is path on a node container's own API, reached by container name on
// the compose network.
func nodeURL(flow, node, path string) string {
	return "http://" + containerName(flow, node) + nodeAddr + path
}
```

In `studio/api.go`'s `nodeProxy`, replace `"http://"+containerName(id, node)+nodeAddr+path+"?"+r.URL.RawQuery` with `nodeURL(id, node, path+"?"+r.URL.RawQuery)`.

- [ ] **Step 3: The snapshot in `studio/engine.go`**

Add `"encoding/json"`, `"net/http"`, `"slices"` and `"time"` to the imports. Replace the `NodeState` type with:

```go
// NodeState is one node in a snapshot. State is Docker's container state
// (running, exited, …) or "missing" for producers and consumers, and "ready" or
// "missing" for topics. The other fields are filled while the flow runs; zero
// values are left out of the JSON, except Lag, which is nil only when the broker
// did not answer.
type NodeState struct {
	State      string             `json:"state"`
	Total      int64              `json:"total,omitempty"`
	Rate       float64            `json:"rate,omitempty"` // records per second, set by the SSE stream
	Errors     int64              `json:"errors,omitempty"`
	LastError  string             `json:"lastError,omitempty"`
	TailSeq    int64              `json:"tailSeq,omitempty"`
	Boot       string             `json:"boot,omitempty"`
	Lag        *int64             `json:"lag,omitempty"`        // consumers: the group's lag on the node's topic
	Assigned   map[string][]int32 `json:"assigned,omitempty"`   // consumers: partitions this node holds, by topic
	Partitions int32              `json:"partitions,omitempty"` // topics
	EndOffset  int64              `json:"endOffset,omitempty"`  // topics: summed over partitions
	Warning    string             `json:"warning,omitempty"`
}
```

Split `State` so the snapshot can reuse its body without reading the file twice: rename the existing `func (e *Engine) State(ctx context.Context, id string) (FlowState, error)` to `func (e *Engine) stateOf(ctx context.Context, f Flow) (FlowState, error)`, delete its first lines that read `f, err := e.store.Get(id)` and the error check after them, change its `flowContainers(ctx, e.docker, id)` to `flowContainers(ctx, e.docker, f.ID)`, and replace its doc comment with `// stateOf is State for a flow already read from the store.` Then add:

```go
// State reports the flow's container states. A producer or consumer with no
// container in a running flow is "missing" (removed with docker rm -f, or added
// to the file after the deploy).
func (e *Engine) State(ctx context.Context, id string) (FlowState, error) {
	f, err := e.store.Get(id)
	if err != nil {
		return FlowState{}, err
	}
	return e.stateOf(ctx, f)
}

// Snapshot is State plus, while the flow runs, each running node's counters
// (/stats) and the broker's view (consumer lag and partitions, topic partitions
// and end offsets). The SSE stream adds rates.
func (e *Engine) Snapshot(ctx context.Context, id string) (FlowState, error) {
	f, err := e.store.Get(id)
	if err != nil {
		return FlowState{}, err
	}
	st, err := e.stateOf(ctx, f)
	if err != nil || st.Status != "running" {
		return st, err
	}
	for node, ns := range st.Nodes {
		if ns.State != "running" {
			continue
		}
		s, err := nodeStatsOf(ctx, id, node)
		if err != nil {
			ns.LastError = "stats: " + err.Error()
		} else {
			ns.Total, ns.Errors, ns.LastError, ns.TailSeq, ns.Boot = s.Total, s.Errors, s.LastError, s.TailSeq, s.Boot
		}
		st.Nodes[node] = ns
	}

	specs, _ := Resolve(f)
	topics := map[string]TopicData{} // topic node id → its data
	var groups, names []string
	for _, n := range f.Nodes {
		if n.Type == "topic" {
			var d TopicData
			json.Unmarshal(n.Data, &d)
			topics[n.ID] = d
			names = append(names, d.Name)
		}
	}
	for _, s := range specs {
		if s.Type == "consumer" {
			groups = append(groups, s.Group)
		}
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	kctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// Errors leave the broker's fields out of this snapshot; called with no names
	// these list every group or topic on the broker, hence the guards.
	var lags kadm.DescribedGroupLags
	var ends kadm.ListedOffsets
	if len(groups) > 0 {
		lags, _ = e.adm.Lag(kctx, groups...)
	}
	if len(names) > 0 {
		ends, _ = e.adm.ListEndOffsets(kctx, names...)
	}
	applyKafka(&st, id, specs, topics, lags, ends)
	return st, nil
}

// nodeStatsOf asks a running node container for its counters (1 s budget).
func nodeStatsOf(ctx context.Context, flow, node string) (nodeStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL(flow, node, "/stats"), nil)
	if err != nil {
		return s, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return s, err
	}
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(&s)
	return s, err
}

// applyKafka adds the broker's view to a running flow's snapshot: for each topic
// node its partition count and end offset summed over partitions (with a warning
// when the count is not the flow's), for each consumer node its group's lag on its
// topic and the partitions whose group member is this node's client. Whatever the
// broker did not answer is left out.
func applyKafka(st *FlowState, flow string, specs []NodeSpec, topics map[string]TopicData, lags kadm.DescribedGroupLags, ends kadm.ListedOffsets) {
	if ends != nil {
		for node, t := range topics {
			ns := NodeState{State: "missing"}
			for p, o := range ends[t.Name] {
				if p >= 0 && o.Err == nil {
					ns.Partitions++
					ns.EndOffset += o.Offset
				}
			}
			if ns.Partitions > 0 {
				ns.State = "ready"
				if int(ns.Partitions) != t.Partitions {
					ns.Warning = fmt.Sprintf("the topic has %d partitions; the flow asks for %d", ns.Partitions, t.Partitions)
				}
			}
			st.Nodes[node] = ns
		}
	}
	for _, s := range specs {
		gl, ok := lags[s.Group]
		if s.Type != "consumer" || !ok || gl.Error() != nil {
			continue
		}
		ns := st.Nodes[s.Node]
		var lag int64
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Err == nil && ml.Lag > 0 {
				lag += ml.Lag
			}
			if ml.Member != nil && ml.Member.ClientID == containerName(flow, s.Node) {
				if ns.Assigned == nil {
					ns.Assigned = map[string][]int32{}
				}
				ns.Assigned[s.Topic] = append(ns.Assigned[s.Topic], ml.Partition)
			}
		}
		slices.Sort(ns.Assigned[s.Topic])
		ns.Lag = &lag
		st.Nodes[s.Node] = ns
	}
}
```

`kadm` is already imported in `engine.go` (the `Engine.adm` field).

- [ ] **Step 4: `/state` returns the snapshot**

In `studio/api.go`, in `flowState`, replace `s.engine.State(r.Context(), r.PathValue("id"))` with `s.engine.Snapshot(r.Context(), r.PathValue("id"))`. (`NodeRunning` keeps using `State`: it only needs container states.)

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing.

- [ ] **Step 5: `verify-studio` waits for the lag to drain**

In `Makefile`, in the `verify-studio` recipe, insert right after the line `	done; \` (the end of the loop that waits for the consumer tail):

```make
	for i in $$(seq 20); do \
		curl -sS "$(STUDIO_URL)/api/flows/$$id/state" | grep -q '"consumer-1":{[^}]*"lag":0[,}]' && break; \
		[ "$$i" = 20 ] && { echo "STUDIO FAILED: consumer lag never reached 0: $$(curl -sS $(STUDIO_URL)/api/flows/$$id/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio state: $$(curl -sS $(STUDIO_URL)/api/flows/$$id/state)"; \
```

(The consumer autocommits every 5 s, so the lag of the two records sent above reaches 0 within ~5 s. `lag` sits before the nested `assigned` object in the JSON, so `[^}]*` cannot run past the node.)

Run:

```bash
docker compose config --quiet && make down && make verify; make down; docker ps -aq -f label=studio.flow
```

Expected: a `studio state: {"status":"running","nodes":{"consumer-1":{"state":"running","total":2,…,"lag":0,"assigned":{"studio-verify":[0]}},"producer-1":{…},"topic-1":{"state":"ready","partitions":1,"endOffset":…}}}` line, then `STUDIO OK (<id>)` and `VERIFY OK`; the last command prints nothing; `git status --short flows` prints nothing.

- [ ] **Step 6: Commit**

```bash
git add studio/engine.go studio/engine_test.go studio/resolve.go studio/api.go Makefile
git commit -m "studio: snapshot with node counters, lag, partitions and end offsets; /state returns it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Live status over SSE

**Files:**
- Modify: `studio/engine.go` (`withRates`, `streamTicks`), `studio/api.go` (route, `events`), `studio/engine_test.go`, `studio/api_test.go`, `Makefile` (`verify-studio`)

**Interfaces:**
- Consumes: `Engine.Snapshot`, `FlowState`, `NodeState` (Task 2); `storeErr` (M1).
- Produces: `func withRates(cur *FlowState, prev FlowState, dt float64)`; `func streamTicks(ctx context.Context, w io.Writer, flush func() error, snap func(context.Context) (FlowState, error), every time.Duration)`; route `GET /api/flows/{id}/events` (Task 4's `EventSource` reads it).

- [ ] **Step 1: Write the failing tests**

Append to `studio/engine_test.go` (add `"bytes"`, `"context"` and `"time"` to its imports if missing):

```go
func TestWithRates(t *testing.T) {
	prev := FlowState{Nodes: map[string]NodeState{
		"steady":    {Boot: "a", Total: 10},
		"restarted": {Boot: "a", Total: 10},
		"fell":      {Boot: "a", Total: 50},
	}}
	cur := FlowState{Nodes: map[string]NodeState{
		"steady":    {Boot: "a", Total: 31},
		"restarted": {Boot: "b", Total: 3},
		"fell":      {Boot: "a", Total: 40},
		"new":       {Boot: "a", Total: 5},
		"topic-1":   {State: "ready", EndOffset: 99},
	}}
	withRates(&cur, prev, 2)
	for node, want := range map[string]float64{"steady": 10.5, "restarted": 0, "fell": 0, "new": 0, "topic-1": 0} {
		if got := cur.Nodes[node].Rate; got != want {
			t.Errorf("%s: rate %v, want %v", node, got, want)
		}
	}
}

func TestStreamTicks(t *testing.T) {
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	snap := func(context.Context) (FlowState, error) {
		calls++
		switch calls {
		case 1:
			return FlowState{Status: "running", Nodes: map[string]NodeState{"p": {State: "running", Boot: "a"}}}, nil
		case 2:
			return FlowState{}, errors.New("docker: down")
		case 3:
			return FlowState{Status: "running", Nodes: map[string]NodeState{"p": {State: "running", Boot: "a", Total: 5}}}, nil
		}
		cancel() // the browser went away
		return FlowState{}, context.Canceled
	}
	done := make(chan struct{})
	go func() {
		streamTicks(ctx, &out, func() error { return nil }, snap, time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the stream did not end with its context")
	}
	s := out.String()
	if strings.Count(s, "event: tick\ndata: {") != 2 ||
		!strings.Contains(s, "event: problem\ndata: {\"error\":\"docker: down\"}\n\n") ||
		strings.Count(s, `"rate":`) != 1 {
		t.Fatalf("want two ticks around a problem event, the second tick with a rate; got:\n%s", s)
	}
}
```

In `studio/api_test.go`, add `{"POST", "/api/flows/deadbeef/events", 405},` to the case table of `TestUnroutedAPIAnswersJSON`.

Run: `cd studio && go test ./...`
Expected: FAIL — `undefined: withRates`, `undefined: streamTicks`.

- [ ] **Step 2: Rates and the stream in `studio/engine.go`**

Add `"io"` and `"math"` to the imports and append:

```go
// withRates sets each node's records per second against the previous snapshot,
// taken dt seconds earlier. A node whose boot changed (its container restarted)
// or whose total fell gets no rate this time rather than a wrong one.
func withRates(cur *FlowState, prev FlowState, dt float64) {
	if dt <= 0 {
		return
	}
	for id, ns := range cur.Nodes {
		p, ok := prev.Nodes[id]
		if !ok || p.Boot != ns.Boot || ns.Total < p.Total {
			continue
		}
		ns.Rate = math.Round(float64(ns.Total-p.Total)/dt*10) / 10
		cur.Nodes[id] = ns
	}
}

// streamTicks is the body of GET /api/flows/{id}/events: every period it writes
// the flow's snapshot as an SSE `tick` event with rates against the previous
// tick; a failed snapshot is a `problem` event and the stream goes on. It returns
// when ctx ends (the browser went away) or a flush fails. Each open stream polls
// on its own: one tab, one loop.
func streamTicks(ctx context.Context, w io.Writer, flush func() error, snap func(context.Context) (FlowState, error), every time.Duration) {
	var prev FlowState
	last := time.Now()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		st, err := snap(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			b, _ := json.Marshal(map[string]string{"error": err.Error()})
			fmt.Fprintf(w, "event: problem\ndata: %s\n\n", b)
		} else {
			now := time.Now()
			withRates(&st, prev, now.Sub(last).Seconds())
			prev, last = st, now
			b, _ := json.Marshal(st)
			fmt.Fprintf(w, "event: tick\ndata: %s\n\n", b)
		}
		if flush() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
```

- [ ] **Step 3: The route in `studio/api.go`**

After the `state` route add `mux.HandleFunc("GET /api/flows/{id}/events", s.events)`, append `"/api/flows/{id}/events"` to the method-less fallback list, and add after `flowState`:

```go
// events streams the flow's snapshot once a second as server-sent events.
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.Get(id); storeErr(w, err) {
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	streamTicks(r.Context(), w, rc.Flush, func(ctx context.Context) (FlowState, error) {
		return s.engine.Snapshot(ctx, id)
	}, time.Second)
}
```

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok`, `gofmt -l` prints nothing.

- [ ] **Step 4: `verify-studio` watches a timer flow's ticks**

In `Makefile`, change the `verify-studio` help comment to `## Check the studio end to end: health, cross-site refusal, save rules, deploy (rollback, 409, restart), send, tail, lag, live ticks, stop, delete`, and insert right before the line `	echo "STUDIO OK ($$id)"`:

```make
	tflow='{"name":"verify-timer","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"timer","interval_ms":100,"key":"","value":"{\"n\": {{.Seq}}}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify-timer","partitions":1,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify-timer","auto_offset_reset":"latest","sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"}]}'; \
	tid=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$tflow" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	trap "curl -sS -X DELETE $(STUDIO_URL)/api/flows/$$id >/dev/null 2>&1; curl -sS -X DELETE $(STUDIO_URL)/api/flows/$$tid >/dev/null 2>&1; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1" EXIT; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$tid/deploy >/dev/null || { echo "STUDIO FAILED: timer deploy"; exit 1; }; \
	tick=$$(curl -sN --max-time 20 $(STUDIO_URL)/api/flows/$$tid/events | grep -m1 '"consumer-1":{[^}]*"rate":.*"producer-1":{[^}]*"rate":'); \
	[ -n "$$tick" ] || { echo "STUDIO FAILED: no tick with consumer and producer rates for the timer flow"; exit 1; }; \
	echo "studio tick: $$tick"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$tid || { echo "STUDIO FAILED: delete the timer flow"; exit 1; }; \
```

(`rate` is omitted while it is 0, so its presence means > 0. The snapshot's node map is keyed in sorted order — `consumer-1` before `producer-1` — and `rate` precedes the consumer's nested `assigned` object. `grep -m1` ends `curl` at the first matching tick.)

Run:

```bash
docker compose config --quiet && make down && make verify; make down; docker ps -aq -f label=studio.flow
```

Expected: `studio tick: data: {"status":"running","nodes":{"consumer-1":{…"rate":…},"producer-1":{…"rate":…},"topic-1":{…}}}`, then `STUDIO OK (<id>)` and `VERIFY OK`; the last command prints nothing; `git status --short flows` prints nothing.

- [ ] **Step 5: Commit**

```bash
git add studio/engine.go studio/engine_test.go studio/api.go studio/api_test.go Makefile
git commit -m "studio: live status over server-sent events with per-node rates

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: UI — the live stream, numbers on the nodes, a drawer that follows `tailSeq`

**Files:**
- Modify: `studio/ui/src/flow/api.ts`, `studio/ui/src/App.tsx`, `studio/ui/src/nodes/StudioNodes.tsx`, `studio/ui/src/index.css`
- Replace: `studio/ui/src/TailDrawer.tsx`

**Interfaces:**
- Consumes: `GET /api/flows/{id}/events` (`event: tick`, data = the snapshot; Task 3), the snapshot's node fields (Task 2), `GET …/nodes/{node}/tail?since=` and `POST …/send` (M2).
- Produces: in `api.ts`, `type NodeRuntime` (mirrors Go's `NodeState`), `type FlowState = { status: 'running' | 'stopped'; nodes: Record<string, NodeRuntime> }`, `function watch(id: string, onTick: (s: FlowState) => void): () => void`; `api.state` is removed (the UI no longer polls). `RuntimeContext` in `StudioNodes.tsx` becomes `Record<string, NodeRuntime>`. `TailDrawer` props become `{ flowId: string; node: StudioNode; tailSeq?: number; boot?: string }`.

`EventSource` reconnects by itself after a dropped connection, so `watch` keeps the last tick on screen through a reconnect instead of blanking the flow.

- [ ] **Step 1: `studio/ui/src/flow/api.ts`**

Replace the `FlowState` type and its comment with:

```ts
// One node of the live snapshot (Go: NodeState in engine.go). Absent fields are zero,
// except lag, which is absent when the broker did not answer.
export type NodeRuntime = {
  state: string
  total?: number
  rate?: number
  errors?: number
  lastError?: string
  tailSeq?: number
  boot?: string
  lag?: number
  assigned?: Record<string, number[]>
  partitions?: number
  endOffset?: number
  warning?: string
}

// The snapshot GET /api/flows/{id}/state answers and every SSE tick carries.
export type FlowState = { status: 'running' | 'stopped'; nodes: Record<string, NodeRuntime> }

// watch opens the flow's event stream: onTick gets a snapshot once a second.
// EventSource reconnects by itself; the returned function closes the stream.
export function watch(id: string, onTick: (s: FlowState) => void): () => void {
  const es = new EventSource(`/api/flows/${id}/events`)
  es.addEventListener('tick', (e) => onTick(JSON.parse((e as MessageEvent<string>).data)))
  return () => es.close()
}
```

and delete the method `state: (id: string) => call<FlowState>(`/api/flows/${id}/state`),` from the `api` object (only that line: `NodeRuntime` has a `state` field of its own).

- [ ] **Step 2: Numbers on the nodes — `studio/ui/src/nodes/StudioNodes.tsx`**

Add `import type { NodeRuntime } from '../flow/api'`, change the context to:

```tsx
// The live snapshot per node id while the flow runs; empty while it is stopped.
export const RuntimeContext = createContext<Record<string, NodeRuntime>>({})

// One line of live numbers under a deployed node's summary.
function runtimeLine(type: NodeType, rt: NodeRuntime): string {
  if (type === 'topic') {
    return [`${rt.partitions ?? 0} partitions`, `end ${rt.endOffset ?? 0}`, rt.warning].filter(Boolean).join(' · ')
  }
  const parts = [`${rt.total ?? 0} msgs`, `${(rt.rate ?? 0).toFixed(1)}/s`]
  if (rt.errors) parts.push(`${rt.errors} errors`)
  if (rt.lag !== undefined) parts.push(`lag ${rt.lag}`)
  const held = Object.values(rt.assigned ?? {}).flat()
  if (held.length > 0) parts.push(`p${held.join(',')}`)
  return parts.join(' · ')
}
```

and replace `Shell` with:

```tsx
// The frame every node shares: type as title (plus its state while deployed), a
// one-line summary, a line of live numbers while deployed (the last error as its
// tooltip), and the input/output handles the allowed-edge table gives its type.
function Shell({ id, type, selected, children }: { id: string; type: NodeType; selected?: boolean; children: ReactNode }) {
  const rt = useContext(RuntimeContext)[id]
  return (
    <div className={`node ${type}${selected ? ' selected' : ''}`} title={rt?.lastError || undefined}>
      <div className="node-title">
        {type} {rt && <span className={`node-state ${rt.state}`}>{rt.state}</span>}
      </div>
      <div className="node-summary">{children}</div>
      {rt && <div className="node-runtime">{runtimeLine(type, rt)}</div>}
      {hasInput(type) && <Handle type="target" position={Position.Left} />}
      {hasOutput(type) && <Handle type="source" position={Position.Right} />}
    </div>
  )
}
```

- [ ] **Step 3: `studio/ui/src/App.tsx` — the stream replaces the poll**

1. Change the `react` import to `import { useCallback, useEffect, useState } from 'react'` (`useMemo` is no longer used) and the API import to `import { api, describe, watch, type FlowState, type FlowSummary, type NodeRuntime } from './flow/api'`.
2. Above `function Studio()` add `const NO_NODES: Record<string, NodeRuntime> = {}`.
3. Replace the block from the comment `// The open flow's container states, once a second (M3 replaces this poll with SSE).` down to and including the `nodeStates` `useMemo` with:

```tsx
  // The open flow's live snapshot: one `tick` a second over SSE (spec 3.6).
  const flowId = current?.id
  useEffect(() => {
    setFlowState(null)
    if (!flowId) return
    return watch(flowId, setFlowState)
  }, [flowId])
  const running = flowState?.status === 'running'
```

4. In `deploy` and in `stop`, delete the line `setFlowState(await api.state(current.id))` — the next tick arrives within a second.
5. Change `<RuntimeContext.Provider value={nodeStates}>` to `<RuntimeContext.Provider value={flowState?.nodes ?? NO_NODES}>`.
6. Pass the node's `tailSeq` and `boot` to the drawer: `<TailDrawer key={`${current.id}/${node.id}`} flowId={current.id} node={node} tailSeq={flowState?.nodes[node.id]?.tailSeq} boot={flowState?.nodes[node.id]?.boot} />`.

- [ ] **Step 4: Replace `studio/ui/src/TailDrawer.tsx`**

```tsx
import { useEffect, useRef, useState } from 'react'
import { api, describe, type TailEntry } from './flow/api'
import type { StudioNode } from './nodes/types'

type Props = { flowId: string; node: StudioNode; tailSeq?: number; boot?: string }

// The selected node's last records. It fetches only when the node's tailSeq (from
// the SSE tick) moves past what it has, and starts over when boot changes: the
// container restarted and numbers its records from 1 again. A producer's drawer
// also has Send, which renders the node's own key and value templates.
export default function TailDrawer({ flowId, node, tailSeq = 0, boot = '' }: Props) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [sent, setSent] = useState('')
  const since = useRef(0)
  const box = useRef<HTMLElement>(null)

  useEffect(() => {
    since.current = 0
    setEntries([])
  }, [boot])

  useEffect(() => {
    if (tailSeq <= since.current) return
    let live = true
    api.tail(flowId, node.id, since.current).then(
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
  }, [flowId, node.id, boot, tailSeq])

  // Keep the newest record in view.
  useEffect(() => {
    box.current?.scrollTo({ top: box.current.scrollHeight })
  }, [entries])

  const send = () =>
    api.send(flowId, node.id).then(
      (r) => setSent(`sent to partition ${r.partition} at offset ${r.offset}`),
      (e) => setSent(describe(e)),
    )

  return (
    <section className="drawer" ref={box}>
      <header>
        <strong>{node.id}</strong> tail
        {node.type === 'producer' && <button onClick={send}>Send</button>}
        {sent && <span className="hint">{sent}</span>}
        {error && <span className="error">{error}</span>}
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

- [ ] **Step 5: CSS**

Append to `studio/ui/src/index.css`:

```css
.node-runtime { margin-top: 2px; font-size: 11px; color: #555; }
.node-state.ready { background: #c8e6c9; }
.drawer .error { color: #b00020; }
```

- [ ] **Step 6: Build, then check against the running stack**

```bash
cd studio/ui && npm run build
cd ../.. && make up >/dev/null && curl -s localhost:8082/ | grep -o '<title>[^<]*</title>'
```

Then spec risk 10 — the Vite dev proxy must not buffer the event stream. With the stack still up:

```bash
id=$(curl -sS -X POST localhost:8082/api/flows -H 'Content-Type: application/json' --data '{"name":"sse-proxy-check","nodes":[],"edges":[]}' | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/')
(cd studio/ui && npm run dev >/dev/null 2>&1 & echo $! > /tmp/studio-vite.pid); sleep 3
curl -sN --max-time 4 "http://localhost:5173/api/flows/$id/events" | grep -c '^event: tick'
kill "$(cat /tmp/studio-vite.pid)"; curl -sS -X DELETE "localhost:8082/api/flows/$id"; make down
```

Expected: the build passes with no TypeScript error or warning; the title is `Pipeline Studio`; the proxied stream counts at least 3 ticks in 4 seconds (a buffering proxy would deliver 0 before the timeout). If it counts 0, report it: the documented fallback is the compose'd UI on :8082. No `npm run dev` process may be left running (`lsof -nP -iTCP:5173 -sTCP:LISTEN` prints nothing). Do not leave `npm run dev` running. In your report, map each behaviour to the lines implementing it: the stream opens per open flow and closes on switch; badges and the numbers line show while running and vanish when stopped; the drawer fetches only when `tailSeq` moves, starts over on a new `boot`, keeps the newest record in view, and shows the send result and a tail error side by side. The click-through is part of the human's M3 demo.

- [ ] **Step 7: Commit**

```bash
git add studio/ui/src
git commit -m "studio ui: live numbers over SSE; the tail drawer follows tailSeq and boot

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: README and the spec

**Files:**
- Modify: `README.md`, `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`

Before writing, check each statement against the code (`studio/node.go`, `studio/engine.go`, `studio/api.go`, `studio/resolve.go`) and report any mismatch instead of writing something untrue.

- [ ] **Step 1: README — Pipeline Studio section**

1. Replace the bullet that starts `This milestone deploys manual producers and log consumers.` with:

```markdown
- Producers send by hand or on a timer (`interval_ms`, at least 10, rendering the key and value templates with `{{.Seq}}`, `{{.Now}}` and `{{.Rand}}`). A deploy still refuses the http sink, consumer forwarding and more than one instance (M4), and transforms (M5), naming the node.
- While a flow runs, every node shows live numbers, streamed once a second from `GET /api/flows/<id>/events` (server-sent events): producers and consumers their record count, rate and errors (the last error as a tooltip); consumers their group's lag and the partitions they hold; topics their partition count and end offset, with a warning when an existing topic has a different partition count from the flow's. `GET /api/flows/<id>/state` returns the same snapshot without rates.
```

2. In the bullet that starts `Select a deployed producer or consumer to open its tail:`, replace `its last 100 records, refreshed every second.` with `its last 100 records, fetched whenever the node's count moves and started over when its container restarts.`

- [ ] **Step 2: The spec**

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`:

1. §3.3 table, the `GET /api/flows/{id}/state` row's middle cell becomes: ``the flow's snapshot, as an SSE tick carries it but without rates: container states, each running node's counters, consumer lag and partitions, topic partitions and end offsets``.
2. §3.4 "Inside a node container": replace the whole Stats bullet (it starts `- **Stats`) with:

```markdown
- **Stats:** counters count records where they pass — produced by a producer, fetched by a consumer — with fetch and produce errors and the last error; `/stats` returns `{boot, total, errors, lastError, tailSeq}`, where `boot` is random per process so a restarted container starts over visibly. `/tail?since=` returns records from a 100-entry ring buffer (values truncated to 4 KiB). The control plane adds the container state from Docker and lag and partitions from the broker. The HTTP server listens on `:9000` inside the compose network only.
```

3. §3.6: replace its first paragraph (from `One \`EventSource\` per open flow` to `when \`r.Context()\` is done.`) with:

```markdown
One `EventSource` per open flow on `GET /api/flows/{id}/events`. Each open stream runs its own loop: every second it takes a snapshot — `ContainerList` by label (container states), `GET /stats` on every running node container, `adm.Lag` for the flow's groups and `adm.ListEndOffsets` for its topics — computes `rate = Δtotal / Δt` against its previous snapshot (none when a node's `boot` changed), and sends it as a `tick`. The loop ends when `r.Context()` is done. One tab is one loop; a poller shared between streams is an optimisation for many viewers. A consumer node's `assigned` partitions come from the group description in `adm.Lag`, whose members carry the node's container name as their client id; with M4's `instances` they move into an `instances` array.
```

   and replace the `event: tick` example block with:

```
event: tick
data: {"status":"running","nodes":{
  "consumer-1":{"state":"running","total":118,"rate":1,"tailSeq":118,"boot":"3f9a0c1d","lag":2,"assigned":{"orders":[0,1,2]}},
  "producer-1":{"state":"running","total":120,"rate":1,"tailSeq":120,"boot":"a1b2c3d4"},
  "topic-1":{"state":"ready","partitions":3,"endOffset":120}}}
```

4. §7 M3: add a bullet after the first one: `- Built as decided in its plan: one snapshot loop per open stream, Kafka asked every tick, counters counted where records pass, assignment taken from the group description, \`/state\` returning the snapshot without rates.`

- [ ] **Step 3: Check and commit**

```bash
docker compose config --quiet && (cd studio && go vet ./... && gofmt -l . && go test ./...)
git add README.md docs/superpowers/specs/2026-10-06-pipeline-studio-design.md
git commit -m "Docs for Studio M3: live status, timer source; spec amendments

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

M3 demo for the human (spec §7): `make up`, open http://localhost:8082. A timer producer at `interval_ms: 100` → topic `orders` (3 partitions) → consumer: Deploy; the consumer shows ≈ 10/s. Add a second consumer node in the same group, Save, Stop, Deploy: the two consumers split the partitions (`p0,1` and `p2`). Deploy a second flow with a new group and `earliest` on the same topic: its lag starts high and drains to 0. `docker rm -f` one of two group members: within seconds the other holds every partition and the removed one shows `missing`.
