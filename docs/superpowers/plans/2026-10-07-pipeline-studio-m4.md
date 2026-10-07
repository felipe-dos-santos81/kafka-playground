# Pipeline Studio M4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chaining and fan-out: consumers forward to a next topic and post to an http sink, a producer's webhook is shown, a consumer runs as N instances in one group, the flow list stays current, and two flows run at once — one feeding the other.

**Architecture:** The node container's consumer becomes a `consumer` type whose `handle` takes each record through the tail, the http sink (`postJSON`) and the forward (`ProduceSync` on its own client). `Resolve` fills the consumer's `NodeSpec` with `Forward` and `SinkURL` and expands `instances: N` into N specs numbered 1..N; containers of instances carry a `studio.instance` label and a `-<i>` name suffix. The control plane folds container states into one `NodeState` per node with an `instances` list, asks each instance's `/stats`, matches each instance's Kafka client id in the group description, and sums rates; the tail proxy picks an instance with `?instance=`. The UI gets sink, instances and webhook forms, an instance picker in the tail drawer, `2/3 running` badges, and a flow list re-read every 5 s.

**Tech Stack:** Go 1.27.1 standard library (`net/http` client for the sink), `github.com/twmb/franz-go` v1.22.1 (`kgo`), `kadm` v1.19.0, moby client v0.6.1; UI: React 19 + `@xyflow/react` 12.12.0. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` — §3.3 (tail `?instance=`), §3.4 "Inside a node container" (consumer order, http sink, forward, CA bundle; containers `-<i>`), §3.6 (`instances` in the snapshot), §4.2 (`sink`, `instances`), §7 M4. M3 shipped on `main`; the spec's M4–M6 detail is commit `2fae441`.

## Global Constraints

- No new Go or npm dependency; `go 1.27.1` stays. Images and the studio tag (`kafka-playground/studio:0.1.0`, `pull_policy: build`) do not change; the studio image's last stage gains the build stage's `/etc/ssl/certs/ca-certificates.crt` so `https` sinks work.
- A consumer takes each record through, in order: the tail (and a stdout log line), the http sink when set, the forward when set. A failed sink or forward counts as an error (`lastError` starts `sink: ` or `forward: `), is logged, and is not retried; the offset still commits.
- http sink: `POST` the record's value as is with `Content-Type: application/json`, 5 s budget; any status outside 2xx is an error.
- Forward: same key and value, to the topic the consumer's outgoing edge names, through the consumer's own `kgo` client (`ProduceSync`, 10 s budget).
- Instances: a consumer with `instances: N > 1` runs N containers named `studio-<flow>-<node>-<i>` (`i` = 1..N), labelled `studio.flow`, `studio.node` and `studio.instance=<i>`, each with its container name as Kafka client id. `instances` ≤ 1 keeps the M2 name and no `studio.instance` label. A deploy whose container names would clash answers 422 naming the node.
- Snapshot: a node with instances carries `instances: [{instance, state, total, rate, errors, lastError, tailSeq, boot, assigned}]`; its own `total`, `rate` and `errors` are the sums, `lag` is the group's, `state` is `running` only when every instance runs (otherwise the first other state in instance order), `lastError` the first instance's, prefixed `#<i>: `. Zero values stay omitted, `lag` stays present whenever the broker answered.
- `GET /api/flows/{id}/nodes/{node}/tail?since=N&instance=I`: no `instance` on a node with instances means its first; an instance that does not exist or does not run answers 409.
- A deploy still refuses transforms: 422 `transforms run from M5` on the transform node.
- Everything that already holds keeps holding: every `/api` answer JSON (except the event stream), the cross-origin write guard (node containers send no browser headers, so their POSTs pass), M2's 409/422/502 rules, `make verify` ending in `STUDIO OK (...)` and `VERIFY OK`.
- After Go changes: `(cd studio && go vet ./... && gofmt -l . && go test ./...)`, `gofmt -l` prints nothing. After UI changes: `(cd studio/ui && npm run build)`. Before finishing a task that touches the Makefile or the Dockerfile: `docker compose config --quiet && make down && make verify`, then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.
- Makefile: GNU make 3.81, BSD tools, recipe lines start with a real tab, shell `$` written `$$`.
- `NodeState` in `studio/engine.go` and `NodeRuntime` in `studio/ui/src/flow/api.ts` mirror each other; `verify-studio` greps that JSON and relies on its field order (`lag` before `assigned`, `instances` last, node ids sorted).
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Keep README.md in sync.

## Review Focus

1. A node id equal to another node's instance container name (`consumer-1` with two instances next to a node `consumer-1-2`) must be refused with a 422 naming the node before anything starts, not fail halfway with a 502 and a rollback. Pinned in Task 2, Step 1 (`TestClashes`, `TestDeployRefusesNameClash`).
2. An http sink that answers 500 or is unreachable must count an error with a `sink:` last error while the consumer carries on and still forwards. Pinned in Task 1, Step 1 (`TestConsumerHandle`).
3. One instance of three exited or removed with `docker rm -f`: the node must show it (`exited`/`missing` in its entry, `2/3 running` in the UI), keep the others' numbers, and answer 409 for that instance's tail. Pinned in Task 2, Step 1 (`TestNodeStates`, `TestPickInstance`) and Task 3 (badge).
4. A flow file edited after its deploy (instances 1 → 3, or 3 → 1) must show what runs, not phantom `missing` containers of the other kind. Pinned in Task 2, Step 1 (`TestNodeStates`).
5. A studio consumer put in a group the compose kcat consumers use: the broker refuses the join (`INCONSISTENT_GROUP_PROTOCOL`, checked live while writing this plan); it must show as the node's errors and last error, never crash, and the README must say why. The example flow must not do it. Pinned in Task 4 (README, `flows/0a1b2c3d.json`).

## Decisions this plan makes (Task 4 writes them into the spec)

1. **Instance 0 is the single container.** A node with one container keeps its M2 name and no `studio.instance` label, so flows deployed before M4 are still recognised after an upgrade; instances are numbered 1..N.
2. **Name clashes are a 422.** `clashes` checks every container name before Docker is touched.
3. **One type for nodes and instances.** An `instances` entry is a `NodeState` with `instance` set; no second struct to mirror in TypeScript.
4. **`?instance=` defaults to the first instance**; an unknown or stopped instance is 409, like any node that does not run.
5. **Sink and forward are independent.** A failed sink does not stop the forward; every record still goes to stdout and the tail.
6. **What the deploy ran decides.** When the file was edited after a deploy, the containers' labels decide between one container and instances; empty slots of the other kind are dropped.
7. **The example flow gets its own group**, `orders-studio`: franz-go's cooperative-sticky and kcat's range/roundrobin assignors share no protocol.
8. **The snapshot stays sequential** (the order the user chose: M6 parallelises `/stats`). With several instances a tick can take longer than 1 s; rates stay right because Δt is measured.

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `studio/resolve.go` | `NodeSpec` gains `Forward`, `SinkURL`, `Instance`; `Resolve` fills them; `containerName`/`nodeURL` take an instance; `instancesOf`; `clashes`; `notYetRunnable` keeps only transforms | 1, 2 |
| `studio/node.go` | `consumer` (`handle`, `consume`), `postJSON`; client id per instance | 1, 2 |
| `studio/docker.go` | `studio.instance` label, instance container names | 2 |
| `studio/engine.go` | `NodeState.Instance`/`Instances`, clash check in `Deploy`, `nodeStates`, `nodeOf`, per-instance stats, `applyKafka`, `NodeRunning`/`pickInstance`, `withRates`/`rate`/`instanceOf` | 2 |
| `studio/api.go` | tail/send proxy with `?instance=` | 2 |
| `studio/Dockerfile` | CA bundle in the last stage | 1 |
| `studio/*_test.go` | `node_test.go`, `resolve_test.go`, `engine_test.go` | 1, 2 |
| `Makefile` | `verify-studio`: two chained flows; three instances | 1, 2 |
| `studio/ui/src/flow/api.ts`, `App.tsx`, `Inspector.tsx`, `TailDrawer.tsx`, `nodes/StudioNodes.tsx`, `index.css` | forms, webhook line, instance picker, badges, flow list refresh | 3 |
| `README.md`, `flows/0a1b2c3d.json`, spec | docs | 4 |

---

### Task 1: Forwarding and the http sink

**Files:**
- Modify: `studio/resolve.go`, `studio/node.go`, `studio/Dockerfile`, `Makefile`
- Test: `studio/node_test.go`, `studio/resolve_test.go`

**Interfaces:**
- Consumes: `NodeSpec`, `Resolve`, `notYetRunnable` (`resolve.go`); `tail`, `counters`, `runNode` (`node.go`); test helpers `clone`, `good`, `node`, `edge` (`flow_test.go`).
- Produces: `NodeSpec` fields `Forward string` (JSON `forward,omitempty`) and `SinkURL string` (`sink_url,omitempty`); `type consumer struct{ spec NodeSpec; tail *tail; counts *counters; post func(ctx context.Context, url string, body []byte) error; produce func(context.Context, *kgo.Record) error }` with `handle(ctx, *kgo.Record)` and `consume(ctx, *kgo.Client)`; `postJSON(ctx context.Context, url string, body []byte) error`. Task 2 extends `NodeSpec` and keeps these.

- [ ] **Step 1: Write the failing tests**

In `studio/node_test.go`, add `"io"` to the imports (after `"fmt"`) and append:

```go
func TestConsumerHandle(t *testing.T) {
	var posted []string
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		posted = append(posted, r.Header.Get("Content-Type")+" "+string(b))
		if strings.Contains(string(b), "bad") {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer sink.Close()
	var forwarded []*kgo.Record
	c := &consumer{
		spec:   NodeSpec{Topic: "orders", Forward: "archive", SinkURL: sink.URL},
		tail:   &tail{},
		counts: &counters{},
		post:   postJSON,
		produce: func(_ context.Context, r *kgo.Record) error {
			if string(r.Key) == "down" {
				return errors.New("broker down")
			}
			forwarded = append(forwarded, r)
			return nil
		},
	}
	for _, r := range []struct{ key, value string }{{"k1", `{"id":1}`}, {"k2", `{"bad":true}`}, {"down", `{"id":3}`}} {
		c.handle(context.Background(), &kgo.Record{Topic: "orders", Key: []byte(r.key), Value: []byte(r.value)})
	}
	// Every record reaches the sink; a failed sink does not stop the forward.
	if len(posted) != 3 || posted[0] != `application/json {"id":1}` {
		t.Fatalf("sink got %q", posted)
	}
	if len(forwarded) != 2 || forwarded[0].Topic != "archive" || string(forwarded[0].Key) != "k1" || string(forwarded[1].Value) != `{"bad":true}` {
		t.Fatalf("forwarded %+v", forwarded)
	}
	s := c.counts.stats("b", c.tail.last())
	if s.Total != 3 || s.Errors != 2 || !strings.HasPrefix(s.LastError, "forward: ") || s.TailSeq != 3 {
		t.Fatalf("want 3 records, 2 errors (sink 500, forward), the last a forward error; got %+v", s)
	}

	// Without a sink or a forward, a record is only counted and tailed.
	bare := &consumer{spec: NodeSpec{Topic: "orders"}, tail: &tail{}, counts: &counters{}}
	bare.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{}`)})
	if s := bare.counts.stats("b", bare.tail.last()); s.Total != 1 || s.Errors != 0 {
		t.Fatalf("bare consumer: %+v", s)
	}

	// An unreachable sink is an error too.
	sink.Close()
	if err := postJSON(context.Background(), sink.URL, []byte(`{}`)); err == nil {
		t.Fatal("want an error from a closed sink")
	}
}
```

In `studio/resolve_test.go`, insert before `func TestNotYetRunnable`:

```go
func TestResolveConsumerSinkAndForward(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"http://studio:8082/x"}}`)
	f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
	f.Edges = append(f.Edges, edge("consumer-1", "topic-2"))
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Forward: "archive", SinkURL: "http://studio:8082/x"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("consumer spec:\n got %+v\nwant %+v", specs, want)
	}
	if len(topics) != 2 {
		t.Fatalf("want both topics created, got %+v", topics)
	}
}
```

In `TestNotYetRunnable`'s cases, replace the `"http sink"` case with the first case below and the `"forward to a topic"` case with the other two; the `"two instances"` case between them stays as it is until Task 2:

```go
		{"http sink runs", func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"http://x"}}`)
		}, ""},
		{"forward to a topic runs", func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"))
		}, ""},
		{"transform", func(f *Flow) {
			f.Nodes = append(f.Nodes,
				node("transform-1", "transform", `{"expr":"msg"}`),
				node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, "transform-1"},
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL to compile — `undefined: consumer`, `undefined: postJSON`, `unknown field Forward in struct literal`.

- [ ] **Step 3: `resolve.go` — the consumer's forward and sink**

Add two fields at the end of `NodeSpec`:

```go
	Forward         string `json:"forward,omitempty"`     // consumer: the topic it forwards every record to
	SinkURL         string `json:"sink_url,omitempty"`    // consumer: the http sink's URL
```

In `Resolve`, add the map after `topicName`:

```go
	forward := map[string]string{}   // consumer node id → the topic it forwards to
```

and, between the node loop and `var specs []NodeSpec`, a loop that fills it:

```go
	for _, e := range f.Edges {
		if byID[e.Source].Type == "consumer" && byID[e.Target].Type == "topic" {
			forward[e.Source] = topicName[e.Target]
		}
	}
```

Replace the consumer case's `specs = append(specs, NodeSpec{… Type: "consumer" …})` line with:

```go
			spec := NodeSpec{Flow: f.ID, Node: dst.ID, Type: "consumer", Topic: topicName[src.ID], Group: d.Group, AutoOffsetReset: d.AutoOffsetReset, Forward: forward[dst.ID]}
			if d.Sink.Kind == "http" {
				spec.SinkURL = d.Sink.URL
			}
			specs = append(specs, spec)
```

Replace `notYetRunnable` (keep its doc comment) with:

```go
func notYetRunnable(f Flow) []Problem {
	var ps []Problem
	for _, n := range f.Nodes {
		switch n.Type {
		case "consumer":
			var d ConsumerData
			json.Unmarshal(n.Data, &d)
			if d.Instances > 1 {
				ps = append(ps, Problem{Node: n.ID, Message: "more than one instance runs from M4"})
			}
		case "transform":
			ps = append(ps, Problem{Node: n.ID, Message: "transforms run from M5"})
		}
	}
	return ps
}
```

- [ ] **Step 4: `node.go` — the consumer type and the http sink**

Change the file's header comment's last line to:

```go
// network: POST /send (producers), GET /tail?since=N and GET /stats. A consumer
// also posts each record to its http sink and forwards it to its next topic.
```

Replace the whole `consume` function (and its comment) with:

```go
// consumer takes each fetched record of one consumer node through the tail, the
// http sink (when set) and the forward (when set). A failed sink or forward is
// counted and logged, not retried: autocommit still moves past the record.
type consumer struct {
	spec    NodeSpec
	tail    *tail
	counts  *counters
	post    func(ctx context.Context, url string, body []byte) error // the http sink
	produce func(context.Context, *kgo.Record) error                 // the forward
}

func (c *consumer) handle(ctx context.Context, r *kgo.Record) {
	c.counts.ok()
	c.tail.push(r)
	log.Printf("%s[%d]@%d key=%s %s", r.Topic, r.Partition, r.Offset, r.Key, r.Value)
	if c.spec.SinkURL != "" {
		if err := c.post(ctx, c.spec.SinkURL, r.Value); err != nil {
			c.counts.fail(fmt.Errorf("sink: %w", err))
			log.Printf("sink: %v", err)
		}
	}
	if c.spec.Forward != "" {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := c.produce(pctx, &kgo.Record{Topic: c.spec.Forward, Key: r.Key, Value: r.Value})
		cancel()
		if err != nil {
			c.counts.fail(fmt.Errorf("forward: %w", err))
			log.Printf("forward to %s: %v", c.spec.Forward, err)
		}
	}
}

// consume polls the group until ctx ends, handing every record to c.
func (c *consumer) consume(ctx context.Context, cl *kgo.Client) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(topic string, partition int32, err error) {
			c.counts.fail(err)
			log.Printf("fetch %s[%d]: %v", topic, partition, err)
		})
		fs.EachRecord(func(r *kgo.Record) { c.handle(ctx, r) })
	}
}

// postJSON is the http sink: it POSTs body as JSON within 5 s; any answer but
// 2xx is an error.
func postJSON(ctx context.Context, url string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20)) // drained, so the connection is reused
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("%s answered %s", url, res.Status)
	}
	return nil
}
```

In `runNode`, right after `t, counts, boot := &tail{}, &counters{}, NewID()`, add the produce function both node kinds share:

```go
	produce := func(ctx context.Context, rec *kgo.Record) error {
		return cl.ProduceSync(ctx, rec).FirstErr()
	}
```

then make the producer use it:

```go
		p := &producer{spec: spec, tail: t, counts: counts, produce: produce}
```

and replace `go consume(ctx, cl, t, counts)` with:

```go
		c := &consumer{spec: spec, tail: t, counts: counts, post: postJSON, produce: produce}
		go c.consume(ctx, cl)
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok  kafka-playground/studio`; `gofmt -l` prints nothing (run `gofmt -w` on edited files: the new `NodeSpec` fields shift alignment).

- [ ] **Step 6: The CA bundle**

In `studio/Dockerfile`, make the last stage read:

```dockerfile
FROM scratch
# The CA bundle lets an http sink post to https URLs.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /studio /studio
USER 65534
EXPOSE 8082
ENTRYPOINT ["/studio"]
```

(`golang:1.27.1-alpine` ships `/etc/ssl/certs/ca-certificates.crt`; checked while writing this plan.)

- [ ] **Step 7: `verify-studio` runs two chained flows**

Flow A: a manual producer → `studio-verify-a` → consumer-1, which forwards to `studio-verify-a-out` → consumer-2. Flow B: a manual producer → `studio-verify-b` → a consumer whose http sink posts to flow A's producer `send` URL, reached as `http://studio:8082` on the compose network. A record sent to B must show up in A's consumer-2 tail. That covers the http sink, a node container passing the cross-site guard, the forward, and two flows at once.

In `Makefile`, change the `verify-studio` help comment to:

```make
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, live ticks, chained flows, stop, delete
```

and insert these lines right before the recipe's last line (`⇥echo "STUDIO OK ($$id)"`); every line starts with a real tab:

```make
	a='{"name":"verify-chain-a","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"manual","key":"","value":"{}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify-a","partitions":1,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify-a","auto_offset_reset":"earliest","sink":{"kind":"log"}}},{"id":"topic-2","type":"topic","position":{"x":600,"y":0},"data":{"name":"studio-verify-a-out","partitions":1,"replication_factor":1}},{"id":"consumer-2","type":"consumer","position":{"x":800,"y":0},"data":{"group":"studio-verify-a-out","auto_offset_reset":"earliest","sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"},{"id":"e3","source":"consumer-1","target":"topic-2"},{"id":"e4","source":"topic-2","target":"consumer-2"}]}'; \
	created=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$a") || { echo "STUDIO FAILED: create chain flow A: $$created"; exit 1; }; \
	aid=$$(echo "$$created" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	b='{"name":"verify-chain-b","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"manual","key":"","value":"{}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify-b","partitions":1,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify-b","auto_offset_reset":"earliest","sink":{"kind":"http","url":"http://studio:8082/api/flows/'"$$aid"'/nodes/producer-1/send"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"}]}'; \
	created=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$b") || { echo "STUDIO FAILED: create chain flow B: $$created"; exit 1; }; \
	bid=$$(echo "$$created" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	trap "for f in $$id $$tid $$aid $$bid; do curl -sS -X DELETE $(STUDIO_URL)/api/flows/\$$f >/dev/null 2>&1; done; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1" EXIT; \
	for f in $$aid $$bid; do curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$f/deploy >/dev/null || { echo "STUDIO FAILED: deploy chain flow $$f"; exit 1; }; done; \
	rec="chain-$$(date +%s)"; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$bid/nodes/producer-1/send --data "{\"id\":\"$$rec\"}" >/dev/null || { echo "STUDIO FAILED: send to chain flow B"; exit 1; }; \
	for i in $$(seq 30); do \
		curl -sS "$(STUDIO_URL)/api/flows/$$aid/nodes/consumer-2/tail?since=0" | grep -q "$$rec" && break; \
		[ "$$i" = 30 ] && { echo "STUDIO FAILED: $$rec never reached flow A's last consumer; flow B: $$(curl -sS $(STUDIO_URL)/api/flows/$$bid/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio chain: $$rec went through flow B's http sink into flow A and was forwarded"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$bid && curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$aid || { echo "STUDIO FAILED: delete the chain flows"; exit 1; }; \
```

The `trap` line replaces the one the timer block set: it deletes every flow this recipe made (`\$$f` reaches the shell as `$f`, expanded when the trap runs).

- [ ] **Step 8: Run the end-to-end check**

Run: `docker compose config --quiet && make down && make verify`
Expected: a line `studio chain: chain-<n> went through flow B's http sink into flow A and was forwarded`, then `STUDIO OK (...)` and `VERIFY OK`. Then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

If the chain never arrives: `docker logs studio-<bid>-consumer-1` shows the sink's error (`sink: … answered …`), `docker logs studio-<aid>-consumer-1` the forward's.

- [ ] **Step 9: Commit**

```bash
git add studio/resolve.go studio/node.go studio/node_test.go studio/resolve_test.go studio/Dockerfile Makefile
git commit -m "studio: consumers forward to a topic and post to an http sink" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Instances

**Files:**
- Modify: `studio/resolve.go`, `studio/docker.go`, `studio/engine.go`, `studio/api.go`, `studio/node.go`, `Makefile`
- Test: `studio/resolve_test.go`, `studio/engine_test.go`

**Interfaces:**
- Consumes: Task 1's `NodeSpec` (`Forward`, `SinkURL`), `Resolve`, `notYetRunnable`; M3's `NodeState`, `FlowState`, `stateOf`, `Snapshot`, `nodeStatsOf`, `applyKafka`, `NodeRunning`, `withRates` (`engine.go`); `startNode`, `labelFlow`, `labelNode` (`docker.go`); `nodeProxy` (`api.go`).
- Produces: `NodeSpec.Instance int` (JSON `instance,omitempty`); `containerName(flow, node string, instance int) string`; `nodeURL(flow, node string, instance int, path string) string`; `instancesOf(n Node) []int`; `clashes(specs []NodeSpec) []Problem`; `labelInstance = "studio.instance"`; `NodeState` fields `Instance int` (JSON `instance,omitempty`, first) and `Instances []NodeState` (`instances,omitempty`, last); `nodeStates(f Flow, cs []container.Summary) map[string]NodeState`; `nodeOf(map[int]string) NodeState`; `withStats(ctx, flow, node string, ns NodeState) NodeState`; `nodeStatsOf(ctx, flow, node string, instance int)`; `(*Engine).NodeRunning(ctx, id, node string, instance int) (int, error)`; `pickInstance(node string, ns NodeState, instance int) (int, error)`; `rate(cur, prev NodeState, seen bool, dt float64) float64`; `instanceOf(ns NodeState, i int) (NodeState, bool)`. Task 3's UI reads `instance` and `instances` from the tick and calls `…/tail?instance=`.

- [ ] **Step 1: Write the failing tests**

In `studio/resolve_test.go`: change `containerName("0a1b2c3d", "consumer-1")` to `containerName("0a1b2c3d", "consumer-1", 0)`; change the `"two instances"` case of `TestNotYetRunnable` to

```go
		{"two instances run", func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":2,"sink":{"kind":"log"}}`)
		}, ""},
```

and insert before `func TestNotYetRunnable`:

```go
func TestResolveInstances(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":3,"sink":{"kind":"log"}}`)
	specs, _ := Resolve(f)
	if len(specs) != 4 {
		t.Fatalf("want the producer and three consumer instances, got %+v", specs)
	}
	for i, s := range specs[1:] {
		want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: i + 1}
		if !reflect.DeepEqual(s, want) {
			t.Fatalf("instance %d:\n got %+v\nwant %+v", i+1, s, want)
		}
	}
	if got := containerName("0a1b2c3d", "consumer-1", 2); got != "studio-0a1b2c3d-consumer-1-2" {
		t.Fatalf("containerName of instance 2: %s", got)
	}
}

func TestClashes(t *testing.T) {
	specs := []NodeSpec{
		{Flow: "f", Node: "consumer-1", Instance: 1},
		{Flow: "f", Node: "consumer-1", Instance: 2},
		{Flow: "f", Node: "consumer-1-2"},
		{Flow: "f", Node: "consumer-2"},
	}
	ps := clashes(specs)
	if len(ps) != 1 || ps[0].Node != "consumer-1-2" || !strings.Contains(ps[0].Message, "studio-f-consumer-1-2") {
		t.Fatalf("want one clash on consumer-1-2 naming the container, got %v", ps)
	}
	if ps := clashes(specs[:2]); ps != nil {
		t.Fatalf("a node's own instances do not clash, got %v", ps)
	}
}
```

In `studio/engine_test.go`: add `"encoding/json"`, `"strconv"` and `"github.com/moby/moby/api/types/container"` to the imports; in `TestApplyKafka` change `containerName("f", "consumer-1")` and `containerName("f", "consumer-2")` to `containerName("f", "consumer-1", 0)` and `containerName("f", "consumer-2", 0)`; insert before `func TestApplyKafka`:

```go
// A container-name clash is refused like any other problem, before Docker or
// Kafka (both nil here) are touched.
func TestDeployRefusesNameClash(t *testing.T) {
	e := &Engine{store: Store{dir: t.TempDir()}}
	f := clone(good)
	f.ID = "0123abcd"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":2,"sink":{"kind":"log"}}`)
	f.Nodes = append(f.Nodes, node("consumer-1-2", "consumer", `{"group":"h","sink":{"kind":"log"}}`))
	f.Edges = append(f.Edges, edge("topic-1", "consumer-1-2"))
	if err := e.store.Put(f); err != nil {
		t.Fatal(err)
	}
	var ps Problems
	if err := e.Deploy(context.Background(), f.ID); !errors.As(err, &ps) || len(ps) != 1 || ps[0].Node != "consumer-1-2" {
		t.Fatalf("want one problem on consumer-1-2, got %v", err)
	}
}

func TestNodeStates(t *testing.T) {
	ctr := func(node string, instance int, state container.ContainerState) container.Summary {
		labels := map[string]string{labelFlow: "f", labelNode: node}
		if instance > 0 {
			labels[labelInstance] = strconv.Itoa(instance)
		}
		return container.Summary{Labels: labels, State: state}
	}
	f := clone(good)
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":3,"sink":{"kind":"log"}}`)

	// Instance 1 runs, 2 exited, 3 was removed by hand; the producer has no container.
	got := nodeStates(f, []container.Summary{ctr("consumer-1", 1, container.StateRunning), ctr("consumer-1", 2, container.StateExited)})
	want := map[string]NodeState{
		"producer-1": {State: "missing"},
		"consumer-1": {State: "exited", Instances: []NodeState{{Instance: 1, State: "running"}, {Instance: 2, State: "exited"}, {Instance: 3, State: "missing"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("instances:\n got %+v\nwant %+v", got, want)
	}

	// Deployed with one consumer, file edited to three since: what runs is what counts.
	got = nodeStates(f, []container.Summary{ctr("producer-1", 0, container.StateRunning), ctr("consumer-1", 0, container.StateRunning)})
	if c := got["consumer-1"]; c.State != "running" || c.Instances != nil {
		t.Fatalf("deployed single, file says 3: want running with no instances, got %+v", c)
	}

	// Deployed with three, file edited to one since.
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"log"}}`)
	got = nodeStates(f, []container.Summary{ctr("consumer-1", 1, container.StateRunning), ctr("consumer-1", 2, container.StateRunning)})
	if c := got["consumer-1"]; c.State != "running" || len(c.Instances) != 2 {
		t.Fatalf("deployed 2, file says 1: want running with its 2 instances, got %+v", c)
	}
}

func TestPickInstance(t *testing.T) {
	single := NodeState{State: "running"}
	multi := NodeState{State: "exited", Instances: []NodeState{{Instance: 1, State: "running"}, {Instance: 2, State: "exited"}}}
	for _, c := range []struct {
		name   string
		ns     NodeState
		asked  int
		want   int
		errHas string // "" means no error
	}{
		{"single", single, 0, 0, ""},
		{"single asked for an instance", single, 2, 0, "no instance 2"},
		{"single exited", NodeState{State: "exited"}, 0, 0, "is exited"},
		{"multi defaults to its first", multi, 0, 1, ""},
		{"multi, a stopped instance", multi, 2, 0, "instance 2 is exited"},
		{"multi, no such instance", multi, 7, 0, "no instance 7"},
	} {
		got, err := pickInstance("consumer-1", c.ns, c.asked)
		if c.errHas == "" && (err != nil || got != c.want) {
			t.Errorf("%s: got %d, %v; want %d", c.name, got, err, c.want)
		}
		if c.errHas != "" && (err == nil || !errors.Is(err, ErrNotRunning) || !strings.Contains(err.Error(), c.errHas)) {
			t.Errorf("%s: want ErrNotRunning naming %q, got %v", c.name, c.errHas, err)
		}
	}
}

func TestApplyKafkaInstances(t *testing.T) {
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"consumer-1": {State: "running", Instances: []NodeState{{Instance: 1, State: "running"}, {Instance: 2, State: "running"}}},
	}}
	specs := []NodeSpec{
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: 1},
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: 2},
	}
	one := &kadm.DescribedGroupMember{ClientID: containerName("f", "consumer-1", 1)}
	two := &kadm.DescribedGroupMember{ClientID: containerName("f", "consumer-1", 2)}
	lags := kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": {
		0: {Topic: "orders", Partition: 0, Lag: 1, Member: two},
		1: {Topic: "orders", Partition: 1, Lag: 2, Member: one},
		2: {Topic: "orders", Partition: 2, Lag: 3, Member: two},
	}}}}
	applyKafka(&st, "f", specs, nil, lags, nil)
	c := st.Nodes["consumer-1"]
	if c.Lag == nil || *c.Lag != 6 || c.Assigned != nil {
		t.Fatalf("node: want the group's lag 6 and no node-level assignment, got %+v", c)
	}
	if a := c.Instances[0].Assigned; !reflect.DeepEqual(a, map[string][]int32{"orders": {1}}) {
		t.Fatalf("instance 1: want [1], got %v", a)
	}
	if a := c.Instances[1].Assigned; !reflect.DeepEqual(a, map[string][]int32{"orders": {0, 2}}) {
		t.Fatalf("instance 2: want [0 2], got %v", a)
	}
}
```

and append to the end of `TestWithRates` (inside the function):

```go
	// A node with instances: each instance against its own previous sample, the node their sum.
	prev = FlowState{Nodes: map[string]NodeState{"c": {Instances: []NodeState{{Instance: 1, Boot: "a", Total: 10}, {Instance: 2, Boot: "a", Total: 4}}}}}
	cur = FlowState{Nodes: map[string]NodeState{"c": {Total: 26, Instances: []NodeState{{Instance: 1, Boot: "a", Total: 20}, {Instance: 2, Boot: "b", Total: 1}, {Instance: 3, Boot: "a", Total: 5}}}}}
	withRates(&cur, prev, 2)
	c := cur.Nodes["c"]
	if c.Rate != 5 || c.Instances[0].Rate != 5 || c.Instances[1].Rate != 0 || c.Instances[2].Rate != 0 {
		t.Fatalf("want instance 1 at 5/s, the restarted and the new one without a rate, the node at 5/s; got %+v", c)
	}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cd studio && go test ./...`
Expected: FAIL to compile — `too many arguments in call to containerName`, `undefined: clashes`, `undefined: nodeStates`, `undefined: pickInstance`, `unknown field Instance`.

- [ ] **Step 3: `resolve.go` — instances in specs and names**

Make the imports:

```go
import (
	"encoding/json"
	"fmt"
	"strconv"
)
```

Add the last `NodeSpec` field:

```go
	Instance        int    `json:"instance,omitempty"`    // consumer: 1..n when it runs n > 1 instances, else 0
```

Replace `containerName` and `nodeURL` (with their comments) with:

```go
// containerName is a node container's name and, on the compose network, its host
// name; instance 0 is a node's only container, 1..n one of its instances.
func containerName(flow, node string, instance int) string {
	name := "studio-" + flow + "-" + node
	if instance > 0 {
		name += "-" + strconv.Itoa(instance)
	}
	return name
}

// nodeURL is path on a node container's own API, reached by container name on
// the compose network.
func nodeURL(flow, node string, instance int, path string) string {
	return "http://" + containerName(flow, node, instance) + nodeAddr + path
}

// instancesOf is the instance numbers of a node's containers: [0] for a node
// with one container, 1..n for a consumer with instances: n > 1.
func instancesOf(n Node) []int {
	if n.Type != "consumer" {
		return []int{0}
	}
	var d ConsumerData
	json.Unmarshal(n.Data, &d)
	if d.Instances <= 1 {
		return []int{0}
	}
	is := make([]int, d.Instances)
	for i := range is {
		is[i] = i + 1
	}
	return is
}
```

In `Resolve`'s consumer case, replace `specs = append(specs, spec)` with:

```go
			for _, i := range instancesOf(dst) {
				spec.Instance = i
				specs = append(specs, spec)
			}
```

Insert before `notYetRunnable`'s comment:

```go
// clashes names each node whose container would take a name another node's
// container already has: a consumer "consumer-1" with two instances runs
// studio-<flow>-consumer-1-2, which is also a node "consumer-1-2"'s name.
func clashes(specs []NodeSpec) []Problem {
	var ps []Problem
	owner := map[string]string{} // container name → node id
	for _, s := range specs {
		name := containerName(s.Flow, s.Node, s.Instance)
		if other, ok := owner[name]; ok && other != s.Node {
			ps = append(ps, Problem{Node: s.Node, Message: fmt.Sprintf("its container name %s is also node %s's; rename one of them", name, other)})
			continue
		}
		owner[name] = s.Node
	}
	return ps
}
```

and make `notYetRunnable`'s body keep only transforms:

```go
func notYetRunnable(f Flow) []Problem {
	var ps []Problem
	for _, n := range f.Nodes {
		if n.Type == "transform" {
			ps = append(ps, Problem{Node: n.ID, Message: "transforms run from M5"})
		}
	}
	return ps
}
```

- [ ] **Step 4: `docker.go` and `node.go` — the label, the name, the client id**

In `docker.go`: add `"strconv"` to the imports; extend the header comment's last line to `// studio.flow and studio.node (and studio.instance for one of a consumer's` / `// instances); nothing else about it is stored.`; make the constants

```go
const (
	labelFlow     = "studio.flow"
	labelNode     = "studio.node"
	labelInstance = "studio.instance" // only on a consumer's instances 1..n
)
```

and in `startNode` replace `name := containerName(spec.Flow, spec.Node)` with

```go
	name := containerName(spec.Flow, spec.Node, spec.Instance)
	labels := map[string]string{labelFlow: spec.Flow, labelNode: spec.Node}
	if spec.Instance > 0 {
		labels[labelInstance] = strconv.Itoa(spec.Instance)
	}
```

and the config's `Labels: map[string]string{labelFlow: spec.Flow, labelNode: spec.Node},` with `Labels: labels,`.

In `node.go`'s `runNode`, the client id becomes `kgo.ClientID(containerName(spec.Flow, spec.Node, spec.Instance))`.

- [ ] **Step 5: `engine.go` — states, stats, Kafka and rates per instance**

Add `"maps"` and `"strconv"` to the imports.

Replace `NodeState`'s comment and its first and last lines so the type reads:

```go
// NodeState is one node in a snapshot. State is Docker's container state
// (running, exited, …) or "missing" for producers and consumers, and "ready" or
// "missing" for topics. The other fields are filled while the flow runs; zero
// values are left out of the JSON, except Lag, which is nil only when the broker
// did not answer. A consumer with instances: n > 1 lists one NodeState per
// container in Instances (each with its Instance number); the node's own Total,
// Rate and Errors are their sums and its State is "running" only when all run.
type NodeState struct {
	Instance   int                `json:"instance,omitempty"` // an entry of Instances: 1..n
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
	Instances  []NodeState        `json:"instances,omitempty"`
}
```

In `Deploy`, right after the `nothing to run` check, add:

```go
	if ps := clashes(specs); ps != nil {
		return Problems(ps)
	}
```

Replace `stateOf` with this and the two functions after it:

```go
// stateOf is State for a flow already read from the store.
func (e *Engine) stateOf(ctx context.Context, f Flow) (FlowState, error) {
	cs, err := flowContainers(ctx, e.docker, f.ID)
	if err != nil {
		return FlowState{}, err
	}
	st := FlowState{Status: "stopped", Nodes: map[string]NodeState{}}
	if len(cs) == 0 {
		return st, nil
	}
	st.Status = "running"
	st.Nodes = nodeStates(f, cs)
	return st, nil
}

// nodeStates is each producer and consumer node's state in a running flow: one
// slot per container the file asks for, "missing" until a container fills it,
// plus a slot for every container there is. When the file was edited after the
// deploy, what the deploy ran decides between one container and instances: the
// other kind's empty slots are dropped.
func nodeStates(f Flow, cs []container.Summary) map[string]NodeState {
	slots := map[string]map[int]string{} // node id → instance → container state
	for _, n := range f.Nodes {
		if n.Type == "producer" || n.Type == "consumer" {
			slots[n.ID] = map[int]string{}
			for _, i := range instancesOf(n) {
				slots[n.ID][i] = "missing"
			}
		}
	}
	multi := map[string]bool{} // node id → whether its containers are instances 1..n
	for _, c := range cs {
		node := c.Labels[labelNode]
		i, _ := strconv.Atoi(c.Labels[labelInstance]) // no label: a node's only container, 0
		if slots[node] == nil {
			slots[node] = map[int]string{}
		}
		slots[node][i] = string(c.State)
		multi[node] = i > 0
	}
	nodes := map[string]NodeState{}
	for node, insts := range slots {
		if m, deployed := multi[node]; deployed {
			for i, state := range insts {
				if state == "missing" && (i > 0) != m {
					delete(insts, i)
				}
			}
		}
		nodes[node] = nodeOf(insts)
	}
	return nodes
}

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

In `Snapshot`, replace the loop that asks each running node for its stats (`for node, ns := range st.Nodes { if ns.State != "running" { continue } … }`) with:

```go
	for node, ns := range st.Nodes {
		if len(ns.Instances) == 0 {
			if ns.State == "running" {
				st.Nodes[node] = withStats(ctx, id, node, ns)
			}
			continue
		}
		for k, in := range ns.Instances {
			if in.State == "running" {
				in = withStats(ctx, id, node, in)
			}
			ns.Instances[k] = in
			ns.Total += in.Total
			ns.Errors += in.Errors
			if ns.LastError == "" && in.LastError != "" {
				ns.LastError = fmt.Sprintf("#%d: %s", in.Instance, in.LastError)
			}
		}
		st.Nodes[node] = ns
	}
```

Replace `nodeStatsOf`'s comment, signature and request line, and put `withStats` before it:

```go
// withStats is ns (a node, or one of its instances) with the counters its
// container reports on /stats.
func withStats(ctx context.Context, flow, node string, ns NodeState) NodeState {
	s, err := nodeStatsOf(ctx, flow, node, ns.Instance)
	if err != nil {
		ns.LastError = "stats: " + err.Error()
		return ns
	}
	ns.Total, ns.Errors, ns.LastError, ns.TailSeq, ns.Boot = s.Total, s.Errors, s.LastError, s.TailSeq, s.Boot
	return ns
}

// nodeStatsOf asks a running node container for its counters (1 s budget).
func nodeStatsOf(ctx context.Context, flow, node string, instance int) (nodeStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL(flow, node, instance, "/stats"), nil)
```

(the rest of `nodeStatsOf` stays).

In `applyKafka`'s comment, replace `topic and the partitions whose group member is this node's client. Whatever the` / `broker did not answer is left out.` with `topic and the partitions whose group member is this node's client (or, for a` / `node with instances, each instance's client). Whatever the broker did not` / `answer is left out.`; in its consumer loop replace everything from `ns := st.Nodes[s.Node]` to `st.Nodes[s.Node] = ns` with:

```go
		ns := st.Nodes[s.Node]
		var lag int64
		var held []int32
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Err == nil && ml.Lag > 0 {
				lag += ml.Lag
			}
			if ml.Member != nil && ml.Member.ClientID == containerName(flow, s.Node, s.Instance) {
				held = append(held, ml.Partition)
			}
		}
		ns.Lag = &lag
		if held != nil {
			slices.Sort(held)
			assigned := map[string][]int32{s.Topic: held}
			if s.Instance == 0 {
				ns.Assigned = assigned
			}
			for k := range ns.Instances {
				if ns.Instances[k].Instance == s.Instance {
					ns.Instances[k].Assigned = assigned
				}
			}
		}
		st.Nodes[s.Node] = ns
```

Replace `NodeRunning` (and its comment) with:

```go
// NodeRunning returns which of node's containers to talk to: instance as asked,
// or for a node with instances and no instance asked, its first. It is
// ErrNotRunning, wrapped with the state, unless that container runs.
func (e *Engine) NodeRunning(ctx context.Context, id, node string, instance int) (int, error) {
	st, err := e.State(ctx, id)
	if err != nil {
		return 0, err
	}
	if st.Status != "running" {
		return 0, ErrNotRunning
	}
	ns, ok := st.Nodes[node]
	if !ok {
		return 0, fmt.Errorf("node %s has no container: %w", node, ErrNotRunning)
	}
	return pickInstance(node, ns, instance)
}

// pickInstance is NodeRunning's choice within one node's state.
func pickInstance(node string, ns NodeState, instance int) (int, error) {
	if len(ns.Instances) == 0 {
		if instance > 0 {
			return 0, fmt.Errorf("node %s has no instance %d: %w", node, instance, ErrNotRunning)
		}
		if ns.State != "running" {
			return 0, fmt.Errorf("node %s is %s: %w", node, ns.State, ErrNotRunning)
		}
		return 0, nil
	}
	if instance == 0 {
		instance = ns.Instances[0].Instance
	}
	for _, in := range ns.Instances {
		if in.Instance == instance {
			if in.State != "running" {
				return 0, fmt.Errorf("node %s instance %d is %s: %w", node, instance, in.State, ErrNotRunning)
			}
			return instance, nil
		}
	}
	return 0, fmt.Errorf("node %s has no instance %d: %w", node, instance, ErrNotRunning)
}
```

Replace `withRates` (and its comment) with:

```go
// withRates sets each node's records per second against the previous snapshot,
// taken dt seconds earlier; a node with instances gets the sum of theirs. A node
// or instance whose boot changed (its container restarted) or whose total fell
// gets no rate this time rather than a wrong one.
func withRates(cur *FlowState, prev FlowState, dt float64) {
	if dt <= 0 {
		return
	}
	for id, ns := range cur.Nodes {
		p, seen := prev.Nodes[id]
		if len(ns.Instances) == 0 {
			ns.Rate = rate(ns, p, seen, dt)
		} else {
			var sum float64
			for k, in := range ns.Instances {
				pi, ok := instanceOf(p, in.Instance)
				ns.Instances[k].Rate = rate(in, pi, ok, dt)
				sum += ns.Instances[k].Rate
			}
			ns.Rate = math.Round(sum*10) / 10
		}
		cur.Nodes[id] = ns
	}
}

// rate is cur's records per second against prev, taken dt seconds earlier, rounded
// to 0.1; 0 when there was no prev or cur's container restarted since.
func rate(cur, prev NodeState, seen bool, dt float64) float64 {
	if !seen || prev.Boot != cur.Boot || cur.Total < prev.Total {
		return 0
	}
	return math.Round(float64(cur.Total-prev.Total)/dt*10) / 10
}

// instanceOf finds instance i among ns's instances.
func instanceOf(ns NodeState, i int) (NodeState, bool) {
	for _, in := range ns.Instances {
		if in.Instance == i {
			return in, true
		}
	}
	return NodeState{}, false
}
```

- [ ] **Step 6: `api.go` — the proxy picks an instance**

Add `"strconv"` to the imports. Replace `nodeProxy`'s comment and the three lines that check and proxy with:

```go
// nodeProxy forwards to path on the node's own container once it runs (for a
// consumer with instances, the one ?instance= names, else its first); otherwise
// it answers 409 naming the state.
func (s *server) nodeProxy(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, node := r.PathValue("id"), r.PathValue("node")
		asked, _ := strconv.Atoi(r.URL.Query().Get("instance")) // absent or not a number: the default
		instance, err := s.engine.NodeRunning(r.Context(), id, node, asked)
		if err != nil {
			engineErr(w, err)
			return
		}
		proxy(w, r, nodeURL(id, node, instance, path+"?"+r.URL.RawQuery))
	}
}
```

- [ ] **Step 7: Run the tests to see them pass**

Run: `cd studio && go vet ./... && gofmt -l . && go test ./...`
Expected: `ok  kafka-playground/studio`; `gofmt -l` prints nothing.

- [ ] **Step 8: `verify-studio` runs three instances**

In `Makefile`, change the `verify-studio` help comment to:

```make
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, live ticks, chained flows, instances, stop, delete
```

and insert these lines right before `⇥echo "STUDIO OK ($$id)"` (after Task 1's chain block); every line starts with a real tab:

```make
	c='{"name":"verify-instances","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"manual","key":"","value":"{}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify-instances","partitions":3,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify-instances","auto_offset_reset":"earliest","instances":3,"sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"}]}'; \
	created=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$c") || { echo "STUDIO FAILED: create the instances flow: $$created"; exit 1; }; \
	cid=$$(echo "$$created" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	trap "for f in $$id $$tid $$aid $$bid $$cid; do curl -sS -X DELETE $(STUDIO_URL)/api/flows/\$$f >/dev/null 2>&1; done; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1" EXIT; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$cid/deploy >/dev/null || { echo "STUDIO FAILED: deploy the instances flow"; exit 1; }; \
	[ "$$(docker ps -q -f label=studio.flow=$$cid -f label=studio.node=consumer-1 | wc -l | tr -d ' ')" = 3 ] || { echo "STUDIO FAILED: want 3 consumer-1 containers"; exit 1; }; \
	for i in $$(seq 45); do \
		[ "$$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state | grep -o '"assigned":{"studio-verify-instances":\[[0-2]\]}' | wc -l | tr -d ' ')" = 3 ] && break; \
		[ "$$i" = 45 ] && { echo "STUDIO FAILED: the 3 instances never held one partition each: $$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio instances: $$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state)"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$cid/nodes/consumer-1/tail?since=0&instance=3"); \
	[ "$$code" = 200 ] || { echo "STUDIO FAILED: tail of instance 3: $$code, want 200"; exit 1; }; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$cid/nodes/consumer-1/tail?since=0&instance=4"); \
	[ "$$code" = 409 ] || { echo "STUDIO FAILED: tail of a fourth instance: $$code, want 409"; exit 1; }; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$cid || { echo "STUDIO FAILED: delete the instances flow"; exit 1; }; \
```

Three franz-go members join one at a time, so the group rebalances before each holds one partition; 45 s covers it.

- [ ] **Step 9: Run the end-to-end check**

Run: `docker compose config --quiet && make down && make verify`
Expected: `studio chain: …`, then `studio instances: {"status":"running","nodes":{"consumer-1":{"state":"running","lag":0,"instances":[{"instance":1,…,"assigned":{"studio-verify-instances":[0]}},…` with one partition per instance, then `STUDIO OK (...)` and `VERIFY OK`. Then `make down`; `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 10: Commit**

```bash
git add studio/resolve.go studio/docker.go studio/engine.go studio/api.go studio/node.go studio/resolve_test.go studio/engine_test.go Makefile
git commit -m "studio: consumers run as N instances in one group; snapshot and tail per instance" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: UI — sink, instances, webhook, instance picker, flow list

**Files:**
- Modify: `studio/ui/src/flow/api.ts`, `studio/ui/src/App.tsx`, `studio/ui/src/Inspector.tsx`, `studio/ui/src/TailDrawer.tsx`, `studio/ui/src/nodes/StudioNodes.tsx`, `studio/ui/src/index.css`

**Interfaces:**
- Consumes: Task 2's snapshot JSON (`instance`, `instances` on a node; each entry has `state`, `total`, `rate`, `errors`, `lastError`, `tailSeq`, `boot`, `assigned`) and `GET …/tail?since=N&instance=I`; existing `ConsumerData` (`instances?`, `sink: {kind, url?}`) in `nodes/types.ts` — unchanged.
- Produces: `NodeRuntime` fields `instance?: number`, `instances?: NodeRuntime[]`; `api.tail(id, node, since, instance = 0)`; `TailDrawer` props `instance`, `instances`, `onInstance`; `Inspector` prop `flowId?`.

There is no UI test runner; `npm run build` (which runs `tsc`) is the check, and the report maps each behaviour below to the lines that implement it.

- [ ] **Step 1: `flow/api.ts`**

Make `NodeRuntime`'s comment and its first and last fields read:

```ts
// One node of the live snapshot (Go: NodeState in engine.go). Absent fields are zero,
// except lag, which is absent when the broker did not answer. A consumer with
// instances lists one entry per container in instances; its total, rate and errors
// are their sums.
export type NodeRuntime = {
  instance?: number
  state: string
```

and, after `warning?: string`, add `instances?: NodeRuntime[]`. Replace `api.tail` with:

```ts
  // instance picks one of a consumer's instances; 0 is the node's only container.
  tail: (id: string, node: string, since: number, instance = 0) =>
    call<TailEntry[]>(`/api/flows/${id}/nodes/${node}/tail?since=${since}${instance ? `&instance=${instance}` : ''}`),
```

- [ ] **Step 2: `App.tsx`**

Replace the effect that calls `refresh()` once with:

```tsx
  // Re-read every 5 s, so flows deployed or stopped elsewhere (another tab, curl) show their state.
  useEffect(() => {
    refresh()
    const t = setInterval(refresh, 5000)
    return () => clearInterval(t)
  }, [refresh])
```

After the `deployedSnapshot` state, add:

```tsx
  const [pickedInstance, setPickedInstance] = useState(0) // the tail drawer's instance, for a consumer with instances
```

After `const node = picked.length === 1 ? picked[0] : null`, add:

```tsx

  // The tail follows one container: the node's only one, or the picked instance (else the first).
  const rt = node ? flowState?.nodes[node.id] : undefined
  const instances = rt?.instances?.map((i) => i.instance ?? 0) ?? []
  const instance = instances.includes(pickedInstance) ? pickedInstance : (instances[0] ?? 0)
  const tailed = rt?.instances?.find((i) => i.instance === instance) ?? rt
```

Pass the flow to the inspector: `<Inspector node={node} flowId={current?.id} onChange={updateData} />`. Replace the `<TailDrawer … />` element with:

```tsx
        <TailDrawer
          key={`${current.id}/${node.id}/${instance}`}
          flowId={current.id}
          node={node}
          instance={instance}
          instances={instances}
          onInstance={setPickedInstance}
          tailSeq={tailed?.tailSeq}
          boot={tailed?.boot}
        />
```

The `key` includes the instance, so picking another instance starts a fresh drawer (its own `since`, entries and boot).

- [ ] **Step 3: `TailDrawer.tsx`**

Replace `type Props` with:

```tsx
type Props = {
  flowId: string
  node: StudioNode
  instance: number // 0: the node's only container; else one of instances
  instances: number[] // a consumer's instance numbers; empty when it runs one container
  onInstance: (i: number) => void
  tailSeq?: number
  boot?: string
}
```

Change the comment's last line and the signature to:

```tsx
// also has Send, which renders the node's own key and value templates. A consumer
// with instances tails one of them, picked in the header.
export default function TailDrawer({ flowId, node, instance, instances, onInstance, tailSeq = 0, boot = '' }: Props) {
```

Make the fetch `api.tail(flowId, node.id, since.current, instance)` and its effect's dependencies `[flowId, node.id, instance, boot, tailSeq]`. In the header, right after `<strong>{node.id}</strong> tail`, add:

```tsx
        {instances.length > 0 && (
          <select value={instance} onChange={(e) => onInstance(Number(e.target.value))}>
            {instances.map((i) => (
              <option key={i} value={i}>
                instance {i}
              </option>
            ))}
          </select>
        )}
```

- [ ] **Step 4: `nodes/StudioNodes.tsx`**

Replace the end of `runtimeLine` (from `const held = …` to its `return`) with:

```tsx
  const held = (a?: Record<string, number[]>) => Object.values(a ?? {}).flat()
  if (rt.instances) {
    for (const i of rt.instances) {
      if (held(i.assigned).length > 0) parts.push(`#${i.instance} p${held(i.assigned).join(',')}`)
    }
  } else if (held(rt.assigned).length > 0) {
    parts.push(`p${held(rt.assigned).join(',')}`)
  }
  return parts.join(' · ')
```

Replace `Shell`'s comment and its first lines down to the runtime line with:

```tsx
// The frame every node shares: type as title (plus its state while deployed, or
// how many of its instances run), a one-line summary, a line of live numbers while
// deployed (the last error as its tooltip), and the input/output handles the
// allowed-edge table gives its type.
function Shell({ id, type, selected, children }: { id: string; type: NodeType; selected?: boolean; children: ReactNode }) {
  const rt = useContext(RuntimeContext)[id]
  const up = rt?.instances?.filter((i) => i.state === 'running').length ?? 0
  const badge = rt?.instances ? `${up}/${rt.instances.length} running` : rt?.state
  const badgeClass = rt?.instances ? (up === rt.instances.length ? 'running' : 'exited') : rt?.state
  return (
    <div className={`node ${type}${selected ? ' selected' : ''}`} title={rt?.lastError || undefined}>
      <div className="node-title">
        {type} {rt && <span className={`node-state ${badgeClass}`}>{badge}</span>}
      </div>
      <div className="node-summary">{children}</div>
      {rt && (type === 'topic' || rt.state === 'running' || up > 0) && <div className="node-runtime">{runtimeLine(type, rt)}</div>}
```

(the two `Handle` lines and the closing tags stay).

- [ ] **Step 5: `Inspector.tsx`**

Add the prop:

```tsx
type Props = {
  node: StudioNode | null
  flowId?: string // the open flow, for the producer's webhook line
  onChange: (id: string, patch: Record<string, unknown>) => void
}

export default function Inspector({ node, flowId, onChange }: Props) {
```

In the producer form, after the templates hint (`<p className="hint">Templates may use …</p>`), add:

```tsx
          {flowId && (
            <>
              <label>Webhook</label>
              <code className="curl">{`curl -X POST '${location.origin}/api/flows/${flowId}/nodes/${node.id}/send?key=k1' --data '{"id": 1}'`}</code>
              <p className="hint">While the flow runs, the body is produced as the value. Other nodes reach the studio at http://studio:8082.</p>
            </>
          )}
```

In the consumer form, replace `<label>Sink</label>` and `<p>{node.data.sink.kind}</p>` with:

```tsx
          <label>Instances</label>
          <input
            type="number"
            min={1}
            max={10}
            value={node.data.instances ?? 1}
            onChange={(e) => set({ instances: Number(e.target.value) })}
          />
          <p className="hint">Containers in the group (1–10); they split the topic's partitions.</p>
          <label>Sink</label>
          <select
            value={node.data.sink.kind}
            onChange={(e) =>
              set({ sink: e.target.value === 'http' ? { kind: 'http', url: node.data.sink.url ?? '' } : { kind: 'log' } })
            }
          >
            <option value="log">log: the tail and the container log</option>
            <option value="http">http: POST each value</option>
          </select>
          {node.data.sink.kind === 'http' && (
            <>
              <label>URL</label>
              <input
                value={node.data.sink.url ?? ''}
                placeholder="http://studio:8082/api/flows/<id>/nodes/<node>/send"
                onChange={(e) => set({ sink: { kind: 'http', url: e.target.value } })}
              />
              <p className="hint">Each value is POSTed as JSON within 5 s; any answer but 2xx counts as an error.</p>
            </>
          )}
          <p className="hint">Wire it to a topic to forward every record there with the same key.</p>
```

- [ ] **Step 6: `index.css`**

Append:

```css
.inspector code.curl { display: block; padding: 4px 6px; background: #f5f5f5; font-size: 11px; white-space: pre-wrap; word-break: break-all; user-select: all; }
```

- [ ] **Step 7: Build**

Run: `cd studio/ui && npm run build`
Expected: `tsc` passes and Vite prints `✓ built` (the existing chunk-size warning is fine).

In the report, map each behaviour to its lines: the flow list re-read every 5 s; the badge `2/3 running` (red unless all run) and the numbers line while any instance runs; `#<i> p<partitions>` per instance; the instance picker and `?instance=` on the tail; a fresh drawer per instance (`key`); the sink picker dropping `url` on `log`; the instances field; the webhook line with the flow id.

- [ ] **Step 8: Commit**

```bash
git add studio/ui/src
git commit -m "studio ui: sink and instances forms, webhook line, instance picker, live flow list" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: README, the example flow and the spec

**Files:**
- Modify: `README.md`, `flows/0a1b2c3d.json`, `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`

- [ ] **Step 1: README**

In the Pipeline Studio section:

Replace the sentence `` `flows/0a1b2c3d.json` is an example that uses M4 features (two consumer instances, forwarding), so Deploy refuses it for now. `` with:

```markdown
`flows/0a1b2c3d.json` is an example: a timer producing to `orders` once a second, read by a two-instance consumer that forwards to `orders-archive`. The flow list shows each flow's state, re-read every 5 s.
```

Replace the bullet that starts `- Producers send by hand or on a timer` with these bullets:

```markdown
- Producers send by hand or on a timer (`interval_ms`, at least 10, rendering the key and value templates with `{{.Seq}}`, `{{.Now}}` and `{{.Rand}}`). The Inspector shows a producer's webhook: a `curl` line for `…/nodes/<node>/send`. Node containers reach the studio as `http://studio:8082`.
- A consumer's sink is `log` (its tail and `docker logs`) or `http`, which POSTs each value as JSON within 5 s; any answer but 2xx counts as an error. Wire a consumer to a topic and it forwards every record there with the same key. A failed sink or forward is counted and logged, not retried, and the offset still commits: that record is not forwarded (at-most-once).
- `instances` (1–10) runs that many containers of a consumer, `studio-<flow>-<node>-<i>`, in its group; they split the topic's partitions. The node shows how many run (`2/3 running`) and which partitions each holds; the tail drawer picks an instance (`…/tail?instance=<i>`).
- Flows feed each other through an http sink pointed at another flow's producer `send` URL, as `make verify` does with two flows. A deploy still refuses transforms (M5), naming the node.
- Studio consumers (franz-go, cooperative-sticky assignor) cannot share a group with the compose kcat consumers (librdkafka, range/roundrobin): the broker refuses the join with `INCONSISTENT_GROUP_PROTOCOL`, which shows as the node's errors. Give studio consumers their own groups.
```

- [ ] **Step 2: The example flow**

In `flows/0a1b2c3d.json`, change consumer-1's `"group": "orders-workers"` to `"group": "orders-studio"` (Decision 7). Nothing else changes; the file must still parse: `python3 -m json.tool flows/0a1b2c3d.json >/dev/null`.

- [ ] **Step 3: The spec**

In §7's M4 section, add after its `make verify-studio` bullet:

```markdown
- Built as decided in its plan: a single-container node keeps its M2 name and no `studio.instance` label, so flows deployed before M4 stay recognised; clashing container names are a 422; after a file edit, what the deploy ran decides between one container and instances; a node's `lastError` is its first instance's, prefixed `#<i>: `; the example flow's group is `orders-studio`, because franz-go's and kcat's assignors share no protocol and the broker refuses a mixed group.
```

- [ ] **Step 4: Check and commit**

Run: `docker compose config --quiet && (cd studio && go test ./...)`
Expected: both pass (docs only).

```bash
git add README.md flows/0a1b2c3d.json docs/superpowers/specs/2026-10-06-pipeline-studio-design.md
git commit -m "Docs for Studio M4: forwarding, http sink, instances, webhook; example flow in its own group" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
