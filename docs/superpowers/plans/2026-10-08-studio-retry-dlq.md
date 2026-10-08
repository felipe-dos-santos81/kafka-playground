# Pipeline Studio retry and DLQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A consumer can retry a failure that may pass later through `<input>__retry`, after a delay and up to N times, and send what is spent, or what can never pass, to `<input>__dlq`, with the counts on the canvas.

**Architecture:** Consumer data gains `retry: {attempts, delay_ms}` and `dlq: true`. `Resolve` puts the derived topic names and the retry group on the consumer's `NodeSpec` and adds both topics to the deploy. In the node runner, `handle` ends a record's path at its first failure and `sendOn` writes it, as read, with `studio-*` headers, to the retry topic or the DLQ. A second franz-go client in the same container (group `<group>__retry`) reads the retry topic, waits until each record is due, and runs it through the same `handle`. `/stats` reports `retried` and `dlq`. The snapshot adds `waiting`, the retry group's lag, and the UI shows all three and the headers.

**Tech Stack:** Go 1.27 (franz-go v1.22.1, kadm v1.19.0, expr-lang/expr v1.17.8), React 19 + @xyflow/react 12.12.0 + TypeScript, Playwright 1.63.0, GNU make 3.81.

**Spec:** `docs/superpowers/specs/2026-10-08-studio-retry-dlq-design.md`. The Studio spec (`docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`) stays the authority for everything the retry spec does not change.

Tasks 1–8 were applied, in order, to a scratch worktree of `2d76101` and checked there. `go vet`, `gofmt -l`, `go test -race`, `tsc -b . e2e` and `make test` passed. After `make down`, `make verify` printed `studio retry: "retried":1,"dlq":1`, `STUDIO OK`, `16 passed`, `UI OK` and `VERIFY OK`. A final `make down` left `docker ps -aq -f label=studio.flow` and `git status --short flows` empty. Two mistakes found in that run are fixed below: Task 7's `sed` left a trailing comma, and a `dlq: false` default marked every existing flow as unsaved (departure 7). Every code block is exact: apply it as written. Every "replace" names text that occurs once in the file. After editing Go files, run `gofmt -w` on them: it realigns struct fields and comments, which the blocks below do not always show aligned.

## Global Constraints

- No new dependencies and no version changes. Pin every image to an exact version; never `latest`.
- Local only: no auth, no TLS, ports on `127.0.0.1`.
- Derived names, verbatim: retry topic `<input>__retry`, DLQ `<input>__dlq`, retry group `<group>__retry`, where `<input>` is the name of the topic the consumer reads.
- Deploy refusals, verbatim: `retry needs a DLQ: records go there once their attempts run out`, `retry attempts must be between 1 and 10`, `retry delay_ms must be between 100 and 60000`, `topic name %q is too long for its __retry and __dlq topics` (an input name over 242 characters), `group is too long for its __retry group`.
- Headers, verbatim: `studio-group`, `studio-attempt` (tries that failed so far, counting this one), `studio-error` (first line, at most 1 KiB), `studio-origin` (`topic[partition]@offset`, set on the first failure and kept).
- `attempts` is the number of retries after the first try, so `3` tries a record at most four times.
- `studio/flow.go` (`ConsumerData`) and `studio/ui/src/nodes/types.ts` change together. `studio/ui/src/flow/schema.ts` needs no change (departure 7). Every validation rule lives only in `flow.go`.
- `NodeState` (`studio/engine.go`) and `NodeRuntime` (`studio/ui/src/flow/api.ts`) change together. The new fields `retried`, `dlq` and `waiting` go after `unmatched` and before `instances`, because `verify-studio` greps the JSON and relies on its field order.
- Without a DLQ, a consumer behaves exactly as today. With one, the first failure ends a record's path.
- Makefile: GNU make 3.81 with BSD tools, recipes indented with real tabs, a shell `$` written as `$$`. Every topic `verify-studio` creates is named in its exit trap.
- The UI tests own the `studio-ui-` namespace: every flow, topic and group a test makes starts with it, derived names included.
- Go code passes `gofmt -l` with no output.
- Keep `README.md`, `AGENTS.md` and the specs in sync with behaviour.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Where the plan departs from the spec

Task 9 records each of these in the retry spec.

1. **One runtime line.** The retry counts join the consumer's existing runtime line (`… · 1 retried · 0 waiting · 1 dlq`) instead of a second line. Each shows once it is nonzero; `waiting` shows once the retry group has committed. The UI tests read that one `runtime-<id>` element.
2. **Headers in the tail** show as extra `name: value` items on the record's row, after its value.
3. **`handle` takes one record at a time** (`consumer.mu`). The main loop and the retry loop share the transform's and the router's VMs, and those are not safe for concurrent use.
4. **`verify-studio` uses two consumers** on one input topic. The spec's single consumer with an http sink and a transform cannot show a transform failure: the sink runs first and fails, so the record is retried instead. `consumer-1` (failing sink, retry, DLQ) shows retry-then-DLQ. `consumer-2` (log sink, failing transform, DLQ) shows straight-to-DLQ. Both write to the same `…__dlq`, which also shows two groups sharing a DLQ.
5. **`refuse_then_deploy`'s failure text** becomes generic ("deployed though it must be refused"), because its third use refuses a missing DLQ, not an expression.
6. **A failed forward's log line** reads `forward: <err>`, like its counted error, instead of `forward to <topic>: <err>`.
7. **No `dlq` default in `schema.ts`.** The spec put `dlq: false` in the consumer's `defaultData`. `App.tsx` compares the canvas, whose nodes are filled with defaults, against the file as loaded. A new default therefore marks every existing flow without the field as unsaved, which disables Deploy and Rewind; the UI suite caught this. `dlq` is optional instead (`dlq?: boolean`, absent means false), like `instances`.

## Review Focus

The tests that pin these are in the tasks named.

1. **Stop during a retry wait.** The record must not be lost. It stays unmarked and the next deploy retries it (Task 4, `TestConsumerRetryRecord`, the cancelled-context case).
2. **Two groups retrying on one input topic** share `<input>__retry`. Each must retry only its own records and mark the other's without handling them (Task 4, the `studio-group: h` case).
3. **A record on the retry topic that no consumer sent** (written with Console or kcat, no headers) must be skipped, not crash or loop (Task 4, the `nil` headers case).
4. **A DLQ or retry write cut by Stop** must leave the record uncommitted. Any other failed write is an error, and the record commits (Task 3, the end of `TestConsumerFailures`).
5. **A consumer that reads a DLQ** (a log consumer on `orders__dlq` with its own DLQ on) must not take the record's `studio-attempt` as its own tries. It keeps the record's other headers and its first `studio-origin` (Task 3, `TestConsumerFailuresOfARecordWithHeaders`).

---

### Task 1: Consumer data and deploy checks

**Files:**
- Modify: `studio/flow.go`
- Modify: `studio/ui/src/nodes/types.ts`
- Test: `studio/flow_test.go`

**Interfaces:**
- Produces: `ConsumerData.Retry *RetryData`, `ConsumerData.DLQ bool`; `type RetryData struct { Attempts int; DelayMS int }`; `retryTopic(topic string) string`, `dlqTopic(topic string) string`, `retryGroup(group string) string`; const `maxInputTopic` (242). UI: `ConsumerData.retry?: RetryData | null`, `ConsumerData.dlq?: boolean` (absent: false), `type RetryData = { attempts: number; delay_ms: number }`. `defaultData` is unchanged.

- [ ] **Step 1: Write the failing tests**

In `studio/flow_test.go`, in `TestValidate`'s table, insert these cases just before the line `{"data missing", Deploy,`:

```go
		{"retry and DLQ valid", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":3,"delay_ms":5000},"dlq":true}`)
		}, ""},
		{"DLQ alone valid", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","dlq":true}`) }, ""},
		{"retry null is no retry", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":null}`) }, ""},
		{"retry without DLQ", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":3,"delay_ms":5000}}`)
		}, "retry needs a DLQ: records go there once their attempts run out"},
		{"retry attempts 0", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":0,"delay_ms":5000},"dlq":true}`)
		}, "retry attempts must be between 1 and 10"},
		{"retry attempts 11", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":11,"delay_ms":5000},"dlq":true}`)
		}, "retry attempts must be between 1 and 10"},
		{"retry delay 99", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":3,"delay_ms":99},"dlq":true}`)
		}, "retry delay_ms must be between 100 and 60000"},
		{"retry delay 60001", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":3,"delay_ms":60001},"dlq":true}`)
		}, "retry delay_ms must be between 100 and 60000"},
		{"retry half-built saves", Save, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":0}}`) }, ""},
		{"input topic too long for its DLQ", Deploy, func(f *Flow) {
			f.Nodes[1].Data = json.RawMessage(topic(strings.Repeat("a", 243)))
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","dlq":true}`)
		}, "is too long for its __retry and __dlq topics"},
		{"input topic of 242 fits", Deploy, func(f *Flow) {
			f.Nodes[1].Data = json.RawMessage(topic(strings.Repeat("a", 242)))
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":1,"delay_ms":100},"dlq":true}`)
		}, ""},
		{"input topic of 243 without retry or DLQ", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic(strings.Repeat("a", 243))) }, ""},
		{"group too long for its retry group", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"` + strings.Repeat("g", 249) + `","retry":{"attempts":1,"delay_ms":100},"dlq":true}`)
		}, "group is too long for its __retry group"},
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd studio && go test -run TestValidate ./...`
Expected: FAIL. `retry without DLQ`, the out-of-range cases and the too-long cases want a problem and get none.

- [ ] **Step 3: Add the data fields and helpers**

In `studio/flow.go`, replace:

```go
	Instances       int    `json:"instances"`         // 0 = 1
	Sink            Sink   `json:"sink"`
}
```

with:

```go
	Instances       int    `json:"instances"`         // 0 = 1
	Sink            Sink   `json:"sink"`
	Retry *RetryData `json:"retry"` // nil: failures are not retried
	DLQ   bool       `json:"dlq"`   // failures end in the DLQ
}

// RetryData is how a consumer retries a failure that may pass later (its sink's or
// its forward's): through its retry topic, each try DelayMS after the last, up to
// Attempts times after the first try.
type RetryData struct {
	Attempts int `json:"attempts"`
	DelayMS  int `json:"delay_ms"`
}

// A consumer's retry and DLQ topics are named after the topic it reads, and the
// group its retry loop reads in after its group.
func retryTopic(topic string) string { return topic + "__retry" }
func dlqTopic(topic string) string   { return topic + "__dlq" }
func retryGroup(group string) string { return group + "__retry" }

// maxInputTopic is the longest name a topic read by a consumer with a retry or a
// DLQ may have: <name>__retry must stay within Kafka's 249 characters.
const maxInputTopic = 249 - len("__retry")
```

- [ ] **Step 4: Check them in Validate**

In `studio/flow.go`, replace:

```go
	next := map[string][]string{} // node id → targets of its valid edges, for the loop check
```

with:

```go
	next := map[string][]string{} // node id → targets of its valid edges, for the loop check
	from := map[string]string{}   // node id → the source of a valid edge into it: the topic a consumer reads
```

Replace:

```go
		next[e.Source] = append(next[e.Source], e.Target)
	}
```

with:

```go
		next[e.Source] = append(next[e.Source], e.Target)
		from[e.Target] = e.Source
	}
```

Replace:

```go
	topicOf := map[string]string{}    // topic node id → its name
```

with:

```go
	topicOf := map[string]string{}    // topic node id → its name
	var failing []string              // consumers with a retry or a DLQ: their input topic names two more
```

In the `case "consumer":` branch, replace:

```go
			if inDegree[n.ID] != 1 {
				add(n.ID, "", "a consumer needs exactly one edge from a topic")
			}
```

with:

```go
			if d.Retry != nil {
				if !d.DLQ {
					add(n.ID, "", "retry needs a DLQ: records go there once their attempts run out")
				}
				if d.Retry.Attempts < 1 || d.Retry.Attempts > 10 {
					add(n.ID, "", "retry attempts must be between 1 and 10")
				}
				if d.Retry.DelayMS < 100 || d.Retry.DelayMS > 60000 {
					add(n.ID, "", "retry delay_ms must be between 100 and 60000")
				}
				if len(retryGroup(d.Group)) > 255 {
					add(n.ID, "", "group is too long for its __retry group")
				}
			}
			if d.Retry != nil || d.DLQ {
				failing = append(failing, n.ID)
			}
			if inDegree[n.ID] != 1 {
				add(n.ID, "", "a consumer needs exactly one edge from a topic")
			}
```

The name check needs every topic's name, so it runs after the node loop. Replace:

```go
	// Every container a deploy starts needs a name of its own
```

with:

```go
	for _, id := range failing {
		if name := topicOf[from[id]]; len(name) > maxInputTopic {
			add(id, "", "topic name %q is too long for its __retry and __dlq topics", name)
		}
	}
	// Every container a deploy starts needs a name of its own
```

- [ ] **Step 5: Mirror the fields in the UI**

In `studio/ui/src/nodes/types.ts`, replace:

```ts
  sink: { kind: 'log' | 'http'; url?: string }
}
```

with:

```ts
  sink: { kind: 'log' | 'http'; url?: string }
  retry?: RetryData | null // null or absent: failures are not retried
  dlq?: boolean // absent: false
}
export type RetryData = { attempts: number; delay_ms: number }
```

Leave `defaultData` in `studio/ui/src/flow/schema.ts` alone. A default for `dlq` would be filled into every opened flow that lacks it. `App.tsx` would then see those flows as unsaved and disable Deploy (departure 7).

- [ ] **Step 6: Run the tests**

Run: `cd studio && gofmt -w flow.go flow_test.go && go vet ./... && go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add studio/flow.go studio/flow_test.go studio/ui/src/nodes/types.ts
git commit -m "studio: consumer retry and dlq data — deploy refuses a retry without a DLQ, attempts outside 1–10, delay_ms outside 100–60000, and names too long for <input>__retry or <group>__retry

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Resolve gives the consumer its retry and DLQ topics

**Files:**
- Modify: `studio/resolve.go`
- Test: `studio/resolve_test.go`

**Interfaces:**
- Consumes: `ConsumerData.Retry`, `ConsumerData.DLQ`, `retryTopic`, `dlqTopic`, `retryGroup` (Task 1).
- Produces: `NodeSpec.Retry *RetrySpec`, `NodeSpec.DLQ string`; `type RetrySpec struct { Topic, Group string; Attempts, DelayMS int }` (JSON `topic`, `group`, `attempts`, `delay_ms`). `Resolve`'s topics include `<input>__retry` and `<input>__dlq` with the input topic's partitions, once each. A topic node drawn with that name wins.

- [ ] **Step 1: Write the failing test**

Append to `studio/resolve_test.go`:

```go
func TestResolveRetryAndDLQ(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":3,"delay_ms":5000},"dlq":true}`)
	f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"orders__dlq","partitions":1,"replication_factor":1}`))
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g",
		Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry", Attempts: 3, DelayMS: 5000}, DLQ: "orders__dlq"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("the consumer's spec names its retry topic and group and its DLQ:\n got %+v\nwant %+v", specs, want)
	}
	wantTopics := []TopicData{
		{Name: "orders", Partitions: 3, ReplicationFactor: 1},
		{Name: "orders__dlq", Partitions: 1, ReplicationFactor: 1}, // drawn as a node: created once, as drawn
		{Name: "orders__retry", Partitions: 3, ReplicationFactor: 1},
	}
	if !reflect.DeepEqual(topics, wantTopics) {
		t.Fatalf("topics:\n got %+v\nwant %+v", topics, wantTopics)
	}
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `cd studio && go test -run TestResolveRetryAndDLQ ./...`
Expected: FAIL to compile: `unknown field Retry in struct literal of type NodeSpec` and `undefined: RetrySpec`.

- [ ] **Step 3: Add the spec fields**

In `studio/resolve.go`, replace:

```go
import (
	"encoding/json"
	"strconv"
)
```

with:

```go
import (
	"encoding/json"
	"slices"
	"strconv"
)
```

Replace:

```go
	RouterNode      string  `json:"router_node,omitempty"`    // consumer: the router's node id, which its counts are reported under
}
```

with:

```go
	RouterNode      string  `json:"router_node,omitempty"`    // consumer: the router's node id, which its counts are reported under
	Retry *RetrySpec `json:"retry,omitempty"` // consumer: how its retry loop retries failures; nil without retry
	DLQ   string     `json:"dlq,omitempty"`   // consumer: the topic its failures end in; "" without a DLQ
}

// RetrySpec is a consumer's retry: the topic its failures wait in, the group its
// retry loop reads it in, how many retries a record gets after its first try and
// how long each waits.
type RetrySpec struct {
	Topic    string `json:"topic"`
	Group    string `json:"group"`
	Attempts int    `json:"attempts"`
	DelayMS  int    `json:"delay_ms"`
}
```

- [ ] **Step 4: Fill them in Resolve and add the topics**

Replace:

```go
// name its wired topics. A consumer's transform and router run in its own
// containers, so neither has a spec of its own.
```

with:

```go
// name its wired topics. A consumer's transform and router run in its own
// containers, so neither has a spec of its own. A consumer's retry and DLQ topics
// are created with its input topic's partitions, unless the flow draws them.
```

Replace:

```go
	topicName := map[string]string{} // topic node id → topic name
```

with:

```go
	topicName := map[string]string{} // topic node id → topic name
	partitions := map[string]int{}   // topic node id → its partitions
```

Replace:

```go
			topicName[n.ID] = d.Name
```

with:

```go
			topicName[n.ID] = d.Name
			partitions[n.ID] = d.Partitions
```

Replace:

```go
	var specs []NodeSpec
	for _, e := range f.Edges {
```

with:

```go
	var specs []NodeSpec
	var derived []TopicData // the consumers' retry and DLQ topics
	for _, e := range f.Edges {
```

Replace:

```go
			spec.Forward = topicName[next]
```

with:

```go
			spec.Forward = topicName[next]
			if d.Retry != nil {
				spec.Retry = &RetrySpec{Topic: retryTopic(spec.Topic), Group: retryGroup(d.Group), Attempts: d.Retry.Attempts, DelayMS: d.Retry.DelayMS}
				derived = append(derived, TopicData{Name: spec.Retry.Topic, Partitions: partitions[src.ID], ReplicationFactor: 1})
			}
			if d.DLQ {
				spec.DLQ = dlqTopic(spec.Topic)
				derived = append(derived, TopicData{Name: spec.DLQ, Partitions: partitions[src.ID], ReplicationFactor: 1})
			}
```

Replace the function's last lines:

```go
	return specs, topics
}
```

with:

```go
	for _, t := range derived { // once each, and as drawn when the flow has it as a node
		if !slices.ContainsFunc(topics, func(have TopicData) bool { return have.Name == t.Name }) {
			topics = append(topics, t)
		}
	}
	return specs, topics
}
```

- [ ] **Step 5: Run the tests**

Run: `cd studio && gofmt -w resolve.go && go vet ./... && go test ./...`
Expected: PASS. (`Deploy` already passes every topic from `Resolve` to `createTopics`, and `rewindTarget` uses only `Group` and `Topic`, so neither needs a change.)

- [ ] **Step 6: Commit**

```bash
git add studio/resolve.go studio/resolve_test.go
git commit -m "studio: Resolve gives a consumer its <input>__retry topic, <group>__retry group and <input>__dlq, and the deploy creates both topics with the input topic's partitions unless the flow draws them

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: A failed record goes to the retry topic or the DLQ

**Files:**
- Modify: `studio/node.go`
- Test: `studio/node_test.go`

**Interfaces:**
- Consumes: `NodeSpec.Retry`, `NodeSpec.DLQ`, `RetrySpec` (Task 2).
- Produces: `tailEntry.Headers map[string]string` (JSON `headers`, left out when empty); `nodeStats.Retried`, `nodeStats.DLQ` (JSON `retried`, `dlq`, left out at 0); `consumer.retried`, `consumer.dead` (`atomic.Int64`), `consumer.mu`; `(*consumer).fromRetry(r *kgo.Record) bool`; `(*consumer).sendOn(ctx, r, err error, transient bool) bool`; `header(r *kgo.Record, key string) string`; `failureHeaders(r, group string, failed int, err error) []kgo.RecordHeader`; consts `headerGroup`, `headerAttempt`, `headerError`, `headerOrigin`, `errorHeaderMax`.

- [ ] **Step 1: Write the failing tests**

In `studio/node_test.go`, replace the import lines:

```go
	"net/http/httptest"
	"strings"
```

with:

```go
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
```

Append to `studio/node_test.go`:

```go
// headersOf is a record's headers as a map, for comparing.
func headersOf(r *kgo.Record) map[string]string {
	m := map[string]string{}
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

// With retry and a DLQ, what may pass later goes to the retry topic while tries
// remain, then to the DLQ; what never will goes straight to the DLQ. Each goes as
// read, with the studio-* headers, and the first failure ends the record's path.
func TestConsumerFailures(t *testing.T) {
	ctx := context.Background()
	var sent []*kgo.Record
	sinkUp := false
	tr, err := newTransform(`{qty: msg.qty * 2}`)
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{
		spec: NodeSpec{Topic: "orders", Group: "g", SinkURL: "http://sink", Forward: "archive",
			Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry", Attempts: 2, DelayMS: 100}, DLQ: "orders__dlq"},
		tail:      &tail{},
		counts:    &counters{},
		transform: tr,
		post: func(context.Context, string, []byte) error {
			if sinkUp {
				return nil
			}
			return errors.New("503 Service Unavailable")
		},
		produce: func(_ context.Context, r *kgo.Record) error {
			sent = append(sent, r)
			return nil
		},
	}
	in := &kgo.Record{Topic: "orders", Partition: 2, Offset: 57, Key: []byte("k"), Value: []byte(`{"qty":2}`)}
	if !c.handle(ctx, in) {
		t.Fatal("a record sent on commits")
	}
	if len(sent) != 1 || sent[0].Topic != "orders__retry" || string(sent[0].Key) != "k" || string(sent[0].Value) != `{"qty":2}` {
		t.Fatalf("a sink failure goes to the retry topic as read, and not on to the forward; sent %+v", sent)
	}
	want := map[string]string{"studio-group": "g", "studio-attempt": "1", "studio-error": "sink: 503 Service Unavailable", "studio-origin": "orders[2]@57"}
	if h := headersOf(sent[0]); !reflect.DeepEqual(h, want) {
		t.Fatalf("headers: got %v, want %v", h, want)
	}

	// Read back from the retry topic, it fails again: 2 failed tries, 2 attempts, so
	// one more retry; the third failure is past them: the DLQ.
	for i, wantTopic := range []string{"orders__retry", "orders__dlq"} {
		back := sent[0]
		back.Partition, back.Offset = 0, int64(i)
		sent = nil
		c.handle(ctx, back)
		h := headersOf(sent[0])
		if len(sent) != 1 || sent[0].Topic != wantTopic || h["studio-attempt"] != strconv.Itoa(i+2) || h["studio-origin"] != "orders[2]@57" {
			t.Fatalf("failure %d: want %s with studio-attempt %d and the first origin; sent %+v, headers %v", i+2, wantTopic, i+2, sent, h)
		}
	}

	// A transform error never passes: straight to the DLQ.
	sinkUp, sent = true, nil
	c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{}`)})
	if len(sent) != 1 || sent[0].Topic != "orders__dlq" || headersOf(sent[0])["studio-attempt"] != "1" || !strings.HasPrefix(headersOf(sent[0])["studio-error"], "transform: ") {
		t.Fatalf("a transform error goes straight to the DLQ; sent %+v", sent)
	}

	// A router error neither.
	rt, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	c.transform, c.router, sent = nil, rt, nil
	c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":4}`)})
	if len(sent) != 1 || sent[0].Topic != "orders__dlq" || !strings.HasPrefix(headersOf(sent[0])["studio-error"], "router: rule 1: ") {
		t.Fatalf("a router error goes straight to the DLQ; sent %+v", sent)
	}

	// Retries are tailed but not counted in total again; every failure is an error.
	if s := c.counts.read(); s.Total != 3 || s.Errors != 5 || c.retried.Load() != 2 || c.dead.Load() != 3 || c.tail.last() != 5 {
		t.Fatalf("want total 3, 5 errors, 2 retried, 3 dead-lettered, 5 tailed; got %+v, %d, %d, %d", s, c.retried.Load(), c.dead.Load(), c.tail.last())
	}
	if h := c.tail.since(0)[1].Headers; h["studio-attempt"] != "1" {
		t.Fatalf("the tail keeps a retried record's headers; got %v", h)
	}

	// A send cut by a closed client (Stop) leaves the record uncommitted; any other
	// failed send is an error, and the record commits.
	c.produce = func(context.Context, *kgo.Record) error { return kgo.ErrClientClosed }
	if c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":5}`)}) {
		t.Fatal("a send cut by a closed client must leave the record uncommitted")
	}
	c.produce = func(context.Context, *kgo.Record) error { return errors.New("broker down") }
	if !c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":6}`)}) || !strings.HasPrefix(c.counts.read().LastError, "dlq: broker down") {
		t.Fatalf("a failed send is counted and the record commits; got %+v", c.counts.read())
	}
}

// A consumer reading a DLQ (its own DLQ on) does not take the record's
// studio-attempt for its own tries: only its retry topic's records carry those.
func TestConsumerFailuresOfARecordWithHeaders(t *testing.T) {
	var sent []*kgo.Record
	c := &consumer{
		spec:    NodeSpec{Topic: "orders__dlq", Group: "watch", SinkURL: "http://sink", DLQ: "orders__dlq__dlq"},
		tail:    &tail{},
		counts:  &counters{},
		post:    func(context.Context, string, []byte) error { return errors.New("down") },
		produce: func(_ context.Context, r *kgo.Record) error { sent = append(sent, r); return nil },
	}
	in := &kgo.Record{Topic: "orders__dlq", Value: []byte(`{}`), Headers: []kgo.RecordHeader{
		{Key: "trace", Value: []byte("t1")}, {Key: "studio-group", Value: []byte("g")}, {Key: "studio-attempt", Value: []byte("3")}, {Key: "studio-origin", Value: []byte("orders[0]@1")}}}
	c.handle(context.Background(), in)
	want := map[string]string{"trace": "t1", "studio-group": "watch", "studio-attempt": "1", "studio-error": "sink: down", "studio-origin": "orders[0]@1"}
	if len(sent) != 1 || !reflect.DeepEqual(headersOf(sent[0]), want) {
		t.Fatalf("want its own headers kept, studio-* replaced, the origin kept; got %+v", sent)
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd studio && go test -run 'TestConsumerFailures' ./...`
Expected: FAIL to compile: `c.retried undefined`, `c.dead undefined`, `c.tail.since(0)[1].Headers undefined`.

- [ ] **Step 3: Carry headers in the tail and the counts in /stats**

In `studio/node.go`, replace the imports:

```go
	"os/signal"
	"strconv"
	"sync"
```

with:

```go
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
```

Replace:

```go
	Key       string `json:"key"`
	Value     string `json:"value"`
}
```

with:

```go
	Key       string `json:"key"`
	Value     string `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"` // a record a consumer sent on after a failure carries studio-* headers
}
```

In `tail.push`, replace:

```go
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
```

with:

```go
	var hs map[string]string
	if len(r.Headers) > 0 {
		hs = map[string]string{}
		for _, h := range r.Headers {
			hs[h.Key] = string(h.Value)
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
```

and replace:

```go
		Value:     string(v),
	})
```

with:

```go
		Value:     string(v),
		Headers:   hs,
	})
```

Replace:

```go
	Route   *stepTally `json:"route,omitempty"` // a consumer's router, when it runs one
}
```

with:

```go
	Route   *stepTally `json:"route,omitempty"` // a consumer's router, when it runs one
	Retried int64 `json:"retried,omitempty"` // a consumer's records sent to its retry topic
	DLQ     int64 `json:"dlq,omitempty"`     // a consumer's records sent to its DLQ
}
```

- [ ] **Step 4: Route failures in handle**

In `studio/node.go`, replace the whole block from the comment `// consumer takes each fetched record of one consumer node through the tail, the` down to the end of `func (c *consumer) handle` (its closing `}` just before `// consume polls the group until ctx ends`) with:

```go
// consumer takes each fetched record of one consumer node through the tail, the
// http sink (when set), the transform (when set) and the forward (when set).
// Without a DLQ, a failed sink, transform, router or forward is counted and
// logged, not retried: autocommit still moves past the record. With one, the
// first failure ends the record's path and sendOn sends it to the retry topic or
// the DLQ. The sink and the writes run under context.WithoutCancel: a stop
// (SIGTERM) must not fail them with "context canceled" before Close commits past
// this record. The main loop and the retry loop share one consumer; mu lets one
// record through at a time, as the transform's and the router's VMs need.
type consumer struct {
	spec      NodeSpec
	tail      *tail
	counts    *counters
	transform *transform                                               // nil without one
	router    *router                                                  // nil without one
	post      func(ctx context.Context, url string, body []byte) error // the http sink
	produce   func(context.Context, *kgo.Record) error                 // the forward, and the sends to the retry topic and the DLQ
	retried   atomic.Int64                                             // records sent to the retry topic
	dead      atomic.Int64                                             // records sent to the DLQ
	mu        sync.Mutex
}

// handle reports whether r may be committed: false only when a write failed
// because the client was closed (a stop that outlasted its grace), so the record
// is redelivered rather than lost. Any other failure is counted and logged, and r
// is still committed. A record from the retry topic is tailed but not counted in
// total again.
func (c *consumer) handle(ctx context.Context, r *kgo.Record) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	work := context.WithoutCancel(ctx)
	if !c.fromRetry(r) {
		c.counts.ok()
	}
	c.tail.push(r)
	log.Printf("%s[%d]@%d key=%s %s", r.Topic, r.Partition, r.Offset, r.Key, r.Value)
	// failed counts and logs one step's failure. With a DLQ it sends r on and ends
	// r's path (end); commit then says whether r may be committed.
	failed := func(err error, transient bool) (end, commit bool) {
		c.counts.fail(err)
		log.Print(err)
		if c.spec.DLQ == "" {
			return false, true
		}
		return true, c.sendOn(work, r, err, transient)
	}
	if c.spec.SinkURL != "" {
		if err := c.post(work, c.spec.SinkURL, r.Value); err != nil {
			if end, commit := failed(fmt.Errorf("sink: %w", err), true); end {
				return commit
			}
		}
	}
	value := r.Value
	if c.transform != nil {
		out, err := c.transform.run(value)
		if err != nil {
			if end, commit := failed(fmt.Errorf("transform: %w", err), false); end {
				return commit
			}
		}
		if out == nil {
			return true // failed or dropped (nil): nothing to forward
		}
		value = out
	}
	forward := c.spec.Forward
	if c.router != nil {
		topic, err := c.router.route(value)
		if err != nil {
			if end, commit := failed(fmt.Errorf("router: %w", err), false); end {
				return commit
			}
		}
		forward = topic // "": failed or unmatched, nothing to forward
	}
	if forward != "" {
		pctx, cancel := context.WithTimeout(work, 10*time.Second)
		err := c.produce(pctx, &kgo.Record{Topic: forward, Key: r.Key, Value: value})
		cancel()
		if err != nil {
			err = fmt.Errorf("forward: %w", err)
			if errors.Is(err, kgo.ErrClientClosed) {
				c.counts.fail(err)
				log.Print(err)
				return false
			}
			if end, commit := failed(err, true); end {
				return commit
			}
		}
	}
	return true
}

// fromRetry says whether r was read from c's retry topic, not from its input topic.
func (c *consumer) fromRetry(r *kgo.Record) bool {
	return c.spec.Retry != nil && r.Topic == c.spec.Retry.Topic
}

// sendOn sends r, which just failed with err, to the retry topic when the failure
// may pass later (transient), retry is on and tries remain, else to the DLQ. What
// it sends is r as read (key and value, not the transformed value), so a retry
// runs r's whole path again, with failureHeaders. It reports whether r may be
// committed: a failed send is counted and logged and r still commits, unless the
// client was closed (Stop), which leaves r to be redelivered.
func (c *consumer) sendOn(ctx context.Context, r *kgo.Record, err error, transient bool) bool {
	failed := 1 // tries of r that failed, this one included
	if c.fromRetry(r) {
		n, _ := strconv.Atoi(header(r, headerAttempt))
		failed += n
	}
	to, what, sent := c.spec.DLQ, "dlq", &c.dead
	if retry := c.spec.Retry; transient && retry != nil && failed <= retry.Attempts {
		to, what, sent = retry.Topic, "retry", &c.retried
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	serr := c.produce(pctx, &kgo.Record{Topic: to, Key: r.Key, Value: r.Value, Headers: failureHeaders(r, c.spec.Group, failed, err)})
	cancel()
	if serr != nil {
		c.counts.fail(fmt.Errorf("%s: %w", what, serr))
		log.Printf("%s to %s: %v", what, to, serr)
		return !errors.Is(serr, kgo.ErrClientClosed)
	}
	sent.Add(1)
	return true
}

// The headers a consumer sets on a record it sends to its retry topic or its DLQ.
const (
	headerGroup   = "studio-group"   // the consumer's group: a retry loop skips other groups' records
	headerAttempt = "studio-attempt" // tries of the record that failed so far
	headerError   = "studio-error"   // the last failure: its first line, at most errorHeaderMax bytes
	headerOrigin  = "studio-origin"  // where the record was first read: topic[partition]@offset
)

const errorHeaderMax = 1 << 10

// header is r's last value for key, "" without one.
func header(r *kgo.Record, key string) string {
	v := ""
	for _, h := range r.Headers {
		if h.Key == key {
			v = string(h.Value)
		}
	}
	return v
}

// failureHeaders are r's own headers plus the studio-* ones for its failed'th
// failed try, err. studio-origin keeps where r was first read.
func failureHeaders(r *kgo.Record, group string, failed int, err error) []kgo.RecordHeader {
	origin := header(r, headerOrigin)
	if origin == "" {
		origin = fmt.Sprintf("%s[%d]@%d", r.Topic, r.Partition, r.Offset)
	}
	msg, _, _ := strings.Cut(err.Error(), "\n")
	if len(msg) > errorHeaderMax {
		msg = msg[:errorHeaderMax]
	}
	hs := slices.DeleteFunc(slices.Clone(r.Headers), func(h kgo.RecordHeader) bool { return strings.HasPrefix(h.Key, "studio-") })
	return append(hs,
		kgo.RecordHeader{Key: headerGroup, Value: []byte(group)},
		kgo.RecordHeader{Key: headerAttempt, Value: []byte(strconv.Itoa(failed))},
		kgo.RecordHeader{Key: headerError, Value: []byte(msg)},
		kgo.RecordHeader{Key: headerOrigin, Value: []byte(origin)},
	)
}
```

- [ ] **Step 5: Report the counts**

In `runNode`, replace:

```go
	var consuming chan struct{} // closed when a consumer's poll loop has returned; nil for a producer
```

with:

```go
	var consuming chan struct{} // closed when a consumer's poll loop has returned; nil for a producer
	var c *consumer             // nil for a producer
```

Replace:

```go
		if rt != nil {
			route := rt.read()
			s.Route = &route
		}
		reply(w, http.StatusOK, s)
```

with:

```go
		if rt != nil {
			route := rt.read()
			s.Route = &route
		}
		if c != nil {
			s.Retried, s.DLQ = c.retried.Load(), c.dead.Load()
		}
		reply(w, http.StatusOK, s)
```

Replace `		c := &consumer{spec: spec,` with `		c = &consumer{spec: spec,` (the rest of that line stays).

- [ ] **Step 6: Run the tests**

Run: `cd studio && gofmt -w node.go node_test.go && go vet ./... && go test -race ./...`
Expected: PASS. The existing `TestConsumerHandle`, `TestConsumerHandleCommitDecision`, `TestConsumerTransform` and `TestConsumerRouter` pass unchanged: without a DLQ nothing changes.

- [ ] **Step 7: Commit**

```bash
git add studio/node.go studio/node_test.go
git commit -m "studio: with a DLQ a consumer's first failure ends the record's path — a sink or forward failure goes to <input>__retry while tries remain, then to the DLQ, a transform or router failure straight there, as read with studio-* headers; /stats retried and dlq; the tail keeps headers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The retry loop

**Files:**
- Modify: `studio/node.go`
- Test: `studio/node_test.go`

**Interfaces:**
- Consumes: `consumer.handle`, `header`, `headerGroup`, `NodeSpec.Retry` (Tasks 2–3).
- Produces: `(*consumer).retry(ctx, cl *kgo.Client)`, `(*consumer).retryRecord(ctx, r *kgo.Record) bool`, `wait(ctx, d time.Duration) bool`, `groupOpts(group, topic string, reset kgo.Offset) []kgo.Opt`. A consumer with retry runs a second client in group `<group>__retry`. Stop commits both clients' marks in parallel within `commitBudget`, then closes both.

- [ ] **Step 1: Write the failing test**

Append to `studio/node_test.go`:

```go
// The retry loop handles only its own group's records, each once it is due; Stop
// during the wait leaves the record unhandled and unmarked.
func TestConsumerRetryRecord(t *testing.T) {
	ctx := context.Background()
	c := &consumer{
		spec:   NodeSpec{Topic: "orders", Group: "g", Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry", Attempts: 3, DelayMS: 100}, DLQ: "orders__dlq"},
		tail:   &tail{},
		counts: &counters{},
	}
	ours := []kgo.RecordHeader{{Key: "studio-group", Value: []byte("g")}, {Key: "studio-attempt", Value: []byte("1")}}
	retried := func(at time.Time) *kgo.Record {
		return &kgo.Record{Topic: "orders__retry", Value: []byte(`{}`), Headers: ours, Timestamp: at}
	}

	// Another group's record, and one no consumer sent: marked, not handled.
	for _, hs := range [][]kgo.RecordHeader{{{Key: "studio-group", Value: []byte("h")}}, nil} {
		if !c.retryRecord(ctx, &kgo.Record{Topic: "orders__retry", Value: []byte(`{}`), Headers: hs, Timestamp: time.Now()}) {
			t.Fatalf("headers %v: a record that is not ours is marked", hs)
		}
	}
	if c.tail.last() != 0 {
		t.Fatal("a record that is not ours is not handled")
	}

	// Ours, written a second ago: due, handled at once.
	start := time.Now()
	if !c.retryRecord(ctx, retried(start.Add(-time.Second))) || c.tail.last() != 1 || time.Since(start) > 90*time.Millisecond {
		t.Fatalf("a due record is handled at once: tailed %d in %s", c.tail.last(), time.Since(start))
	}

	// Ours, written just now: handled once the 100 ms delay is out.
	start = time.Now()
	if !c.retryRecord(ctx, retried(start)) || c.tail.last() != 2 || time.Since(start) < 90*time.Millisecond {
		t.Fatalf("a record waits out its delay: tailed %d in %s", c.tail.last(), time.Since(start))
	}

	// Stop during the wait: not handled, not marked.
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	if c.retryRecord(stopped, retried(time.Now())) || c.tail.last() != 2 {
		t.Fatal("a wait cut by Stop leaves the record unhandled and unmarked")
	}
	if s := c.counts.read(); s.Total != 0 {
		t.Fatalf("retries are not counted in total: %+v", s)
	}
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `cd studio && go test -run TestConsumerRetryRecord ./...`
Expected: FAIL to compile: `c.retryRecord undefined`.

- [ ] **Step 3: Add the loop**

In `studio/node.go`, replace:

```go
// sinkClient does not follow redirects
```

with:

```go
// retry is the retry loop: it polls c's retry topic in its retry group (cl) until
// ctx ends, takes each record through retryRecord and marks the ones it may
// commit. The first it may not ends the loop: Stop came.
func (c *consumer) retry(ctx context.Context, cl *kgo.Client) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(topic string, partition int32, err error) {
			c.counts.fail(fmt.Errorf("retry fetch: %w", err))
			log.Printf("retry fetch %s[%d]: %v", topic, partition, err)
		})
		for it := fs.RecordIter(); !it.Done(); {
			r := it.Next()
			if !c.retryRecord(ctx, r) {
				return
			}
			cl.MarkCommitRecords(r)
		}
	}
}

// retryRecord takes one record of the retry topic. Another group's (a shared input
// topic) or one no consumer sent is only marked. Ours waits until it is due, the
// retry delay after it was written, then goes through handle. It reports whether
// r may be marked: false when Stop cut the wait (or closed the client), which
// leaves r for the next deploy to retry.
func (c *consumer) retryRecord(ctx context.Context, r *kgo.Record) bool {
	if header(r, headerGroup) != c.spec.Group {
		return true
	}
	due := r.Timestamp.Add(time.Duration(c.spec.Retry.DelayMS) * time.Millisecond)
	if !wait(ctx, time.Until(due)) {
		return false
	}
	return c.handle(ctx, r)
}

// wait waits d (nothing when d ≤ 0) and reports whether it did: false when ctx
// ended first.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// sinkClient does not follow redirects
```

(`r.Timestamp` is when the record entered the retry topic: franz-go stamps a record whose `Timestamp` is zero when it is produced, and `sendOn` leaves it zero.)

- [ ] **Step 4: Start a second client and stop both**

Replace:

```go
// runNode is `studio node`: it runs until SIGTERM (Stop). A consumer then waits
// up to 3 s for the batch in hand to finish its sink and forward, commits the
// records it handled (it marks each one; unhandled ones are redelivered) and
// closes its client, which leaves the group.
```

with:

```go
// runNode is `studio node`: it runs until SIGTERM (Stop). A consumer then waits
// up to 3 s for the batches in hand (its main loop's, and its retry loop's) to
// finish their sink and writes, commits the records it handled in each group (it
// marks each one; unhandled ones are redelivered) and closes its clients, which
// leaves the groups.
```

Replace:

```go
// groupSessionTimeout is how long
```

with:

```go
// groupOpts are a consumer group client's options: topic read in group, from
// reset while the group has no commit, committing only the records marked.
func groupOpts(group, topic string, reset kgo.Offset) []kgo.Opt {
	return []kgo.Opt{kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(reset), kgo.AutoCommitMarks(),
		kgo.SessionTimeout(groupSessionTimeout)}
}

// groupSessionTimeout is how long
```

Replace:

```go
	opts := []kgo.Opt{kgo.SeedBrokers(brokers), kgo.ClientID(spec.ref().name())}
	if spec.Type == "consumer" {
		reset := kgo.NewOffset().AtStart()
		if spec.AutoOffsetReset == "latest" {
			reset = kgo.NewOffset().AtEnd()
		}
		opts = append(opts, kgo.ConsumerGroup(spec.Group), kgo.ConsumeTopics(spec.Topic), kgo.ConsumeResetOffset(reset), kgo.AutoCommitMarks(),
			kgo.SessionTimeout(groupSessionTimeout))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
```

with:

```go
	opts := func(more ...kgo.Opt) []kgo.Opt {
		return append([]kgo.Opt{kgo.SeedBrokers(brokers), kgo.ClientID(spec.ref().name())}, more...)
	}
	var group []kgo.Opt
	if spec.Type == "consumer" {
		reset := kgo.NewOffset().AtStart()
		if spec.AutoOffsetReset == "latest" {
			reset = kgo.NewOffset().AtEnd()
		}
		group = groupOpts(spec.Group, spec.Topic, reset)
	}
	cl, err := kgo.NewClient(opts(group...)...)
	if err != nil {
		log.Fatal(err)
	}
	clients := []*kgo.Client{cl} // a consumer's: committed and closed on stop
	var retryCl *kgo.Client      // a consumer's retry loop's, in its retry group; nil without retry
	if spec.Type == "consumer" && spec.Retry != nil {
		if retryCl, err = kgo.NewClient(opts(groupOpts(spec.Retry.Group, spec.Retry.Topic, kgo.NewOffset().AtStart())...)...); err != nil {
			log.Fatal(err)
		}
		clients = append(clients, retryCl)
	}
```

Replace:

```go
	var consuming chan struct{} // closed when a consumer's poll loop has returned; nil for a producer
```

with:

```go
	var consuming chan struct{} // closed when a consumer's poll loops have returned; nil for a producer
```

Replace:

```go
		consuming = make(chan struct{})
		go func() {
			c.consume(ctx, cl)
			close(consuming)
		}()
```

with:

```go
		consuming = make(chan struct{})
		var loops sync.WaitGroup
		loops.Go(func() { c.consume(ctx, cl) })
		if retryCl != nil {
			loops.Go(func() { c.retry(ctx, retryCl) })
		}
		go func() {
			loops.Wait()
			close(consuming)
		}()
```

Replace:

```go
	if consuming != nil { // let the batch in hand finish its sink and forward
		select {
		case <-consuming:
		case <-time.After(batchWait):
			log.Printf("stop: the batch in hand did not finish in %s; its unhandled records stay uncommitted", batchWait)
		}
		commit, cancel := context.WithTimeout(context.Background(), commitBudget)
		if err := cl.CommitMarkedOffsets(commit); err != nil {
			log.Printf("stop: commit: %v", err)
		}
		cancel()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	defer cancel()
	srv.Shutdown(shutdown)
	cl.Close()
```

with:

```go
	if consuming != nil { // let the batches in hand finish their sink and writes
		select {
		case <-consuming:
		case <-time.After(batchWait):
			log.Printf("stop: the batches in hand did not finish in %s; their unhandled records stay uncommitted", batchWait)
		}
		commit, cancel := context.WithTimeout(context.Background(), commitBudget)
		var commits sync.WaitGroup // each group's commit at once, within one budget
		for _, k := range clients {
			commits.Go(func() {
				if err := k.CommitMarkedOffsets(commit); err != nil {
					log.Printf("stop: commit: %v", err)
				}
			})
		}
		commits.Wait()
		cancel()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	defer cancel()
	srv.Shutdown(shutdown)
	for _, k := range clients {
		k.Close()
	}
```

The stop budgets (`batchWait`, `commitBudget`, `shutdownBudget`) are unchanged, so they stay inside `stopGraceSeconds`.

- [ ] **Step 5: Run the tests**

Run: `cd studio && gofmt -w node.go node_test.go && go vet ./... && go test -race ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add studio/node.go studio/node_test.go
git commit -m "studio: a consumer with retry runs a second client in <group>__retry on <input>__retry — it skips other groups' records, waits until each of its own is due, runs it through handle; Stop commits both groups at once

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The snapshot's retried, dlq and waiting

**Files:**
- Modify: `studio/engine.go`
- Test: `studio/engine_test.go`

**Interfaces:**
- Consumes: `nodeStats.Retried`, `nodeStats.DLQ` (Task 3), `NodeSpec.Retry` (Task 2).
- Produces: `NodeState.Retried int64`, `NodeState.DLQ int64`, `NodeState.Waiting *int64` (JSON `retried`, `dlq`, `waiting`, between `unmatched` and `instances`); `groupLag(parts map[int32]kadm.GroupMemberLag, reset string) *int64`.

- [ ] **Step 1: Write the failing tests**

Append to `studio/engine_test.go`:

```go
// A consumer with retry: waiting is its retry group's lag on its retry topic,
// none until that group has committed, counted from the start for the rest.
func TestApplyKafkaWaiting(t *testing.T) {
	spec := NodeSpec{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry"}}
	main := kadm.DescribedGroupLag{Group: "g", Lag: kadm.GroupLag{"orders": {0: {Topic: "orders", Partition: 0, Commit: kadm.Offset{At: 4}}}}}
	three := int64(3)
	for _, c := range []struct {
		name  string
		retry map[int32]kadm.GroupMemberLag
		want  *int64
	}{
		{"no commit yet", map[int32]kadm.GroupMemberLag{0: {Topic: "orders__retry", Partition: 0, Lag: 2, Commit: kadm.Offset{At: -1}}}, nil},
		{"committed", map[int32]kadm.GroupMemberLag{
			0: {Topic: "orders__retry", Partition: 0, Lag: 2, Commit: kadm.Offset{At: 1}},
			1: {Topic: "orders__retry", Partition: 1, Lag: 1, Commit: kadm.Offset{At: -1}},
		}, &three},
	} {
		st := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
		lags := kadm.DescribedGroupLags{"g": main, "g__retry": {Group: "g__retry", Lag: kadm.GroupLag{"orders__retry": c.retry}}}
		applyKafka(&st, "f", []NodeSpec{spec}, nil, lags, nil)
		got := st.Nodes["consumer-1"]
		if (got.Waiting == nil) != (c.want == nil) || got.Waiting != nil && *got.Waiting != *c.want || got.Lag == nil || *got.Lag != 0 {
			t.Fatalf("%s: want waiting %v and lag 0, got waiting %v, lag %v", c.name, c.want, got.Waiting, got.Lag)
		}
	}
}

// retried and dlq come from /stats and are summed over instances.
func TestRetryCounts(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"boot":"b","total":7,"tailSeq":7,"retried":2,"dlq":1}`)
	}))
	defer ts.Close()
	got := withStats(context.Background(), ts.URL, NodeState{State: "running"}, false)
	if got.Retried != 2 || got.DLQ != 1 {
		t.Fatalf("want retried 2 and dlq 1 from /stats, got %+v", got)
	}
	ns := NodeState{Instances: []NodeState{{Instance: 1, Retried: 2, DLQ: 1}, {Instance: 2, Retried: 1}}}
	if ns.sumInstances(); ns.Retried != 3 || ns.DLQ != 1 {
		t.Fatalf("want the instances' sums, got %+v", ns)
	}
}

// verify-studio greps the snapshot's JSON and relies on its field order: lag
// before assigned, the retry counts after them, instances last.
func TestNodeStateFieldOrder(t *testing.T) {
	zero := int64(0)
	b, err := json.Marshal(NodeState{State: "running", Lag: &zero, Assigned: map[string][]int32{"orders": {0}}, Retried: 1, DLQ: 1, Waiting: &zero,
		Instances: []NodeState{{Instance: 1, State: "running"}}})
	if err != nil {
		t.Fatal(err)
	}
	got, last := string(b), -1
	for _, key := range []string{`"lag"`, `"assigned"`, `"retried"`, `"dlq"`, `"waiting"`, `"instances"`} {
		i := strings.Index(got, key)
		if i <= last {
			t.Fatalf("%s out of order in %s", key, got)
		}
		last = i
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd studio && go test -run 'TestApplyKafkaWaiting|TestRetryCounts|TestNodeStateFieldOrder' ./...`
Expected: FAIL to compile: `unknown field Retried in struct literal of type NodeState`.

- [ ] **Step 3: Add the fields and fill them**

In `studio/engine.go`, replace:

```go
	Unmatched  int64              `json:"unmatched,omitempty"` // routers: records dropped, no rule matched and no default
```

with:

```go
	Unmatched  int64              `json:"unmatched,omitempty"` // routers: records dropped, no rule matched and no default
	Retried    int64              `json:"retried,omitempty"`   // consumers: records sent to the retry topic
	DLQ        int64              `json:"dlq,omitempty"`       // consumers: records sent to the DLQ
	Waiting    *int64             `json:"waiting,omitempty"`   // consumers: records waiting in the retry topic (its retry group's lag)
```

In `Snapshot`, replace:

```go
	for _, s := range specs {
		if s.Type == "consumer" {
			groups = append(groups, s.Group)
		}
	}
```

with:

```go
	for _, s := range specs {
		if s.Type == "consumer" {
			groups = append(groups, s.Group)
		}
		if s.Retry != nil {
			groups = append(groups, s.Retry.Group)
		}
	}
```

In `withStats`, replace:

```go
	ns.TailSeq, ns.Boot, ns.step, ns.route, ns.answered = s.TailSeq, s.Boot, s.Step, s.Route, true
```

with:

```go
	ns.TailSeq, ns.Boot, ns.step, ns.route, ns.answered = s.TailSeq, s.Boot, s.Step, s.Route, true
	ns.Retried, ns.DLQ = s.Retried, s.DLQ
```

In `sumCounts`, replace:

```go
	ns.Total, ns.Errors, ns.Rate, ns.LastError, ns.Branches, ns.Unmatched = 0, 0, 0, "", nil, 0
	for _, in := range cs {
		ns.Total += in.Total
		ns.Errors += in.Errors
		ns.Rate += in.Rate
		ns.Unmatched += in.Unmatched
```

with:

```go
	ns.Total, ns.Errors, ns.Rate, ns.LastError, ns.Branches, ns.Unmatched, ns.Retried, ns.DLQ = 0, 0, 0, "", nil, 0, 0, 0
	for _, in := range cs {
		ns.Total += in.Total
		ns.Errors += in.Errors
		ns.Rate += in.Rate
		ns.Unmatched += in.Unmatched
		ns.Retried += in.Retried
		ns.DLQ += in.DLQ
```

and its comment:

```go
// sumCounts gives ns the sums of cs's counters and rates, and the first of their
// last errors, prefixed with its instance.
```

with:

```go
// sumCounts gives ns the sums of cs's counters and rates (retried and DLQ
// included), and the first of their last errors, prefixed with its instance.
```

- [ ] **Step 4: One lag rule for both groups**

In `applyKafka`'s comment, replace:

```go
// for each consumer node its group's lag on its topic (none until the group has
// committed; then committed partitions from their commit, the others from where
// auto_offset_reset starts: the beginning for earliest, nothing for latest) and
// the partitions whose
```

with:

```go
// for each consumer node its group's lag on its topic (groupLag), its retry
// group's on its retry topic as waiting, and the partitions whose
```

In its body, replace:

```go
		var lag, fromReset int64 // over committed partitions; over the others, by auto_offset_reset
		committed := false
		held := map[*NodeState][]int32{} // container → partitions its client holds
		for _, ml := range gl.Lag[s.Topic] {
			switch {
			case ml.Err != nil:
			case ml.Commit.At >= 0:
				committed = true
				lag += max(ml.Lag, 0)
			case s.AutoOffsetReset != "latest": // no commit: kadm's lag runs from the partition's start
				fromReset += max(ml.Lag, 0)
			}
			if ml.Member != nil {
				if c, ok := client[ml.Member.ClientID]; ok {
					held[c] = append(held[c], ml.Partition)
				}
			}
		}
		if committed { // a group with no commit yet shows no lag at all
			lag += fromReset
			ns.Lag = &lag
		}
```

with:

```go
		ns.Lag = groupLag(gl.Lag[s.Topic], s.AutoOffsetReset)
		if s.Retry != nil {
			if rl, ok := lags[s.Retry.Group]; ok && rl.Error() == nil {
				ns.Waiting = groupLag(rl.Lag[s.Retry.Topic], "earliest") // the retry loop reads from the start
			}
		}
		held := map[*NodeState][]int32{} // container → partitions its client holds
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Member != nil {
				if c, ok := client[ml.Member.ClientID]; ok {
					held[c] = append(held[c], ml.Partition)
				}
			}
		}
```

Replace:

```go
// Running is the set of flows
```

with:

```go
// groupLag is a group's lag over a topic's partitions, from kadm: none until the
// group has committed; then committed partitions count from their commit, the
// others from where reset starts (the beginning for earliest, nothing for
// latest). Partitions with an error are left out.
func groupLag(parts map[int32]kadm.GroupMemberLag, reset string) *int64 {
	var lag, fromReset int64 // over committed partitions; over the others, by reset
	committed := false
	for _, ml := range parts {
		switch {
		case ml.Err != nil:
		case ml.Commit.At >= 0:
			committed = true
			lag += max(ml.Lag, 0)
		case reset != "latest": // no commit: kadm's lag runs from the partition's start
			fromReset += max(ml.Lag, 0)
		}
	}
	if !committed { // a group with no commit yet shows no lag at all
		return nil
	}
	lag += fromReset
	return &lag
}

// Running is the set of flows
```

- [ ] **Step 5: Run the tests**

Run: `cd studio && gofmt -w engine.go engine_test.go && go vet ./... && go test -race ./...`
Expected: PASS, the existing `TestApplyKafka` lag cases included.

- [ ] **Step 6: Commit**

```bash
git add studio/engine.go studio/engine_test.go
git commit -m "studio: the snapshot gives a consumer retried and dlq, summed over instances, and waiting, its retry group's lag on the retry topic; groupLag is the one lag rule for both groups

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The UI — On failure, the counts, the headers

**Files:**
- Create: `studio/ui/src/OnFailure.tsx`
- Modify: `studio/ui/src/Inspector.tsx`
- Modify: `studio/ui/src/flow/api.ts`
- Modify: `studio/ui/src/nodes/StudioNodes.tsx`
- Modify: `studio/ui/src/TailDrawer.tsx`
- Modify: `studio/ui/src/index.css`

**Interfaces:**
- Consumes: `ConsumerData.retry`, `ConsumerData.dlq`, `RetryData` (Task 1); the snapshot's `retried`, `dlq`, `waiting` (Task 5); `/tail`'s `headers` (Task 3).
- Produces: the Inspector's "On failure" fieldset, whose accessible name is `On failure`. It holds the controls labelled `Retry` (checkbox), `Attempts`, `Delay (ms)` and `DLQ` (checkbox), and hints with the derived topic names, or `wire a topic first`. The consumer's runtime line gains `<n> retried`, `<n> waiting` and `<n> dlq`. A tail row gains `name: value` items. `NodeRuntime` gains `retried?`, `dlq?`, `waiting?`; `TailEntry` gains `headers?`.

The UI has no unit tests. This task is checked by type-checking and the build here, and by the UI tests of Task 8.

- [ ] **Step 1: Mirror the snapshot and the tail**

In `studio/ui/src/flow/api.ts`, replace:

```ts
  unmatched?: number // routers: records dropped, no rule matched and no default
```

with:

```ts
  unmatched?: number // routers: records dropped, no rule matched and no default
  retried?: number // consumers: records sent to the retry topic
  dlq?: number // consumers: records sent to the DLQ
  waiting?: number // consumers: records waiting in the retry topic, once its group has committed
```

Replace:

```ts
export type TailEntry = { seq: number; time: string; partition: number; offset: number; key: string; value: string }
```

with:

```ts
export type TailEntry = {
  seq: number
  time: string
  partition: number
  offset: number
  key: string
  value: string
  headers?: Record<string, string> // studio-* on a record a consumer sent on after a failure
}
```

- [ ] **Step 2: Add the On failure group**

Create `studio/ui/src/OnFailure.tsx`:

```tsx
import { useEdges, useNodes, type Edge } from '@xyflow/react'
import type { ConsumerData, StudioNode } from './nodes/types'

type Props = {
  id: string // the consumer's node id
  data: ConsumerData
  set: (patch: Partial<ConsumerData>) => void
}

const DEFAULT_RETRY = { attempts: 3, delay_ms: 5000 }

// inputTopic is the name of the topic a consumer reads, '' while it reads none
// (or one not named yet).
function inputTopic(id: string, nodes: StudioNode[], edges: Edge[]): string {
  const from = edges.find((e) => e.target === id)?.source
  const topic = nodes.find((n) => n.id === from)
  return topic?.type === 'topic' ? topic.data.name : ''
}

// A consumer's failure handling (Go: ConsumerData's Retry and DLQ): retry through
// <input>__retry, then the DLQ <input>__dlq. The topics are named after the input
// topic and created on deploy, which refuses a retry without the DLQ.
export default function OnFailure({ id, data, set }: Props) {
  const input = inputTopic(id, useNodes<StudioNode>(), useEdges())
  const topic = (suffix: string) => <p className="hint">{input ? `${input}${suffix}` : 'wire a topic first'}</p>
  const retry = data.retry ?? null
  return (
    <fieldset className="on-failure">
      <legend>On failure</legend>
      <label htmlFor="inspector-retry">Retry</label>
      <input id="inspector-retry"
        type="checkbox"
        checked={retry !== null}
        onChange={(e) => set({ retry: e.target.checked ? DEFAULT_RETRY : null })}
      />
      {retry && (
        <>
          <label htmlFor="inspector-attempts">Attempts</label>
          <input id="inspector-attempts"
            type="number"
            min={1}
            max={10}
            value={retry.attempts}
            onChange={(e) => set({ retry: { ...retry, attempts: Number(e.target.value) } })}
          />
          <label htmlFor="inspector-delay-ms">Delay (ms)</label>
          <input id="inspector-delay-ms"
            type="number"
            min={100}
            max={60000}
            value={retry.delay_ms}
            onChange={(e) => set({ retry: { ...retry, delay_ms: Number(e.target.value) } })}
          />
        </>
      )}
      {topic('__retry')}
      <label htmlFor="inspector-dlq">DLQ</label>
      <input id="inspector-dlq" type="checkbox" checked={data.dlq ?? false} onChange={(e) => set({ dlq: e.target.checked })} />
      {topic('__dlq')}
      <p className="hint">
        A failed sink or forward is tried again after the delay, up to Attempts times, then goes to the DLQ; a failed
        transform or router goes there at once. Retry needs the DLQ.
      </p>
    </fieldset>
  )
}
```

In `studio/ui/src/Inspector.tsx`, replace:

```tsx
import { DEFAULT_INTERVAL_MS } from './flow/schema'
```

with:

```tsx
import { DEFAULT_INTERVAL_MS } from './flow/schema'
import OnFailure from './OnFailure'
```

and replace:

```tsx
          <p className="hint">Wire it to a topic to forward every record there
```

with:

```tsx
          <OnFailure id={node.id} data={node.data} set={set} />
          <p className="hint">Wire it to a topic to forward every record there
```

In `studio/ui/src/index.css`, replace:

```css
.inspector fieldset.rule { margin: 8px 0; padding: 4px 8px 8px; border: 1px solid #ddd; border-radius: 4px; }
```

with:

```css
.inspector fieldset.rule { margin: 8px 0; padding: 4px 8px 8px; border: 1px solid #ddd; border-radius: 4px; }
.inspector fieldset.on-failure { margin: 12px 0 8px; padding: 4px 8px 8px; border: 1px solid #ddd; border-radius: 4px; }
.inspector input[type='checkbox'] { width: auto; }
```

- [ ] **Step 3: Show the counts and the headers**

In `studio/ui/src/nodes/StudioNodes.tsx`, replace:

```ts
  if (rt.unmatched) parts.push(`${rt.unmatched} unmatched`)
```

with:

```ts
  if (rt.unmatched) parts.push(`${rt.unmatched} unmatched`)
  if (rt.retried) parts.push(`${rt.retried} retried`)
  if (rt.waiting !== undefined) parts.push(`${rt.waiting} waiting`)
  if (rt.dlq) parts.push(`${rt.dlq} dlq`)
```

In `studio/ui/src/TailDrawer.tsx`, replace:

```tsx
              <code>{e.value}</code>
```

with:

```tsx
              <code>{e.value}</code>
              {Object.entries(e.headers ?? {}).map(([k, v]) => (
                <code key={k}>
                  {k}: {v}
                </code>
              ))}
```

- [ ] **Step 4: Type-check and build**

Run: `make test`
Expected: ends with `docker compose config --quiet` and no error. The UI build (`tsc -b . e2e && vite build`) passes.

- [ ] **Step 5: Commit**

```bash
git add studio/ui/src/OnFailure.tsx studio/ui/src/Inspector.tsx studio/ui/src/flow/api.ts studio/ui/src/nodes/StudioNodes.tsx studio/ui/src/TailDrawer.tsx studio/ui/src/index.css
git commit -m "ui: a consumer's On failure group (Retry with attempts and delay, DLQ, the topic names they use); its runtime line shows retried, waiting and dlq; the tail shows a record's headers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: verify-studio checks retry and DLQ end to end

**Files:**
- Modify: `Makefile` (target `verify-studio`)

**Interfaces:**
- Consumes: everything above, through the running stack.
- Produces: a `verify-studio` step that prints `studio retry: "retried":1,"dlq":1`. The exit trap also deletes `studio-verify-retry`, `-retry-out`, `-retry__retry` and `-retry__dlq`.

The flow: `producer-1 → topic-1 (studio-verify-retry)`, read by two consumers. `consumer-1` (group `studio-verify-q1`) has an http sink to `http://studio:8082/api/flows/00000000/nodes/producer-1/send`, which answers 404 because no such flow exists, plus `retry {attempts: 1, delay_ms: 1000}` and `dlq: true`. `consumer-2` (group `studio-verify-q2`) has a log sink, a transform `{qty: msg.qty * 2}` that fails on `{}`, a forward topic `studio-verify-retry-out` and `dlq: true`. A drawn topic `studio-verify-retry__dlq` feeds `consumer-3` (log). One record `{}` should reach the DLQ twice: from q1 with `studio-attempt` 2 after one retry, and from q2 with `studio-attempt` 1 at once.

- [ ] **Step 1: Write the check**

In `Makefile`, make these replacements. Each "replace" names text that occurs once. The recipe lines start with a real tab.

Replace the target's help text:

```
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, rewind, live ticks, chained flows, instances, transforms, routers, stop, delete; removes its flows and topics
```

with:

```
verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, rewind, live ticks, chained flows, instances, transforms, routers, retry and DLQ, stop, delete; removes its flows and topics
```

Replace:

```
[ "$$code" = 422 ] || { echo "STUDIO FAILED: $$3 deployed with an expression that does not compile ($$code)"; return 1; }
```

with:

```
[ "$$code" = 422 ] || { echo "STUDIO FAILED: $$3 deployed though it must be refused ($$code)"; return 1; }
```

Replace:

```
--topic "studio-verify|studio-verify-(timer|a|a-out|b|instances|t-in|t-out|r-in|r-big|r-other)"
```

with:

```
--topic "studio-verify|studio-verify-(timer|a|a-out|b|instances|t-in|t-out|r-in|r-big|r-other|retry|retry-out|retry__retry|retry__dlq)"
```

Replace `refuse_then_deploy "$$x" "$$broken" "the transform flow"` with `refuse_then_deploy "$$x" "$$broken" "the transform flow with an expression that does not compile"`, and `refuse_then_deploy "$$r" "$$broken" "the router flow"` with `refuse_then_deploy "$$r" "$$broken" "the router flow with a rule that does not compile"`.

Replace these two lines (tab-indented):

```
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$rid || { echo "STUDIO FAILED: delete the router flow"; exit 1; }; \
	echo "STUDIO OK ($$id)"
```

with:

```
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$rid || { echo "STUDIO FAILED: delete the router flow"; exit 1; }; \
	q=$$(flow3 verify-retry "$$manual" studio-verify-retry 1 '{"group":"studio-verify-q1","auto_offset_reset":"earliest","sink":{"kind":"http","url":"http://studio:8082/api/flows/00000000/nodes/producer-1/send"},"retry":{"attempts":1,"delay_ms":1000},"dlq":true}' \
		',{"id":"consumer-2","type":"consumer","position":{"x":400,"y":150},"data":{"group":"studio-verify-q2","auto_offset_reset":"earliest","sink":{"kind":"log"},"dlq":true}},{"id":"transform-1","type":"transform","position":{"x":600,"y":150},"data":{"expr":"{qty: msg.qty * 2}"}},{"id":"topic-2","type":"topic","position":{"x":800,"y":150},"data":{"name":"studio-verify-retry-out","partitions":1,"replication_factor":1}},{"id":"topic-3","type":"topic","position":{"x":600,"y":-150},"data":{"name":"studio-verify-retry__dlq","partitions":1,"replication_factor":1}},{"id":"consumer-3","type":"consumer","position":{"x":800,"y":-150},"data":{"group":"studio-verify-q3","auto_offset_reset":"earliest","sink":{"kind":"log"}}}' \
		',{"id":"e3","source":"topic-1","target":"consumer-2"},{"id":"e4","source":"consumer-2","target":"transform-1"},{"id":"e5","source":"transform-1","target":"topic-2"},{"id":"e6","source":"topic-3","target":"consumer-3"}'); \
	broken=$$(echo "$$q" | sed 's/,"dlq":true}/}/'); \
	refuse_then_deploy "$$q" "$$broken" "the retry flow without a DLQ" || exit 1; qid=$$fid; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$qid/nodes/producer-1/send --data '{}' >/dev/null || { echo "STUDIO FAILED: send to the retry flow"; exit 1; }; \
	for i in $$(seq 45); do \
		dlq=$$(curl -sS "$(STUDIO_URL)/api/flows/$$qid/nodes/consumer-3/tail?since=0"); \
		echo "$$dlq" | grep -q '"studio-attempt":"2","studio-error":"sink: [^"]*","studio-group":"studio-verify-q1"' && \
			echo "$$dlq" | grep -q '"studio-attempt":"1","studio-error":"transform: [^}]*"studio-group":"studio-verify-q2"' && \
			curl -sS $(STUDIO_URL)/api/flows/$$qid/state | grep -q '"consumer-1":{"state":"running",.*"retried":1,"dlq":1' && break; \
		[ "$$i" = 45 ] && { echo "STUDIO FAILED: want the record retried once then dead-lettered by studio-verify-q1, and dead-lettered at once by studio-verify-q2; DLQ tail $$dlq; state $$(curl -sS $(STUDIO_URL)/api/flows/$$qid/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio retry: $$(curl -sS $(STUDIO_URL)/api/flows/$$qid/state | grep -o '"retried":[0-9]*,"dlq":[0-9]*')"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$qid || { echo "STUDIO FAILED: delete the retry flow"; exit 1; }; \
	echo "STUDIO OK ($$id)"
```

How the greps work:
- `sed 's/,"dlq":true}/}/'` drops the first `,"dlq":true`, which is consumer-1's, comma included, so the JSON stays valid. That leaves a retry without a DLQ, which deploy must refuse.
- The tail's headers are a JSON object with sorted keys (`studio-attempt`, `studio-error`, `studio-group`, `studio-origin`). The sink's error text has no `"`. The transform's has no `}`.
- Only consumer-1 has `retried`, so `.*"retried":1,"dlq":1` can only match its counts.

- [ ] **Step 2: Check it parses, then run it**

Run: `make -n verify-studio >/dev/null && make down && make verify-studio`
Expected: the dry run prints nothing. The run ends with `studio retry: "retried":1,"dlq":1` and `STUDIO OK (<id>)`.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "make: verify-studio checks retry and DLQ — a retry without a DLQ refused, a failing sink retried once then dead-lettered, a failing transform dead-lettered at once, both seen with their studio-* headers on a drawn DLQ topic

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: UI tests for retry and DLQ

**Files:**
- Modify: `studio/ui/e2e/nodes.spec.ts`

**Interfaces:**
- Consumes: the On failure group, the runtime line and the tail headers (Task 6). Also `field`, `manual`, `consumer`, `chain`, `simple` from `./studio`.
- Produces: tests 15 and 16 of the UI suite.

- [ ] **Step 1: Write the tests**

In `studio/ui/e2e/nodes.spec.ts`, replace the import line:

```ts
import { chain, consumer, deployFlow, edge, edgeOf, expect, node, nodeOf, runtimeOf, simple, tail, test, timer, topBar, topic } from './studio'
```

with:

```ts
import { chain, consumer, deployFlow, edge, edgeOf, expect, field, manual, node, nodeOf, runtimeOf, simple, tail, test, timer, topBar, topic } from './studio'
```

Append to the file:

```ts
// An http sink every record fails at: the studio answers 404 for a flow that does not exist.
const failingSink = { kind: 'http', url: 'http://studio:8082/api/flows/00000000/nodes/producer-1/send' }

test("a consumer's On failure group names its topics, and a retry needs the DLQ", async ({ page, studio }) => {
  const name = studio.unique('retry-names')
  await studio.create(simple(name))
  await studio.open(page, name)
  await nodeOf(page, 'consumer-1').click()
  const onFailure = page.locator('.inspector').getByRole('group', { name: 'On failure' })
  await expect(onFailure).toContainText(`${name}__retry`)
  await expect(onFailure).toContainText(`${name}__dlq`)

  await field(page, 'Retry').check()
  await expect(field(page, 'Attempts')).toHaveValue('3')
  await expect(field(page, 'Delay (ms)')).toHaveValue('5000')
  await page.getByRole('button', { name: 'Save' }).click()
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: consumer-1: retry needs a DLQ: records go there once their attempts run out')
})

test('a consumer retries a failing sink, then sends the record to its DLQ', async ({ page, studio }) => {
  const name = studio.unique('retry')
  const retrying = consumer(name, { sink: failingSink, retry: { attempts: 1, delay_ms: 1000 }, dlq: true })
  await deployFlow(await studio.create(chain(name, manual, topic(name), retrying)))
  await studio.open(page, name)

  await nodeOf(page, 'producer-1').click()
  await tail(page).getByRole('button', { name: 'Send' }).click()
  await expect(tail(page)).toContainText('at offset 0')
  const line = runtimeOf(page, 'consumer-1')
  await expect(line).toContainText(/(?<!\d)1 retried/, { timeout: 30_000 })
  await expect(line).toContainText(/(?<!\d)1 dlq/, { timeout: 15_000 })
  await expect(line).toContainText(/(?<!\d)0 waiting/, { timeout: 15_000 }) // the retry group committed past it

  await nodeOf(page, 'consumer-1').click()
  await expect(tail(page)).toContainText('studio-attempt: 1') // the try from the retry topic
})
```

Every name these tests make, including `…__retry`, `…__dlq` and the group `…__retry`, starts with `studio-ui-`. The run's cleanup removes them all.

- [ ] **Step 2: Type-check, then run the suite**

Run: `make test && make verify-ui`
Expected: `make test` passes; `make verify-ui` ends with `16 passed` and `UI OK`.

- [ ] **Step 3: Commit**

```bash
git add studio/ui/e2e/nodes.spec.ts
git commit -m "ui tests: a consumer's On failure group names its topics and Deploy wants a DLQ for a retry; a failing sink shows 1 retried, 1 dlq, 0 waiting and the retried record's studio-attempt in the tail

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Docs, and the full check

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`
- Modify: `docs/superpowers/specs/2026-10-08-studio-retry-dlq-design.md`
- Modify: `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`

**Interfaces:**
- Consumes: the behaviour of Tasks 1–8.
- Produces: docs in sync with that behaviour.

- [ ] **Step 1: README**

In `README.md`, in the **Consumer** bullet under `### Nodes`, replace:

```
Sink and forward are independent; a failure is counted and logged, not retried, and the record still commits, so a failed forward is lost (at-most-once).
```

with:

```
Sink and forward are independent; a failure is counted and logged, not retried, and the record still commits, so a failed forward is lost (at-most-once), unless the consumer has a DLQ (below).
```

Insert a new bullet right after the **Router** bullet:

```
- **Retry and DLQ** (a consumer's "On failure" in the Inspector): with **DLQ** on, a record's first failure ends its path. A failure that may pass later (the http sink, the forward) goes to `<input>__retry` when **Retry** is on and tries remain, else to `<input>__dlq`; a transform or router failure goes straight to `<input>__dlq`. `<input>` is the topic the consumer reads: `orders-1` gives `orders-1__retry` and `orders-1__dlq`, created on deploy with its partitions. What is sent is the record as read (key and value), with headers `studio-group`, `studio-attempt` (failed tries so far), `studio-error` and `studio-origin` (`topic[partition]@offset`). A second client in the same container, in group `<group>__retry`, reads the retry topic, waits until each record is `delay_ms` (100–60000) old and runs it through the whole path again, up to `attempts` (1–10) retries after the first try; another group's records there are skipped. A retry runs the sink again even if it succeeded before: at-least-once. Deploy refuses a retry without the DLQ. The consumer's line shows `retried`, `waiting` (records still in the retry topic) and `dlq`. To watch the DLQ, draw a topic named `<input>__dlq` and wire a log consumer to it: its tail shows each record's headers.
```

- [ ] **Step 2: AGENTS.md**

In `AGENTS.md`, insert after the line that starts `- A transform has no container:`:

```
- Retry and DLQ are consumer settings, not nodes: `ConsumerData.Retry`/`DLQ` (`flow.go`) become `NodeSpec.Retry` (`RetrySpec`) and `DLQ` in `Resolve`, with derived names `<input>__retry`, `<input>__dlq` and group `<group>__retry` (`retryTopic`, `dlqTopic`, `retryGroup`). The retry loop is a second client in the consumer's container (`consumer.retry`); `handle` serves both loops one record at a time (`consumer.mu`), and the `studio-group` header keeps a shared retry topic's groups apart. Its counts arrive in `/stats` `retried`/`dlq`; `waiting` is the retry group's lag (`groupLag`).
```

- [ ] **Step 3: The Studio spec**

In `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`:

In §3.4, replace:

```
  so that record is lost to the next topic (at-most-once for forwards), except
  when it failed because the client was closing, which leaves it unmarked and
  redelivered.
```

with:

```
  so that record is lost to the next topic (at-most-once for forwards), except
  when it failed because the client was closing, which leaves it unmarked and
  redelivered. A consumer with a DLQ (Retry and DLQ, §7) instead ends a record's
  path at its first failure and sends it to its retry topic or its DLQ; a second
  client in the same container retries it.
```

In §3.6, after the sentence that ends `plus \`branches\` (records per rule, then the default's) and \`unmatched\`, summed over instances.`, add:

```
 A consumer with retry or a DLQ also carries `retried` and `dlq`, summed over instances, and `waiting`, its retry group's lag on its retry topic (none until that group commits).
```

In the §4.2 table, insert after the `| | \`instances\` | M4 | …` row:

```
| | `retry` | Retry/DLQ | absent or `null`, or `{attempts, delay_ms}`: attempts 1–10 (retries after the first try), delay_ms 100–60000; needs `dlq` |
| | `dlq` | Retry/DLQ | `true` sends failures to `<input>__dlq`; the input topic's name must leave room for `__retry` (≤ 242 chars), the group for `__retry` (≤ 248) |
```

In §7, insert before `### Not planned`:

```
### Retry and DLQ.

Designed in `docs/superpowers/specs/2026-10-08-studio-retry-dlq-design.md`.

- Consumer settings `retry: {attempts, delay_ms}` and `dlq: true`; topics `<input>__retry` and `<input>__dlq`, created on deploy; a retry group `<group>__retry`.
- With a DLQ, a record's first failure ends its path: a sink or forward failure goes to the retry topic while tries remain, then to the DLQ; a transform or router failure straight to the DLQ; as read, with `studio-*` headers.
- A second client in the consumer's container reads the retry topic, waits until each record is due, and runs it through the same path; another group's records are skipped.
- **Demo:** a consumer with a failing http sink, `retry {attempts: 1, delay_ms: 1000}` and a DLQ: one record shows `1 retried`, then `1 dlq` and `0 waiting`; a log consumer on `<input>__dlq` shows its headers.
- Built as decided in its plan: the retry counts join the consumer's runtime line; headers show in the tail; `handle` takes one record at a time, as the main and retry loops share the transform's and router's VMs; `verify-studio` reads one record through two consumers (a failing sink with retry, a failing transform) into one shared DLQ.
```

- [ ] **Step 4: The retry spec**

In `docs/superpowers/specs/2026-10-08-studio-retry-dlq-design.md`:

In §2, replace the bullet:

```
- `studio/flow.go` (`ConsumerData`), `studio/ui/src/flow/schema.ts` (the
  consumer's `defaultData`: no retry, `dlq: false`) and
  `studio/ui/src/nodes/types.ts` change together (AGENTS.md).
```

with:

```
- `studio/flow.go` (`ConsumerData`) and `studio/ui/src/nodes/types.ts` change
  together (AGENTS.md). `defaultData` in `studio/ui/src/flow/schema.ts` gets no
  `dlq`: a filled-in default would make every existing flow look unsaved, so
  `dlq` is optional in the UI (absent: false).
```

Replace the status line `Status: approved in brainstorming on 2026-10-08. The Studio spec` with `Status: approved in brainstorming on 2026-10-08; built from \`docs/superpowers/plans/2026-10-08-studio-retry-dlq.md\`. The Studio spec`.

In §6, replace:

```
- **Consumer node:** while running, with retry or DLQ on, a second runtime line
  `4 retried · 2 waiting · 1 dlq`, each part shown once its container has
  answered (`waiting` once the retry group has committed).
- **Tail drawer:** a record's headers, when it has any, as `name: value` lines
  under its value.
```

with:

```
- **Consumer node:** while running, its runtime line gains `4 retried`,
  `2 waiting` and `1 dlq`; `retried` and `dlq` once nonzero, `waiting` once the
  retry group has committed.
- **Tail drawer:** a record's headers, when it has any, as `name: value` items
  after its value.
```

In §7, replace the **`verify-studio`** bullet with:

```
- **`verify-studio`:** a flow producer (manual) → topic `studio-verify-retry`,
  read by consumer-1 (http sink to a studio URL that answers 404,
  `retry {attempts: 1, delay_ms: 1000}`, `dlq: true`) and by consumer-2 (log
  sink, transform `{qty: msg.qty * 2}` → `studio-verify-retry-out`, `dlq: true`),
  and a drawn topic `studio-verify-retry__dlq` → a log consumer. One record `{}`:
  consumer-1's sink fails, it is retried once and fails again, then goes to the
  DLQ with `studio-attempt` 2; consumer-2's transform fails and it goes there at
  once with `studio-attempt` 1. Poll until the DLQ consumer's tail shows both and
  consumer-1 shows `"retried":1,"dlq":1`. The same flow without consumer-1's DLQ
  is refused first. (One consumer cannot show both: its sink runs, and fails,
  before its transform.) The exit trap deletes `studio-verify-retry`, `…-out`,
  `…__retry` and `…__dlq`.
```

In §4.3, after the paragraph that ends `leaves it uncommitted to be redelivered.`, add:

```
A consumer's main loop and its retry loop share one `handle`, which takes one
record at a time: the transform's and the router's VMs are not safe for
concurrent use.
```

- [ ] **Step 5: The UI tests spec**

In `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`, replace `Fourteen tests (the last three added since, below)` with `Sixteen tests (the last five added since, below)`. Then append to the `### Added since` list:

```
15. **Retry and DLQ settings** (`nodes.spec.ts`). The consumer's "On failure"
    group shows `<topic>__retry` and `<topic>__dlq`; ticking Retry shows
    Attempts 3 and Delay (ms) 5000; Save and Deploy: the top bar shows
    `422: consumer-1: retry needs a DLQ: records go there once their attempts run out`.
16. **Retry, then DLQ** (`nodes.spec.ts`). A consumer whose http sink always
    fails, with `retry {attempts: 1, delay_ms: 1000}` and a DLQ: one send shows
    `1 retried`, then `1 dlq` and `0 waiting` on its runtime line, and its tail
    shows `studio-attempt: 1` on the retried record.
```

- [ ] **Step 6: The full check**

Run:

```sh
make test
make down && make verify
make down
docker ps -aq -f label=studio.flow
git status --short flows
```

Expected:
- `make test` passes.
- `make verify` ends with `STUDIO OK`, `16 passed`, `UI OK` and `VERIFY OK`.
- The last two commands print nothing.

- [ ] **Step 7: Commit**

```bash
git add README.md AGENTS.md docs/superpowers/specs/2026-10-06-pipeline-studio-design.md docs/superpowers/specs/2026-10-08-studio-retry-dlq-design.md docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md
git commit -m "docs: retry and DLQ — README, AGENTS, the Studio spec (record path, snapshot, consumer data, milestone), the retry and UI-tests specs record what was built

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
