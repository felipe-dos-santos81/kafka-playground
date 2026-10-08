# Pipeline Studio retry and DLQ topics — design

Status: approved in brainstorming on 2026-10-08; built from `docs/superpowers/plans/2026-10-08-studio-retry-dlq.md`. The Studio spec
(`docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`) stays the
authority for everything this does not change; this document adds failure
handling to its consumer and, once built, its sections are updated to match
(§9 below).

## 1. Goal

Teach the non-blocking retry pattern. A consumer's failed record is not lost:
a failure that may pass later goes to a retry topic and is tried again after a
delay, up to a number of attempts, then to a dead-letter topic (DLQ); a failure
that never will goes straight to the DLQ. Retries wait on their own topic, so
the main topic keeps flowing.

```
orders-1 ─▶ consumer-1 (http sink down)
               │ fails ──▶ orders-1__retry ──(after delay_ms)──▶ consumer-1, again
               │                                    │ attempts used up
               └─ transform error ─────────────────┴──▶ orders-1__dlq
```

Agreed in brainstorming:

- **Retry, then DLQ** (not tiered retry topics, not a DLQ with manual replay).
- **Approach A:** the consumer's own container runs the retry loop as a second
  client; no retry consumer is drawn on the canvas, and nothing is re-produced
  to the main topic (every group on it would see the record again).
- **Settings on the consumer, topics by derived name:** the consumer says
  `retry: {attempts, delay_ms}` and `dlq: true`; the topics are
  `<input>__retry` and `<input>__dlq`, never drawn, never named by hand.

Out of scope: tiered retry topics, backoff that grows per attempt, replaying the
DLQ, retry or DLQ for producers, per-error-kind settings.

## 2. Data model

Consumer data gains two optional fields:

```json
{ "id": "consumer-1", "type": "consumer", "position": { "x": 0, "y": 0 },
  "data": { "group": "orders-studio", "auto_offset_reset": "earliest",
            "sink": { "kind": "http", "url": "http://example:8080/hook" },
            "retry": { "attempts": 3, "delay_ms": 5000 },
            "dlq": true } }
```

- `retry`: absent or `null` for none. `attempts` is the number of retries after
  the first try (so `3` tries a record at most four times); `delay_ms` is how
  long a record waits in the retry topic before its next try.
- `dlq`: `true` sends failures to the DLQ; absent or `false` for none.
- Topic names derive from the consumer's input topic (the topic node its edge
  comes from): `orders-1` gives `orders-1__retry` and `orders-1__dlq`. They are
  not editable.
- Edges and `allowedEdges` do not change: the retry and DLQ topics are not nodes.
- `studio/flow.go` (`ConsumerData`) and `studio/ui/src/nodes/types.ts` change
  together (AGENTS.md). `defaultData` in `studio/ui/src/flow/schema.ts` gets no
  `dlq`: a filled-in default would make every existing flow look unsaved, so
  `dlq` is optional in the UI (absent: false).

## 3. Validation (`studio/flow.go`)

- **Save:** unchanged; a consumer's data is checked on deploy.
- **Deploy**, each a 422 naming the consumer:
  - `retry needs a DLQ: records go there once their attempts run out` (retry set,
    `dlq` not true);
  - `retry attempts must be between 1 and 10`;
  - `retry delay_ms must be between 100 and 60000`;
  - with retry or DLQ, the input topic's name may be at most 242 characters, so
    `<input>__retry` stays within Kafka's 249
    (`topic name %q is too long for its __retry and __dlq topics`);
  - with retry, the group may be at most 248 characters, so `<group>__retry`
    stays within 255 (`group is too long for its __retry group`).
- A topic node the user draws with a derived name (`orders-1__dlq`, to wire a
  log consumer to the DLQ) is allowed: it is the same topic. The topic-name
  uniqueness check stays among topic nodes; derived names are not in it.
- The forward-loop check does not change: it follows edges, and retry and DLQ
  writes are not edges.

## 4. Resolve and the node runner

### 4.1 Resolve

The consumer's `NodeSpec` gains:

```go
Retry *RetrySpec `json:"retry,omitempty"` // consumer: where and how its failures are retried
DLQ   string     `json:"dlq,omitempty"`   // consumer: the topic its spent or hopeless records go to

type RetrySpec struct {
	Topic    string `json:"topic"`
	Group    string `json:"group"`
	Attempts int    `json:"attempts"`
	DelayMS  int    `json:"delay_ms"`
}
```

`Resolve` adds `<input>__retry` and `<input>__dlq` to the topics a deploy
creates, with the input topic node's partitions and replication factor 1. A
topic already in the list by that name (a drawn node) is not added twice; the
drawn node's settings win. An existing topic is used as it is, as for any topic
(risk 7 in the Studio spec).

### 4.2 Which failure goes where

Per record in the consumer (`node.go`; the failure path and the retry loop are
in `retry.go`), the path stays tail → http sink →
transform → router → forward. With `DLQ` set, the first failure ends the
record's path (a failed sink no longer goes on to the forward, so a retry never
forwards twice) and sends it on:

| Failure | Kind | Goes to |
|---|---|---|
| http sink | may pass later | retry topic while tries remain, else DLQ |
| forward | may pass later | retry topic while tries remain, else DLQ |
| transform (not JSON, runtime error, unencodable result) | never will | DLQ |
| router (not JSON, failing condition) | never will | DLQ |
| transform returns `nil`, router unmatched | not a failure | dropped, as today |

"While tries remain": the record has failed `n` times counting this one; it goes
to the retry topic if retry is set and `n ≤ attempts`, else to the DLQ.

Without `DLQ`, nothing changes: a failure is counted and logged, the record goes
on where it can, and it commits (at-most-once).

### 4.3 What is written

The record as the consumer read it from its input topic: its original key and
value (not the transformed value), so a retry runs the whole path again. Its
headers are the original ones with these four set (replacing any earlier value
of these four; other headers, `studio-` ones included, are kept):

| Header | Value |
|---|---|
| `studio-group` | the consumer's group |
| `studio-attempt` | tries that failed so far, counting this one (`1`, `2`, …) |
| `studio-error` | the failure's first line, cut at 1 KiB without splitting a character (`sink: … answered 503 Service Unavailable`; a forward's names its topic: `forward to orders-archive: …`) |
| `studio-origin` | where the record was first read: `orders-1[2]@57` |

`studio-origin` is set once, on the first failure, and kept on retries.

A retry runs the http sink again even if it succeeded the first time: delivery
is at-least-once with retry on.

A write to the retry topic or the DLQ that fails counts as an error
(`retry: …` or `dlq: …`) and is logged, as a failed forward is today; the record
still commits, unless the write failed because Stop closed the client, which
leaves it uncommitted to be redelivered.

A consumer's main loop and its retry loop call `handle` at once. The transform
and the router each lock their own VM, which is not safe for concurrent use, so
a slow sink in one loop never holds up the other.

### 4.4 The retry loop

A consumer with retry runs a second franz-go client in the same container:
group `<group>__retry`, topic `<input>__retry`, reading from the start
(`AtStart`), with the main client's session timeout, `AutoCommitMarks`, and the
container name as client id. Per record:

1. `studio-group` is not this consumer's group: mark it committed and skip it
   (another group's retry on a shared input topic, or a record someone else
   wrote). Not counted.
2. Wait until the record's timestamp plus `delay_ms`, but never longer than
   `delay_ms` (a record dated in the future, written by hand). Stop ends the
   wait; the record is left unmarked, so the next deploy retries it.
3. Run it through the same `handle` as the main loop, its prior failure count
   read from `studio-attempt`; a failure goes on per §4.2.

Only the retry loop waits; the main loop never does. With `instances: N`, every
instance runs a retry client in the shared retry group, so the retry topic's
partitions spread over instances like the main topic's.

Records in the retry topic are read only while retry is on and the group keeps
its name. Turning retry off, or renaming the group, leaves them where they are
(the README says so); nothing moves them to the DLQ.

### 4.5 Stop

Both loops get the same `batchWait` to finish the batch in hand, both clients
commit their marked offsets in parallel within `commitBudget`, then both close.
The stop budgets in `node.go` stay inside `stopGraceSeconds`.

## 5. Counts and the snapshot

- `/stats` gains `retried` (records written to the retry topic) and `dlq`
  (records written to the DLQ), each reported (from 0) only by a consumer that
  runs with retry, or with a DLQ. `total` still counts only
  the main loop's records, so msg/s keeps its meaning; `errors` counts every
  failure, retries' included, and the retry loop's fetch errors (`retry fetch: …`);
  a retried record is pushed to the tail like any other (its partition and
  offset are the retry topic's).
- `NodeState` (`engine.go`) and `NodeRuntime` (`api.ts`) gain, together, as the
  last fields before `instances` (after `unmatched`), so `verify-studio`'s
  field-order greps keep working:
  - `retried` and `dlq` (`*int64`, left out when no container reports them),
    summed over instances;
  - `waiting` (`*int64`): the lag of `<group>__retry` on `<input>__retry`, the
    records waiting for their retry. Its group is added to the snapshot's one
    `adm.Lag` call. The lag arithmetic in `applyKafka` becomes a function of
    (group, topic, reset) used for both, so `waiting` follows `lag`'s rules: none
    until the group's first commit; partitions without a commit count from the
    start.
- `tailEntry` gains `headers` (`map[string]string`, left out when empty), so a
  record's `studio-*` headers reach the tail drawer.

## 6. UI

- **Inspector (consumer):** an "On failure" group after the sink:
  - a **Retry** checkbox; ticked, it shows **Attempts** (number, default 3) and
    **Delay (ms)** (number, default 5000); unticked, `retry` is removed;
  - a **DLQ** checkbox;
  - under each, the topic it will use (`orders-1__retry`, `orders-1__dlq`), or
    "wire a topic first" while the consumer has no named input topic.
  The two checkboxes are not coupled in the UI: deploy's refusal (§3) is shown
  in the top bar like any other. Every control has a label tied to it.
- **Consumer node:** while running, its runtime line gains `4 retried`,
  `2 waiting` and `1 dlq`: `retried` and `dlq` from 0 once its container answers,
  when it was deployed with Retry or with a DLQ (what runs, not the edited
  canvas); `waiting` once the retry group has committed.
- **Tail drawer:** a record's headers, when it has any, as `name: value` items
  after its value.

## 7. Tests

- **Go:**
  - `flow_test`: one case per §3 rule (retry without DLQ, attempts 0 and 11,
    delay_ms 99 and 60001, a 243-character input topic, a 249-character group
    with retry) and a valid consumer with both;
  - `resolve_test`: the consumer's spec carries `Retry` (topic, group, attempts,
    delay) and `DLQ`; both topics are created with the input topic's partitions;
    a drawn `<input>__dlq` node is not created twice;
  - `node_test`, through `handle` with fake `post` and `produce`: a sink failure
    goes to the retry topic with `studio-attempt: 1` and the other headers and
    does not reach the forward; the failure past `attempts` goes to the DLQ; a
    transform error and a router error go straight to the DLQ; without `DLQ`,
    today's behaviour; the retry loop marks and skips another group's record; a
    wait cut by Stop leaves the record unmarked;
  - `engine_test`: `waiting` from the retry group's lag (none before its first
    commit); `retried` and `dlq` summed over instances; `instances` still last in
    the JSON.
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
- **UI suite** (`studio/ui/e2e/nodes.spec.ts`): the Inspector's "On failure"
  group shows the derived names; Retry without DLQ is refused on deploy with the
  §3 message in the top bar; a deployed retry flow shows `retried`, `waiting`
  and `dlq` on the consumer node.

## 8. Errors at a glance

| Condition | Result |
|---|---|
| retry without DLQ, attempts or delay out of range, a name too long | 422 on deploy, naming the consumer |
| sink or forward fails, tries remain | to `<input>__retry`; consumer error; commits |
| sink or forward fails, attempts used up (or no retry) | to `<input>__dlq`; consumer error; commits |
| transform or router fails | to `<input>__dlq`; error on that node and the consumer; commits |
| write to retry or DLQ fails | consumer error (`retry: …`, `dlq: …`); commits, unless Stop closed the client |
| retry record of another group | marked and skipped, not counted |
| any failure without DLQ | as today: counted, logged, commits |

## 9. Documentation

- `README.md`: under the consumer, what retry and DLQ do, the derived topic
  names, the `studio-*` headers, at-least-once with retry (a sink may get the
  same record twice), and how to watch the DLQ (draw a topic named
  `<input>__dlq` and wire a log consumer to it).
- The Studio spec: §3.4 (the per-record path's failure branch), §3.6 (the new
  snapshot fields), §4.2 (consumer data), §4.3 (the new deploy checks), and a §7
  "Retry and DLQ" entry pointing here and recording what was built as decided.
- `AGENTS.md`: retry and DLQ topics take derived names (`<input>__retry`,
  `<input>__dlq`), run in the consumer's container as a second client in group
  `<group>__retry`, and the `studio-group` header keeps a shared retry topic's
  groups apart; `RetrySpec`/`NodeSpec` change with `ConsumerData`.
- The UI tests spec (`2026-10-07-studio-ui-tests-design.md`) §5: the new tests
  and the quoted refusal.

## 10. Assumptions

1. franz-go records carry headers (`kgo.RecordHeader`) both ways, and a second
   `kgo.Client` in one process, in another group, runs independently of the
   first. The plan confirms both through Context7 (`/twmb/franz-go`).
2. `msg.qty * 2` with `qty` absent is a runtime error in expr-lang (`nil * 2`),
   not `0`; the plan confirms it with a probe, else `verify-studio` uses another
   failing expression.
3. A studio URL that answers 404 to a POST exists without new code (an unknown
   flow's `…/send`); the plan picks it.
4. A franz-go producer stamps each record with the time it is produced, so a
   retry record's timestamp is when it entered the retry topic.
