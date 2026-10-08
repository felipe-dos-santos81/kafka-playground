# Topic containers M2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The retry container (`orders-1__retry`) gains the redelivery worker. It sends parked records back to `orders-1` once their backoff has passed. Once their attempts are used up, or when a header is bad, it moves them to `orders-1__dlq` instead. The DLQ container gains the age of its oldest record. `make verify-topics` proves both and still ends in `TOPICS OK`.

**Architecture:**
- **`redeliver.go`** (new) holds the worker.
  - `decide` is a pure function: a record, the time and the settings give one verdict (skip, wait, redeliver or dead-letter), following spec §3.5's rules in order.
  - `onward` builds the headers a record carries onward.
  - The `worker` keeps one queue per partition. `drain` handles the queued records in offset order and pauses a partition while its first record waits. The kgo calls are function fields, so `drain` is unit-tested without a broker.
- **`metrics.go`** gains `oldestTimes`: a per-partition cache of the oldest record's time. It is refetched with `ListOffsetsAfterMilli(0)` only when a start offset moves, and only the DLQ role uses it.
- **`main.go`** starts the worker for `ROLE=retry` and now waits for it, the reconcile loop and the HTTP server before it exits. That is the shutdown drain M1 deferred.

**Tech Stack:** Go 1.27.1, franz-go `kgo` v1.22.1 and `kadm` v1.19.0, `prometheus/client_golang` v1.24.1, Docker Compose, GNU make 3.81 with BSD tools.

**Spec:** `docs/superpowers/specs/2026-10-08-topic-containers-design.md`. This plan is milestone M2 (§8). The behaviour is in §3.4 (the failure contract and headers), §3.5 (the worker), §3.6 (log lines), §3.7 (DLQ inspection and replay), §4.1 and §4.2 (metrics), §9 (the questions answered before M2) and §10 (unit tests; `verify-topics` step 5).

**Provenance:** every file and patch below was built and tested before this plan was written.
- **Unit tests:** the Go code compiled, passed `go vet` and gofmt, and all 23 top-level tests pass. Two mutations were caught: a waiting record overtaken, and `MAX_ATTEMPTS` off by one.
- **Against a throwaway `apache/kafka:4.3.1`:** the worker redelivered, dead-lettered and skipped the spec's four step-5 records with the exact log lines. The DLQ metric followed `delete-records`. Shutdown was immediate.
- **In a scratch copy of this repository:** `make verify-topics` printed `TOPICS OK`, including step 5. The README's park, replay and delete-records commands worked as written. A Studio consumer with Retry on `orders-1` retried through the owned `orders-1__retry` while the worker skipped its record.
- **Copy the code exactly.**

**M1 carry-overs (from the M1 reviews):**
- Shutdown drain: Task 3.
- The `metrics.go` comment that overclaimed dashboard compatibility: Task 2.
- `apply` returns `(int, error)` in the code, though the M1 plan shows the old signature. Nothing in M2 calls it, so this needs no work.
- Spec §9 questions 1, 2 and 4: answered on the broker before this plan. Task 4 records the answers in the spec.

## Global Constraints

- Nothing under `studio/` changes. No new Go dependency: the module stays at `github.com/twmb/franz-go v1.22.1`, `github.com/twmb/franz-go/pkg/kadm v1.19.0`, `github.com/twmb/franz-go/pkg/kmsg v1.14.0` and `github.com/prometheus/client_golang v1.24.1`.
- Header names, exactly (spec §3.4): `studio-group`, `studio-attempt`, `studio-error`, `studio-origin`, `studio-first-failure`, `studio-backoff-ms`. If a header repeats, its last value counts, as in Studio's `header()`.
- `studio-first-failure` is RFC 3339 UTC with milliseconds, `2026-10-08T14:00:00.000Z`. `studio-origin` is `topic[partition]@offset`.
- The worker's group is `<base>-<instance>__redelivery`, a franz-go group that starts at the beginning of the retry topic.
- Decision order (spec §3.5):
  1. a non-empty `studio-group` → skip and mark;
  2. a `studio-attempt` that is not a positive integer, or a `studio-backoff-ms` outside `0`–`3600000` or not a number → DLQ, reason `bad_header`, `studio-error` set to `retry: bad header <name> "<value>"`;
  3. `studio-attempt` ≥ `MAX_ATTEMPTS` → DLQ, reason `attempts`;
  4. now before the record's timestamp plus its backoff → wait;
  5. otherwise → back to the main topic.

  A missing `studio-attempt` counts as 1; a missing `studio-backoff-ms` means `BACKOFF_MS`.
- Waiting pauses the record's partition (`PauseFetchPartitions`) and resumes it when the partition's queue is empty. A record never overtakes an earlier one on its partition. The worker's fetches wait at most 1 s (`FetchMaxWait`).
- A record is marked (`MarkCommitRecords`, with `AutoCommitMarks`) only after `ProduceSync` succeeds. A failed produce holds the partition for 1 s (`produceRetry`) and tries again. Revoked partitions commit their marks and are dropped and resumed. On shutdown the worker commits its marks, then closes.
- Log lines, exactly (spec §3.6; `verify-topics` and the README quote them):
  - `<retry topic>: redelivered <pos> to <main> after <s>s (attempt <n> of <max>)`
  - `<retry topic>: dead-lettered <pos> to <dlq>: attempt <n> of <max>`
  - `<retry topic>: dead-lettered <pos> to <dlq>: retry: bad header studio-backoff-ms "soon"`
  - `<retry topic>: skipped <pos>: studio-group <group> runs its own retry loop`
- Metrics (spec §4.1):
  - retry role: `topic_owner_redeliveries_total{topic}`, `topic_owner_dead_lettered_total{topic,reason}` (reasons `attempts` and `bad_header`, both created at 0), `topic_owner_skipped_total{topic}`, `topic_owner_backoff_seconds{topic}` (histogram, `ExponentialBuckets(0.1, 2, 12)`);
  - DLQ role: `topic_owner_oldest_message_timestamp_seconds{topic,partition}`, for non-empty partitions only, refetched only when a partition's start offset moves.
- `main` exits only after the HTTP server's `Shutdown`, the reconcile loop and the worker have finished.
- `AGENTS.md` rules hold:
  - Makefile: real tabs, `## ` help, `$$` for a shell `$`, user text through `$(call shq,$(value var))`, GNU make 3.81 with BSD tools.
  - `verify-topics` owns the `owner-verify` prefix and its trap names everything it makes.
  - README, `AGENTS.md` and the spec change with the behaviour.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **The main topic is missing when a record is due** (its owner was deleted, or the topic was deleted by hand). The record is not lost: nothing is marked, the partition is held, and the produce is tried again every second until it works (`TestDrainProduceFails`, Task 1).
2. **A record with a short backoff behind one with a long backoff on the same partition.** It must not overtake the long one, because order on a partition is kept (`TestDrain`, Task 1; mutation-checked).
3. **A rebalance while a record waits** (a restart, or a second retry container by mistake). The waiting records are dropped unmarked and their partition is resumed, so a later owner reads them again and nothing stays paused (`TestForget`, Task 1).
4. **A publisher that appends headers instead of replacing them**, so a record has two `studio-attempt` headers. The last value counts, as in Studio (`TestDecide` "the last of repeated headers counts", Task 1).
5. **A DLQ emptied with `delete-records` after a replay.** Its oldest-record series disappears instead of showing a stale time (`TestOldestTimes`, Task 2).

---

### Task 1: The redelivery worker

**Files:**
- Create: `topic-owner/redeliver.go`, `topic-owner/redeliver_test.go`

**Interfaces:**
- Consumes (M1, unchanged):
  - `Config` (`Base`, `Instance`, `Role`, `Brokers`, `MaxAttempts`, `BackoffMS`), `Config.Name()`, `Config.Topic()`, `RoleRetry` from `config.go`;
  - the test helpers in `config_test.go`, `reconcile_test.go` and `metrics_test.go`. Their names (`env`, `want`, `have`, `alters`, `fakeOwner`, `testCollector`) do not collide with this task's helpers (`at`, `parked`, `headerList`, `testWorker`, `newTestWorker`, `keyed`).
- Produces:
  - the header constants `headerGroup`, `headerAttempt`, `headerError`, `headerOrigin`, `headerFirstFailure`, `headerBackoff`, and the constants `maxBackoffMS`, `firstFailureLayout`, `produceRetry`;
  - `func redeliveryGroup(name string) string`;
  - `type verdict` with `verdictSkip`, `verdictWait`, `verdictRedeliver`, `verdictDeadLetter`; `type decision`;
  - `func lastHeader(r *kgo.Record, key string) (string, bool)`, `func decide(r *kgo.Record, now time.Time, maxAttempts int, defaultBackoff time.Duration) decision`, `func position(r *kgo.Record) string`, `func onward(r *kgo.Record, badErr string) []kgo.RecordHeader`;
  - `type workerMetrics`, `func newWorkerMetrics(reg prometheus.Registerer, topic string) workerMetrics`;
  - `type worker`, `func newWorker(cfg Config, reg prometheus.Registerer) (*worker, error)`, `func (w *worker) run(ctx context.Context)`, `func (w *worker) drain(ctx context.Context) time.Time`, `func (w *worker) take(ctx context.Context, r *kgo.Record) time.Time`, `func (w *worker) forget(partitions []int32)`.

- [ ] **Step 1: Write the failing test**

`topic-owner/redeliver_test.go`:

```go
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
)

// at is when every test record was parked (its broker timestamp in the retry topic).
var at = time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)

// parked is a record of orders-1__retry with headers kv ("key=value").
func parked(offset int64, kv ...string) *kgo.Record {
	r := &kgo.Record{Topic: "orders-1__retry", Partition: 0, Offset: offset, Timestamp: at, Key: []byte("k"), Value: []byte(`{"id":1}`)}
	for _, s := range kv {
		k, v, _ := strings.Cut(s, "=")
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return r
}

func TestDecide(t *testing.T) {
	const max = 3
	backoff := 5 * time.Second
	for _, tc := range []struct {
		name    string
		r       *kgo.Record
		now     time.Time
		verdict verdict
		attempt int
		wait    time.Duration // backoff asked for
		reason  string
		badErr  string
	}{
		{name: "no headers: attempt 1, default backoff, due", r: parked(0), now: at.Add(5 * time.Second), verdict: verdictRedeliver, attempt: 1, wait: backoff},
		{name: "no headers, not due yet", r: parked(0), now: at.Add(4 * time.Second), verdict: verdictWait, attempt: 1, wait: backoff},
		{name: "a Studio loop owns it", r: parked(0, "studio-group=orders-studio", "studio-attempt=99"), now: at, verdict: verdictSkip},
		{name: "an empty studio-group is the worker's", r: parked(0, "studio-group="), now: at.Add(time.Hour), verdict: verdictRedeliver, attempt: 1, wait: backoff},
		{name: "attempt below the maximum", r: parked(0, "studio-attempt=2", "studio-backoff-ms=1000"), now: at.Add(time.Second), verdict: verdictRedeliver, attempt: 2, wait: time.Second},
		{name: "attempt at the maximum", r: parked(0, "studio-attempt=3"), now: at, verdict: verdictDeadLetter, attempt: 3, wait: backoff, reason: "attempts"},
		{name: "attempt above the maximum", r: parked(0, "studio-attempt=7"), now: at, verdict: verdictDeadLetter, attempt: 7, wait: backoff, reason: "attempts"},
		{name: "a bad attempt", r: parked(0, "studio-attempt=x"), now: at, verdict: verdictDeadLetter, reason: "bad_header", badErr: `retry: bad header studio-attempt "x"`},
		{name: "attempt 0 is bad", r: parked(0, "studio-attempt=0"), now: at, verdict: verdictDeadLetter, reason: "bad_header", badErr: `retry: bad header studio-attempt "0"`},
		{name: "a bad backoff", r: parked(0, "studio-backoff-ms=soon"), now: at, verdict: verdictDeadLetter, reason: "bad_header", badErr: `retry: bad header studio-backoff-ms "soon"`},
		{name: "backoff 0 is due at once", r: parked(0, "studio-backoff-ms=0"), now: at, verdict: verdictRedeliver, attempt: 1},
		{name: "backoff 3600000 is the longest", r: parked(0, "studio-backoff-ms=3600000"), now: at.Add(59 * time.Minute), verdict: verdictWait, attempt: 1, wait: time.Hour},
		{name: "backoff above 3600000 is bad", r: parked(0, "studio-backoff-ms=3600001"), now: at, verdict: verdictDeadLetter, reason: "bad_header", badErr: `retry: bad header studio-backoff-ms "3600001"`},
		{name: "the last of repeated headers counts", r: parked(0, "studio-attempt=3", "studio-attempt=1"), now: at.Add(time.Hour), verdict: verdictRedeliver, attempt: 1, wait: backoff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := decide(tc.r, tc.now, max, backoff)
			if d.verdict != tc.verdict || d.attempt != tc.attempt || d.reason != tc.reason || d.badErr != tc.badErr {
				t.Fatalf("got %+v", d)
			}
			if tc.verdict == verdictWait || tc.verdict == verdictRedeliver {
				if d.backoff != tc.wait || !d.due.Equal(at.Add(tc.wait)) {
					t.Fatalf("backoff %v due %v, want %v from %v", d.backoff, d.due, tc.wait, at)
				}
			}
		})
	}
}

// headerList renders headers as "key=value" in order.
func headerList(hs []kgo.RecordHeader) []string {
	var out []string
	for _, h := range hs {
		out = append(out, h.Key+"="+string(h.Value))
	}
	return out
}

func TestOnward(t *testing.T) {
	// Missing origin and first failure are added from the record itself.
	got := headerList(onward(parked(7, "studio-attempt=1", "x-trace=abc"), ""))
	want := []string{"studio-attempt=1", "x-trace=abc", "studio-origin=orders-1__retry[0]@7", "studio-first-failure=2026-10-08T14:00:00.000Z"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	// Present ones are kept as they are; nothing is added twice.
	got = headerList(onward(parked(7, "studio-origin=orders-1[2]@17", "studio-first-failure=2026-10-08T13:00:00.000Z"), ""))
	want = []string{"studio-origin=orders-1[2]@17", "studio-first-failure=2026-10-08T13:00:00.000Z"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	// A bad header replaces studio-error, keeping everything else.
	got = headerList(onward(parked(3, "studio-error=sink: 503", "studio-backoff-ms=soon"), `retry: bad header studio-backoff-ms "soon"`))
	want = []string{"studio-backoff-ms=soon", "studio-origin=orders-1__retry[0]@3", "studio-first-failure=2026-10-08T14:00:00.000Z", `studio-error=retry: bad header studio-backoff-ms "soon"`}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// testWorker is a worker on orders-1__retry with MAX_ATTEMPTS 3 and
// BACKOFF_MS 5000 whose produce fails while *produceErr is set. It records
// what it produced, marked, paused and resumed.
type testWorker struct {
	*worker
	produced, marked, pauses []string
	produceErr               error
}

func newTestWorker(now time.Time) *testWorker {
	tw := &testWorker{}
	cfg := Config{Base: "orders", Instance: 1, Role: RoleRetry, MaxAttempts: 3, BackoffMS: 5000}
	tw.worker = &worker{
		cfg: cfg, main: "orders-1", dlq: "orders-1__dlq",
		m:      newWorkerMetrics(prometheus.NewRegistry(), cfg.Topic()),
		now:    func() time.Time { return now },
		queues: map[int32][]*kgo.Record{}, paused: map[int32]bool{},
	}
	tw.produce = func(_ context.Context, r *kgo.Record) error {
		if tw.produceErr != nil {
			return tw.produceErr
		}
		tw.produced = append(tw.produced, r.Topic+" "+string(r.Key))
		return nil
	}
	tw.mark = func(r *kgo.Record) { tw.marked = append(tw.marked, position(r)) }
	tw.pause = func(p int32) { tw.pauses = append(tw.pauses, "pause") }
	tw.resume = func(p int32) { tw.pauses = append(tw.pauses, "resume") }
	return tw
}

func keyed(offset int64, key string, kv ...string) *kgo.Record {
	r := parked(offset, kv...)
	r.Key = []byte(key)
	return r
}

func TestDrain(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	now := at.Add(2 * time.Second)
	tw := newTestWorker(now)
	tw.queues[0] = []*kgo.Record{
		keyed(0, "a", "studio-backoff-ms=1000"),     // due: back to orders-1
		keyed(1, "b", "studio-group=orders-studio"), // Studio's: skipped
		keyed(2, "c", "studio-attempt=3"),           // attempts used up: DLQ
		keyed(3, "d", "studio-backoff-ms=10000"),    // due at +10 s: the partition waits here
		keyed(4, "e", "studio-backoff-ms=0"),        // due, but behind d: must not overtake it
	}
	next := tw.drain(context.Background())
	if want := []string{"orders-1 a", "orders-1__dlq c"}; !slices.Equal(tw.produced, want) {
		t.Fatalf("produced %q, want %q", tw.produced, want)
	}
	if want := []string{"orders-1__retry[0]@0", "orders-1__retry[0]@1", "orders-1__retry[0]@2"}; !slices.Equal(tw.marked, want) {
		t.Fatalf("marked %q, want %q", tw.marked, want)
	}
	if !next.Equal(at.Add(10*time.Second)) || len(tw.queues[0]) != 2 || !slices.Equal(tw.pauses, []string{"pause"}) {
		t.Fatalf("next %v, queue %d, pauses %q; want d's due time, d and e held, the partition paused once", next, len(tw.queues[0]), tw.pauses)
	}
	if got := testutil.ToFloat64(tw.m.redeliveries); got != 1 {
		t.Fatalf("redeliveries %v", got)
	}

	// d is due: d then e go back, in order, and the partition resumes.
	tw.now = func() time.Time { return at.Add(10 * time.Second) }
	if next := tw.drain(context.Background()); !next.IsZero() {
		t.Fatalf("next %v, want none", next)
	}
	if want := []string{"orders-1 a", "orders-1__dlq c", "orders-1 d", "orders-1 e"}; !slices.Equal(tw.produced, want) {
		t.Fatalf("produced %q", tw.produced)
	}
	if len(tw.queues) != 0 || !slices.Equal(tw.pauses, []string{"pause", "resume"}) {
		t.Fatalf("queues %d, pauses %q", len(tw.queues), tw.pauses)
	}
	if got := testutil.ToFloat64(tw.m.deadLettered.WithLabelValues("attempts")); got != 1 {
		t.Fatalf("dead-lettered (attempts) %v", got)
	}
	if got := testutil.ToFloat64(tw.m.skipped); got != 1 {
		t.Fatalf("skipped %v", got)
	}
}

// A failed produce marks nothing and holds the partition for produceRetry;
// the record goes when the produce works again.
func TestDrainProduceFails(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	now := at.Add(time.Minute)
	tw := newTestWorker(now)
	tw.produceErr = errors.New("UNKNOWN_TOPIC_OR_PARTITION")
	tw.queues[0] = []*kgo.Record{keyed(0, "a"), keyed(1, "b")}
	if next := tw.drain(context.Background()); !next.Equal(now.Add(produceRetry)) {
		t.Fatalf("next %v, want now + %v", next, produceRetry)
	}
	if len(tw.marked) != 0 || len(tw.queues[0]) != 2 || !tw.paused[0] {
		t.Fatalf("marked %q, queue %d, paused %v", tw.marked, len(tw.queues[0]), tw.paused[0])
	}
	tw.produceErr = nil
	tw.drain(context.Background())
	if want := []string{"orders-1__retry[0]@0", "orders-1__retry[0]@1"}; !slices.Equal(tw.marked, want) || tw.paused[0] {
		t.Fatalf("marked %q, paused %v", tw.marked, tw.paused[0])
	}
}

// A revoked partition's held records are dropped unmarked, and the partition
// is resumed so it fetches again if it comes back.
func TestForget(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	tw := newTestWorker(at)
	tw.queues[0] = []*kgo.Record{keyed(0, "a", "studio-backoff-ms=60000")}
	tw.drain(context.Background())
	tw.forget([]int32{0, 1})
	if len(tw.queues) != 0 || len(tw.paused) != 0 || !slices.Equal(tw.pauses, []string{"pause", "resume"}) || len(tw.marked) != 0 {
		t.Fatalf("queues %d, paused %v, pauses %q, marked %q", len(tw.queues), tw.paused, tw.pauses, tw.marked)
	}
}

func TestWorkerMetricsStartAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	newWorkerMetrics(reg, "orders-1__retry")
	want := `
# HELP topic_owner_dead_lettered_total Records the worker moved to the DLQ, by reason.
# TYPE topic_owner_dead_lettered_total counter
topic_owner_dead_lettered_total{reason="attempts",topic="orders-1__retry"} 0
topic_owner_dead_lettered_total{reason="bad_header",topic="orders-1__retry"} 0
# HELP topic_owner_redeliveries_total Records republished to the main topic.
# TYPE topic_owner_redeliveries_total counter
topic_owner_redeliveries_total{topic="orders-1__retry"} 0
# HELP topic_owner_skipped_total Records left to a Studio retry loop (studio-group set).
# TYPE topic_owner_skipped_total counter
topic_owner_skipped_total{topic="orders-1__retry"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "topic_owner_dead_lettered_total", "topic_owner_redeliveries_total", "topic_owner_skipped_total"); err != nil {
		t.Fatal(err)
	}
	if redeliveryGroup("orders-1") != "orders-1__redelivery" {
		t.Fatal(redeliveryGroup("orders-1"))
	}
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `cd topic-owner && go test ./...`
Expected: FAIL, with undefined `decide`, `verdictRedeliver`, `onward`, `worker` and `newWorkerMetrics`.

- [ ] **Step 3: Write the implementation**

`topic-owner/redeliver.go`:

```go
// The redelivery worker (ROLE=retry): it reads <name>__retry in group
// <name>__redelivery and takes the records no Studio loop owns (no
// studio-group header). Each waits out its backoff, then goes back to <name>;
// once its attempts are used up, or when its headers are bad, it goes to
// <name>__dlq instead. Records are marked for commit only after their produce
// is acknowledged: delivery is at-least-once.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The failure contract's headers (spec §3.4): Studio's four, plus two.
const (
	headerGroup        = "studio-group"         // set only by a Studio consumer: its own loop retries the record
	headerAttempt      = "studio-attempt"       // failed tries so far: 1, 2, …
	headerError        = "studio-error"         // the last failure, its first line
	headerOrigin       = "studio-origin"        // topic[partition]@offset where the record was first read
	headerFirstFailure = "studio-first-failure" // when it first failed, RFC 3339 UTC with milliseconds
	headerBackoff      = "studio-backoff-ms"    // how long to wait, counted from the record's timestamp in the retry topic
)

const (
	maxBackoffMS       = 3600000                         // a larger studio-backoff-ms is a bad header
	firstFailureLayout = "2006-01-02T15:04:05.000Z07:00" // studio-first-failure, in UTC: …T14:00:00.000Z
	produceRetry       = time.Second                     // how long a partition waits after a failed produce
)

// redeliveryGroup is the worker's consumer group. It ends in __redelivery, so
// it never equals a Studio retry group (<group>__retry).
func redeliveryGroup(name string) string { return name + "__redelivery" }

type verdict int

const (
	verdictSkip       verdict = iota // a Studio loop owns the record
	verdictWait                      // not due yet
	verdictRedeliver                 // due: back to the main topic
	verdictDeadLetter                // to the DLQ
)

// decision is what the worker does with one record of the retry topic.
type decision struct {
	verdict verdict
	group   string        // skip: the Studio group that owns the record
	attempt int           // failed tries so far
	backoff time.Duration // the backoff the record asked for
	due     time.Time     // wait, redeliver: when it may go back
	reason  string        // dead letter: "attempts" or "bad_header"
	badErr  string        // dead letter for a bad header: its new studio-error
}

// lastHeader is r's last value for key, and whether r has the header at all.
func lastHeader(r *kgo.Record, key string) (string, bool) {
	v, ok := "", false
	for _, h := range r.Headers {
		if h.Key == key {
			v, ok = string(h.Value), true
		}
	}
	return v, ok
}

// decide applies spec §3.5's rules, in order: a Studio group skips; a bad
// studio-attempt or studio-backoff-ms dead-letters; attempt ≥ maxAttempts
// dead-letters; a record not due waits; the rest go back. A missing
// studio-attempt counts as 1, a missing studio-backoff-ms as defaultBackoff.
func decide(r *kgo.Record, now time.Time, maxAttempts int, defaultBackoff time.Duration) decision {
	if g, _ := lastHeader(r, headerGroup); g != "" {
		return decision{verdict: verdictSkip, group: g}
	}
	d := decision{attempt: 1, backoff: defaultBackoff}
	if s, ok := lastHeader(r, headerAttempt); ok {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return badHeader(headerAttempt, s)
		}
		d.attempt = n
	}
	if s, ok := lastHeader(r, headerBackoff); ok {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n > maxBackoffMS {
			return badHeader(headerBackoff, s)
		}
		d.backoff = time.Duration(n) * time.Millisecond
	}
	d.due = r.Timestamp.Add(d.backoff)
	switch {
	case d.attempt >= maxAttempts:
		d.verdict, d.reason = verdictDeadLetter, "attempts"
	case now.Before(d.due):
		d.verdict = verdictWait
	default:
		d.verdict = verdictRedeliver
	}
	return d
}

func badHeader(key, value string) decision {
	return decision{verdict: verdictDeadLetter, reason: "bad_header", badErr: fmt.Sprintf("retry: bad header %s %q", key, value)}
}

// position is where r is: topic[partition]@offset.
func position(r *kgo.Record) string { return fmt.Sprintf("%s[%d]@%d", r.Topic, r.Partition, r.Offset) }

// onward is r's headers for its next topic: all of them, plus studio-origin
// (r's own position) and studio-first-failure (r's timestamp) when r has none;
// with badErr set, it replaces studio-error.
func onward(r *kgo.Record, badErr string) []kgo.RecordHeader {
	hs := make([]kgo.RecordHeader, 0, len(r.Headers)+3)
	for _, h := range r.Headers {
		if badErr != "" && h.Key == headerError {
			continue
		}
		hs = append(hs, h)
	}
	if _, ok := lastHeader(r, headerOrigin); !ok {
		hs = append(hs, kgo.RecordHeader{Key: headerOrigin, Value: []byte(position(r))})
	}
	if _, ok := lastHeader(r, headerFirstFailure); !ok {
		hs = append(hs, kgo.RecordHeader{Key: headerFirstFailure, Value: []byte(r.Timestamp.UTC().Format(firstFailureLayout))})
	}
	if badErr != "" {
		hs = append(hs, kgo.RecordHeader{Key: headerError, Value: []byte(badErr)})
	}
	return hs
}

// workerMetrics are the retry role's own series (spec §4.1).
type workerMetrics struct {
	redeliveries prometheus.Counter
	deadLettered *prometheus.CounterVec
	skipped      prometheus.Counter
	backoff      prometheus.Histogram
}

func newWorkerMetrics(reg prometheus.Registerer, topic string) workerMetrics {
	labels := prometheus.Labels{"topic": topic}
	m := workerMetrics{
		redeliveries: prometheus.NewCounter(prometheus.CounterOpts{Name: "topic_owner_redeliveries_total", Help: "Records republished to the main topic.", ConstLabels: labels}),
		deadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "topic_owner_dead_lettered_total", Help: "Records the worker moved to the DLQ, by reason.", ConstLabels: labels}, []string{"reason"}),
		skipped:      prometheus.NewCounter(prometheus.CounterOpts{Name: "topic_owner_skipped_total", Help: "Records left to a Studio retry loop (studio-group set).", ConstLabels: labels}),
		backoff:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "topic_owner_backoff_seconds", Help: "The backoff each redelivered record asked for.", ConstLabels: labels, Buckets: prometheus.ExponentialBuckets(0.1, 2, 12)}),
	}
	m.deadLettered.WithLabelValues("attempts") // both reasons start at 0, so increase() sees the first
	m.deadLettered.WithLabelValues("bad_header")
	reg.MustRegister(m.redeliveries, m.deadLettered, m.skipped, m.backoff)
	return m
}

// worker redelivers the records of one retry topic.
type worker struct {
	cfg       Config
	main, dlq string
	m         workerMetrics
	now       func() time.Time
	produce   func(context.Context, *kgo.Record) error // waits for the broker's acknowledgement
	mark      func(*kgo.Record)                        // marks a record for commit
	pause     func(partition int32)
	resume    func(partition int32)
	cl        *kgo.Client // nil in tests

	mu     sync.Mutex
	queues map[int32][]*kgo.Record // per partition, the records read and not yet done, in offset order
	paused map[int32]bool
}

func newWorker(cfg Config, reg prometheus.Registerer) (*worker, error) {
	w := &worker{
		cfg: cfg, main: cfg.Name(), dlq: cfg.Name() + "__dlq",
		m: newWorkerMetrics(reg, cfg.Topic()), now: time.Now,
		queues: map[int32][]*kgo.Record{}, paused: map[int32]bool{},
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(redeliveryGroup(cfg.Name())),
		kgo.ConsumeTopics(cfg.Topic()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // a new group takes every parked record
		kgo.AutoCommitMarks(),
		kgo.FetchMaxWait(time.Second), // a resumed partition's records arrive within a second, not after a 5 s long poll
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			w.forget(revoked[cfg.Topic()])
			cl.CommitMarkedOffsets(ctx)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			w.forget(lost[cfg.Topic()])
		}),
	)
	if err != nil {
		return nil, err
	}
	w.cl = cl
	w.produce = func(ctx context.Context, r *kgo.Record) error { return cl.ProduceSync(ctx, r).FirstErr() }
	w.mark = func(r *kgo.Record) { cl.MarkCommitRecords(r) }
	w.pause = func(p int32) { cl.PauseFetchPartitions(map[string][]int32{cfg.Topic(): {p}}) }
	w.resume = func(p int32) { cl.ResumeFetchPartitions(map[string][]int32{cfg.Topic(): {p}}) }
	return w, nil
}

// run polls and redelivers until ctx ends, then commits what it marked and
// leaves the group.
func (w *worker) run(ctx context.Context) {
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		w.cl.CommitMarkedOffsets(cctx)
		w.cl.Close()
	}()
	for {
		next := w.drain(ctx)
		pctx, cancel := ctx, context.CancelFunc(func() {})
		if !next.IsZero() {
			pctx, cancel = context.WithDeadline(ctx, next) // wake when the first waiting record is due
		}
		fs := w.cl.PollFetches(pctx)
		cancel()
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				log.Printf("%s: fetch %s[%d]: %v", w.cfg.Topic(), t, p, err)
			}
		})
		w.mu.Lock()
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			w.queues[p.Partition] = append(w.queues[p.Partition], p.Records...)
		})
		w.mu.Unlock()
	}
}

// drain takes, partition by partition, the queued records that are done
// waiting, in order, and stops at the first that must wait (or whose produce
// failed): its partition stays paused until then, so later records cannot
// overtake it. It returns the earliest time a held record is due, zero if none.
func (w *worker) drain(ctx context.Context) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	var next time.Time
	for p, q := range w.queues {
		for len(q) > 0 {
			if until := w.take(ctx, q[0]); !until.IsZero() {
				if next.IsZero() || until.Before(next) {
					next = until
				}
				break
			}
			q = q[1:]
		}
		switch {
		case len(q) > 0:
			w.queues[p] = q
			if !w.paused[p] {
				w.pause(p)
				w.paused[p] = true
			}
		default:
			delete(w.queues, p)
			if w.paused[p] {
				w.resume(p)
				delete(w.paused, p)
			}
		}
	}
	return next
}

// take does what r's decision says and returns zero when r is done, else
// when to look at it again.
func (w *worker) take(ctx context.Context, r *kgo.Record) time.Time {
	topic, now := w.cfg.Topic(), w.now()
	d := decide(r, now, w.cfg.MaxAttempts, time.Duration(w.cfg.BackoffMS)*time.Millisecond)
	switch d.verdict {
	case verdictSkip:
		w.mark(r)
		w.m.skipped.Inc()
		log.Printf("%s: skipped %s: studio-group %s runs its own retry loop", topic, position(r), d.group)
	case verdictWait:
		return d.due
	case verdictDeadLetter:
		if err := w.produce(ctx, &kgo.Record{Topic: w.dlq, Key: r.Key, Value: r.Value, Headers: onward(r, d.badErr)}); err != nil {
			log.Printf("%s: dead-letter %s to %s: %v", topic, position(r), w.dlq, err)
			return now.Add(produceRetry)
		}
		w.mark(r)
		w.m.deadLettered.WithLabelValues(d.reason).Inc()
		why := d.badErr
		if why == "" {
			why = fmt.Sprintf("attempt %d of %d", d.attempt, w.cfg.MaxAttempts)
		}
		log.Printf("%s: dead-lettered %s to %s: %s", topic, position(r), w.dlq, why)
	case verdictRedeliver:
		if err := w.produce(ctx, &kgo.Record{Topic: w.main, Key: r.Key, Value: r.Value, Headers: onward(r, "")}); err != nil {
			log.Printf("%s: redeliver %s to %s: %v", topic, position(r), w.main, err)
			return now.Add(produceRetry)
		}
		w.mark(r)
		w.m.redeliveries.Inc()
		w.m.backoff.Observe(d.backoff.Seconds())
		log.Printf("%s: redelivered %s to %s after %.1fs (attempt %d of %d)", topic, position(r), w.main, now.Sub(r.Timestamp).Seconds(), d.attempt, w.cfg.MaxAttempts)
	}
	return time.Time{}
}

// forget drops the queues of partitions this member no longer owns and
// resumes them, so they fetch again if they come back. Their records were not
// marked; the next owner reads them again.
func (w *worker) forget(partitions []int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range partitions {
		delete(w.queues, p)
		if w.paused[p] {
			w.resume(p)
			delete(w.paused, p)
		}
	}
}
```

- [ ] **Step 4: Run the checks**

Run: `cd topic-owner && go vet ./... && test -z "$(gofmt -l .)" && go test -v ./... 2>&1 | grep -E '^(--- |ok)'`
Expected: 21 top-level `--- PASS` lines, including `TestDecide`, `TestOnward`, `TestDrain`, `TestDrainProduceFails`, `TestForget` and `TestWorkerMetricsStartAtZero`, then `ok  	kafka-playground/topic-owner`. `go.mod` does not change: `git diff --stat topic-owner/go.mod` prints nothing.

- [ ] **Step 5: Commit**

```bash
git add topic-owner/redeliver.go topic-owner/redeliver_test.go
git commit -m "topic-owner: the redelivery worker — per-record decision (skip, wait, back, DLQ), headers carried onward, partitions paused while a record waits, marks only after the broker's ack

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: The DLQ's oldest record

**Files:**
- Modify: `topic-owner/metrics.go`, `topic-owner/metrics_test.go`

**Interfaces:**
- Consumes: `partitionState`, `collector`, `newCollector`, `RoleDLQ` (M1).
- Produces:
  - `descOldest`;
  - `type oldestTimes struct { fetch func(context.Context) (kadm.ListedOffsets, error); mu sync.Mutex; known map[int32]oldestAt }`;
  - `type oldestAt struct { start int64; at time.Time }`;
  - `func (o *oldestTimes) get(ctx context.Context, topic string, parts []partitionState) (map[int32]time.Time, error)`;
  - a new field, `collector.oldest *oldestTimes`, set by `newCollector` for `ROLE=dlq` only.

- [ ] **Step 1: Write the failing tests**

Apply this patch to `topic-owner/metrics_test.go`. It adds `TestOldestTimes` and `TestCollectorOldest`, and the imports `time` and `kadm`:

```diff
--- a/topic-owner/metrics_test.go
+++ b/topic-owner/metrics_test.go
@@ -5,8 +5,10 @@
 	"errors"
 	"strings"
 	"testing"
+	"time"
 
 	"github.com/prometheus/client_golang/prometheus/testutil"
+	"github.com/twmb/franz-go/pkg/kadm"
 )
 
 func testCollector(s topicState, err error, reconciled bool) *collector {
@@ -115,3 +117,75 @@
 		t.Fatal(err)
 	}
 }
+
+// The DLQ's oldest record per partition is fetched once, and again only when
+// the partition's start offset moves; an empty partition has none.
+func TestOldestTimes(t *testing.T) {
+	t0 := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
+	fetches := 0
+	listed := kadm.ListedOffsets{"orders-1__dlq": {
+		0: {Topic: "orders-1__dlq", Partition: 0, Offset: 0, Timestamp: t0.UnixMilli()},
+		1: {Topic: "orders-1__dlq", Partition: 1, Offset: 0, Timestamp: -1}, // empty: the broker has no timestamp
+	}}
+	o := &oldestTimes{known: map[int32]oldestAt{}, fetch: func(context.Context) (kadm.ListedOffsets, error) {
+		fetches++
+		return listed, nil
+	}}
+	parts := []partitionState{{partition: 0, start: 0, end: 2}, {partition: 1, start: 0, end: 0}}
+	for range 2 {
+		times, err := o.get(context.Background(), "orders-1__dlq", parts)
+		if err != nil || len(times) != 1 || !times[0].Equal(t0) {
+			t.Fatalf("times %v, err %v; want partition 0 at %v only", times, err, t0)
+		}
+	}
+	if fetches != 1 {
+		t.Fatalf("%d fetches for an unchanged start, want 1", fetches)
+	}
+
+	// delete-records moved partition 0's start to 1: fetch again, the new oldest.
+	listed["orders-1__dlq"][0] = kadm.ListedOffset{Topic: "orders-1__dlq", Partition: 0, Offset: 1, Timestamp: t0.Add(time.Minute).UnixMilli()}
+	parts[0].start = 1
+	times, err := o.get(context.Background(), "orders-1__dlq", parts)
+	if err != nil || fetches != 2 || !times[0].Equal(t0.Add(time.Minute)) {
+		t.Fatalf("times %v, err %v, %d fetches; want the record at offset 1, fetched once more", times, err, fetches)
+	}
+
+	// Everything replayed and deleted: no oldest record, no fetch.
+	parts[0].start = 2
+	if times, _ := o.get(context.Background(), "orders-1__dlq", parts); len(times) != 0 || fetches != 2 {
+		t.Fatalf("times %v, %d fetches; want none, no fetch", times, fetches)
+	}
+
+	// A failed fetch reports its error and serves nothing stale.
+	parts[0].end = 3
+	o.fetch = func(context.Context) (kadm.ListedOffsets, error) { return nil, errors.New("unable to dial") }
+	if times, err := o.get(context.Background(), "orders-1__dlq", parts); err == nil || len(times) != 0 {
+		t.Fatalf("times %v, err %v", times, err)
+	}
+}
+
+// The DLQ role's collector serves the oldest record's timestamp in seconds.
+func TestCollectorOldest(t *testing.T) {
+	t0 := time.Date(2026, 10, 8, 14, 0, 0, 500_000_000, time.UTC)
+	c := &collector{
+		cfg:        Config{Base: "orders", Instance: 1, Role: RoleDLQ},
+		reconciled: func() bool { return true },
+		read: func(context.Context) (topicState, error) {
+			return topicState{partitions: []partitionState{{partition: 0, replicas: 1, isr: 1, start: 4, end: 6, size: 100}}}, nil
+		},
+		oldest: &oldestTimes{known: map[int32]oldestAt{}, fetch: func(context.Context) (kadm.ListedOffsets, error) {
+			return kadm.ListedOffsets{"orders-1__dlq": {0: {Topic: "orders-1__dlq", Partition: 0, Offset: 4, Timestamp: t0.UnixMilli()}}}, nil
+		}},
+	}
+	want := `
+# HELP topic_owner_oldest_message_timestamp_seconds Timestamp of the partition's oldest record (at its log start), for non-empty partitions. DLQ role only.
+# TYPE topic_owner_oldest_message_timestamp_seconds gauge
+topic_owner_oldest_message_timestamp_seconds{partition="0",topic="orders-1__dlq"} 1.7914680005e+09
+# HELP topic_owner_kafka_up 1 when this scrape's admin calls to Kafka succeeded.
+# TYPE topic_owner_kafka_up gauge
+topic_owner_kafka_up{topic="orders-1__dlq"} 1
+`
+	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "topic_owner_oldest_message_timestamp_seconds", "topic_owner_kafka_up"); err != nil {
+		t.Fatal(err)
+	}
+}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd topic-owner && go test ./...`
Expected: FAIL, with `undefined: oldestTimes`, `undefined: oldestAt` and `unknown field oldest in struct literal`.

- [ ] **Step 3: Write the implementation**

Apply to `topic-owner/metrics.go`. The first hunk is the carried-over comment fix:

```diff
--- a/topic-owner/metrics.go
+++ b/topic-owner/metrics.go
@@ -5,6 +5,7 @@
 	"errors"
 	"slices"
 	"strconv"
+	"sync"
 	"time"
 
 	"github.com/prometheus/client_golang/prometheus"
@@ -16,7 +17,7 @@
 const scrapeDeadline = 3 * time.Second
 
 // Series that mean exactly what kafka-exporter's mean keep its names and labels,
-// so its dashboards work on them; the rest are topic_owner_*.
+// so queries written for kafka-exporter work on them; the rest are topic_owner_*.
 var (
 	descInfo            = prometheus.NewDesc("topic_owner_info", "The topic this container owns, with its base, instance and role.", []string{"topic", "base", "topic_instance", "role"}, nil)
 	descReconciled      = prometheus.NewDesc("topic_owner_reconciled", "1 when the last reconcile left the topic in its desired state.", []string{"topic"}, nil)
@@ -28,6 +29,7 @@
 	descLogSize         = prometheus.NewDesc("topic_owner_partition_log_size_bytes", "Size of the partition's log segments, summed over replicas.", []string{"topic", "partition"}, nil)
 	descGroupOffset     = prometheus.NewDesc("kafka_consumergroup_current_offset", "Offset the consumer group committed on the partition.", []string{"consumergroup", "topic", "partition"}, nil)
 	descGroupLag        = prometheus.NewDesc("kafka_consumergroup_lag", "Log end offset minus the group's committed offset, for committed partitions.", []string{"consumergroup", "topic", "partition"}, nil)
+	descOldest          = prometheus.NewDesc("topic_owner_oldest_message_timestamp_seconds", "Timestamp of the partition's oldest record (at its log start), for non-empty partitions. DLQ role only.", []string{"topic", "partition"}, nil)
 )
 
 // partitionState is one partition as a scrape reads it.
@@ -49,16 +51,23 @@
 	cfg        Config
 	reconciled func() bool                               // whether the last reconcile left the topic in its desired state
 	read       func(context.Context) (topicState, error) // the topic and its groups, as the broker has them
+	oldest     *oldestTimes                              // the DLQ role's oldest record per partition; nil for the others
 }
 
 func newCollector(cfg Config, adm *kadm.Client, reconciled func() bool) *collector {
-	return &collector{cfg: cfg, reconciled: reconciled, read: func(ctx context.Context) (topicState, error) {
+	c := &collector{cfg: cfg, reconciled: reconciled, read: func(ctx context.Context) (topicState, error) {
 		return readTopic(ctx, adm, cfg.Topic())
 	}}
+	if cfg.Role == RoleDLQ {
+		c.oldest = &oldestTimes{known: map[int32]oldestAt{}, fetch: func(ctx context.Context) (kadm.ListedOffsets, error) {
+			return adm.ListOffsetsAfterMilli(ctx, 0, cfg.Topic()) // the first record with a timestamp ≥ 0: the oldest
+		}}
+	}
+	return c
 }
 
 func (c *collector) Describe(ch chan<- *prometheus.Desc) {
-	for _, d := range []*prometheus.Desc{descInfo, descReconciled, descKafkaUp, descPartitions, descUnderReplicated, descEndOffset, descStartOffset, descLogSize, descGroupOffset, descGroupLag} {
+	for _, d := range []*prometheus.Desc{descInfo, descReconciled, descKafkaUp, descPartitions, descUnderReplicated, descEndOffset, descStartOffset, descLogSize, descGroupOffset, descGroupLag, descOldest} {
 		ch <- d
 	}
 }
@@ -73,6 +82,13 @@
 	ctx, cancel := context.WithTimeout(context.Background(), scrapeDeadline)
 	defer cancel()
 	s, err := c.read(ctx)
+	if c.oldest != nil {
+		times, oerr := c.oldest.get(ctx, topic, s.partitions)
+		for p, t := range times {
+			gauge(descOldest, float64(t.UnixMilli())/1000, topic, strconv.Itoa(int(p)))
+		}
+		err = errors.Join(err, oerr)
+	}
 	gauge(descKafkaUp, boolValue(err == nil), topic)
 	if len(s.partitions) > 0 {
 		gauge(descPartitions, float64(len(s.partitions)), topic)
@@ -103,6 +119,50 @@
 	}
 }
 
+// oldestTimes keeps, per partition, the time of its oldest record and the
+// start offset that record was at, so a scrape fetches again only when a
+// start offset moved (retention, delete-records, a first record).
+type oldestTimes struct {
+	fetch func(context.Context) (kadm.ListedOffsets, error) // the oldest record's offset and timestamp, per partition
+
+	mu    sync.Mutex
+	known map[int32]oldestAt
+}
+
+type oldestAt struct {
+	start int64
+	at    time.Time
+}
+
+// get is the oldest record's time of each non-empty partition in parts.
+func (o *oldestTimes) get(ctx context.Context, topic string, parts []partitionState) (map[int32]time.Time, error) {
+	o.mu.Lock()
+	defer o.mu.Unlock()
+	nonEmpty := func(p partitionState) bool { return p.start >= 0 && p.end > p.start }
+	var err error
+	stale := func(p partitionState) bool {
+		k, ok := o.known[p.partition]
+		return nonEmpty(p) && (!ok || k.start != p.start)
+	}
+	if slices.ContainsFunc(parts, stale) {
+		var listed kadm.ListedOffsets
+		if listed, err = o.fetch(ctx); err == nil {
+			for _, p := range parts {
+				if l, ok := listed.Lookup(topic, p.partition); ok && l.Err == nil && l.Timestamp >= 0 && nonEmpty(p) {
+					o.known[p.partition] = oldestAt{start: p.start, at: time.UnixMilli(l.Timestamp)}
+				}
+			}
+		}
+	}
+	times := map[int32]time.Time{}
+	for _, p := range parts {
+		if k, ok := o.known[p.partition]; ok && nonEmpty(p) && k.start == p.start {
+			times[p.partition] = k.at
+		}
+	}
+	return times, err
+}
+
 func boolValue(b bool) float64 {
 	if b {
 		return 1
```

What it does:
- `get` fetches only when a non-empty partition's start offset is not the one it cached. The check uses the map's presence flag, because a missing entry's zero value would otherwise match a partition that starts at 0. A test pins that case.
- A failed fetch returns its error, which turns `topic_owner_kafka_up` to 0, and serves no stale time.

- [ ] **Step 4: Run the checks**

Run: `cd topic-owner && go vet ./... && test -z "$(gofmt -l .)" && go test -v ./... 2>&1 | grep -E '^(--- |ok)'`
Expected: 23 top-level `--- PASS` lines, then `ok  	kafka-playground/topic-owner`.

- [ ] **Step 5: Commit**

```bash
git add topic-owner/metrics.go topic-owner/metrics_test.go
git commit -m "topic-owner: the DLQ's oldest record time per partition, fetched again only when a start offset moves; a metrics comment no longer overclaims dashboard compatibility

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Run the worker, drain on shutdown, check it end to end

**Files:**
- Modify: `topic-owner/main.go`, `Makefile` (the `verify-topics` target)

**Interfaces:**
- Consumes:
  - `newWorker` and `(*worker).run` (Task 1);
  - the log lines and metric names of Tasks 1–2;
  - M1's `verify-topics` helpers `own`, `healthy` and `promis`, the trap (which already names `owner-verify-1__retry`, `owner-verify-1__dlq`, their topics and the group `owner-verify-1__redelivery`), and the `orders-audit` service, whose image is the stack's pinned `cp-kcat`.
- Produces:
  - the retry role runs the worker;
  - `main` waits for the HTTP shutdown, the reconcile loop and the worker;
  - `verify-topics` step 5, which prints `topics worker: a redelivered, b and d dead-lettered, c left to its Studio loop, 2 parked` before `TOPICS OK`.

- [ ] **Step 1: Start the worker and drain on shutdown**

Apply to `topic-owner/main.go`:

```diff
--- a/topic-owner/main.go
+++ b/topic-owner/main.go
@@ -1,7 +1,8 @@
 // Topic owner: one long-running container per topic. ROLE=main owns
-// <base>-<instance>, retry owns <base>-<instance>__retry, dlq owns
-// <base>-<instance>__dlq. Each creates its topic, keeps it in the desired state
-// (reconcile.go) and serves /healthz and /metrics (metrics.go) on :9000.
+// <base>-<instance>, retry owns <base>-<instance>__retry and runs the
+// redelivery worker (redeliver.go), dlq owns <base>-<instance>__dlq. Each
+// creates its topic, keeps it in the desired state (reconcile.go) and serves
+// /healthz and /metrics (metrics.go) on :9000.
 // `topic-owner -healthcheck` is the compose healthcheck (the scratch image has
 // no curl); it prints why the topic is not in its desired state.
 package main
@@ -14,6 +15,7 @@
 	"net/http"
 	"os"
 	"os/signal"
+	"sync"
 	"syscall"
 	"time"
 
@@ -45,18 +47,30 @@
 
 	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
 	defer stop()
-	go o.run(ctx)
+	var wg sync.WaitGroup // what must finish before exit: the reconcile loop, the worker
+	wg.Go(func() { o.run(ctx) })
+	if cfg.Role == RoleRetry {
+		w, err := newWorker(cfg, reg)
+		if err != nil {
+			log.Fatalf("topic-owner: %v", err)
+		}
+		wg.Go(func() { w.run(ctx) })
+	}
 	srv := &http.Server{Addr: addr, Handler: routes(o.health, reg)}
+	shutDown := make(chan struct{})
 	go func() {
 		<-ctx.Done()
 		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
 		defer cancel()
 		srv.Shutdown(sctx)
+		close(shutDown)
 	}()
 	log.Printf("%s: owner (role %s) on %s", cfg.Topic(), cfg.Role, addr)
 	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
 		log.Fatalf("topic-owner: %v", err)
 	}
+	<-shutDown // Shutdown has drained the open requests
+	wg.Wait()  // the worker has committed what it marked and left its group
 }
 
 // routes serves GET /healthz (200 "ok", or 503 and why not) and GET /metrics.
```

Run: `cd topic-owner && go vet ./... && test -z "$(gofmt -l .)" && go test ./... && go build -o /dev/null .`
Expected: `ok  	kafka-playground/topic-owner`, and the build succeeds.

- [ ] **Step 2: Add verify-topics step 5**

Apply to `Makefile` (recipe lines start with real tabs). The patch also updates the target's `## ` help line:

```diff
--- a/Makefile
+++ b/Makefile
@@ -245,7 +245,7 @@
 
 # Runs owners of its own base, owner-verify, as one-offs of the orders-1 service
 # (same image, network and healthcheck; labels and environment overridden).
-verify-topics: up ## Check the topic owners end to end: refusals, reconcile, metrics in Prometheus; cleans up its containers and topics
+verify-topics: up ## Check the topic owners end to end: refusals, reconcile, metrics, the redelivery worker and the DLQ; cleans up its containers, topics and group
 	@trap 'docker rm -f owner-verify-refused owner-verify-1 owner-verify-1__retry owner-verify-1__dlq >/dev/null 2>&1; $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --delete --topic "owner-verify-1(__retry|__dlq)?" >/dev/null 2>&1; $(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --delete --group owner-verify-1__redelivery >/dev/null 2>&1' EXIT; \
 	own() { n=$$1 r=$$2 p=$$3; shift 3; docker rm -f "$$n" >/dev/null 2>&1; \
 		out=$$($(COMPOSE) run -d --no-deps --name "$$n" -l topic-owner.base=owner-verify -l topic-owner.instance=1 -l topic-owner.role="$$r" \
@@ -287,6 +287,33 @@
 		[ "$$i" = 30 ] && { echo "TOPICS FAILED: Prometheus never scraped owner-verify-1 with 3 partitions: $$(curl -sS $(PROMETHEUS_URL)/api/v1/query --data-urlencode 'query={topic="owner-verify-1"}')"; exit 1; }; sleep 1; \
 	done; \
 	echo "topics prometheus: owner-verify-1 up, 3 partitions"; \
+	own owner-verify-1__dlq dlq 3 && healthy owner-verify-1__dlq || exit 1; \
+	own owner-verify-1__retry retry 3 -e MAX_ATTEMPTS=2 && healthy owner-verify-1__retry || exit 1; \
+	for i in $$(seq 30); do \
+		promis 'count(kafka_topic_partition_current_offset{topic="owner-verify-1__dlq"})' 3 && break; \
+		[ "$$i" = 30 ] && { echo "TOPICS FAILED: Prometheus never scraped owner-verify-1__dlq"; exit 1; }; sleep 1; \
+	done; \
+	kc() { $(COMPOSE) run --rm -T --no-deps --entrypoint kcat orders-audit -b kafka:19092 "$$@" 2>/dev/null; }; \
+	park() { k=$$1; shift; echo "{\"id\":\"$$k\"}" | kc -P -t owner-verify-1__retry -k "$$k" "$$@" || { echo "TOPICS FAILED: park $$k in owner-verify-1__retry"; return 1; }; }; \
+	park a -H studio-attempt=1 -H studio-backoff-ms=1000 && park b -H studio-attempt=2 && \
+		park c -H studio-group=owner-verify-studio && park d -H studio-backoff-ms=soon || exit 1; \
+	for i in $$(seq 30); do \
+		main=$$(kc -C -t owner-verify-1 -o beginning -e -J); dlq=$$(kc -C -t owner-verify-1__dlq -o beginning -e -J); \
+		echo "$$main" | grep '"key":"a"' | grep -q '"studio-origin","owner-verify-1__retry\[' && echo "$$dlq" | grep -q '"key":"b"' && \
+			echo "$$dlq" | grep '"key":"d"' | grep -qF 'retry: bad header studio-backoff-ms \"soon\"' && break; \
+		[ "$$i" = 30 ] && { echo "TOPICS FAILED: want a back on owner-verify-1 with its studio-origin, b and d (bad header) on the DLQ: main $$main dlq $$dlq"; exit 1; }; sleep 1; \
+	done; \
+	echo "$$main$$dlq" | grep -q '"key":"c"' && { echo "TOPICS FAILED: c (studio-group set) left owner-verify-1__retry: $$main $$dlq"; exit 1; }; \
+	for i in $$(seq 30); do \
+		promis 'topic_owner_redeliveries_total{topic="owner-verify-1__retry"}' 1 && \
+			promis 'sum(topic_owner_dead_lettered_total{topic="owner-verify-1__retry"})' 2 && \
+			promis 'topic_owner_skipped_total{topic="owner-verify-1__retry"}' 1 && \
+			promis 'sum(kafka_topic_partition_current_offset{topic="owner-verify-1__dlq"} - kafka_topic_partition_oldest_offset{topic="owner-verify-1__dlq"})' 2 && \
+			promis 'count(topic_owner_oldest_message_timestamp_seconds{topic="owner-verify-1__dlq"}) > bool 0' 1 && \
+			promis 'sum(kafka_consumergroup_lag{consumergroup="owner-verify-1__redelivery"})' 0 && break; \
+		[ "$$i" = 30 ] && { echo "TOPICS FAILED: want 1 redelivered, 2 dead-lettered, 1 skipped, 2 parked, an oldest time and no lag: $$(curl -sS $(PROMETHEUS_URL)/api/v1/query --data-urlencode 'query={__name__=~"topic_owner_(redeliveries|dead_lettered|skipped)_total|topic_owner_oldest_message_timestamp_seconds"}')"; exit 1; }; sleep 1; \
+	done; \
+	echo "topics worker: a redelivered, b and d dead-lettered, c left to its Studio loop, 2 parked"; \
 	echo "TOPICS OK"
 
 verify-ui: up studio/ui/.chromium ## Check the studio UI in Chromium (Playwright); installs Chromium once
```

The new step:
1. Starts `owner-verify-1__dlq`, then `owner-verify-1__retry` with `MAX_ATTEMPTS=2`.
2. Waits until Prometheus has scraped the DLQ, so M3's `increase()` will have a base.
3. Parks four records with kcat:
   - (a) `studio-attempt=1`, `studio-backoff-ms=1000`;
   - (b) `studio-attempt=2`;
   - (c) `studio-group=owner-verify-studio`;
   - (d) `studio-backoff-ms=soon`.
4. Checks:
   - a is back on `owner-verify-1`, with a `studio-origin` pointing into the retry topic;
   - b and d are on the DLQ, d with `retry: bad header studio-backoff-ms "soon"`;
   - c is on neither;
   - Prometheus shows 1 redelivered, 2 dead-lettered, 1 skipped, 2 parked, an oldest-record time, and the worker group's lag at 0.

`kc` runs kcat from the `orders-audit` service's pinned image, on the compose network. `-J` prints headers as a JSON list.

Run: `make -n verify-topics >/dev/null && echo parse ok`
Expected: `parse ok`.

- [ ] **Step 3: Run the check end to end**

Run: `make test && make down && make verify-topics`
Expected: `make test` passes. Then, after the stack starts (the topic id varies):

```
topics refused: a base name with a dot, replication factor 3
topics reconcile: Topic: owner-verify-1	TopicId: …	PartitionCount: 2	ReplicationFactor: 1	Configs: min.insync.replicas=1,retention.ms=3600000
topics partitions: raised 2 -> 3, a decrease refused
topics prometheus: owner-verify-1 up, 3 partitions
topics worker: a redelivered, b and d dead-lettered, c left to its Studio loop, 2 parked
TOPICS OK
```

- [ ] **Step 4: Run the spec's M2 demo on the stack**

With the stack still up from Step 3, run each command; you should see:

- `echo '{"id":1}' | make kcat args='-P -t orders-1__retry -k a -H studio-attempt=1 -H studio-backoff-ms=2000 -H "studio-error=sink: demo"'`
- `echo '{"id":2}' | make kcat args='-P -t orders-1__retry -k b -H studio-attempt=3 -H "studio-error=sink: demo"'`
- `sleep 8`
- `make kcat args='-C -t orders-1 -o beginning -e -J'`: key `a`, with `"studio-origin","orders-1__retry[` among its headers.
- `make kcat args='-C -t orders-1__dlq -o beginning -e -J'`: key `b`.
- `make query q='topic_owner_redeliveries_total'` and `make query q='sum(topic_owner_dead_lettered_total)'`: each `"1"`.
- `docker logs orders-1__retry 2>&1 | tail -2`: a `redelivered … to orders-1 after 2.…s (attempt 1 of 3)` line and a `dead-lettered … to orders-1__dlq: attempt 3 of 3` line.

Then time the shutdown, which the drain must keep prompt: `time docker compose stop orders-1__retry`. Expected: well under the 10 s stop timeout.

Run: `make down && docker ps -aq -f label=topic-owner.role | wc -l`
Expected: `0`.

- [ ] **Step 5: Commit**

```bash
git add topic-owner/main.go Makefile
git commit -m "topic owners: the retry container runs the redelivery worker; main waits for the worker, the reconcile loop and the HTTP server before it exits; verify-topics step 5 parks four records and checks where each went

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Documentation and the full check

**Files:**
- Modify: `README.md`, `AGENTS.md`, `docs/superpowers/specs/2026-10-08-topic-containers-design.md`

**Interfaces:**
- Consumes: everything above. The docs quote the headers, the group, the log lines, the metric names, the commands and the `MAX_ATTEMPTS` mapping exactly as Tasks 1–3 made them.
- Produces:
  - the README sections `### Retry and the DLQ` and `#### Inspect and replay the DLQ`, five new metric rows, and four PromQL queries;
  - an `AGENTS.md` layout entry for `redeliver.go`, and the header-contract rule;
  - in the spec: the status, the 1 s fetch wait in §3.5, the M2 demo's `sleep 8`, and the answers to §9 questions 1, 2 and 4.

- [ ] **Step 1: README**

Apply to `README.md`:

```diff
--- a/README.md
+++ b/README.md
@@ -87,7 +87,7 @@
 - It serves `/metrics` for its topic on port 9000 inside the network. Prometheus (http://localhost:9090) finds the containers by their labels, with no target list.
 - `make owners` lists them. `make query q='kafka_topic_partitions'` asks Prometheus. `make kcat args='-C -t orders-1 -o beginning -e -J'` runs kcat on the compose network (default `-L`).
 
-The retry container owns its topic like the other two. The redelivery worker that will read it is a later milestone.
+The retry container also runs the redelivery worker (below).
 
 ### Configuration
 
@@ -98,7 +98,7 @@
 | `PARTITIONS` | `1` | Raised on an existing topic, never lowered. |
 | `REPLICATION_FACTOR` | `1` | Anything else is refused: there is one broker. |
 | `TOPIC_CONFIG_<NAME>` | role defaults | `TOPIC_CONFIG_RETENTION_MS: 3600000` sets `retention.ms`. An empty value removes a role default. |
-| `MAX_ATTEMPTS`, `BACKOFF_MS` | `3`, `5000` | Retry role only (1–10 and 100–60000). |
+| `MAX_ATTEMPTS`, `BACKOFF_MS` | `3`, `5000` | Retry role only, refused on the others: the tries before the DLQ (1–10), and the backoff in ms for a record without `studio-backoff-ms` (100–60000). |
 
 Role defaults: the retry topic gets `message.timestamp.type=LogAppendTime`, so the broker stamps each record. The DLQ gets `retention.ms=-1`, so parked records stay.
 
@@ -142,6 +142,61 @@
 
 Instances share nothing: `orders-2` has its own topics and settings. Prometheus finds the new containers by their labels.
 
+### Retry and the DLQ
+
+A consumer of `orders-1` that fails a record parks it in `orders-1__retry`, then commits the original. It sends the record's own key and value, and the record's own headers with these set:
+
+| Header | Set by | Value | What the worker does with it |
+|---|---|---|---|
+| `studio-group` | Studio consumers only | the consumer's group | Skips the record: that Studio consumer's own retry loop handles it. Never set it from another client. |
+| `studio-attempt` | the publisher, on every failure | failed tries so far: `1`, `2`, … | Missing counts as 1. Not a positive integer: DLQ. At `MAX_ATTEMPTS` or above: DLQ. |
+| `studio-backoff-ms` | the publisher, on every failure | `0`–`3600000`, counted from the record's time in `orders-1__retry` | Missing: `BACKOFF_MS`. Anything else: DLQ. |
+| `studio-error` | the publisher | the error's first line, at most 1 KiB | Replaces it only for a bad header: `retry: bad header studio-backoff-ms "soon"`. |
+| `studio-origin` | the first publisher, once | `topic[partition]@offset` where the record was first read | Sets it to the record's place in `orders-1__retry` when missing. |
+| `studio-first-failure` | the first publisher, once | RFC 3339 UTC, `2026-10-08T14:00:00.000Z` | Sets it to the record's time in `orders-1__retry` when missing. |
+
+The `studio-` headers are Studio's: a Studio consumer with Retry writes them to the same topics, so Console, the DLQ and a Studio tail read both alike.
+
+The retry container runs the redelivery worker, in group `orders-1__redelivery` (franz-go only: never point `kcat -G` at it). For each record without `studio-group`, in partition order:
+
+- It waits until the record's time plus its backoff. The retry topic's `LogAppendTime` makes that the broker's clock. While a record waits, the records behind it on its partition wait too, so a long backoff holds up shorter ones; more partitions on `orders-1__retry` reduce that.
+- Then it sends the record back to `orders-1`, with its key, value and headers. Once `studio-attempt` reaches `MAX_ATTEMPTS`, or when a header is bad, it sends it to `orders-1__dlq` instead.
+- It commits a record only after the broker acknowledges the send. A crash in between sends it twice: delivery is at-least-once.
+
+What a consumer of `orders-1` sees:
+
+- Every group on `orders-1` gets the redelivered record, including groups that never failed it, such as an `orders-audit`-style fan-out group. A Studio consumer with Retry avoids that with its own loop.
+- A redelivered record has `studio-attempt` and `studio-origin`; an original has neither. To act on a record once, dedupe on `studio-origin` when present, else on the record's own `topic[partition]@offset`.
+- Key order is not kept: the record comes back after records sent while it waited.
+
+`MAX_ATTEMPTS` counts tries, the first included: with 3, a record is tried 3 times. Studio's `attempts` counts retries after the first try, so Studio's `attempts: 3` is `MAX_ATTEMPTS: 4`.
+
+Park a record by hand, as a failing consumer would:
+
+```sh
+echo '{"id":42}' | make kcat args='-P -t orders-1__retry -k order-42 -H studio-attempt=1 -H studio-backoff-ms=5000 -H "studio-error=sink: http 503"'
+make kcat args='-C -t orders-1 -o -1 -e -J'   # about 5 s later: order-42, with studio-origin and studio-first-failure added
+```
+
+Read headers with `-J`. kcat's `%h` joins them with commas and does not escape a comma inside `studio-error`.
+
+#### Inspect and replay the DLQ
+
+- Console (http://localhost:8080) → Topics → `orders-1__dlq` shows each record with its headers. `make kcat args='-C -t orders-1__dlq -o beginning -e -J'` prints them.
+- To replay, copy key and value back to `orders-1`. The headers stay behind, so each record starts over with fresh attempts:
+
+  ```sh
+  make kcat args='-C -t orders-1__dlq -o beginning -e -f "%k\t%s\n"' | make kcat args='-P -t orders-1 -K "\t"'
+  ```
+
+- Kafka cannot delete one record. After a full replay, move the DLQ's start past what you replayed, with one entry per partition (`-1` is the end):
+
+  ```sh
+  docker compose exec -T kafka /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:19092 --offset-json-file /dev/stdin <<'EOF'
+  {"partitions":[{"topic":"orders-1__dlq","partition":0,"offset":-1},{"topic":"orders-1__dlq","partition":1,"offset":-1},{"topic":"orders-1__dlq","partition":2,"offset":-1}],"version":1}
+  EOF
+  ```
+
 ### Metrics
 
 Every series has a `topic` label. Where a series means what a [kafka-exporter](https://github.com/danielqsj/kafka_exporter) series means, it has the same name and labels, so kafka-exporter queries over these series work.
@@ -156,8 +211,20 @@
 | `topic_owner_info` | `base`, `topic_instance` and `role` (Prometheus reserves `instance`, which is the container name) |
 | `topic_owner_reconciled` | 1 when the topic is in its desired state |
 | `topic_owner_kafka_up` | 1 when the last scrape's admin calls succeeded (a missing topic shows as `topic_owner_reconciled` 0, not here) |
+| `topic_owner_redeliveries_total` | retry role: records sent back to the main topic |
+| `topic_owner_dead_lettered_total` | retry role: records moved to the DLQ, by `reason` (`attempts`, `bad_header`) |
+| `topic_owner_skipped_total` | retry role: records left to a Studio retry loop |
+| `topic_owner_backoff_seconds` | retry role: the backoff each redelivered record asked for (a histogram) |
+| `topic_owner_oldest_message_timestamp_seconds` | DLQ role: the time of each non-empty partition's oldest record |
+
+Some queries:
+
+- Messages in per second: `sum by (topic) (rate(kafka_topic_partition_current_offset[1m]))`.
+- Records waiting in a retry topic: `sum by (topic) (kafka_consumergroup_lag{consumergroup=~".+__redelivery"})`.
+- Records parked in a DLQ: `sum by (topic) (kafka_topic_partition_current_offset{topic=~".+__dlq"} - kafka_topic_partition_oldest_offset{topic=~".+__dlq"})`.
+- Age of the oldest parked record, in seconds: `time() - min by (topic) (topic_owner_oldest_message_timestamp_seconds)`.
 
-Messages in per second: `sum by (topic) (rate(kafka_topic_partition_current_offset[1m]))`. Bytes in and out per topic are not exported. Only the broker's JMX has them, and the JMX agent would need a jar and a change to the `kafka` service.
+Bytes in and out per topic are not exported. Only the broker's JMX has them, and the JMX agent would need a jar and a change to the `kafka` service.
 
 Prometheus reads the Docker socket with `group_add: ["0"]`, because Docker Desktop shows the socket as `root:root 0660` inside containers. On native Linux, use the host's docker gid instead. The socket gives root on the host, which is one more reason everything stays on `127.0.0.1`.
 
```

- [ ] **Step 2: AGENTS.md**

Apply to `AGENTS.md`:

```diff
--- a/AGENTS.md
+++ b/AGENTS.md
@@ -8,7 +8,7 @@
 - `producer/`: producer page. Go (franz-go), `main.go` plus embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
 - `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `stream.go`, `docker.go`, `kafka.go`, `transform.go`, `router.go`) and node runner (`node.go`, with its retry loop and failure path in `retry.go`, run as `studio node` in one container per producer, consumer and consumer instance), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
 - `studio/ui/e2e/`: the UI tests (Playwright, `playwright.config.ts` beside them): `flows.ts` (the API and flow parts), `locators.ts`, `studio.ts` (the fixture), and one `*.spec.ts` per area. `make verify-ui` runs them against the running stack.
-- `topic-owner/`: topic owner. Go (franz-go, client_golang): `config.go` (environment, name rules, role defaults), `reconcile.go` (the desired-state diff and its loop), `metrics.go` (`/metrics`), `main.go` (`/healthz`, `-healthcheck`); multi-stage `Dockerfile` onto `scratch`. It runs as one container per topic.
+- `topic-owner/`: topic owner. Go (franz-go, client_golang): `config.go` (environment, name rules, role defaults), `reconcile.go` (the desired-state diff and its loop), `metrics.go` (`/metrics`), `redeliver.go` (the retry role's redelivery worker), `main.go` (`/healthz`, `-healthcheck`); multi-stage `Dockerfile` onto `scratch`. It runs as one container per topic.
 - `prometheus/`: `prometheus.yml`, bind-mounted read-only into the `prometheus` service.
 - `flows/`: Studio flow files, bind-mounted into the `studio` container; `flows/0a1b2c3d.json` is the example.
 - `Makefile`: day-to-day commands; `make test` runs the static checks and unit tests, `make verify` the end-to-end test (`verify-studio` deletes its flows and its `studio-verify…` topics when it exits, and `verify-topics` its `owner-verify…` containers, topics and group; name a new test topic or group in that trap too).
@@ -40,6 +40,7 @@
 - The UI tests (`studio/ui/e2e/`, Playwright, `make verify-ui`) find elements by role, label and text, and by `data-testid` where there is none (`node-<id>`, `runtime-<id>`, `tail`). A change to the UI's visible text, roles or those ids runs `make verify-ui`; the top-bar messages they check are quoted in the Studio spec or the UI tests spec (§5), so rewording one goes through the spec. Everything they make is named `studio-ui-…`, a namespace the suite owns: each test removes every such flow, and the run removes every such topic and group after the last test. Don't give anything else that prefix.
 - Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). So does a topic owner started with `docker compose run` (labels `topic-owner.role` and `com.docker.compose.oneoff=True`), which `docker compose down` leaves behind. Keep that line in `down`; use `make nodes` to see them. `down` passes `-v`: the `kafka` and `prometheus` images declare anonymous volumes that would otherwise stay.
 - A topic owner's service, `container_name` and topic are the same string: `<base>-<instance>`, plus `__retry` or `__dlq` (the suffixes Studio uses). The name rules live in `topic-owner/config.go`, on top of Studio's `topicNameRe` and `maxInputTopic`. Its labels are `topic-owner.base`, `topic-owner.instance` and `topic-owner.role`, never `studio.flow`. Prometheus finds owners by `topic-owner.role`, so a new instance needs no Prometheus change: add one block of three services with their own anchors (README, "Add an instance").
+- The redelivery worker's header contract is Studio's (`studio/retry.go`) plus `studio-backoff-ms` and `studio-first-failure` (`topic-owner/redeliver.go`). A change to a header's name or format changes both files, the README table and both specs together. The worker owns the records without `studio-group`; its group `<base>-<instance>__redelivery` is franz-go only.
 - A topic owner's series that mean what kafka-exporter's mean keep its names and labels; the rest are `topic_owner_*`. `verify-topics` greps metric names and log lines; renaming one changes it too. `verify-topics` owns the `owner-verify` prefix: don't give anything else that name.
 - Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
 - Keep README.md, and the spec when behaviour departs from it, in sync with any behaviour change.
```

- [ ] **Step 3: The spec**

Apply to `docs/superpowers/specs/2026-10-08-topic-containers-design.md`:

```diff
--- a/docs/superpowers/specs/2026-10-08-topic-containers-design.md
+++ b/docs/superpowers/specs/2026-10-08-topic-containers-design.md
@@ -1,8 +1,8 @@
 # Topic containers — design
 
-Status: approved on 2026-10-08. M1 is built, from
-`docs/superpowers/plans/2026-10-08-topic-containers-m1.md`; M2 and M3 follow,
-one plan each.
+Status: approved on 2026-10-08. M1 and M2 are built, from
+`docs/superpowers/plans/2026-10-08-topic-containers-m1.md` and
+`docs/superpowers/plans/2026-10-08-topic-containers-m2.md`; M3 follows.
 
 A topic deployed as three long-running containers on the playground's broker:
 `orders-1`, `orders-1__retry` and `orders-1__dlq`. Each container owns one
@@ -444,7 +444,9 @@
 3. resumes the partition once its queue is empty.
 
 The other partitions keep flowing. The poll loop sleeps until the earliest due
-time among the queues, or until new records arrive. kgo keeps a paused
+time among the queues, or until new records arrive. The worker's fetches wait
+at most 1 s on the broker (`FetchMaxWait`), so a resumed partition's records
+arrive within a second instead of after kgo's default 5 s long poll. kgo keeps a paused
 partition assigned, and it holds back records already buffered for that
 partition without dropping them (source, kgo v1.22.1). On a rebalance, the
 queues of revoked partitions are dropped, because their records were never
@@ -1190,7 +1192,7 @@
   ```sh
   echo '{"id":1}' | make kcat args='-P -t orders-1__retry -k a -H studio-attempt=1 -H studio-backoff-ms=2000 -H "studio-error=sink: demo"'
   echo '{"id":2}' | make kcat args='-P -t orders-1__retry -k b -H studio-attempt=3 -H "studio-error=sink: demo"'
-  sleep 3
+  sleep 8
   make kcat args='-C -t orders-1 -o beginning -e -J'
   # key a, with studio-origin set
   make kcat args='-C -t orders-1__dlq -o beginning -e -J'
@@ -1229,9 +1231,15 @@
    their assignment and buffered records are held back, not dropped. M2's
    first task is a test against the broker: pause with a queue, resume, and
    see no record lost or duplicated.
+   **Answered on 2026-10-08, against `apache/kafka:4.3.1`:** 60 records read
+   through a pause of four polls arrived in order, with no duplicate and no
+   gap, and no record came back while paused.
 2. **`ListOffsetsAfterMilli(ctx, 0, topic)` returns the oldest record's
    timestamp.** This is not yet confirmed on a broker. Check it in M2's first
    task; the fallback is a one-record read from `AtStart()` (4.1).
+   **Answered on 2026-10-08:** it returns the oldest record's offset and
+   timestamp, and the new oldest after `kafka-delete-records.sh` moves the
+   start. An empty partition gives timestamp `-1`. No fallback is needed.
 3. **`UpdatePartitions` with an equal count.** The doc says "equal to or
    larger", but the broker may refuse equal. The reconcile diff never calls it
    when the counts are equal (3.3). M1's tests pin that.
@@ -1240,6 +1248,11 @@
    machine that is the same clock, so the delay is unchanged. M1's verify runs
    with `verify-studio` in the same `make verify`, but on different topics.
    Confirm once by hand on `orders-1__retry`.
+   **Answered on 2026-10-08:** a record produced with a timestamp an hour old
+   reads back with the broker's append time. A Studio consumer with Retry on
+   `orders-1` (attempts 1, a failing sink) showed `retried` 1 and `dlq` 1
+   through the owned `orders-1__retry`, and the worker logged that it skipped
+   the Studio record.
 5. **Studio changes, listed, not planned (Studio is out of scope):**
    - (a) Studio could write `studio-first-failure`. It cannot use
      `studio-backoff-ms`, because its loop has its own delay.
```

- [ ] **Step 4: Run the full check**

Run: `make test && make down && make verify`
Expected: `make test` passes, and `make verify` prints `STUDIO OK (…)`, `… passed`, `UI OK`, the five `topics …` lines, `TOPICS OK`, then `VERIFY OK`.

- [ ] **Step 5: Leave a clean machine**

Run: `make down && docker ps -aq -f label=studio.flow && docker ps -aq -f label=topic-owner.role && git status --short flows`
Expected: no output after `make down`'s own lines.

- [ ] **Step 6: Commit**

```bash
git add README.md AGENTS.md docs/superpowers/specs/2026-10-08-topic-containers-design.md
git commit -m "docs: retry and the DLQ — README header contract, worker semantics, park/inspect/replay commands, new metrics and queries; AGENTS layout and header-contract rule; spec status M2 built, the 1 s fetch wait, the §9 answers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
