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
