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
