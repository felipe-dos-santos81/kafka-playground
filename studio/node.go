// `studio node`: one producer or consumer of a deployed flow, in its own
// container. Its whole configuration is env STUDIO_NODE (a NodeSpec) plus
// KAFKA_BROKERS. It serves the control plane on :9000 inside the compose
// network: POST /send (producers), GET /tail?since=N and GET /stats. A consumer
// also posts each record to its http sink and forwards it to its next topic, or
// to the one its router picks.
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
	"slices"
	"strconv"
	"strings"
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

	// Stop budgets, inside the stopGraceSeconds a stopped container gets: the batch
	// in hand, then the commit, then the HTTP server, with Close left the rest.
	batchWait      = (stopGraceSeconds - 2) * time.Second
	commitBudget   = time.Second
	shutdownBudget = time.Second
)

var errInvalidJSON = errors.New("value is not valid JSON")

// tailEntry is one record as the tail drawer shows it.
type tailEntry struct {
	Seq       int64             `json:"seq"`
	Time      string            `json:"time"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"` // a record a consumer sent on after a failure carries studio-* headers
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
	t.entries = append(t.entries, tailEntry{
		Seq:       t.seq,
		Time:      r.Timestamp.UTC().Format(time.RFC3339Nano),
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       string(r.Key),
		Value:     string(v),
		Headers:   hs,
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
	Boot    string     `json:"boot"` // random per process: a restarted container starts its counters and tail over
	tally              // records produced (producers) or fetched (consumers), the failures, the last one
	TailSeq int64      `json:"tailSeq"`           // seq of the newest tail record; the drawer fetches when it moves
	Step    *stepTally `json:"step,omitempty"`    // a consumer's transform, when it runs one
	Route   *stepTally `json:"route,omitempty"`   // a consumer's router, when it runs one
	Retried int64      `json:"retried,omitempty"` // a consumer's records sent to its retry topic
	DLQ     int64      `json:"dlq,omitempty"`     // a consumer's records sent to its DLQ
}

// tally is what counters read: every record counted, the ones that failed, and
// the last failure. A node has one; a consumer's transform has its own (Step).
type tally struct {
	Total     int64  `json:"total"`
	Errors    int64  `json:"errors"`
	LastError string `json:"lastError"`
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

func (c *counters) read() tally {
	c.mu.Lock()
	defer c.mu.Unlock()
	return tally{Total: c.total.Load(), Errors: c.errors.Load(), LastError: c.lastError}
}

func (c *counters) stats(boot string, tailSeq int64) nodeStats {
	return nodeStats{Boot: boot, tally: c.read(), TailSeq: tailSeq}
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
		if err := jsonOut(v); err != nil {
			return nil, fmt.Errorf("value template %w", err)
		}
		body = []byte(v)
	} else if !json.Valid(body) {
		return nil, errInvalidJSON
	}
	rec := &kgo.Record{Topic: p.spec.Topic, Value: body}
	if key != "" {
		rec.Key = []byte(key)
	}
	return rec, nil
}

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
	p.counts.ok()
	p.tail.push(rec)
	return nil
}

// send produces one record. A body is the value as is (curl, webhooks), keyed by
// ?key=; an empty body renders the node's own templates (the UI's Send button).
// It answers {partition, offset}: 400 for a body that is not JSON, 500 (and an
// error counted) for templates that fail or render no JSON.
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
			log.Printf("timer: %v", err)
			continue
		}
		if err := p.produceOne(ctx, rec); err != nil && ctx.Err() == nil {
			log.Printf("produce: %v", err)
		}
	}
}

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
		fs.EachRecord(func(r *kgo.Record) {
			if c.handle(ctx, r) {
				cl.MarkCommitRecords(r) // only a handled record may be committed
			}
		})
	}
}

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

// sinkClient does not follow redirects: a 3xx would turn the POST into a GET and
// could pass for success, so it is an error like any other non-2xx answer.
var sinkClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// postJSON is the http sink: it POSTs body as JSON within 5 s; any answer but
// 2xx (a redirect included) is an error.
func postJSON(ctx context.Context, url string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := sinkClient.Do(req)
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

// runNode is `studio node`: it runs until SIGTERM (Stop). A consumer then waits
// up to 3 s for the batches in hand (its main loop's, and its retry loop's) to
// finish their sink and writes, commits the records it handled in each group (it
// marks each one; unhandled ones are redelivered) and closes its clients, which
// leaves the groups.
// groupOpts are a consumer group client's options: topic read in group, from
// reset while the group has no commit, committing only the records marked.
func groupOpts(group, topic string, reset kgo.Offset) []kgo.Opt {
	return []kgo.Opt{kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(reset), kgo.AutoCommitMarks(),
		kgo.SessionTimeout(groupSessionTimeout)}
}

// groupSessionTimeout is how long the broker keeps a consumer that stopped
// heartbeating (killed, or its container gone) in its group; until then a new
// member's join waits. franz-go's default, 45 s, would stall a redeploy after a
// crash that long. Heartbeats stay every 3 s.
const groupSessionTimeout = 10 * time.Second

func runNode() {
	var spec NodeSpec
	if err := json.Unmarshal([]byte(os.Getenv("STUDIO_NODE")), &spec); err != nil {
		log.Fatal("STUDIO_NODE: ", err)
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	t, counts, boot := &tail{}, &counters{}, NewID()
	var tr *transform           // a consumer's transform; nil without one
	var rt *router              // a consumer's router; nil without one
	var consuming chan struct{} // closed when a consumer's poll loops have returned; nil for a producer
	var c *consumer             // nil for a producer
	produce := func(ctx context.Context, rec *kgo.Record) error {
		return cl.ProduceSync(ctx, rec).FirstErr()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /tail", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		reply(w, http.StatusOK, t.since(since))
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s := counts.stats(boot, t.last())
		if tr != nil {
			s.Step = &stepTally{tally: tr.counts.read()}
		}
		if rt != nil {
			route := rt.read()
			s.Route = &route
		}
		if c != nil {
			s.Retried, s.DLQ = c.retried.Load(), c.dead.Load()
		}
		reply(w, http.StatusOK, s)
	})
	if spec.Type == "producer" {
		p := &producer{spec: spec, tail: t, counts: counts, produce: produce}
		mux.HandleFunc("POST /send", p.send)
		if spec.Source == "timer" {
			go p.run(ctx, time.Duration(spec.IntervalMS)*time.Millisecond)
		}
	} else {
		mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
			fail(w, http.StatusConflict, "only producer nodes send")
		})
		if spec.Transform != "" {
			if tr, err = newTransform(spec.Transform); err != nil {
				log.Fatal("transform: ", err) // Validate compiled the same source on deploy
			}
		}
		if spec.RouterNode != "" {
			if rt, err = newRouter(spec.Routes, spec.RouteDefault); err != nil {
				log.Fatal("router: ", err) // Validate compiled the same rules on deploy
			}
		}
		c = &consumer{spec: spec, tail: t, counts: counts, transform: tr, router: rt, post: postJSON, produce: produce}
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
	}
	srv := &http.Server{Addr: nodeAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("node %s (%s) on topic %s, boot %s", spec.Node, spec.Type, spec.Topic, boot)

	<-ctx.Done()
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
}
