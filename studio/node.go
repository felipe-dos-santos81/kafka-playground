// `studio node`: one producer or consumer of a deployed flow, in its own
// container. Its whole configuration is env STUDIO_NODE (a NodeSpec) plus
// KAFKA_BROKERS. It serves the control plane on :9000 inside the compose
// network: POST /send (producers), GET /tail?since=N and GET /stats. A consumer
// also posts each record to its http sink and forwards it to its next topic, or
// to the one its router picks; retry.go has its retry loop and its failure path.
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
	hs := headerMap(r.Headers)
	for k, h := range hs {
		if len(h) > tailValueMax {
			hs[k] = h[:tailValueMax]
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

// headerMap is hs by key, a repeated key's last value winning; nil without headers.
func headerMap(hs []kgo.RecordHeader) map[string]string {
	if len(hs) == 0 {
		return nil
	}
	m := map[string]string{}
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
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
	Retried *int64     `json:"retried,omitempty"` // a consumer's records sent to its retry topic; only with retry on
	DLQ     *int64     `json:"dlq,omitempty"`     // a consumer's records sent to its DLQ; only with a DLQ
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
	pctx, cancel := context.WithTimeout(ctx, sendTimeout)
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
// http sink (when set), the transform (when set), the router (when set) and the
// forward (when set). Without a DLQ, a failed step is counted and logged, not
// retried: autocommit still moves past the record. With one, the first failure
// ends the record's path and sendOn (retry.go) sends it to the retry topic or the
// DLQ. The sink and the writes run under context.WithoutCancel: a stop (SIGTERM)
// must not fail them with "context canceled" before Close commits past this
// record. The main loop and the retry loop call handle at once; the transform and
// the router each lock their own VM.
type consumer struct {
	spec         NodeSpec
	tail         *tail
	counts       *counters
	transform    *transform                                               // nil without one
	router       *router                                                  // nil without one
	post         func(ctx context.Context, url string, body []byte) error // the http sink
	produce      func(context.Context, *kgo.Record) error                 // the forward, and the sends to the retry topic and the DLQ
	retried      atomic.Int64                                             // records sent to the retry topic
	deadLettered atomic.Int64                                             // records sent to the DLQ
}

// handle reports whether r may be committed: false only when a write failed
// because the client was closed (a stop that outlasted its grace), so the record
// is redelivered rather than lost. Any other failure is counted and logged, and r
// is still committed. A record from the retry topic is tailed but not counted in
// total again.
func (c *consumer) handle(ctx context.Context, r *kgo.Record) bool {
	work := context.WithoutCancel(ctx)
	if !c.fromRetry(r) {
		c.counts.ok()
	}
	c.tail.push(r)
	log.Printf("%s[%d]@%d key=%s %s", r.Topic, r.Partition, r.Offset, r.Key, r.Value)
	f := c.path(work, r)
	switch {
	case f == nil:
		return true
	case errors.Is(f.err, kgo.ErrClientClosed):
		return false
	default:
		return c.sendOn(work, r, *f)
	}
}

// path takes r through the sink, the transform, the router and the forward;
// stepFailed counts and logs each failed step. Without a DLQ, r goes on past a
// failure where it can and path returns nil. With one, the first failure ends r's
// path and path returns it; so does a forward cut by a closed client, DLQ or not.
func (c *consumer) path(ctx context.Context, r *kgo.Record) *failure {
	if c.spec.SinkURL != "" {
		if err := c.post(ctx, c.spec.SinkURL, r.Value); err != nil {
			if f := c.stepFailed(fmt.Errorf("sink: %w", err), true); f != nil {
				return f
			}
		}
	}
	value := r.Value
	if c.transform != nil {
		out, err := c.transform.run(value)
		if err != nil {
			if f := c.stepFailed(fmt.Errorf("transform: %w", err), false); f != nil {
				return f
			}
		}
		if out == nil {
			return nil // failed or dropped (nil): nothing to forward
		}
		value = out
	}
	forward := c.spec.Forward
	if c.router != nil {
		topic, err := c.router.route(value)
		if err != nil {
			if f := c.stepFailed(fmt.Errorf("router: %w", err), false); f != nil {
				return f
			}
		}
		forward = topic // "": failed or unmatched, nothing to forward
	}
	if forward != "" {
		if err := c.write(ctx, &kgo.Record{Topic: forward, Key: r.Key, Value: value}); err != nil {
			return c.stepFailed(fmt.Errorf("forward to %s: %w", forward, err), true)
		}
	}
	return nil
}

// stepFailed counts and logs a failed step of a record's path. It returns the
// failure when it ends the path (a DLQ is set, or a closed client cut a write),
// nil when the record goes on.
func (c *consumer) stepFailed(err error, retryable bool) *failure {
	c.counts.fail(err)
	log.Print(err)
	if c.spec.DLQ == "" && !errors.Is(err, kgo.ErrClientClosed) {
		return nil
	}
	return &failure{err: err, retryable: retryable}
}

// sendTimeout bounds one produce: a producer's record, a forward, a send to the
// retry topic or the DLQ.
const sendTimeout = 10 * time.Second

// write produces rec within sendTimeout.
func (c *consumer) write(ctx context.Context, rec *kgo.Record) error {
	wctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	return c.produce(wctx, rec)
}

// poll reads cl's group until ctx ends, handing each record to take and marking it
// for commit when take says it may be. The first record it may not ends the loop:
// Stop closed the client, or cut a retry's wait. The main loop takes records with
// handle, the retry loop with retryRecord; fetch errors count as the consumer's,
// prefixed with what ("fetch", "retry fetch").
func (c *consumer) poll(ctx context.Context, cl *kgo.Client, what string, take func(context.Context, *kgo.Record) bool) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(topic string, partition int32, err error) {
			c.counts.fail(fmt.Errorf("%s: %w", what, err))
			log.Printf("%s %s[%d]: %v", what, topic, partition, err)
		})
		for it := fs.RecordIter(); !it.Done(); {
			r := it.Next()
			if !take(ctx, r) {
				return
			}
			cl.MarkCommitRecords(r) // only a handled record may be committed
		}
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

// runNode is `studio node`: it runs until SIGTERM (Stop). A consumer then waits
// up to 3 s for the batches in hand (its main loop's, and its retry loop's) to
// finish their sink and writes, commits the records it handled in each group (it
// marks each one; unhandled ones are redelivered) and closes its clients, which
// leaves the groups.
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
		if c != nil && c.spec.Retry != nil {
			n := c.retried.Load()
			s.Retried = &n
		}
		if c != nil && c.spec.DLQ != "" {
			n := c.deadLettered.Load()
			s.DLQ = &n
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
		loops.Go(func() { c.poll(ctx, cl, "fetch", c.handle) })
		if retryCl != nil {
			loops.Go(func() { c.poll(ctx, retryCl, "retry fetch", c.retryRecord) })
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
	var closes sync.WaitGroup // each client leaves its group at once
	for _, k := range clients {
		closes.Go(k.Close)
	}
	closes.Wait()
}
