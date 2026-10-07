// `studio node`: one producer or consumer of a deployed flow, in its own
// container. Its whole configuration is env STUDIO_NODE (a NodeSpec) plus
// KAFKA_BROKERS. It serves the control plane on :9000 inside the compose
// network: POST /send (producers), GET /tail?since=N and GET /stats. A consumer
// also posts each record to its http sink and forwards it to its next topic.
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
	produce := func(ctx context.Context, rec *kgo.Record) error {
		return cl.ProduceSync(ctx, rec).FirstErr()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /tail", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		reply(w, http.StatusOK, t.since(since))
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, counts.stats(boot, t.last()))
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
		c := &consumer{spec: spec, tail: t, counts: counts, post: postJSON, produce: produce}
		go c.consume(ctx, cl)
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
