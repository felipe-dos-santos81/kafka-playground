// `studio node`: one producer or consumer of a deployed flow, in its own
// container. Its whole configuration is env STUDIO_NODE (a NodeSpec) plus
// KAFKA_BROKERS. It serves the control plane on :9000 inside the compose
// network: POST /send (producers) and GET /tail?since=N.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// producer serves /send for one producer node.
type producer struct {
	spec    NodeSpec
	tail    *tail
	seq     atomic.Int64 // .Seq of the last rendered send
	produce func(context.Context, *kgo.Record) error
}

// send produces one record. A body is the value as is (curl, webhooks), keyed by
// ?key=; an empty body renders the node's own key and value templates with the
// next .Seq (the UI's Send button). It answers {partition, offset}.
func (p *producer) send(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		fail(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	key := r.URL.Query().Get("key")
	if len(bytes.TrimSpace(value)) == 0 {
		d := templateData{Seq: int(p.seq.Add(1)), Now: time.Now().UTC().Format(time.RFC3339), Rand: rand.IntN(1000)}
		v, err := render(p.spec.Value, d)
		if err != nil {
			fail(w, http.StatusInternalServerError, "value template: "+err.Error())
			return
		}
		if key, err = render(p.spec.Key, d); err != nil {
			fail(w, http.StatusInternalServerError, "key template: "+err.Error())
			return
		}
		value = []byte(v)
	}
	if !json.Valid(value) {
		fail(w, http.StatusBadRequest, "value is not valid JSON")
		return
	}
	rec := &kgo.Record{Topic: p.spec.Topic, Value: value}
	if key != "" {
		rec.Key = []byte(key)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := p.produce(ctx, rec); err != nil {
		fail(w, http.StatusBadGateway, err.Error())
		return
	}
	p.tail.push(rec)
	reply(w, http.StatusOK, map[string]any{"partition": rec.Partition, "offset": rec.Offset})
}

// consume polls the group until ctx ends, feeding every record to the tail.
func consume(ctx context.Context, cl *kgo.Client, t *tail) {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			return
		}
		fs.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch %s[%d]: %v", topic, partition, err)
		})
		fs.EachRecord(t.push)
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

	t := &tail{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /tail", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		reply(w, http.StatusOK, t.since(since))
	})
	if spec.Type == "producer" {
		p := &producer{spec: spec, tail: t, produce: func(ctx context.Context, rec *kgo.Record) error {
			return cl.ProduceSync(ctx, rec).FirstErr()
		}}
		mux.HandleFunc("POST /send", p.send)
	} else {
		mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
			fail(w, http.StatusConflict, "only producer nodes send")
		})
		go consume(ctx, cl, t)
	}
	srv := &http.Server{Addr: nodeAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("node %s (%s) on topic %s", spec.Node, spec.Type, spec.Topic)

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	cl.Close()
}
