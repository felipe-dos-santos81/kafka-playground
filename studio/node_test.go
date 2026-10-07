package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTailKeepsTheLastHundred(t *testing.T) {
	tl := &tail{}
	for range 150 {
		tl.push(&kgo.Record{Value: []byte(`{}`)})
	}
	all := tl.since(0)
	if len(all) != tailSize || all[0].Seq != 51 || all[len(all)-1].Seq != 150 {
		t.Fatalf("want seq 51..150, got %d entries starting at %d", len(all), all[0].Seq)
	}
	if got := tl.since(140); len(got) != 10 || got[0].Seq != 141 {
		t.Fatalf("since 140: want 141..150, got %+v", got)
	}
	if got := tl.since(150); got == nil || len(got) != 0 {
		t.Fatalf("since the last seq: want an empty list, got %#v", got)
	}
	tl.push(&kgo.Record{Value: []byte(strings.Repeat("x", tailValueMax+10))})
	if got := tl.since(150); len(got[0].Value) != tailValueMax {
		t.Fatalf("want the value cut to %d bytes, got %d", tailValueMax, len(got[0].Value))
	}
}

func TestProducerSend(t *testing.T) {
	var sent []*kgo.Record
	brokerDown := false
	p := &producer{
		spec:   NodeSpec{Topic: "orders", Key: "k{{.Seq}}", Value: `{"id": {{.Seq}}}`},
		tail:   &tail{},
		counts: &counters{},
		produce: func(_ context.Context, r *kgo.Record) error {
			if brokerDown {
				return errors.New("broker down")
			}
			r.Partition, r.Offset = 2, int64(len(sent))
			sent = append(sent, r)
			return nil
		},
	}
	post := func(query, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		p.send(w, httptest.NewRequest(http.MethodPost, "/send"+query, strings.NewReader(body)))
		return w
	}

	// An empty body renders the node's templates with the next .Seq.
	for i, want := range []struct{ value, key string }{{`{"id": 1}`, "k1"}, {`{"id": 2}`, "k2"}} {
		if w := post("", ""); w.Code != http.StatusOK {
			t.Fatalf("template send %d: %d %s", i+1, w.Code, w.Body)
		}
		if r := sent[i]; string(r.Value) != want.value || string(r.Key) != want.key || r.Topic != "orders" {
			t.Fatalf("template send %d: got %s %q=%q", i+1, r.Topic, r.Key, r.Value)
		}
	}
	// A body is the value as is; the key comes from ?key=.
	if w := post("?key=a", `{"x": 1}`); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"partition":2`) {
		t.Fatalf("body send: %d %s", w.Code, w.Body)
	}
	if r := sent[2]; string(r.Key) != "a" || string(r.Value) != `{"x": 1}` {
		t.Fatalf("body send produced %q=%q", r.Key, r.Value)
	}
	if w := post("", "{"); w.Code != http.StatusBadRequest || len(sent) != 3 {
		t.Fatalf("invalid JSON: want 400 and nothing produced, got %d with %d records", w.Code, len(sent))
	}
	brokerDown = true
	if w := post("", `{}`); w.Code != http.StatusBadGateway {
		t.Fatalf("broker down: want 502, got %d %s", w.Code, w.Body)
	}
	if got := p.tail.since(0); len(got) != 3 {
		t.Fatalf("want the 3 produced records in the tail, got %d", len(got))
	}
	if s := p.counts.stats("b", p.tail.last()); s.Total != 3 || s.Errors != 1 || s.LastError != "broker down" || s.TailSeq != 3 || s.Boot != "b" {
		t.Fatalf("want 3 produced, 1 error, tailSeq 3; got %+v", s)
	}
}

func TestProducerTimer(t *testing.T) {
	got := make(chan *kgo.Record, 16)
	p := &producer{
		spec:   NodeSpec{Topic: "orders", Value: `{"n": {{.Seq}}}`},
		tail:   &tail{},
		counts: &counters{},
		produce: func(_ context.Context, r *kgo.Record) error {
			got <- r
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.run(ctx, 5*time.Millisecond)
		close(done)
	}()
	for i := 1; i <= 3; i++ {
		select {
		case r := <-got:
			if want := fmt.Sprintf(`{"n": %d}`, i); string(r.Value) != want || r.Topic != "orders" {
				t.Fatalf("record %d: got %s %s, want %s", i, r.Topic, r.Value, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("the timer produced only %d records", i-1)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the timer did not stop when its context ended")
	}
	if p.counts.total.Load() < 3 || p.tail.last() < 3 {
		t.Fatalf("want at least 3 counted and tailed, got %d and %d", p.counts.total.Load(), p.tail.last())
	}
}

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

	// A stop cancels the context mid-batch: the record is still posted and forwarded.
	before := s.Errors
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	c.handle(cancelled, &kgo.Record{Topic: "orders", Key: []byte("k4"), Value: []byte(`{"id":4}`)})
	if len(posted) != 4 || len(forwarded) != 3 || string(forwarded[2].Key) != "k4" {
		t.Fatalf("a cancelled context must not skip the record: posted %q, forwarded %d", posted, len(forwarded))
	}
	if after := c.counts.stats("b", c.tail.last()); after.Total != 4 || after.Errors != before {
		t.Fatalf("a cancelled context must add no errors: %+v", after)
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
