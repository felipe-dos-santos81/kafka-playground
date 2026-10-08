package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	// Templates that render no JSON are the node's error: 500, counted, nothing produced.
	bad := &producer{spec: NodeSpec{Topic: "orders", Value: `{{.Seq}}x`}, tail: &tail{}, counts: &counters{}, produce: p.produce}
	w := httptest.NewRecorder()
	bad.send(w, httptest.NewRequest(http.MethodPost, "/send", nil))
	if s := bad.counts.read(); w.Code != http.StatusInternalServerError || s.Errors != 1 || !strings.Contains(s.LastError, `invalid JSON: "1x"`) || len(sent) != 3 {
		t.Fatalf("a template rendering 1x: want 500 and one error counted, got %d %s and %+v", w.Code, w.Body, s)
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
		// Like ProduceSync: a full channel (the test stopped reading) blocks until
		// the context ends, and then fails with the context's error.
		produce: func(ctx context.Context, r *kgo.Record) error {
			select {
			case got <- r:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
	if s := p.counts.read(); s.Total < 3 || p.tail.last() < 3 || s.Errors != 0 {
		t.Fatalf("want at least 3 counted and tailed, and a send cut by the stop not counted as an error; got %+v, tail %d", s, p.tail.last())
	}

	// A value that renders no JSON is the timer's error, tick after tick; nothing is produced.
	bad := &producer{spec: NodeSpec{Topic: "orders", Value: `{{.Seq}}x`}, tail: &tail{}, counts: &counters{}, produce: p.produce}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go bad.run(ctx, 5*time.Millisecond)
	for deadline := time.Now().Add(2 * time.Second); bad.counts.errors.Load() < 2 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	if s := bad.counts.read(); s.Errors < 2 || !strings.Contains(s.LastError, "invalid JSON") || bad.tail.last() != 0 {
		t.Fatalf("want the timer's errors counted and nothing produced, got %+v", s)
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
	if s.Total != 3 || s.Errors != 2 || !strings.HasPrefix(s.LastError, "forward to archive: ") || s.TailSeq != 3 {
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

// Only a forward cut by a closed client leaves the record uncommitted.
func TestConsumerHandleCommitDecision(t *testing.T) {
	var fwdErr error
	c := &consumer{
		spec:    NodeSpec{Topic: "orders", Forward: "archive"},
		tail:    &tail{},
		counts:  &counters{},
		produce: func(context.Context, *kgo.Record) error { return fwdErr },
	}
	rec := &kgo.Record{Topic: "orders", Value: []byte(`{}`)}
	for _, tc := range []struct {
		err  error
		want bool
	}{{nil, true}, {errors.New("broker down"), true}, {kgo.ErrClientClosed, false}, {fmt.Errorf("wrapped: %w", kgo.ErrClientClosed), false}} {
		fwdErr = tc.err
		if got := c.handle(context.Background(), rec); got != tc.want {
			t.Errorf("forward error %v: handle = %v, want %v", tc.err, got, tc.want)
		}
	}
	if s := c.counts.stats("b", 0); s.Total != 4 || s.Errors != 3 {
		t.Fatalf("every failure is still counted: %+v", s)
	}
}

func TestPostJSONDoesNotFollowRedirects(t *testing.T) {
	var gets int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/moved" {
			gets++
			return
		}
		http.Redirect(w, r, "/moved", http.StatusFound)
	}))
	defer srv.Close()
	err := postJSON(context.Background(), srv.URL, []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "302 Found") || gets != 0 {
		t.Fatalf("want an error \"answered 302 Found\" and no follow-up request; got %v, %d follow-ups", err, gets)
	}
}

func TestConsumerTransform(t *testing.T) {
	var forwarded []string
	tr, err := newTransform(`msg.qty > 0 ? {id: msg.id, total: msg.qty * msg.price} : nil`)
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{
		spec:      NodeSpec{Topic: "orders", Forward: "totals"},
		tail:      &tail{},
		counts:    &counters{},
		transform: tr,
		produce: func(_ context.Context, r *kgo.Record) error {
			forwarded = append(forwarded, string(r.Key)+"="+string(r.Value))
			return nil
		},
	}
	for _, v := range []string{`{"id":3,"qty":1}`, `{"id":1,"qty":2,"price":3}`, `{"id":2,"qty":0}`} {
		if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Key: []byte("k"), Value: []byte(v)}) {
			t.Fatalf("%s: a transform outcome never holds back the commit", v)
		}
	}
	// The first fails (no price: 1 * nil), and the VM it ran on still runs the
	// second, forwarded transformed and with its key; the third is dropped (nil).
	if len(forwarded) != 1 || forwarded[0] != `k={"id":1,"total":6}` {
		t.Fatalf("want only k={\"id\":1,\"total\":6} forwarded, got %q", forwarded)
	}
	if step := tr.counts.read(); step.Total != 3 || step.Errors != 1 {
		t.Fatalf("the transform got 3 records and failed on 1; got %+v", step)
	}
	if s := c.counts.stats("b", c.tail.last()); s.Total != 3 || s.Errors != 1 || s.TailSeq != 3 || !strings.HasPrefix(s.LastError, "transform: ") {
		t.Fatalf("the consumer still counts and tails every record, and counts the transform's failure as its own; got %+v", s)
	}
}

func TestConsumerRouter(t *testing.T) {
	var forwarded []string
	tr, err := newTransform(`msg.qty > 0 ? {id: msg.id, total: msg.qty * msg.price} : nil`)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{
		spec:      NodeSpec{Topic: "orders"},
		tail:      &tail{},
		counts:    &counters{},
		transform: tr,
		router:    rt,
		produce: func(_ context.Context, r *kgo.Record) error {
			forwarded = append(forwarded, r.Topic+":"+string(r.Key)+"="+string(r.Value))
			return nil
		},
	}
	for _, v := range []string{`{"id":1,"qty":2,"price":100}`, `{"id":2,"qty":1,"price":5}`, `{"id":3,"qty":1,"price":"x"}`, `{"id":4,"qty":0}`} {
		if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Key: []byte("k"), Value: []byte(v)}) {
			t.Fatalf("%s: a router outcome never holds back the commit", v)
		}
	}
	// The first is routed transformed and with its key; the second matches no rule
	// and has no default: dropped. The third fails in the transform (1 * "x") and
	// the fourth is dropped by it (nil): neither reaches the router.
	if len(forwarded) != 1 || forwarded[0] != `big:k={"id":1,"total":200}` {
		t.Fatalf(`want only big:k={"id":1,"total":200} forwarded, got %q`, forwarded)
	}
	if r := rt.read(); r.Total != 2 || r.Unmatched != 1 || r.Errors != 0 || r.Branches[0] != 1 {
		t.Fatalf("the router got 2 records: 1 routed by rule 1, 1 unmatched; got %+v", r)
	}

	failing, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	c.transform, c.router, forwarded = nil, failing, nil
	if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"id":4}`)}) {
		t.Fatal("a router error never holds back the commit")
	}
	if len(forwarded) != 0 {
		t.Fatalf("a failing rule routes the record nowhere, not to the default; got %q", forwarded)
	}
	if s := c.counts.read(); !strings.HasPrefix(s.LastError, "router: rule 1: ") {
		t.Fatalf("the consumer counts the router's failure as its own; got %+v", s)
	}
}

// The main loop and the retry loop handle records at once: a slow sink in one
// does not hold up the other, and the transform and router stay safe (-race).
func TestConsumerHandlesConcurrently(t *testing.T) {
	tr, err := newTransform(`{qty: msg.qty * 2}`)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := newRouter([]Route{{When: "msg.qty > 2", Topic: "big"}}, "small")
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	c := &consumer{
		spec:      NodeSpec{Topic: "orders", Group: "g", SinkURL: "http://sink"},
		tail:      &tail{},
		counts:    &counters{},
		transform: tr,
		router:    rt,
		post: func(_ context.Context, _ string, body []byte) error {
			if string(body) == `{"qty":0}` {
				<-release // the slow one
			}
			return nil
		},
		produce: func(context.Context, *kgo.Record) error { return nil },
	}
	slow := make(chan struct{})
	go func() {
		c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"qty":0}`)})
		close(slow)
	}()
	var loops sync.WaitGroup // two loops at once, both on the transform and the router
	for range 2 {
		loops.Go(func() {
			for i := 1; i <= 50; i++ {
				c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(fmt.Sprintf(`{"qty":%d}`, i))})
			}
		})
	}
	done := make(chan struct{})
	go func() {
		loops.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("records behind a slow sink in the other loop never finished")
	}
	close(release)
	<-slow
	if r := rt.read(); r.Total != 101 || r.Errors != 0 {
		t.Fatalf("the router got every record once, without errors; got %+v", r)
	}
}
