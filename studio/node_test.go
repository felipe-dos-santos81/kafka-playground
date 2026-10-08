package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
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

// headersOf is a record's headers as a map, for comparing.
func headersOf(r *kgo.Record) map[string]string {
	m := map[string]string{}
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

// With retry and a DLQ, what may pass later goes to the retry topic while tries
// remain, then to the DLQ; what never will goes straight to the DLQ. Each goes as
// read, with the studio-* headers, and the first failure ends the record's path.
func TestConsumerFailures(t *testing.T) {
	ctx := context.Background()
	var sent []*kgo.Record
	sinkUp := false
	tr, err := newTransform(`{qty: msg.qty * 2}`)
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{
		spec: NodeSpec{Topic: "orders", Group: "g", SinkURL: "http://sink", Forward: "archive",
			Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry", Attempts: 2, DelayMS: 100}, DLQ: "orders__dlq"},
		tail:      &tail{},
		counts:    &counters{},
		transform: tr,
		post: func(context.Context, string, []byte) error {
			if sinkUp {
				return nil
			}
			return errors.New("503 Service Unavailable")
		},
		produce: func(_ context.Context, r *kgo.Record) error {
			sent = append(sent, r)
			return nil
		},
	}
	in := &kgo.Record{Topic: "orders", Partition: 2, Offset: 57, Key: []byte("k"), Value: []byte(`{"qty":2}`)}
	if !c.handle(ctx, in) {
		t.Fatal("a record sent on commits")
	}
	if len(sent) != 1 || sent[0].Topic != "orders__retry" || string(sent[0].Key) != "k" || string(sent[0].Value) != `{"qty":2}` {
		t.Fatalf("a sink failure goes to the retry topic as read, and not on to the forward; sent %+v", sent)
	}
	want := map[string]string{"studio-group": "g", "studio-attempt": "1", "studio-error": "sink: 503 Service Unavailable", "studio-origin": "orders[2]@57"}
	if h := headersOf(sent[0]); !reflect.DeepEqual(h, want) {
		t.Fatalf("headers: got %v, want %v", h, want)
	}

	// Read back from the retry topic, it fails again: 2 failed tries, 2 attempts, so
	// one more retry; the third failure is past them: the DLQ.
	for i, wantTopic := range []string{"orders__retry", "orders__dlq"} {
		back := sent[0]
		back.Partition, back.Offset = 0, int64(i)
		sent = nil
		c.handle(ctx, back)
		h := headersOf(sent[0])
		if len(sent) != 1 || sent[0].Topic != wantTopic || h["studio-attempt"] != strconv.Itoa(i+2) || h["studio-origin"] != "orders[2]@57" {
			t.Fatalf("failure %d: want %s with studio-attempt %d and the first origin; sent %+v, headers %v", i+2, wantTopic, i+2, sent, h)
		}
	}

	// A transform error never passes: straight to the DLQ.
	sinkUp, sent = true, nil
	c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{}`)})
	if len(sent) != 1 || sent[0].Topic != "orders__dlq" || headersOf(sent[0])["studio-attempt"] != "1" || !strings.HasPrefix(headersOf(sent[0])["studio-error"], "transform: ") {
		t.Fatalf("a transform error goes straight to the DLQ; sent %+v", sent)
	}

	// A router error neither.
	rt, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	c.transform, c.router, sent = nil, rt, nil
	c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":4}`)})
	if len(sent) != 1 || sent[0].Topic != "orders__dlq" || !strings.HasPrefix(headersOf(sent[0])["studio-error"], "router: rule 1: ") {
		t.Fatalf("a router error goes straight to the DLQ; sent %+v", sent)
	}

	// Retries are tailed but not counted in total again; every failure is an error.
	if s := c.counts.read(); s.Total != 3 || s.Errors != 5 || c.retried.Load() != 2 || c.dead.Load() != 3 || c.tail.last() != 5 {
		t.Fatalf("want total 3, 5 errors, 2 retried, 3 dead-lettered, 5 tailed; got %+v, %d, %d, %d", s, c.retried.Load(), c.dead.Load(), c.tail.last())
	}
	if h := c.tail.since(0)[1].Headers; h["studio-attempt"] != "1" {
		t.Fatalf("the tail keeps a retried record's headers; got %v", h)
	}

	// A send cut by a closed client (Stop) leaves the record uncommitted; any other
	// failed send is an error, and the record commits.
	c.produce = func(context.Context, *kgo.Record) error { return kgo.ErrClientClosed }
	if c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":5}`)}) {
		t.Fatal("a send cut by a closed client must leave the record uncommitted")
	}
	c.produce = func(context.Context, *kgo.Record) error { return errors.New("broker down") }
	if !c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":6}`)}) || !strings.HasPrefix(c.counts.read().LastError, "dlq: broker down") {
		t.Fatalf("a failed send is counted and the record commits; got %+v", c.counts.read())
	}
}

// A consumer reading a DLQ (its own DLQ on) does not take the record's
// studio-attempt for its own tries: only its retry topic's records carry those.
func TestConsumerFailuresOfARecordWithHeaders(t *testing.T) {
	var sent []*kgo.Record
	c := &consumer{
		spec:    NodeSpec{Topic: "orders__dlq", Group: "watch", SinkURL: "http://sink", DLQ: "orders__dlq__dlq"},
		tail:    &tail{},
		counts:  &counters{},
		post:    func(context.Context, string, []byte) error { return errors.New("down") },
		produce: func(_ context.Context, r *kgo.Record) error { sent = append(sent, r); return nil },
	}
	in := &kgo.Record{Topic: "orders__dlq", Value: []byte(`{}`), Headers: []kgo.RecordHeader{
		{Key: "trace", Value: []byte("t1")}, {Key: "studio-group", Value: []byte("g")}, {Key: "studio-attempt", Value: []byte("3")}, {Key: "studio-origin", Value: []byte("orders[0]@1")}}}
	c.handle(context.Background(), in)
	want := map[string]string{"trace": "t1", "studio-group": "watch", "studio-attempt": "1", "studio-error": "sink: down", "studio-origin": "orders[0]@1"}
	if len(sent) != 1 || !reflect.DeepEqual(headersOf(sent[0]), want) {
		t.Fatalf("want its own headers kept, studio-* replaced, the origin kept; got %+v", sent)
	}
}

// A forward failure may pass later: it goes to the retry topic. A forward cut by a
// closed client (Stop) sends nothing on and leaves the record uncommitted, DLQ or not.
func TestConsumerForwardFailures(t *testing.T) {
	var sent []*kgo.Record
	fwdErr := errors.New("broker down")
	c := &consumer{
		spec: NodeSpec{Topic: "orders", Group: "g", Forward: "archive",
			Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry", Attempts: 1, DelayMS: 100}, DLQ: "orders__dlq"},
		tail:   &tail{},
		counts: &counters{},
		produce: func(_ context.Context, r *kgo.Record) error {
			if r.Topic == "archive" {
				return fwdErr
			}
			sent = append(sent, r)
			return nil
		},
	}
	if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"id":1}`)}) {
		t.Fatal("a forward failure sent on commits")
	}
	if len(sent) != 1 || sent[0].Topic != "orders__retry" || !strings.HasPrefix(headersOf(sent[0])["studio-error"], "forward: broker down") {
		t.Fatalf("a forward failure goes to the retry topic; sent %+v", sent)
	}

	fwdErr, sent = kgo.ErrClientClosed, nil
	if c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"id":2}`)}) {
		t.Fatal("a forward cut by a closed client must leave the record uncommitted")
	}
	if len(sent) != 0 {
		t.Fatalf("a forward cut by a closed client sends nothing on; sent %+v", sent)
	}
}

// The retry loop handles only its own group's records, each once it is due; Stop
// during the wait leaves the record unhandled and unmarked.
func TestConsumerRetryRecord(t *testing.T) {
	ctx := context.Background()
	c := &consumer{
		spec:   NodeSpec{Topic: "orders", Group: "g", Retry: &RetrySpec{Topic: "orders__retry", Group: "g__retry", Attempts: 3, DelayMS: 100}, DLQ: "orders__dlq"},
		tail:   &tail{},
		counts: &counters{},
	}
	ours := []kgo.RecordHeader{{Key: "studio-group", Value: []byte("g")}, {Key: "studio-attempt", Value: []byte("1")}}
	retried := func(at time.Time) *kgo.Record {
		return &kgo.Record{Topic: "orders__retry", Value: []byte(`{}`), Headers: ours, Timestamp: at}
	}

	// Another group's record, and one no consumer sent: marked, not handled.
	for _, hs := range [][]kgo.RecordHeader{{{Key: "studio-group", Value: []byte("h")}}, nil} {
		if !c.retryRecord(ctx, &kgo.Record{Topic: "orders__retry", Value: []byte(`{}`), Headers: hs, Timestamp: time.Now()}) {
			t.Fatalf("headers %v: a record that is not ours is marked", hs)
		}
	}
	if c.tail.last() != 0 {
		t.Fatal("a record that is not ours is not handled")
	}

	// Ours, written a second ago: due, handled at once.
	start := time.Now()
	if !c.retryRecord(ctx, retried(start.Add(-time.Second))) || c.tail.last() != 1 || time.Since(start) > 90*time.Millisecond {
		t.Fatalf("a due record is handled at once: tailed %d in %s", c.tail.last(), time.Since(start))
	}

	// Ours, written just now: handled once the 100 ms delay is out.
	start = time.Now()
	if !c.retryRecord(ctx, retried(start)) || c.tail.last() != 2 || time.Since(start) < 90*time.Millisecond {
		t.Fatalf("a record waits out its delay: tailed %d in %s", c.tail.last(), time.Since(start))
	}

	// Ours, dated an hour ahead (written by hand): waits the delay, not the hour.
	start = time.Now()
	if !c.retryRecord(ctx, retried(start.Add(time.Hour))) || c.tail.last() != 3 || time.Since(start) > time.Second {
		t.Fatalf("a record dated in the future waits at most the delay: tailed %d in %s", c.tail.last(), time.Since(start))
	}

	// Stop during the wait: not handled, not marked.
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	if c.retryRecord(stopped, retried(time.Now())) || c.tail.last() != 3 {
		t.Fatal("a wait cut by Stop leaves the record unhandled and unmarked")
	}
	if s := c.counts.read(); s.Total != 0 {
		t.Fatalf("retries are not counted in total: %+v", s)
	}
}
