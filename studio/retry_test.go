package main

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
			Retry: &RetryData{Attempts: 2, DelayMS: 100}, DLQ: true},
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
	if !c.handle(ctx, in, false) {
		t.Fatal("a record sent on commits")
	}
	if len(sent) != 1 || sent[0].Topic != "orders__retry" || string(sent[0].Key) != "k" || string(sent[0].Value) != `{"qty":2}` {
		t.Fatalf("a sink failure goes to the retry topic as read, and not on to the forward; sent %+v", sent)
	}
	want := map[string]string{"studio-group": "g", "studio-attempt": "1", "studio-error": "sink: 503 Service Unavailable", "studio-origin": "orders[2]@57"}
	if h := headerMap(sent[0].Headers); !reflect.DeepEqual(h, want) {
		t.Fatalf("headers: got %v, want %v", h, want)
	}

	// Read back from the retry topic, it fails again: 2 failed tries, 2 attempts, so
	// one more retry; the third failure is past them: the DLQ.
	for i, wantTopic := range []string{"orders__retry", "orders__dlq"} {
		back := sent[0]
		back.Partition, back.Offset = 0, int64(i)
		sent = nil
		c.handle(ctx, back, true) // read back from the retry topic
		h := headerMap(sent[0].Headers)
		if len(sent) != 1 || sent[0].Topic != wantTopic || h["studio-attempt"] != strconv.Itoa(i+2) || h["studio-origin"] != "orders[2]@57" {
			t.Fatalf("failure %d: want %s with studio-attempt %d and the first origin; sent %+v, headers %v", i+2, wantTopic, i+2, sent, h)
		}
	}

	// A transform error never passes: straight to the DLQ.
	sinkUp, sent = true, nil
	c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{}`)}, false)
	if len(sent) != 1 || sent[0].Topic != "orders__dlq" || headerMap(sent[0].Headers)["studio-attempt"] != "1" || !strings.HasPrefix(headerMap(sent[0].Headers)["studio-error"], "transform: ") {
		t.Fatalf("a transform error goes straight to the DLQ; sent %+v", sent)
	}

	// A router error neither.
	rt, err := newRouter([]Route{{When: "msg.total > 100", Topic: "big"}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	c.transform, c.router, sent = nil, rt, nil
	c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":4}`)}, false)
	if len(sent) != 1 || sent[0].Topic != "orders__dlq" || !strings.HasPrefix(headerMap(sent[0].Headers)["studio-error"], "router: rule 1: ") {
		t.Fatalf("a router error goes straight to the DLQ; sent %+v", sent)
	}

	// Retries are tailed but not counted in total again; every failure is an error.
	if s := c.counts.read(); s.Total != 3 || s.Errors != 5 || c.retried.Load() != 2 || c.deadLettered.Load() != 3 || c.tail.last() != 5 {
		t.Fatalf("want total 3, 5 errors, 2 retried, 3 dead-lettered, 5 tailed; got %+v, %d, %d, %d", s, c.retried.Load(), c.deadLettered.Load(), c.tail.last())
	}
	if h := c.tail.since(0)[1].Headers; h["studio-attempt"] != "1" {
		t.Fatalf("the tail keeps a retried record's headers; got %v", h)
	}

	// A send cut by a closed client (Stop) leaves the record uncommitted; any other
	// failed send is an error, and the record commits.
	c.produce = func(context.Context, *kgo.Record) error { return kgo.ErrClientClosed }
	if c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":5}`)}, false) {
		t.Fatal("a send cut by a closed client must leave the record uncommitted")
	}
	c.produce = func(context.Context, *kgo.Record) error { return errors.New("broker down") }
	if !c.handle(ctx, &kgo.Record{Topic: "orders", Value: []byte(`{"id":6}`)}, false) || !strings.HasPrefix(c.counts.read().LastError, "dlq: broker down") {
		t.Fatalf("a failed send is counted and the record commits; got %+v", c.counts.read())
	}
}

// A consumer reading a DLQ (its own DLQ on) does not take the record's
// studio-attempt for its own tries: only its retry topic's records carry those.
func TestConsumerFailuresOfARecordWithHeaders(t *testing.T) {
	var sent []*kgo.Record
	c := &consumer{
		spec:    NodeSpec{Topic: "orders__dlq", Group: "watch", SinkURL: "http://sink", DLQ: true},
		tail:    &tail{},
		counts:  &counters{},
		post:    func(context.Context, string, []byte) error { return errors.New("down") },
		produce: func(_ context.Context, r *kgo.Record) error { sent = append(sent, r); return nil },
	}
	in := &kgo.Record{Topic: "orders__dlq", Value: []byte(`{}`), Headers: []kgo.RecordHeader{
		{Key: "trace", Value: []byte("t1")}, {Key: "studio-foo", Value: []byte("kept")}, {Key: "studio-group", Value: []byte("g")}, {Key: "studio-attempt", Value: []byte("3")}, {Key: "studio-origin", Value: []byte("orders[0]@1")}}}
	c.handle(context.Background(), in, false)
	want := map[string]string{"trace": "t1", "studio-foo": "kept", "studio-group": "watch", "studio-attempt": "1", "studio-error": "sink: down", "studio-origin": "orders[0]@1"}
	if len(sent) != 1 || !reflect.DeepEqual(headerMap(sent[0].Headers), want) {
		t.Fatalf("want its own headers kept (a studio-foo too), the four studio-* headers replaced, the origin kept; got %+v", sent)
	}
}

// A forward failure may pass later: it goes to the retry topic. A forward cut by a
// closed client (Stop) sends nothing on and leaves the record uncommitted, DLQ or not.
func TestConsumerForwardFailures(t *testing.T) {
	var sent []*kgo.Record
	fwdErr := errors.New("broker down")
	c := &consumer{
		spec: NodeSpec{Topic: "orders", Group: "g", Forward: "archive",
			Retry: &RetryData{Attempts: 1, DelayMS: 100}, DLQ: true},
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
	if !c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"id":1}`)}, false) {
		t.Fatal("a forward failure sent on commits")
	}
	if len(sent) != 1 || sent[0].Topic != "orders__retry" || headerMap(sent[0].Headers)["studio-error"] != "forward to archive: broker down" {
		t.Fatalf("a forward failure goes to the retry topic; sent %+v", sent)
	}

	fwdErr, sent = kgo.ErrClientClosed, nil
	if c.handle(context.Background(), &kgo.Record{Topic: "orders", Value: []byte(`{"id":2}`)}, false) {
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
		spec:   NodeSpec{Topic: "orders", Group: "g", Retry: &RetryData{Attempts: 3, DelayMS: 100}, DLQ: true},
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

// studio-error is cut at errorHeaderMax bytes without splitting a character.
func TestFailureHeadersCutAtACharacter(t *testing.T) {
	err := errors.New(strings.Repeat("a", errorHeaderMax-1) + "é and more")
	hs := failureHeaders(&kgo.Record{Topic: "orders"}, "g", 1, err)
	got := headerMap(hs)["studio-error"]
	if got != strings.Repeat("a", errorHeaderMax-1) {
		t.Fatalf("want the error cut before the split é (%d bytes), got %d bytes ending %q", errorHeaderMax-1, len(got), got[len(got)-3:])
	}
}
