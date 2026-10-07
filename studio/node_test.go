package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
		spec: NodeSpec{Topic: "orders", Key: "k{{.Seq}}", Value: `{"id": {{.Seq}}}`},
		tail: &tail{},
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
}
