package main

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// A lone topic validates for Deploy but has nothing to start; Deploy must say so
// before it touches Docker or Kafka (both nil here).
func TestDeployNothingToRun(t *testing.T) {
	e := &Engine{store: Store{dir: t.TempDir()}}
	f := Flow{ID: "0123abcd", Name: "lone", Nodes: []Node{
		node("topic-1", "topic", `{"name":"orders","partitions":3,"replication_factor":1}`),
	}}
	if err := e.store.Put(f); err != nil {
		t.Fatal(err)
	}
	var ps Problems
	if err := e.Deploy(context.Background(), f.ID); !errors.As(err, &ps) {
		t.Fatalf("want Problems, got %v", err)
	}
}

func TestApplyKafka(t *testing.T) {
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"producer-1": {State: "running", Total: 9},
		"consumer-1": {State: "running", Total: 4},
		"consumer-2": {State: "running"},
	}}
	specs := []NodeSpec{
		{Node: "producer-1", Type: "producer", Topic: "orders"},
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g"},
		{Node: "consumer-2", Type: "consumer", Topic: "orders", Group: "g"},
	}
	topics := map[string]TopicData{"topic-1": {Name: "orders", Partitions: 2}, "topic-2": {Name: "gone", Partitions: 1}}
	one := &kadm.DescribedGroupMember{ClientID: containerName("f", "consumer-1")}
	two := &kadm.DescribedGroupMember{ClientID: containerName("f", "consumer-2")}
	lags := kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": {
		0: {Topic: "orders", Partition: 0, Lag: 2, Member: one},
		1: {Topic: "orders", Partition: 1, Lag: 3, Member: two},
		2: {Topic: "orders", Partition: 2, Lag: -1, Member: one, Err: errors.New("no end offset")},
	}}}}
	ends := kadm.ListedOffsets{
		"orders": {0: {Partition: 0, Offset: 10}, 1: {Partition: 1, Offset: 5}, 2: {Partition: 2, Offset: 7}},
		"gone":   {-1: {Partition: -1, Offset: -1, Err: kerr.UnknownTopicOrPartition}},
	}
	applyKafka(&st, "f", specs, topics, lags, ends)

	c1, c2 := st.Nodes["consumer-1"], st.Nodes["consumer-2"]
	if c1.Total != 4 || c1.Lag == nil || *c1.Lag != 5 || !reflect.DeepEqual(c1.Assigned, map[string][]int32{"orders": {0, 2}}) {
		t.Fatalf("consumer-1: want total 4, lag 5 (errored partition skipped), assigned [0 2]; got %+v lag %v", c1, c1.Lag)
	}
	if c2.Lag == nil || *c2.Lag != 5 || !reflect.DeepEqual(c2.Assigned, map[string][]int32{"orders": {1}}) {
		t.Fatalf("consumer-2: want the group's lag 5 and assigned [1]; got %+v", c2)
	}
	if p := st.Nodes["producer-1"]; p.Total != 9 || p.Lag != nil {
		t.Fatalf("producer-1 must be left as it was, got %+v", p)
	}
	t1 := st.Nodes["topic-1"]
	if t1.State != "ready" || t1.Partitions != 3 || t1.EndOffset != 22 || !strings.Contains(t1.Warning, "3 partitions") || !strings.Contains(t1.Warning, "asks for 2") {
		t.Fatalf("topic-1: want ready, 3 partitions, end 22 and a warning naming 3 and 2; got %+v", t1)
	}
	if t2 := st.Nodes["topic-2"]; t2.State != "missing" || t2.Partitions != 0 {
		t.Fatalf("topic-2: want missing, got %+v", t2)
	}

	// The broker did not answer: container states stay, nothing is invented.
	bare := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
	applyKafka(&bare, "f", specs, topics, nil, nil)
	if len(bare.Nodes) != 1 || bare.Nodes["consumer-1"].Lag != nil {
		t.Fatalf("no Kafka answers: want the snapshot unchanged, got %+v", bare.Nodes)
	}

	// A topic with a partition whose leader did not answer is left out, with no warning.
	topics["topic-3"] = TopicData{Name: "flaky", Partitions: 2}
	ends["flaky"] = map[int32]kadm.ListedOffset{0: {Partition: 0, Offset: 4}, 1: {Partition: 1, Offset: -1, Err: errors.New("leader not available")}}
	st = FlowState{Status: "running", Nodes: map[string]NodeState{}}
	applyKafka(&st, "f", specs, topics, nil, ends)
	if n, ok := st.Nodes["topic-3"]; ok {
		t.Fatalf("topic-3 has a failed partition: want it left out, got %+v", n)
	}
	if n := st.Nodes["topic-1"]; n.State != "ready" {
		t.Fatalf("topic-1 is unaffected by topic-3's failure, got %+v", n)
	}

	// A non-nil but empty answer means the broker answered and knows no such topic
	// (a failed call is dropped by Snapshot, so it never reaches here as empty).
	empty := FlowState{Status: "running", Nodes: map[string]NodeState{}}
	applyKafka(&empty, "f", specs, topics, nil, kadm.ListedOffsets{})
	if n := empty.Nodes["topic-1"]; n.State != "missing" {
		t.Fatalf("empty end offsets: want topic-1 missing, got %+v", n)
	}
}

func TestWithRates(t *testing.T) {
	prev := FlowState{Nodes: map[string]NodeState{
		"steady":    {Boot: "a", Total: 10},
		"restarted": {Boot: "a", Total: 10},
		"fell":      {Boot: "a", Total: 50},
	}}
	cur := FlowState{Nodes: map[string]NodeState{
		"steady":    {Boot: "a", Total: 31},
		"restarted": {Boot: "b", Total: 3},
		"fell":      {Boot: "a", Total: 40},
		"new":       {Boot: "a", Total: 5},
		"topic-1":   {State: "ready", EndOffset: 99},
	}}
	withRates(&cur, prev, 2)
	for node, want := range map[string]float64{"steady": 10.5, "restarted": 0, "fell": 0, "new": 0, "topic-1": 0} {
		if got := cur.Nodes[node].Rate; got != want {
			t.Errorf("%s: rate %v, want %v", node, got, want)
		}
	}
}

func TestStreamTicks(t *testing.T) {
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	snap := func(context.Context) (FlowState, error) {
		calls++
		switch calls {
		case 1:
			return FlowState{Status: "running", Nodes: map[string]NodeState{"p": {State: "running", Boot: "a"}}}, nil
		case 2:
			return FlowState{}, errors.New("docker: down")
		case 3:
			return FlowState{Status: "running", Nodes: map[string]NodeState{"p": {State: "running", Boot: "a", Total: 5}}}, nil
		}
		cancel() // the browser went away
		return FlowState{}, context.Canceled
	}
	done := make(chan struct{})
	go func() {
		streamTicks(ctx, &out, func() error { return nil }, snap, time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the stream did not end with its context")
	}
	s := out.String()
	if strings.Count(s, "event: tick\ndata: {") != 2 ||
		!strings.Contains(s, "event: problem\ndata: {\"error\":\"docker: down\"}\n\n") ||
		strings.Count(s, `"rate":`) != 1 {
		t.Fatalf("want two ticks around a problem event, the second tick with a rate; got:\n%s", s)
	}
}
