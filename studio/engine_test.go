package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

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
}
