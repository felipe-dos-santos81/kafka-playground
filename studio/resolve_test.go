package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	specs, topics := Resolve(f)
	want := []NodeSpec{
		{Flow: "0a1b2c3d", Node: "producer-1", Type: "producer", Topic: "orders", Source: "manual", Value: `{"id": {{.Seq}}}`},
		{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "orders-workers"},
	}
	if !reflect.DeepEqual(specs, want) {
		t.Fatalf("specs:\n got %+v\nwant %+v", specs, want)
	}
	if want := []TopicData{{Name: "orders", Partitions: 3, ReplicationFactor: 1}}; !reflect.DeepEqual(topics, want) {
		t.Fatalf("topics: got %+v, want %+v", topics, want)
	}
	if got := containerName("0a1b2c3d", "consumer-1"); got != "studio-0a1b2c3d-consumer-1" {
		t.Fatalf("containerName: %s", got)
	}
}

func TestResolveConsumerSinkAndForward(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"http://studio:8082/x"}}`)
	f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
	f.Edges = append(f.Edges, edge("consumer-1", "topic-2"))
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Forward: "archive", SinkURL: "http://studio:8082/x"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("consumer spec:\n got %+v\nwant %+v", specs, want)
	}
	if len(topics) != 2 {
		t.Fatalf("want both topics created, got %+v", topics)
	}
}

func TestNotYetRunnable(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *Flow)
		node   string // the node of the single expected problem; "" means runnable
	}{
		{"manual producer and log consumer", func(*Flow) {}, ""},
		{"timer producer runs", func(f *Flow) {
			f.Nodes[0].Data = json.RawMessage(`{"source":"timer","interval_ms":100,"value":"{}"}`)
		}, ""},
		{"http sink runs", func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"http://x"}}`)
		}, ""},
		{"two instances", func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":2,"sink":{"kind":"log"}}`)
		}, "consumer-1"},
		{"forward to a topic runs", func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"))
		}, ""},
		{"transform", func(f *Flow) {
			f.Nodes = append(f.Nodes,
				node("transform-1", "transform", `{"expr":"msg"}`),
				node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, "transform-1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := clone(good)
			c.mutate(&f)
			ps := notYetRunnable(f)
			if c.node == "" {
				if ps != nil {
					t.Fatalf("want runnable, got %v", ps)
				}
				return
			}
			if len(ps) != 1 || ps[0].Node != c.node || !strings.Contains(ps[0].Message, " M") {
				t.Fatalf("want one problem on %s naming its milestone, got %v", c.node, ps)
			}
		})
	}
}
