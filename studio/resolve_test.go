package main

import (
	"encoding/json"
	"reflect"
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
	if got := (nodeRef{"0a1b2c3d", "consumer-1", 0}).name(); got != "studio-0a1b2c3d-consumer-1" {
		t.Fatalf("container name: %s", got)
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

func TestResolveInstances(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":3,"sink":{"kind":"log"}}`)
	specs, _ := Resolve(f)
	if len(specs) != 4 {
		t.Fatalf("want the producer and three consumer instances, got %+v", specs)
	}
	for i, s := range specs[1:] {
		want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: i + 1}
		if !reflect.DeepEqual(s, want) {
			t.Fatalf("instance %d:\n got %+v\nwant %+v", i+1, s, want)
		}
	}
	if got := (nodeRef{"0a1b2c3d", "consumer-1", 2}).name(); got != "studio-0a1b2c3d-consumer-1-2" {
		t.Fatalf("container name of instance 2: %s", got)
	}
}

func TestResolveTransform(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes = append(f.Nodes,
		node("transform-1", "transform", `{"expr":"{id: msg.id}"}`),
		node("topic-2", "topic", `{"name":"archive","partitions":1,"replication_factor":1}`))
	f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "orders-workers", Forward: "archive", Transform: "{id: msg.id}", TransformNode: "transform-1"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("a transform runs in its consumer, which forwards to the transform's topic:\n got %+v\nwant %+v", specs, want)
	}
	if len(topics) != 2 {
		t.Fatalf("want both topics created, got %+v", topics)
	}
}

func TestResolveRouter(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`))
	f.Edges = append(f.Edges, edge("consumer-1", "transform-1"))
	addRouterAfter(&f, "transform-1", `{"rules":[{"when":"msg.total > 100","to":"topic-2"},{"when":"true","to":"topic-3"}],"default":"topic-2"}`, "topic-2", "topic-3")
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "orders-workers",
		Transform: "msg", TransformNode: "transform-1",
		Routes: []Route{{When: "msg.total > 100", Topic: "topic-2"}, {When: "true", Topic: "topic-3"}}, RouteDefault: "topic-2", RouterNode: "router-1"}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("a router runs in its consumer, after its transform, with its rules as topic names:\n got %+v\nwant %+v", specs, want)
	}
	if len(topics) != 3 {
		t.Fatalf("want all three topics created, got %+v", topics)
	}
}

func TestResolveRetryAndDLQ(t *testing.T) {
	f := clone(good)
	f.ID = "0a1b2c3d"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","retry":{"attempts":3,"delay_ms":5000},"dlq":true}`)
	f.Nodes = append(f.Nodes, node("topic-2", "topic", `{"name":"orders__dlq","partitions":1,"replication_factor":1}`))
	specs, topics := Resolve(f)
	want := NodeSpec{Flow: "0a1b2c3d", Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g",
		Retry: &RetryData{Attempts: 3, DelayMS: 5000}, DLQ: true}
	if len(specs) != 2 || !reflect.DeepEqual(specs[1], want) {
		t.Fatalf("the consumer's spec names its retry topic and group and its DLQ:\n got %+v\nwant %+v", specs, want)
	}
	wantTopics := []TopicData{
		{Name: "orders", Partitions: 3, ReplicationFactor: 1},
		{Name: "orders__dlq", Partitions: 1, ReplicationFactor: 1}, // drawn as a node: created once, as drawn
		{Name: "orders__retry", Partitions: 3, ReplicationFactor: 1},
	}
	if !reflect.DeepEqual(topics, wantTopics) {
		t.Fatalf("topics:\n got %+v\nwant %+v", topics, wantTopics)
	}
}
