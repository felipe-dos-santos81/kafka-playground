package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func node(id, typ, data string) Node {
	return Node{ID: id, Type: typ, Data: json.RawMessage(data)}
}

func edge(source, target string) Edge {
	return Edge{ID: source + "-" + target, Source: source, Target: target}
}

// clone deep-copies through JSON so a test can mutate its own flow.
func clone(f Flow) Flow {
	b, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	var c Flow
	if err := json.Unmarshal(b, &c); err != nil {
		panic(err)
	}
	return c
}

// good is a complete producer → topic → consumer flow that deploys cleanly.
var good = Flow{Name: "demo", Nodes: []Node{
	node("producer-1", "producer", `{"source":"manual","value":"{\"id\": {{.Seq}}}"}`),
	node("topic-1", "topic", `{"name":"orders","partitions":3,"replication_factor":1}`),
	node("consumer-1", "consumer", `{"group":"orders-workers","sink":{"kind":"log"}}`),
}, Edges: []Edge{edge("producer-1", "topic-1"), edge("topic-1", "consumer-1")}}

func TestValidate(t *testing.T) {
	topic := func(name string) string {
		return `{"name":"` + name + `","partitions":1,"replication_factor":1}`
	}
	cases := []struct {
		name   string
		level  Level
		mutate func(f *Flow)
		want   string // substring of the single expected message; "" means valid
	}{
		{"valid at deploy", Deploy, func(*Flow) {}, ""},
		{"half built saves", Save, func(f *Flow) { f.Edges = nil; f.Nodes[1].Data = json.RawMessage(`{}`) }, ""},
		{"empty name", Save, func(f *Flow) { f.Name = " " }, "name is required"},
		{"bad node id", Save, func(f *Flow) { f.Nodes = append(f.Nodes, node("Bad Id", "topic", `{}`)) }, "must match"},
		{"duplicate node id", Save, func(f *Flow) { f.Nodes = append(f.Nodes, node("topic-1", "topic", `{}`)) }, "duplicate node id"},
		{"unknown type", Save, func(f *Flow) { f.Nodes = append(f.Nodes, node("sink-1", "sink", `{}`)) }, "unknown node type"},
		{"topic to producer", Save, func(f *Flow) { f.Edges = []Edge{edge("topic-1", "producer-1")} }, "not allowed"},
		{"consumer to consumer", Save, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("consumer-2", "consumer", `{}`))
			f.Edges = []Edge{edge("consumer-1", "consumer-2")}
		}, "not allowed"},
		{"self edge", Save, func(f *Flow) { f.Edges = []Edge{edge("topic-1", "topic-1")} }, "self edge"},
		{"duplicate edge", Save, func(f *Flow) {
			f.Edges = append(f.Edges, Edge{ID: "again", Source: "producer-1", Target: "topic-1"})
		}, "topic-1: duplicate"},
		{"edge without id", Save, func(f *Flow) { f.Edges = append(f.Edges, Edge{Source: "topic-1", Target: "consumer-1"}) }, "id is required"},
		{"duplicate edge id", Save, func(f *Flow) {
			f.Edges = append(f.Edges, Edge{ID: "producer-1-topic-1", Source: "topic-1", Target: "consumer-1"})
		}, "duplicate edge id"},
		{"unknown endpoint", Save, func(f *Flow) { f.Edges[0].Target = "nope" }, "unknown node"},
		{"producer without topic", Deploy, func(f *Flow) { f.Edges = f.Edges[1:] }, "exactly one edge to a topic"},
		{"bad source", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"cron","value":"{}"}`) }, "source must be"},
		{"timer too fast", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"timer","interval_ms":5,"value":"{}"}`) }, "interval_ms"},
		{"value missing", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual"}`) }, "value is required"},
		{"value not json", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual","value":"hello {{.Seq}}"}`) }, "invalid JSON"},
		{"value unknown field", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual","value":"{{.Nope}}"}`) }, "value:"},
		{"key bad template", Deploy, func(f *Flow) { f.Nodes[0].Data = json.RawMessage(`{"source":"manual","value":"{}","key":"{{.Seq"}`) }, "key:"},
		{"topic name invalid", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic("bad topic")) }, "invalid"},
		{"topic name dot", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic(".")) }, "invalid"},
		{"topic name dotdot", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(topic("..")) }, "invalid"},
		{"duplicate topic name", Deploy, func(f *Flow) { f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("orders"))) }, "also used"},
		{"replication factor 2", Deploy, func(f *Flow) {
			f.Nodes[1].Data = json.RawMessage(`{"name":"orders","partitions":1,"replication_factor":2}`)
		}, "replication_factor must be 1"},
		{"partitions 0", Deploy, func(f *Flow) {
			f.Nodes[1].Data = json.RawMessage(`{"name":"orders","partitions":0,"replication_factor":1}`)
		}, "partitions"},
		{"consumer without topic", Deploy, func(f *Flow) { f.Edges = f.Edges[:1] }, "exactly one edge from a topic"},
		{"empty group", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"  "}`) }, "group is required"},
		{"bad offset reset", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","auto_offset_reset":"middle"}`) }, "auto_offset_reset"},
		{"bad sink url", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"http","url":"ftp://x"}}`)
		}, "sink url"},
		{"bad sink kind", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"mail"}}`) }, "sink kind"},
		{"too many instances", Deploy, func(f *Flow) { f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":11}`) }, "instances"},
		{"consumer forwards twice", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("a")), node("topic-3", "topic", topic("b")))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"), edge("consumer-1", "topic-3"))
		}, "at most one"},
		{"transform chain valid", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`), node("topic-2", "topic", topic("orders-archive")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, ""},
		{"transform without output", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"))
		}, "a transform needs"},
		{"transform expr does not compile", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg."}`), node("topic-2", "topic", topic("x")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, "expr: unexpected end of expression"},
		{"transform expr does not compile saves", Save, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg."}`), node("topic-2", "topic", topic("x")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, ""},
		{"transform empty expr", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":" "}`), node("topic-2", "topic", topic("x")))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-2"))
		}, "expr is required"},
		{"forward to the topic it reads", Deploy, func(f *Flow) { f.Edges = append(f.Edges, edge("consumer-1", "topic-1")) }, `forwarding loops back to topic "orders"`},
		{"forward to the topic it reads saves", Save, func(f *Flow) { f.Edges = append(f.Edges, edge("consumer-1", "topic-1")) }, ""},
		{"two consumer cycle", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("archive")), node("consumer-2", "consumer", `{"group":"g2"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"), edge("topic-2", "consumer-2"), edge("consumer-2", "topic-1"))
		}, `forwarding loops back to topic "orders"`},
		{"two consumer cycle saves", Save, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("archive")), node("consumer-2", "consumer", `{"group":"g2"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"), edge("topic-2", "consumer-2"), edge("consumer-2", "topic-1"))
		}, ""},
		{"cycle through a transform", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("transform-1", "transform", `{"expr":"msg"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "transform-1"), edge("transform-1", "topic-1"))
		}, `forwarding loops back to topic "orders"`},
		{"loop names the topic its consumer reads", Deploy, func(f *Flow) {
			// consumer-2 comes first, so the loop is found starting from it: it reads
			// topic-2 ("archive") and forwards to topic-1 ("orders").
			f.Nodes = append([]Node{node("consumer-2", "consumer", `{"group":"g2"}`), node("topic-2", "topic", topic("archive"))}, f.Nodes...)
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"), edge("topic-2", "consumer-2"), edge("consumer-2", "topic-1"))
		}, `forwarding loops back to topic "archive", which it reads`},
		{"instances valid", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":3}`)
		}, ""},
		{"instance container name clash", Deploy, func(f *Flow) {
			f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":2}`)
			f.Nodes = append(f.Nodes, node("consumer-1-2", "consumer", `{"group":"h"}`))
			f.Edges = append(f.Edges, edge("topic-1", "consumer-1-2"))
		}, "-consumer-1-2 is also node consumer-1's"},
		{"forward chain valid", Deploy, func(f *Flow) {
			f.Nodes = append(f.Nodes, node("topic-2", "topic", topic("archive")), node("consumer-2", "consumer", `{"group":"g2"}`))
			f.Edges = append(f.Edges, edge("consumer-1", "topic-2"), edge("topic-2", "consumer-2"))
		}, ""},
		{"data missing", Deploy, func(f *Flow) { f.Nodes[0].Data = nil }, "data is required"},
		{"data wrong shape", Deploy, func(f *Flow) { f.Nodes[1].Data = json.RawMessage(`{"partitions":"three"}`) }, "data:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := clone(good)
			c.mutate(&f)
			ps := Validate(&f, c.level)
			if c.want == "" {
				if len(ps) != 0 {
					t.Fatalf("want valid, got %v", ps)
				}
				return
			}
			if len(ps) != 1 || !strings.Contains(ps[0].Message, c.want) {
				t.Fatalf("want one problem containing %q, got %v", c.want, ps)
			}
		})
	}
}

func TestValidateProblemNamesTheNode(t *testing.T) {
	f := clone(good)
	f.Nodes[2].Data = json.RawMessage(`{"group":""}`)
	ps := Validate(&f, Deploy)
	if len(ps) != 1 || ps[0].Node != "consumer-1" || ps[0].Edge != "" {
		t.Fatalf("want problem on consumer-1, got %+v", ps)
	}
	f = clone(good)
	f.Edges = []Edge{edge("topic-1", "producer-1")}
	ps = Validate(&f, Save)
	if len(ps) != 1 || ps[0].Edge != "topic-1-producer-1" || ps[0].Node != "" {
		t.Fatalf("want problem on the edge, got %+v", ps)
	}
}
