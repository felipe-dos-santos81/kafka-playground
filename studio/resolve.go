// Resolve turns a deployable flow into what runs: the topics to create and one
// NodeSpec per producer or consumer container. A node container never sees the
// graph, only its NodeSpec (env STUDIO_NODE).
package main

import "encoding/json"

// NodeSpec is the whole configuration of one node container.
type NodeSpec struct {
	Flow            string `json:"flow"`
	Node            string `json:"node"`
	Type            string `json:"type"`  // "producer" or "consumer"
	Topic           string `json:"topic"` // produced to, or consumed from
	Group           string `json:"group,omitempty"`
	AutoOffsetReset string `json:"auto_offset_reset,omitempty"`
	Key             string `json:"key,omitempty"`         // producer key template
	Value           string `json:"value,omitempty"`       // producer value template
	Source          string `json:"source,omitempty"`      // producer: "manual" or "timer"
	IntervalMS      int    `json:"interval_ms,omitempty"` // producer: the timer's period
}

// containerName is a node's container name and, on the compose network, its host name.
func containerName(flow, node string) string { return "studio-" + flow + "-" + node }

// nodeURL is path on a node container's own API, reached by container name on
// the compose network.
func nodeURL(flow, node, path string) string {
	return "http://" + containerName(flow, node) + nodeAddr + path
}

// Resolve assumes Validate(&f, Deploy) passed: every data field decodes and every
// producer and consumer has exactly one edge to or from a topic.
func Resolve(f Flow) ([]NodeSpec, []TopicData) {
	byID := map[string]Node{}
	topicName := map[string]string{} // topic node id → topic name
	var topics []TopicData
	for _, n := range f.Nodes {
		byID[n.ID] = n
		if n.Type == "topic" {
			var d TopicData
			json.Unmarshal(n.Data, &d)
			topicName[n.ID] = d.Name
			topics = append(topics, d)
		}
	}
	var specs []NodeSpec
	for _, e := range f.Edges {
		src, dst := byID[e.Source], byID[e.Target]
		switch {
		case src.Type == "producer" && dst.Type == "topic":
			var d ProducerData
			json.Unmarshal(src.Data, &d)
			specs = append(specs, NodeSpec{Flow: f.ID, Node: src.ID, Type: "producer", Topic: topicName[dst.ID], Source: d.Source, IntervalMS: d.IntervalMS, Key: d.Key, Value: d.Value})
		case src.Type == "topic" && dst.Type == "consumer":
			var d ConsumerData
			json.Unmarshal(dst.Data, &d)
			specs = append(specs, NodeSpec{Flow: f.ID, Node: dst.ID, Type: "consumer", Topic: topicName[src.ID], Group: d.Group, AutoOffsetReset: d.AutoOffsetReset})
		}
	}
	return specs, topics
}

// notYetRunnable lists what a deployable flow uses that this milestone's runtime
// cannot run yet. Each line goes when its milestone lands.
func notYetRunnable(f Flow) []Problem {
	var ps []Problem
	types := map[string]string{}
	for _, n := range f.Nodes {
		types[n.ID] = n.Type
		switch n.Type {
		case "consumer":
			var d ConsumerData
			json.Unmarshal(n.Data, &d)
			if d.Sink.Kind == "http" {
				ps = append(ps, Problem{Node: n.ID, Message: "the http sink runs from M4; use log for now"})
			}
			if d.Instances > 1 {
				ps = append(ps, Problem{Node: n.ID, Message: "more than one instance runs from M4"})
			}
		}
	}
	for _, e := range f.Edges {
		if types[e.Source] == "consumer" {
			ps = append(ps, Problem{Node: e.Source, Edge: e.ID, Message: "forwarding from a consumer runs from M4 (through a transform from M5)"})
		}
	}
	return ps
}
