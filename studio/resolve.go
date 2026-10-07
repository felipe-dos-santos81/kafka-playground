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
	Forward         string `json:"forward,omitempty"`     // consumer: the topic it forwards every record to
	SinkURL         string `json:"sink_url,omitempty"`    // consumer: the http sink's URL
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
	forward := map[string]string{}   // consumer node id → the topic it forwards to
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
	for _, e := range f.Edges {
		if byID[e.Source].Type == "consumer" && byID[e.Target].Type == "topic" {
			forward[e.Source] = topicName[e.Target]
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
			spec := NodeSpec{Flow: f.ID, Node: dst.ID, Type: "consumer", Topic: topicName[src.ID], Group: d.Group, AutoOffsetReset: d.AutoOffsetReset, Forward: forward[dst.ID]}
			if d.Sink.Kind == "http" {
				spec.SinkURL = d.Sink.URL
			}
			specs = append(specs, spec)
		}
	}
	return specs, topics
}

// notYetRunnable lists what a deployable flow uses that this milestone's runtime
// cannot run yet. Each line goes when its milestone lands.
func notYetRunnable(f Flow) []Problem {
	var ps []Problem
	for _, n := range f.Nodes {
		switch n.Type {
		case "consumer":
			var d ConsumerData
			json.Unmarshal(n.Data, &d)
			if d.Instances > 1 {
				ps = append(ps, Problem{Node: n.ID, Message: "more than one instance runs from M4"})
			}
		case "transform":
			ps = append(ps, Problem{Node: n.ID, Message: "transforms run from M5"})
		}
	}
	return ps
}
