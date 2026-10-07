// Resolve turns a deployable flow into what runs: the topics to create and one
// NodeSpec per producer or consumer container. A node container never sees the
// graph, only its NodeSpec (env STUDIO_NODE).
package main

import (
	"encoding/json"
	"fmt"
	"strconv"
)

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
	Instance        int    `json:"instance,omitempty"`    // consumer: 1..n when it runs n > 1 instances, else 0
}

// containerName is a node container's name and, on the compose network, its host
// name; instance 0 is a node's only container, 1..n one of its instances.
func containerName(flow, node string, instance int) string {
	name := "studio-" + flow + "-" + node
	if instance > 0 {
		name += "-" + strconv.Itoa(instance)
	}
	return name
}

// nodeURL is path on a node container's own API, reached by container name on
// the compose network.
func nodeURL(flow, node string, instance int, path string) string {
	return "http://" + containerName(flow, node, instance) + nodeAddr + path
}

// instancesOf is the instance numbers of a node's containers: [0] for a node
// with one container, 1..n for a consumer with instances: n > 1.
func instancesOf(n Node) []int {
	if n.Type != "consumer" {
		return []int{0}
	}
	var d ConsumerData
	json.Unmarshal(n.Data, &d)
	if d.Instances <= 1 {
		return []int{0}
	}
	is := make([]int, d.Instances)
	for i := range is {
		is[i] = i + 1
	}
	return is
}

// Resolve assumes Validate(&f, Deploy) passed: every data field decodes and every
// producer has one edge to a topic, and every consumer has one edge from a topic
// and at most one out (to a topic or a transform).
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
			for _, i := range instancesOf(dst) {
				spec.Instance = i
				specs = append(specs, spec)
			}
		}
	}
	return specs, topics
}

// clashes names each node whose container would take a name another node's
// container already has: a consumer "consumer-1" with two instances runs
// studio-<flow>-consumer-1-2, which is also a node "consumer-1-2"'s name.
func clashes(specs []NodeSpec) []Problem {
	var ps []Problem
	owner := map[string]string{} // container name → node id
	for _, s := range specs {
		name := containerName(s.Flow, s.Node, s.Instance)
		if other, ok := owner[name]; ok && other != s.Node {
			ps = append(ps, Problem{Node: s.Node, Message: fmt.Sprintf("its container name %s is also node %s's; rename one of them", name, other)})
			continue
		}
		owner[name] = s.Node
	}
	return ps
}

// notYetRunnable lists what a deployable flow uses that this milestone's runtime
// cannot run yet. Each line goes when its milestone lands.
func notYetRunnable(f Flow) []Problem {
	var ps []Problem
	for _, n := range f.Nodes {
		if n.Type == "transform" {
			ps = append(ps, Problem{Node: n.ID, Message: "transforms run from M5"})
		}
	}
	return ps
}
