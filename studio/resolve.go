// Resolve turns a deployable flow into what runs: the topics to create and one
// NodeSpec per producer or consumer container. A node container never sees the
// graph, only its NodeSpec (env STUDIO_NODE).
package main

import (
	"encoding/json"
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
	Key             string `json:"key,omitempty"`            // producer key template
	Value           string `json:"value,omitempty"`          // producer value template
	Source          string `json:"source,omitempty"`         // producer: "manual" or "timer"
	IntervalMS      int    `json:"interval_ms,omitempty"`    // producer: the timer's period
	Forward         string `json:"forward,omitempty"`        // consumer: the topic it forwards every record to
	SinkURL         string `json:"sink_url,omitempty"`       // consumer: the http sink's URL
	Instance        int    `json:"instance,omitempty"`       // consumer: 1..n when it runs n > 1 instances, else 0
	Transform       string `json:"transform,omitempty"`      // consumer: the expr its records pass through before the forward
	TransformNode   string `json:"transform_node,omitempty"` // consumer: that transform's node id, which its counts are reported under
}

// nodeRef names one node container: instance 0 is a node's only container, 1..n
// one of a consumer's instances.
type nodeRef struct {
	flow, node string
	instance   int
}

// name is the container's name and, on the compose network, its host name.
func (r nodeRef) name() string {
	name := "studio-" + r.flow + "-" + r.node
	if r.instance > 0 {
		name += "-" + strconv.Itoa(r.instance)
	}
	return name
}

// url is path on the container's own API, reached by name on the compose network.
func (r nodeRef) url(path string) string {
	return "http://" + r.name() + nodeAddr + path
}

// ref is the container this spec runs in.
func (s NodeSpec) ref() nodeRef { return nodeRef{s.Flow, s.Node, s.Instance} }

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
	nums := make([]int, d.Instances)
	for i := range nums {
		nums[i] = i + 1
	}
	return nums
}

// Resolve assumes Validate(&f, Deploy) passed: every data field decodes and every
// producer has one edge to a topic, every consumer has one edge from a topic and
// at most one out (to a topic or a transform), and every transform one edge in
// from a consumer and one out to a topic. A consumer's transform runs in its own
// containers, so a transform has no spec of its own.
func Resolve(f Flow) ([]NodeSpec, []TopicData) {
	byID := map[string]Node{}
	topicName := map[string]string{} // topic node id → topic name
	out := map[string]string{}       // consumer or transform node id → the node its edge goes to
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
		if t := byID[e.Source].Type; t == "consumer" || t == "transform" {
			out[e.Source] = e.Target
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
			spec := NodeSpec{Flow: f.ID, Node: dst.ID, Type: "consumer", Topic: topicName[src.ID], Group: d.Group, AutoOffsetReset: d.AutoOffsetReset}
			if d.Sink.Kind == "http" {
				spec.SinkURL = d.Sink.URL
			}
			next := out[dst.ID] // a topic, a transform, or "" when the consumer forwards nothing
			if t := byID[next]; t.Type == "transform" {
				var td TransformData
				json.Unmarshal(t.Data, &td)
				spec.Transform, spec.TransformNode = td.Expr, t.ID
				next = out[t.ID]
			}
			spec.Forward = topicName[next]
			for _, i := range instancesOf(dst) {
				spec.Instance = i
				specs = append(specs, spec)
			}
		}
	}
	return specs, topics
}
