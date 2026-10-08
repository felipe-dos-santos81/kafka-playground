// Resolve turns a deployable flow into what runs: the topics to create and one
// NodeSpec per producer or consumer container. A node container never sees the
// graph, only its NodeSpec (env STUDIO_NODE).
package main

import (
	"encoding/json"
	"slices"
	"strconv"
)

// NodeSpec is the whole configuration of one node container.
type NodeSpec struct {
	Flow            string     `json:"flow"`
	Node            string     `json:"node"`
	Type            string     `json:"type"`  // "producer" or "consumer"
	Topic           string     `json:"topic"` // produced to, or consumed from
	Group           string     `json:"group,omitempty"`
	AutoOffsetReset string     `json:"auto_offset_reset,omitempty"`
	Key             string     `json:"key,omitempty"`            // producer key template
	Value           string     `json:"value,omitempty"`          // producer value template
	Source          string     `json:"source,omitempty"`         // producer: "manual" or "timer"
	IntervalMS      int        `json:"interval_ms,omitempty"`    // producer: the timer's period
	Forward         string     `json:"forward,omitempty"`        // consumer: the topic it forwards every record to
	SinkURL         string     `json:"sink_url,omitempty"`       // consumer: the http sink's URL
	Instance        int        `json:"instance,omitempty"`       // consumer: 1..n when it runs n > 1 instances, else 0
	Transform       string     `json:"transform,omitempty"`      // consumer: the expr its records pass through before the forward
	TransformNode   string     `json:"transform_node,omitempty"` // consumer: that transform's node id, which its counts are reported under
	Routes          []Route    `json:"routes,omitempty"`         // consumer: its router's rules, in order; set, they choose the forward
	RouteDefault    string     `json:"route_default,omitempty"`  // consumer: the router's default topic; "" drops what no rule matches
	RouterNode      string     `json:"router_node,omitempty"`    // consumer: the router's node id, which its counts are reported under
	Retry           *RetrySpec `json:"retry,omitempty"`          // consumer: how its retry loop retries failures; nil without retry
	DLQ             string     `json:"dlq,omitempty"`            // consumer: the topic its failures end in; "" without a DLQ
}

// RetrySpec is a consumer's retry: the topic its failures wait in, the group its
// retry loop reads it in, how many retries a record gets after its first try and
// how long each waits.
type RetrySpec struct {
	Topic    string `json:"topic"`
	Group    string `json:"group"`
	Attempts int    `json:"attempts"`
	DelayMS  int    `json:"delay_ms"`
}

// Route is one router rule as a consumer runs it: a condition and a topic name.
type Route struct {
	When  string `json:"when"`
	Topic string `json:"topic"`
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
// at most one out (to a topic, a transform or a router), every transform one edge
// in from a consumer and one out to a topic or a router, and every router's rules
// name its wired topics. A consumer's transform and router run in its own
// containers, so neither has a spec of its own. A consumer's retry and DLQ topics
// are created with its input topic's partitions, unless the flow draws them.
func Resolve(f Flow) ([]NodeSpec, []TopicData) {
	byID := map[string]Node{}
	topicName := map[string]string{} // topic node id → topic name
	partitions := map[string]int{}   // topic node id → its partitions
	out := map[string]string{}       // consumer or transform node id → the node its edge goes to
	var topics []TopicData
	for _, n := range f.Nodes {
		byID[n.ID] = n
		if n.Type == "topic" {
			var d TopicData
			json.Unmarshal(n.Data, &d)
			topicName[n.ID] = d.Name
			partitions[n.ID] = d.Partitions
			topics = append(topics, d)
		}
	}
	for _, e := range f.Edges {
		if t := byID[e.Source].Type; t == "consumer" || t == "transform" {
			out[e.Source] = e.Target
		}
	}
	var specs []NodeSpec
	var derived []TopicData // the consumers' retry and DLQ topics
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
			next := out[dst.ID] // a topic, a transform, a router, or "" when the consumer forwards nothing
			if t := byID[next]; t.Type == "transform" {
				var td TransformData
				json.Unmarshal(t.Data, &td)
				spec.Transform, spec.TransformNode = td.Expr, t.ID
				next = out[t.ID]
			}
			if r := byID[next]; r.Type == "router" {
				var rd RouterData
				json.Unmarshal(r.Data, &rd)
				for _, rule := range rd.Rules {
					spec.Routes = append(spec.Routes, Route{When: rule.When, Topic: topicName[rule.To]})
				}
				spec.RouteDefault, spec.RouterNode = topicName[rd.Default], r.ID
				next = "" // the router chooses the forward
			}
			spec.Forward = topicName[next]
			if d.Retry != nil {
				spec.Retry = &RetrySpec{Topic: retryTopic(spec.Topic), Group: retryGroup(d.Group), Attempts: d.Retry.Attempts, DelayMS: d.Retry.DelayMS}
				derived = append(derived, TopicData{Name: spec.Retry.Topic, Partitions: partitions[src.ID], ReplicationFactor: 1})
			}
			if d.DLQ {
				spec.DLQ = dlqTopic(spec.Topic)
				derived = append(derived, TopicData{Name: spec.DLQ, Partitions: partitions[src.ID], ReplicationFactor: 1})
			}
			for _, i := range instancesOf(dst) {
				spec.Instance = i
				specs = append(specs, spec)
			}
		}
	}
	for _, t := range derived { // once each, and as drawn when the flow has it as a node
		if !slices.ContainsFunc(topics, func(have TopicData) bool { return have.Name == t.Name }) {
			topics = append(topics, t)
		}
	}
	return specs, topics
}
