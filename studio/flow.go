// Flow model and validation. The JSON shape is React Flow's own (nodes with
// id/type/position/data, edges with id/source/target, viewport) plus id and
// name, so the editor's toObject() round-trips without a mapping layer.
package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"text/template"
)

type Flow struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Nodes    []Node          `json:"nodes"`
	Edges    []Edge          `json:"edges"`
	Viewport json.RawMessage `json:"viewport,omitempty"`
}

// normalize turns absent nodes/edges into empty lists so they serialize as []
// (the UI schema rejects null).
func (f *Flow) normalize() {
	if f.Nodes == nil {
		f.Nodes = []Node{}
	}
	if f.Edges == nil {
		f.Edges = []Edge{}
	}
}

type Node struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Position Position        `json:"position"`
	Data     json.RawMessage `json:"data"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Edge struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
}

// Contents of Node.Data, one struct per node type.
type ProducerData struct {
	Source     string `json:"source"`      // "manual" | "timer"
	IntervalMS int    `json:"interval_ms"` // timer only
	Key        string `json:"key"`         // text/template, empty = keyless
	Value      string `json:"value"`       // text/template that renders JSON
}

type TopicData struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
}

type ConsumerData struct {
	Group           string `json:"group"`
	AutoOffsetReset string `json:"auto_offset_reset"` // "earliest" (default) | "latest"
	Instances       int    `json:"instances"`         // 0 = 1
	Sink            Sink   `json:"sink"`
}

type Sink struct {
	Kind string `json:"kind"` // "" or "log" | "http"
	URL  string `json:"url"`
}

type TransformData struct {
	Expr string `json:"expr"`
}

// templateData is what producer key/value templates are rendered with.
type templateData struct {
	Seq  int
	Now  string
	Rand int
}

// Problem is one validation failure; Node or Edge names the culprit when there is one.
type Problem struct {
	Node    string `json:"node,omitempty"`
	Edge    string `json:"edge,omitempty"`
	Message string `json:"message"`
}

// Level selects how strict Validate is: Save accepts a half-built flow
// (shape and edge pairs only), Deploy accepts only a runnable one.
type Level int

const (
	Save Level = iota
	Deploy
)

// allowedEdges is the authority on which node types may be wired; the UI mirrors it.
var allowedEdges = map[string]map[string]bool{
	"producer":  {"topic": true},
	"topic":     {"consumer": true},
	"consumer":  {"topic": true, "transform": true},
	"transform": {"topic": true},
}

var (
	nodeIDRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,30}$`) // node ids become part of container names
	topicNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
)

// Validate returns every problem found; nil means valid.
func Validate(f *Flow, level Level) []Problem {
	var ps []Problem
	add := func(node, edge, format string, args ...any) {
		ps = append(ps, Problem{Node: node, Edge: edge, Message: fmt.Sprintf(format, args...)})
	}
	if strings.TrimSpace(f.Name) == "" {
		add("", "", "name is required")
	}
	types := map[string]string{} // node id → type
	for _, n := range f.Nodes {
		switch {
		case !nodeIDRe.MatchString(n.ID):
			add(n.ID, "", "node id %q must match %s", n.ID, nodeIDRe)
		case types[n.ID] != "":
			add(n.ID, "", "duplicate node id %q", n.ID)
		case allowedEdges[n.Type] == nil:
			add(n.ID, "", "unknown node type %q", n.Type)
		default:
			types[n.ID] = n.Type
		}
	}
	inDegree := map[string]int{}  // node id → number of incoming edges
	outDegree := map[string]int{} // node id → number of outgoing edges
	next := map[string][]string{} // node id → targets of its valid edges, for the loop check
	edgeIDs := map[string]bool{}
	seen := map[[2]string]bool{}
	for _, e := range f.Edges {
		switch {
		case e.ID == "":
			add("", "", "edge %s → %s: id is required", e.Source, e.Target)
			continue
		case edgeIDs[e.ID]:
			add("", e.ID, "duplicate edge id %q", e.ID)
			continue
		}
		edgeIDs[e.ID] = true
		st, tt := types[e.Source], types[e.Target]
		switch {
		case st == "" || tt == "":
			add("", e.ID, "edge %s → %s: unknown node", e.Source, e.Target)
			continue
		case e.Source == e.Target:
			add("", e.ID, "edge %s → %s: self edge", e.Source, e.Target)
			continue
		case !allowedEdges[st][tt]:
			add("", e.ID, "edge %s → %s: %s → %s is not allowed", e.Source, e.Target, st, tt)
			continue
		case seen[[2]string{e.Source, e.Target}]:
			add("", e.ID, "edge %s → %s: duplicate", e.Source, e.Target)
			continue
		}
		seen[[2]string{e.Source, e.Target}] = true
		inDegree[e.Target]++
		outDegree[e.Source]++
		next[e.Source] = append(next[e.Source], e.Target)
	}
	if level == Save {
		return ps
	}
	topicNames := map[string]string{} // topic name → node id
	topicOf := map[string]string{}    // topic node id → its name
	for _, n := range f.Nodes {
		if types[n.ID] == "" {
			continue // already reported
		}
		switch n.Type {
		case "producer":
			var d ProducerData
			if !decodeData(n, &d, add) {
				continue
			}
			if d.Source != "manual" && d.Source != "timer" {
				add(n.ID, "", `source must be "manual" or "timer"`)
			}
			if d.Source == "timer" && d.IntervalMS < 10 {
				add(n.ID, "", "interval_ms must be at least 10")
			}
			if d.Value == "" {
				add(n.ID, "", "value is required")
			} else if err := checkTemplate(d.Value, true); err != nil {
				add(n.ID, "", "value: %v", err)
			}
			if err := checkTemplate(d.Key, false); err != nil {
				add(n.ID, "", "key: %v", err)
			}
			if outDegree[n.ID] != 1 {
				add(n.ID, "", "a producer needs exactly one edge to a topic")
			}
		case "topic":
			var d TopicData
			if !decodeData(n, &d, add) {
				continue
			}
			switch other := topicNames[d.Name]; {
			case !topicNameRe.MatchString(d.Name) || d.Name == "." || d.Name == "..":
				add(n.ID, "", "topic name %q is invalid", d.Name)
			case other != "":
				add(n.ID, "", "topic %q is also used by node %s", d.Name, other)
			default:
				topicNames[d.Name] = n.ID
			}
			topicOf[n.ID] = d.Name
			if d.Partitions < 1 {
				add(n.ID, "", "partitions must be at least 1")
			}
			if d.ReplicationFactor != 1 {
				add(n.ID, "", "single-broker playground: replication_factor must be 1")
			}
		case "consumer":
			var d ConsumerData
			if !decodeData(n, &d, add) {
				continue
			}
			if strings.TrimSpace(d.Group) == "" || len(d.Group) > 255 {
				add(n.ID, "", "group is required (at most 255 characters)")
			}
			if d.AutoOffsetReset != "" && d.AutoOffsetReset != "earliest" && d.AutoOffsetReset != "latest" {
				add(n.ID, "", `auto_offset_reset must be "earliest" or "latest"`)
			}
			if d.Instances < 0 || d.Instances > 10 {
				add(n.ID, "", "instances must be between 1 and 10")
			}
			switch d.Sink.Kind {
			case "", "log":
			case "http":
				if u, err := url.Parse(d.Sink.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
					add(n.ID, "", "sink url must be an http(s) URL")
				}
			default:
				add(n.ID, "", `sink kind must be "log" or "http"`)
			}
			if inDegree[n.ID] != 1 {
				add(n.ID, "", "a consumer needs exactly one edge from a topic")
			}
			if outDegree[n.ID] > 1 {
				add(n.ID, "", "a consumer may forward to at most one topic or transform")
			}
		case "transform":
			var d TransformData
			if !decodeData(n, &d, add) {
				continue
			}
			if strings.TrimSpace(d.Expr) == "" {
				add(n.ID, "", "expr is required")
			}
			if inDegree[n.ID] != 1 || outDegree[n.ID] != 1 {
				add(n.ID, "", "a transform needs one edge from a consumer and one edge to a topic")
			}
		}
	}
	// Every container a deploy starts needs a name of its own: a consumer
	// "consumer-1" with two instances runs …-consumer-1-2, which is also a node
	// "consumer-1-2"'s container.
	owner := map[string]string{} // container name → node id
	for _, n := range f.Nodes {
		if types[n.ID] != "producer" && types[n.ID] != "consumer" {
			continue
		}
		for _, i := range instancesOf(n) {
			name := nodeRef{f.ID, n.ID, i}.name()
			if other, taken := owner[name]; taken {
				add(n.ID, "", "its container name %s is also node %s's; rename one of them", name, other)
				break
			}
			owner[name] = n.ID
		}
	}
	return append(ps, forwardLoops(f.Nodes, types, next, topicOf)...)
}

// forwardLoops finds every forward loop (topic → consumer → … → the same topic),
// whose records would circulate forever: one problem per loop, on its first
// consumer, naming the topic that consumer reads.
func forwardLoops(nodes []Node, types map[string]string, next map[string][]string, topicOf map[string]string) []Problem {
	var ps []Problem
	state := map[string]int{} // 0 new, 1 on the path, 2 done
	var path []string
	var visit func(id string)
	visit = func(id string) {
		state[id] = 1
		path = append(path, id)
		for _, to := range next[id] {
			switch state[to] {
			case 0:
				visit(to)
			case 1:
				loop := path[slices.Index(path, to):] // closed by the edge id → to
				for k, c := range loop {
					if types[c] == "consumer" {
						reads := loop[(k+len(loop)-1)%len(loop)] // only a topic feeds a consumer
						name := cmp.Or(topicOf[reads], reads)
						ps = append(ps, Problem{Node: c, Message: fmt.Sprintf("forwarding loops back to topic %q, which it reads: records would circulate forever", name)})
						break
					}
				}
			}
		}
		path = path[:len(path)-1]
		state[id] = 2
	}
	for _, n := range nodes {
		if types[n.ID] != "" && state[n.ID] == 0 {
			visit(n.ID)
		}
	}
	return ps
}

func decodeData(n Node, into any, add func(node, edge, format string, args ...any)) bool {
	if len(n.Data) == 0 || string(n.Data) == "null" {
		add(n.ID, "", "data is required")
		return false
	}
	if err := json.Unmarshal(n.Data, into); err != nil {
		add(n.ID, "", "data: %v", err)
		return false
	}
	return true
}

// render executes a producer key or value template with d.
func render(src string, d templateData) (string, error) {
	t, err := template.New("").Parse(src)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, d); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// checkTemplate renders a producer template once with Seq=1; with mustBeJSON the
// result must be valid JSON.
func checkTemplate(src string, mustBeJSON bool) error {
	out, err := render(src, templateData{Seq: 1, Now: "2026-01-01T00:00:00Z"})
	if err != nil {
		return err
	}
	if mustBeJSON && !json.Valid([]byte(out)) {
		return fmt.Errorf("renders to invalid JSON: %s", out)
	}
	return nil
}
