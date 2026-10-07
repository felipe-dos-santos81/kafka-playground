// Engine runs flows as node containers. What runs is never stored: every call
// reads it back from the containers' labels, so a restarted control plane
// carries on where it was (spec 3.4, reconcile).
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/twmb/franz-go/pkg/kadm"
)

var (
	ErrRunning    = errors.New("flow is already running")
	ErrNotRunning = errors.New("flow is not running")
)

// Problems is a deploy refused before anything started; the API answers 422.
type Problems []Problem

func (ps Problems) Error() string {
	return fmt.Sprintf("flow is not deployable (%d problems)", len(ps))
}

type Engine struct {
	store   Store
	docker  *client.Client
	adm     *kadm.Client
	brokers string     // KAFKA_BROKERS handed to every node container
	me      self       // found on the first deploy
	mu      sync.Mutex // ponytail: one lock for every deploy and stop; per-flow locks if deploys ever queue
}

// FlowState is what a flow's containers are doing. M3's SSE tick extends it.
type FlowState struct {
	Status string               `json:"status"` // "running" (it has node containers) or "stopped"
	Nodes  map[string]NodeState `json:"nodes"`
}

// NodeState is one node in a snapshot. State is Docker's container state
// (running, exited, …) or "missing" for producers and consumers, and "ready" or
// "missing" for topics. The other fields are filled while the flow runs; zero
// values are left out of the JSON, except Lag, which is nil only when the broker
// did not answer. A consumer with instances: n > 1 lists one NodeState per
// container in Instances (each with its Instance number); the node's own Total,
// Rate and Errors are their sums and its State is "running" only when all run.
type NodeState struct {
	Instance   int                `json:"instance,omitempty"` // an entry of Instances: 1..n
	State      string             `json:"state"`
	Total      int64              `json:"total,omitempty"`
	Rate       float64            `json:"rate,omitempty"` // records per second, set by the SSE stream
	Errors     int64              `json:"errors,omitempty"`
	LastError  string             `json:"lastError,omitempty"`
	TailSeq    int64              `json:"tailSeq,omitempty"`
	Boot       string             `json:"boot,omitempty"`
	Lag        *int64             `json:"lag,omitempty"`        // consumers: the group's lag on the node's topic
	Assigned   map[string][]int32 `json:"assigned,omitempty"`   // consumers: partitions this node holds, by topic
	Partitions int32              `json:"partitions,omitempty"` // topics
	EndOffset  int64              `json:"endOffset,omitempty"`  // topics: summed over partitions
	Warning    string             `json:"warning,omitempty"`
	Instances  []NodeState        `json:"instances,omitempty"`

	steps map[string]stepStats // a consumer container's transform counts, by node id; applySteps moves them to the transform node
}

// Deploy validates the saved flow, creates its topics and starts one container
// per producer and consumer; on any failure it removes what it started.
func (e *Engine) Deploy(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, err := e.store.Get(id)
	if err != nil {
		return err
	}
	if ps := Validate(&f, Deploy); ps != nil {
		return Problems(ps)
	}
	specs, topics := Resolve(f)
	if len(specs) == 0 {
		return Problems{{Message: "nothing to run: add a producer or a consumer"}}
	}
	cs, err := flowContainers(ctx, e.docker, id)
	if err != nil {
		return err
	}
	if len(cs) > 0 {
		return ErrRunning
	}
	if e.me.image == "" {
		me, err := discoverSelf(ctx, e.docker)
		if err != nil {
			return err
		}
		e.me = me
	}
	if err := createTopics(ctx, e.adm, topics); err != nil {
		return err
	}
	for _, spec := range specs {
		if err := startNode(ctx, e.docker, e.me, e.brokers, spec); err != nil {
			// Roll back even if the request was cancelled; reconcile catches what this misses.
			if rbErr := e.stop(context.WithoutCancel(ctx), id); rbErr != nil && !errors.Is(rbErr, ErrNotRunning) {
				return fmt.Errorf("%w (rollback: %v)", err, rbErr)
			}
			return err
		}
	}
	return nil
}

// Stop removes every container of the flow. Topics and committed offsets stay.
func (e *Engine) Stop(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.store.Get(id); err != nil {
		return err
	}
	return e.stop(ctx, id)
}

func (e *Engine) stop(ctx context.Context, id string) error {
	cs, err := flowContainers(ctx, e.docker, id)
	if err != nil {
		return err
	}
	if len(cs) == 0 {
		return ErrNotRunning
	}
	return removeContainers(ctx, e.docker, cs)
}

// stateOf is State for a flow already read from the store.
func (e *Engine) stateOf(ctx context.Context, f Flow) (FlowState, error) {
	cs, err := flowContainers(ctx, e.docker, f.ID)
	if err != nil {
		return FlowState{}, err
	}
	st := FlowState{Status: "stopped", Nodes: map[string]NodeState{}}
	if len(cs) == 0 {
		return st, nil
	}
	st.Status = "running"
	st.Nodes = nodeStates(f, cs)
	return st, nil
}

// nodeStates is each producer and consumer node's state in a running flow: one
// slot per container the file asks for, "missing" until a container fills it,
// plus a slot for every container there is. When the file was edited after the
// deploy, what the deploy ran decides between one container and instances: the
// other kind's empty slots are dropped.
func nodeStates(f Flow, cs []container.Summary) map[string]NodeState {
	slots := map[string]map[int]string{} // node id → instance → container state
	for _, n := range f.Nodes {
		if n.Type == "producer" || n.Type == "consumer" {
			slots[n.ID] = map[int]string{}
			for _, i := range instancesOf(n) {
				slots[n.ID][i] = "missing"
			}
		}
	}
	ranInstances := map[string]bool{} // node id → whether the deploy ran it as instances 1..n
	for _, c := range cs {
		node := c.Labels[labelNode]
		i, _ := strconv.Atoi(c.Labels[labelInstance]) // no label: a node's only container, 0
		if slots[node] == nil {
			slots[node] = map[int]string{}
		}
		slots[node][i] = string(c.State)
		ranInstances[node] = i > 0
	}
	nodes := map[string]NodeState{}
	for node, insts := range slots {
		if asInstances, deployed := ranInstances[node]; deployed {
			for i, state := range insts {
				isInstance := i > 0
				if state == "missing" && isInstance != asInstances {
					delete(insts, i)
				}
			}
		}
		nodes[node] = nodeOf(insts)
	}
	return nodes
}

// nodeOf folds a node's container states into one NodeState: a lone instance 0
// is the node itself; otherwise every instance is an entry of Instances, in
// order, and the node runs only when all of them run (else it takes the first
// other state).
func nodeOf(insts map[int]string) NodeState {
	if s, ok := insts[0]; ok && len(insts) == 1 {
		return NodeState{State: s}
	}
	ns := NodeState{State: "running"}
	for _, i := range slices.Sorted(maps.Keys(insts)) {
		ns.Instances = append(ns.Instances, NodeState{Instance: i, State: insts[i]})
		if ns.State == "running" && insts[i] != "running" {
			ns.State = insts[i]
		}
	}
	return ns
}

// State reports the flow's container states. A producer or consumer with no
// container in a running flow is "missing" (removed with docker rm -f, or added
// to the file after the deploy).
func (e *Engine) State(ctx context.Context, id string) (FlowState, error) {
	f, err := e.store.Get(id)
	if err != nil {
		return FlowState{}, err
	}
	return e.stateOf(ctx, f)
}

// Snapshot is State plus, while the flow runs, each running node's counters
// (/stats) and the broker's view (consumer lag and partitions, topic partitions
// and end offsets). The SSE stream adds rates.
func (e *Engine) Snapshot(ctx context.Context, id string) (FlowState, error) {
	f, err := e.store.Get(id)
	if err != nil {
		return FlowState{}, err
	}
	st, err := e.stateOf(ctx, f)
	if err != nil || st.Status != "running" {
		return st, err
	}
	for node, ns := range st.Nodes {
		for _, c := range ns.containers() {
			if c.State == "running" {
				*c = withStats(ctx, nodeRef{id, node, c.Instance}, *c)
			}
		}
		ns.sumInstances()
		st.Nodes[node] = ns
	}
	specs, _ := Resolve(f)
	applySteps(&st, specs)
	topics := map[string]TopicData{} // topic node id → its data
	var groups, names []string
	for _, n := range f.Nodes {
		if n.Type == "topic" {
			var d TopicData
			json.Unmarshal(n.Data, &d)
			topics[n.ID] = d
			names = append(names, d.Name)
		}
	}
	for _, s := range specs {
		if s.Type == "consumer" {
			groups = append(groups, s.Group)
		}
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	kctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// Errors leave the broker's fields out of this snapshot; called with no names
	// these list every group or topic on the broker, hence the guards.
	var lags kadm.DescribedGroupLags
	var ends kadm.ListedOffsets
	if len(groups) > 0 {
		lags, _ = e.adm.Lag(kctx, groups...)
	}
	if len(names) > 0 {
		// Only a complete answer counts: a partial map (a shard timed out) would
		// read as missing topics and too few partitions.
		if got, err := e.adm.ListEndOffsets(kctx, names...); err == nil {
			ends = got
		}
	}
	applyKafka(&st, id, specs, topics, lags, ends)
	return st, nil
}

// withStats is ns (a node, or one of its instances) with the counters its
// container r reports on /stats.
func withStats(ctx context.Context, r nodeRef, ns NodeState) NodeState {
	s, err := nodeStatsOf(ctx, r)
	if err != nil {
		ns.LastError = "stats: " + err.Error()
		return ns
	}
	ns.Total, ns.Errors, ns.LastError, ns.TailSeq, ns.Boot = s.Total, s.Errors, s.LastError, s.TailSeq, s.Boot
	ns.steps = s.Steps
	return ns
}

// applySteps gives each transform node its consumer node's state and the counts
// the consumer's containers report for it, summed (a last error is the first
// container's that has one, prefixed with its instance). Its boot joins theirs,
// so a container that restarted gives the transform no rate rather than a wrong one.
// A transform whose consumer runs but that no container reports (added or renamed
// after deploy) is "missing" until one does.
func applySteps(st *FlowState, specs []NodeSpec) {
	done := map[string]bool{}
	for _, s := range specs {
		if s.TransformNode == "" || done[s.TransformNode] {
			continue
		}
		done[s.TransformNode] = true
		consumer := st.Nodes[s.Node]
		t := NodeState{State: cmp.Or(consumer.State, "missing")}
		var boots []string
		for _, c := range consumer.containers() {
			step, ok := c.steps[s.TransformNode]
			if !ok {
				continue
			}
			t.Total += step.Total
			t.Errors += step.Errors
			if t.LastError == "" && step.LastError != "" {
				t.LastError = instanceError(c.Instance, step.LastError)
			}
			boots = append(boots, c.Boot)
		}
		t.Boot = strings.Join(boots, ",")
		if len(boots) == 0 && t.State == "running" {
			t.State = "missing"
		}
		st.Nodes[s.TransformNode] = t
	}
}

// nodeStatsOf asks a running node container for its counters (1 s budget).
func nodeStatsOf(ctx context.Context, r nodeRef) (nodeStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url("/stats"), nil)
	if err != nil {
		return s, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return s, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return s, errors.New(res.Status)
	}
	err = json.NewDecoder(res.Body).Decode(&s)
	return s, err
}

// applyKafka adds the broker's view to a running flow's snapshot: for each topic
// node its partition count and end offset summed over partitions (with a warning
// when the count is not the flow's; a topic with a failed partition is left out),
// for each consumer node its group's lag on its
// topic and the partitions whose group member is this node's client (or, for a
// node whose snapshot state lists instances, each instance's client: the
// containers that run, not what the file now says). Whatever the broker did not
// answer is left out.
func applyKafka(st *FlowState, flow string, specs []NodeSpec, topics map[string]TopicData, lags kadm.DescribedGroupLags, ends kadm.ListedOffsets) {
	if ends != nil {
		for node, t := range topics {
			ns := NodeState{State: "missing"}
			partial := false
			for p, o := range ends[t.Name] {
				if p >= 0 && o.Err != nil {
					partial = true
				} else if p >= 0 {
					ns.Partitions++
					ns.EndOffset += o.Offset
				}
			}
			if partial {
				continue // a partition's leader did not answer: say nothing about this topic
			}
			if ns.Partitions > 0 {
				ns.State = "ready"
				if int(ns.Partitions) != t.Partitions {
					ns.Warning = fmt.Sprintf("the topic has %d partitions; the flow asks for %d", ns.Partitions, t.Partitions)
				}
			}
			st.Nodes[node] = ns
		}
	}
	done := map[string]bool{} // consumer nodes handled: several specs share one
	for _, s := range specs {
		gl, ok := lags[s.Group]
		if s.Type != "consumer" || done[s.Node] || !ok || gl.Error() != nil {
			continue
		}
		done[s.Node] = true
		ns := st.Nodes[s.Node]
		// What runs, not the file, says whose clients to look for.
		client := map[string]*NodeState{} // client id → its container's state
		for _, c := range ns.containers() {
			client[nodeRef{flow, s.Node, c.Instance}.name()] = c
		}
		var lag int64
		held := map[*NodeState][]int32{} // container → partitions its client holds
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Err == nil && ml.Lag > 0 {
				lag += ml.Lag
			}
			if ml.Member != nil {
				if c, ok := client[ml.Member.ClientID]; ok {
					held[c] = append(held[c], ml.Partition)
				}
			}
		}
		ns.Lag = &lag
		for c, ps := range held {
			slices.Sort(ps)
			c.Assigned = map[string][]int32{s.Topic: ps}
		}
		st.Nodes[s.Node] = ns
	}
}

// Running is the set of flows that have node containers, in any state.
func (e *Engine) Running(ctx context.Context) (map[string]bool, error) {
	cs, err := flowContainers(ctx, e.docker, "")
	if err != nil {
		return nil, err
	}
	running := map[string]bool{}
	for _, c := range cs {
		running[c.Labels[labelFlow]] = true
	}
	return running, nil
}

// Reconcile runs at start: containers of flows whose file is gone (deleted
// while the control plane was down) are removed; the rest keep running.
func (e *Engine) Reconcile(ctx context.Context) error {
	cs, err := flowContainers(ctx, e.docker, "")
	if err != nil {
		return err
	}
	var orphans []container.Summary
	for _, c := range cs {
		if _, err := e.store.Get(c.Labels[labelFlow]); errors.Is(err, ErrNotFound) {
			orphans = append(orphans, c)
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	log.Printf("reconcile: removing %d containers of deleted flows", len(orphans))
	return removeContainers(ctx, e.docker, orphans)
}

// NodeRunning returns which of node's containers to talk to: instance as asked,
// or for a node with instances and no instance asked, its first. It is
// ErrNotRunning, wrapped with the state, unless that container runs.
func (e *Engine) NodeRunning(ctx context.Context, id, node string, instance int) (int, error) {
	st, err := e.State(ctx, id)
	if err != nil {
		return 0, err
	}
	if st.Status != "running" {
		return 0, ErrNotRunning
	}
	ns, ok := st.Nodes[node]
	if !ok {
		return 0, fmt.Errorf("node %s has no container: %w", node, ErrNotRunning)
	}
	return pickInstance(node, ns, instance)
}

// pickInstance is NodeRunning's choice within one node's state. A node with one
// container is its own instance 1 too, so ?instance=1 (the documented default)
// works on every node.
func pickInstance(node string, ns NodeState, instance int) (int, error) {
	single := len(ns.Instances) == 0
	switch {
	case instance == 0:
		instance = ns.containers()[0].Instance
	case instance == 1 && single:
		instance = 0
	}
	c := ns.container(instance)
	if c == nil {
		return 0, fmt.Errorf("node %s has no instance %d: %w", node, instance, ErrNotRunning)
	}
	if c.State != "running" {
		what := "node " + node
		if !single {
			what += fmt.Sprintf(" instance %d", instance)
		}
		return 0, fmt.Errorf("%s is %s: %w", what, c.State, ErrNotRunning)
	}
	return instance, nil
}

// withRates sets each node's records per second against the previous snapshot,
// taken dt seconds earlier; a node with instances gets the sum of theirs. A node
// or instance whose boot changed (its container restarted) or whose total fell
// gets no rate this time rather than a wrong one.
func withRates(cur *FlowState, prev FlowState, dt float64) {
	if dt <= 0 {
		return
	}
	for id, ns := range cur.Nodes {
		before, seen := prev.Nodes[id]
		for _, c := range ns.containers() {
			var p *NodeState
			if seen {
				p = before.container(c.Instance)
			}
			c.Rate = rate(*c, p, dt)
		}
		ns.sumInstances()
		cur.Nodes[id] = ns
	}
}

// rate is cur's records per second against prev, taken dt seconds earlier, rounded
// to 0.1; 0 when there is no prev or cur's container restarted since.
func rate(cur NodeState, prev *NodeState, dt float64) float64 {
	if prev == nil || prev.Boot != cur.Boot || cur.Total < prev.Total {
		return 0
	}
	return math.Round(float64(cur.Total-prev.Total)/dt*10) / 10
}

// containers is the states of ns's containers: ns itself when the node has one
// container, else each entry of Instances. Changes through them land in ns.
func (ns *NodeState) containers() []*NodeState {
	if len(ns.Instances) == 0 {
		return []*NodeState{ns}
	}
	cs := make([]*NodeState, len(ns.Instances))
	for k := range ns.Instances {
		cs[k] = &ns.Instances[k]
	}
	return cs
}

// container is the state of ns's container numbered i (0 for a node's only
// container), or nil.
func (ns *NodeState) container(i int) *NodeState {
	for _, c := range ns.containers() {
		if c.Instance == i {
			return c
		}
	}
	return nil
}

// sumInstances gives a node with instances the sums of their counters and rates,
// and the first of their last errors, prefixed with its instance; a node with one
// container is its container already.
func (ns *NodeState) sumInstances() {
	if len(ns.Instances) == 0 {
		return
	}
	ns.Total, ns.Errors, ns.Rate, ns.LastError = 0, 0, 0, ""
	for _, in := range ns.Instances {
		ns.Total += in.Total
		ns.Errors += in.Errors
		ns.Rate += in.Rate
		if ns.LastError == "" && in.LastError != "" {
			ns.LastError = instanceError(in.Instance, in.LastError)
		}
	}
	ns.Rate = math.Round(ns.Rate*10) / 10
}

// instanceError prefixes a container's last error with its instance; a node's
// single container (instance 0) has no prefix.
func instanceError(instance int, err string) string {
	if instance == 0 {
		return err
	}
	return fmt.Sprintf("#%d: %s", instance, err)
}

// streamTicks is the body of GET /api/flows/{id}/events: every period it writes
// the flow's snapshot as an SSE `tick` event with rates against the previous
// tick; a failed snapshot is a `problem` event and the stream goes on. It returns
// when ctx ends (the browser went away) or a flush fails. Each open stream polls
// on its own: one tab, one loop.
func streamTicks(ctx context.Context, w io.Writer, flush func() error, snap func(context.Context) (FlowState, error), every time.Duration) {
	var prev FlowState
	last := time.Now()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		st, err := snap(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			b, _ := json.Marshal(map[string]string{"error": err.Error()})
			fmt.Fprintf(w, "event: problem\ndata: %s\n\n", b)
		} else {
			now := time.Now()
			withRates(&st, prev, now.Sub(last).Seconds())
			prev, last = st, now
			b, _ := json.Marshal(st)
			fmt.Fprintf(w, "event: tick\ndata: %s\n\n", b)
		}
		if flush() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
