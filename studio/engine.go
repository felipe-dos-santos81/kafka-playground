// Engine runs flows as node containers. What runs is never stored: every call
// reads it back from the containers' labels, so a restarted control plane
// carries on where it was (spec 3.4, reconcile).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	ErrNoNode     = errors.New("no such node in the flow")
	ErrNoTopic    = errors.New("the topic does not exist yet: deploy the flow once first")
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

	step     *tally // a consumer container's transform counts; applySteps puts them on the transform node
	answered bool   // its container's /stats answered in this snapshot
}

// A snapshot asks every running container for /stats in parallel and the broker
// for lag and end offsets, all within snapshotBudget, so a tick stays about a
// second apart. A container that does not answer gets a warning, unless it was
// created less than statsGrace ago: it is still starting.
const (
	snapshotBudget = 800 * time.Millisecond
	statsGrace     = 5 * time.Second
)

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

// Rewound is what Rewind did.
type Rewound struct {
	Group      string   `json:"group"`
	Topic      string   `json:"topic"`
	Partitions int      `json:"partitions"`
	To         RewindTo `json:"to"`
}

// Rewind sets the committed offsets of a consumer node's group to the start
// (earliest) or the end (latest) of its topic, so the next deploy reads from
// there whatever auto_offset_reset says. The broker refuses offset commits for a
// group with members, so the flow must be stopped.
func (e *Engine) Rewind(ctx context.Context, id, node string, to RewindTo) (Rewound, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, err := e.store.Get(id)
	if err != nil {
		return Rewound{}, err
	}
	r, err := rewindTarget(f, node, to)
	if err != nil {
		return Rewound{}, err
	}
	cs, err := flowContainers(ctx, e.docker, id)
	if err != nil {
		return Rewound{}, err
	}
	if len(cs) > 0 {
		return Rewound{}, fmt.Errorf("stop the flow first: %w", ErrRunning)
	}
	if r.Partitions, err = rewindGroup(ctx, e.adm, r); err != nil {
		return Rewound{}, err
	}
	return r, nil
}

// stateOf is State for a flow already read from the store.
func (e *Engine) stateOf(ctx context.Context, f Flow) (FlowState, error) {
	st, _, err := e.stateAndContainers(ctx, f)
	return st, err
}

// stateAndContainers is stateOf plus the flow's containers it was read from.
func (e *Engine) stateAndContainers(ctx context.Context, f Flow) (FlowState, []container.Summary, error) {
	cs, err := flowContainers(ctx, e.docker, f.ID)
	if err != nil {
		return FlowState{}, nil, err
	}
	st := FlowState{Status: "stopped", Nodes: map[string]NodeState{}}
	if len(cs) == 0 {
		return st, cs, nil
	}
	st.Status = "running"
	st.Nodes = nodeStates(f, cs)
	return st, cs, nil
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
	st, cs, err := e.stateAndContainers(ctx, f)
	if err != nil || st.Status != "running" {
		return st, err
	}
	created := map[string]time.Time{} // container name → when it was created
	for _, c := range cs {
		if len(c.Names) > 0 {
			created[strings.TrimPrefix(c.Names[0], "/")] = time.Unix(c.Created, 0)
		}
	}
	// Every running container's /stats and the broker's end offsets at once, while
	// this goroutine asks for lag, all under one budget; each goroutine writes only
	// its own container's state (or ends), read after wg.Wait.
	bctx, cancel := context.WithTimeout(ctx, snapshotBudget)
	defer cancel()
	var wg sync.WaitGroup
	nodes := map[string]*NodeState{}
	for node, ns := range st.Nodes {
		nodes[node] = &ns
		for _, c := range ns.containers() {
			if c.State == "running" {
				r := nodeRef{id, node, c.Instance}
				starting := time.Since(created[r.name()]) < statsGrace
				wg.Go(func() { *c = withStats(bctx, r.url("/stats"), *c, starting) })
			}
		}
	}
	specs, _ := Resolve(f)
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
	// Errors leave the broker's fields out of this snapshot; called with no names
	// these list every group or topic on the broker, hence the guards.
	var lags kadm.DescribedGroupLags
	var ends kadm.ListedOffsets
	if len(names) > 0 {
		wg.Go(func() {
			// Only a complete answer counts: a partial map (a shard timed out) would
			// read as missing topics and too few partitions.
			if got, err := e.adm.ListEndOffsets(bctx, names...); err == nil {
				ends = got
			}
		})
	}
	if len(groups) > 0 {
		lags, _ = e.adm.Lag(bctx, groups...)
	}
	wg.Wait()
	for node, ns := range nodes {
		ns.sumInstances()
		st.Nodes[node] = *ns
	}
	applySteps(&st, specs)
	applyKafka(&st, id, specs, topics, lags, ends)
	return st, nil
}

// withStats is ns (a node, or one of its instances) with the counters its
// container reports at url. A container that does not answer keeps no numbers,
// and its warning says why unless it is starting (created less than statsGrace ago).
func withStats(ctx context.Context, url string, ns NodeState, starting bool) NodeState {
	s, err := nodeStatsOf(ctx, url)
	if err != nil {
		if !starting {
			ns.Warning = "stats: " + err.Error()
		}
		return ns
	}
	ns.setCounts(s.tally)
	ns.TailSeq, ns.Boot, ns.step, ns.answered = s.TailSeq, s.Boot, s.Step, true
	return ns
}

// setCounts puts a container's tally into ns.
func (ns *NodeState) setCounts(t tally) {
	ns.Total, ns.Errors, ns.LastError = t.Total, t.Errors, t.LastError
}

// applySteps gives each transform node its consumer node's state and the counts
// the consumer's containers report for it, summed as sumCounts sums instances. Its
// boot joins theirs, so a container that restarted gives the transform no rate
// rather than a wrong one. A transform whose consumer's containers answer but
// none reports it (added after deploy) is "missing" until one does; one whose
// consumer did not answer keeps the consumer's state, with no numbers.
func applySteps(st *FlowState, specs []NodeSpec) {
	for _, s := range specs {
		if s.TransformNode == "" || s.Instance > 1 {
			continue // a consumer's specs repeat per instance; its first does for all
		}
		consumer := st.Nodes[s.Node]
		t := NodeState{State: consumer.State}
		var steps []NodeState
		var boots []string
		answered := false
		for _, c := range consumer.containers() {
			answered = answered || c.answered
			if c.step != nil {
				step := NodeState{Instance: c.Instance}
				step.setCounts(*c.step)
				steps = append(steps, step)
				boots = append(boots, c.Boot)
			}
		}
		t.sumCounts(steps)
		t.Boot = strings.Join(boots, ",")
		if len(boots) == 0 && (t.State == "" || t.State == "running" && answered) {
			t.State = "missing"
		}
		st.Nodes[s.TransformNode] = t
	}
}

// nodeStatsOf asks a node container's /stats at url for its counters, within ctx;
// any answer but 200 is an error.
func nodeStatsOf(ctx context.Context, url string) (nodeStats, error) {
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
// for each consumer node its group's lag on its topic (none until the group has
// committed; then committed partitions from their commit, the others from where
// auto_offset_reset starts: the beginning for earliest, nothing for latest) and
// the partitions whose
// group member is this node's client (or, for a node whose snapshot state lists
// instances, each instance's client: the containers that run, not what the file
// now says). Whatever the broker did not answer is left out.
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
		var lag, fromReset int64 // over committed partitions; over the others, by auto_offset_reset
		committed := false
		held := map[*NodeState][]int32{} // container → partitions its client holds
		for _, ml := range gl.Lag[s.Topic] {
			switch {
			case ml.Err != nil:
			case ml.Commit.At >= 0:
				committed = true
				lag += max(ml.Lag, 0)
			case s.AutoOffsetReset != "latest": // no commit: kadm's lag runs from the partition's start
				fromReset += max(ml.Lag, 0)
			}
			if ml.Member != nil {
				if c, ok := client[ml.Member.ClientID]; ok {
					held[c] = append(held[c], ml.Partition)
				}
			}
		}
		if committed { // a group with no commit yet shows no lag at all
			lag += fromReset
			ns.Lag = &lag
		}
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
// and the first of their warnings, prefixed with its instance; a node with one
// container is its container already.
func (ns *NodeState) sumInstances() {
	if len(ns.Instances) == 0 {
		return
	}
	ns.sumCounts(ns.Instances)
	ns.Warning = ""
	for _, in := range ns.Instances {
		if in.Warning != "" {
			ns.Warning = instanceError(in.Instance, in.Warning)
			break
		}
	}
}

// sumCounts gives ns the sums of cs's counters and rates, and the first of their
// last errors, prefixed with its instance.
func (ns *NodeState) sumCounts(cs []NodeState) {
	ns.Total, ns.Errors, ns.Rate, ns.LastError = 0, 0, 0, ""
	for _, in := range cs {
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
