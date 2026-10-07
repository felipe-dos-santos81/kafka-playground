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
	"net/http"
	"slices"
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
// did not answer.
type NodeState struct {
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
	if ps := notYetRunnable(f); ps != nil {
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
	for _, n := range f.Nodes {
		if n.Type == "producer" || n.Type == "consumer" {
			st.Nodes[n.ID] = NodeState{State: "missing"}
		}
	}
	for _, c := range cs {
		st.Nodes[c.Labels[labelNode]] = NodeState{State: string(c.State)}
	}
	return st, nil
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
		if ns.State != "running" {
			continue
		}
		s, err := nodeStatsOf(ctx, id, node)
		if err != nil {
			ns.LastError = "stats: " + err.Error()
		} else {
			ns.Total, ns.Errors, ns.LastError, ns.TailSeq, ns.Boot = s.Total, s.Errors, s.LastError, s.TailSeq, s.Boot
		}
		st.Nodes[node] = ns
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
		ends, _ = e.adm.ListEndOffsets(kctx, names...)
	}
	applyKafka(&st, id, specs, topics, lags, ends)
	return st, nil
}

// nodeStatsOf asks a running node container for its counters (1 s budget).
func nodeStatsOf(ctx context.Context, flow, node string) (nodeStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var s nodeStats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL(flow, node, "/stats"), nil)
	if err != nil {
		return s, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return s, err
	}
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(&s)
	return s, err
}

// applyKafka adds the broker's view to a running flow's snapshot: for each topic
// node its partition count and end offset summed over partitions (with a warning
// when the count is not the flow's), for each consumer node its group's lag on its
// topic and the partitions whose group member is this node's client. Whatever the
// broker did not answer is left out.
func applyKafka(st *FlowState, flow string, specs []NodeSpec, topics map[string]TopicData, lags kadm.DescribedGroupLags, ends kadm.ListedOffsets) {
	if ends != nil {
		for node, t := range topics {
			ns := NodeState{State: "missing"}
			for p, o := range ends[t.Name] {
				if p >= 0 && o.Err == nil {
					ns.Partitions++
					ns.EndOffset += o.Offset
				}
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
	for _, s := range specs {
		gl, ok := lags[s.Group]
		if s.Type != "consumer" || !ok || gl.Error() != nil {
			continue
		}
		ns := st.Nodes[s.Node]
		var lag int64
		for _, ml := range gl.Lag[s.Topic] {
			if ml.Err == nil && ml.Lag > 0 {
				lag += ml.Lag
			}
			if ml.Member != nil && ml.Member.ClientID == containerName(flow, s.Node) {
				if ns.Assigned == nil {
					ns.Assigned = map[string][]int32{}
				}
				ns.Assigned[s.Topic] = append(ns.Assigned[s.Topic], ml.Partition)
			}
		}
		slices.Sort(ns.Assigned[s.Topic])
		ns.Lag = &lag
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

// NodeRunning is nil when node's container in flow id is running; otherwise
// ErrNotRunning, wrapped with the node's state when the flow itself runs.
func (e *Engine) NodeRunning(ctx context.Context, id, node string) error {
	st, err := e.State(ctx, id)
	if err != nil {
		return err
	}
	if st.Status != "running" {
		return ErrNotRunning
	}
	switch state := st.Nodes[node].State; state {
	case "running":
		return nil
	case "":
		return fmt.Errorf("node %s has no container: %w", node, ErrNotRunning)
	default:
		return fmt.Errorf("node %s is %s: %w", node, state, ErrNotRunning)
	}
}
