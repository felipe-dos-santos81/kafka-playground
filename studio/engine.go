// Engine runs flows as node containers. What runs is never stored: every call
// reads it back from the containers' labels, so a restarted control plane
// carries on where it was (spec 3.4, reconcile).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

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

// NodeState.State is Docker's container state (running, exited, …) or "missing".
type NodeState struct {
	State string `json:"state"`
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
	specs, topics := Resolve(f)
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

// State reports the flow's container states. A producer or consumer with no
// container in a running flow is "missing" (removed with docker rm -f, or added
// to the file after the deploy).
func (e *Engine) State(ctx context.Context, id string) (FlowState, error) {
	f, err := e.store.Get(id)
	if err != nil {
		return FlowState{}, err
	}
	cs, err := flowContainers(ctx, e.docker, id)
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
