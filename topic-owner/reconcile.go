package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// reconcileEvery is how often a container compares its topic with the desired state.
const reconcileEvery = 10 * time.Second

// Actual is a topic as the broker has it.
type Actual struct {
	Exists            bool
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string // topic-level overrides only (source DYNAMIC_TOPIC_CONFIG)
}

// Plan is what one reconcile pass does to bring a topic to its desired state.
type Plan struct {
	Create     bool               // the topic is missing: create it with the desired partitions and configs
	Partitions int                // > 0: raise the partition count to this
	Alter      []kadm.AlterConfig // incremental config changes, in name order
	Logs       []string           // one line per change, in step order: the create line; or the partitions line (when Partitions > 0), then one line per Alter entry in the same order (diff builds them so)
	Problems   []string           // differences no pass can fix
}

// diff is the plan that takes have to want for topic. Broker and default
// configs are never touched; topic-level overrides want does not name are removed.
func diff(topic string, want Config, have Actual) Plan {
	var p Plan
	if !have.Exists {
		p.Create = true
		p.Logs = append(p.Logs, fmt.Sprintf("%s: created with %d partitions", topic, want.Partitions))
		return p
	}
	switch {
	case have.Partitions < want.Partitions:
		p.Partitions = want.Partitions
		p.Logs = append(p.Logs, fmt.Sprintf("%s: partitions %d -> %d", topic, have.Partitions, want.Partitions))
	case have.Partitions > want.Partitions:
		p.Problems = append(p.Problems, fmt.Sprintf("%s: has %d partitions, wants %d: partitions never decrease (delete the topic, or set PARTITIONS=%d)", topic, have.Partitions, want.Partitions, have.Partitions))
	}
	if have.ReplicationFactor != want.ReplicationFactor {
		p.Problems = append(p.Problems, fmt.Sprintf("%s: replication factor %d, wants %d: never changed", topic, have.ReplicationFactor, want.ReplicationFactor))
	}
	names := map[string]bool{}
	for k := range want.Configs {
		names[k] = true
	}
	for k := range have.Configs {
		names[k] = true
	}
	for _, k := range slices.Sorted(maps.Keys(names)) {
		v, wanted := want.Configs[k]
		was, set := have.Configs[k]
		switch {
		case wanted && !set:
			p.Alter = append(p.Alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(v)})
			p.Logs = append(p.Logs, fmt.Sprintf("%s: set %s=%s (was unset)", topic, k, v))
		case wanted && was != v:
			p.Alter = append(p.Alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(v)})
			p.Logs = append(p.Logs, fmt.Sprintf("%s: set %s=%s (was %s)", topic, k, v, was))
		case !wanted:
			p.Alter = append(p.Alter, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
			p.Logs = append(p.Logs, fmt.Sprintf("%s: removed %s (was %s)", topic, k, was))
		}
	}
	return p
}

// owner keeps one topic in its desired state and remembers whether the last
// pass left it there.
type owner struct {
	cfg      Config
	describe func(context.Context) (Actual, error)    // the topic as the broker has it
	apply    func(context.Context, Plan) (int, error) // carries out a plan; reports how many of its Logs landed

	mu      sync.Mutex
	problem string // why the topic is not in its desired state; "" when it is
}

func newOwner(cfg Config, adm *kadm.Client) *owner {
	o := &owner{cfg: cfg, problem: cfg.Topic() + ": not reconciled yet"}
	o.describe = func(ctx context.Context) (Actual, error) { return describe(ctx, adm, cfg.Topic()) }
	o.apply = func(ctx context.Context, p Plan) (int, error) { return apply(ctx, adm, cfg, p) }
	return o
}

// health is "" when the last pass left the topic in its desired state, else why not.
func (o *owner) health() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.problem
}

// run reconciles at once, then every reconcileEvery, until ctx ends.
func (o *owner) run(ctx context.Context) {
	t := time.NewTicker(reconcileEvery)
	defer t.Stop()
	for {
		o.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// pass is one reconcile: describe, diff, apply. It logs each applied change,
// and a problem only when it differs from the last one.
func (o *owner) pass(ctx context.Context) {
	topic := o.cfg.Topic()
	problem := ""
	if have, err := o.describe(ctx); err != nil {
		problem = fmt.Sprintf("%s: kafka: %v", topic, err)
	} else {
		p := diff(topic, o.cfg, have)
		n, err := o.apply(ctx, p)
		for _, l := range p.Logs[:n] {
			log.Print(l)
		}
		if err != nil {
			problem = strings.Join(append([]string{fmt.Sprintf("%s: %v", topic, err)}, p.Problems...), "; ")
		} else {
			problem = strings.Join(p.Problems, "; ")
		}
	}
	o.mu.Lock()
	changed := problem != o.problem
	o.problem = problem
	o.mu.Unlock()
	if changed && problem != "" {
		log.Print(problem)
	}
}

// describe reads topic's partitions, replication factor and topic-level configs.
func describe(ctx context.Context, adm *kadm.Client, topic string) (Actual, error) {
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return Actual{}, err
	}
	td := tds[topic]
	if errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
		return Actual{}, nil
	} else if td.Err != nil {
		return Actual{}, td.Err
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return Actual{}, err
	}
	rc, err := rcs.On(topic, nil)
	if err == nil {
		err = rc.Err
	}
	if err != nil {
		return Actual{}, err
	}
	return Actual{Exists: true, Partitions: len(td.Partitions), ReplicationFactor: td.Partitions.NumReplicas(), Configs: topicLevel(rc.Configs)}, nil
}

// topicLevel is the configs set on the topic itself (source DYNAMIC_TOPIC_CONFIG):
// the ones an owner manages. Broker settings and defaults are not the topic's.
func topicLevel(configs []kadm.Config) map[string]string {
	out := map[string]string{}
	for _, c := range configs {
		if c.Source == kmsg.ConfigSourceDynamicTopicConfig {
			out[c.Key] = c.MaybeValue()
		}
	}
	return out
}

// errRaced is apply's answer when the topic appeared between describe and
// create (a Studio deploy, another owner): nothing was created, and the next
// pass reconciles the topic it finds.
var errRaced = errors.New("created by someone else first; reconciling it on the next pass")

// createErr is CreateTopic's error as apply reports it.
func createErr(err error) error {
	switch {
	case errors.Is(err, kerr.TopicAlreadyExists):
		return errRaced
	case err != nil:
		return fmt.Errorf("create: %w", err)
	}
	return nil
}

// apply carries out p on cfg's topic. It returns how many of p.Logs landed:
// the partitions raise and the config alter are separate steps, in that order.
func apply(ctx context.Context, adm *kadm.Client, cfg Config, p Plan) (int, error) {
	topic := cfg.Topic()
	if p.Create {
		configs := map[string]*string{}
		for k, v := range cfg.Configs {
			configs[k] = kadm.StringPtr(v)
		}
		_, err := adm.CreateTopic(ctx, int32(cfg.Partitions), int16(cfg.ReplicationFactor), configs, topic)
		if err := createErr(err); err != nil {
			return 0, err
		}
		return len(p.Logs), nil
	}
	landed := 0
	if p.Partitions > 0 {
		rs, err := adm.UpdatePartitions(ctx, p.Partitions, topic)
		if err == nil {
			err = rs.Error()
		}
		if err != nil {
			return 0, fmt.Errorf("partitions: %w", err)
		}
		landed = 1
	}
	if len(p.Alter) > 0 {
		rs, err := adm.AlterTopicConfigs(ctx, p.Alter, topic)
		if err == nil {
			for _, r := range rs {
				if r.Err != nil {
					err = fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
				}
			}
		}
		if err != nil {
			return landed, fmt.Errorf("alter configs: %w", err)
		}
	}
	return len(p.Logs), nil
}
