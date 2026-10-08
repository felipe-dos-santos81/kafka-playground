package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func want(partitions int, configs map[string]string) Config {
	return Config{Base: "owner-verify", Instance: 1, Role: RoleMain, Partitions: partitions, ReplicationFactor: 1, Configs: configs}
}

func have(partitions, rf int, configs map[string]string) Actual {
	return Actual{Exists: true, Partitions: partitions, ReplicationFactor: rf, Configs: configs}
}

// alters renders p.Alter as "set k=v" and "delete k".
func alters(p Plan) []string {
	var out []string
	for _, a := range p.Alter {
		if a.Op == kadm.DeleteConfig {
			out = append(out, "delete "+a.Name)
		} else {
			out = append(out, "set "+a.Name+"="+*a.Value)
		}
	}
	return out
}

func TestDiff(t *testing.T) {
	for _, tc := range []struct {
		name       string
		want       Config
		have       Actual
		create     bool
		partitions int
		alter      []string
		logs       []string
		problems   []string
	}{
		{name: "missing", want: want(2, map[string]string{"retention.ms": "3600000"}), have: Actual{},
			create: true, logs: []string{"owner-verify-1: created with 2 partitions"}},
		{name: "in the desired state", want: want(2, map[string]string{"retention.ms": "3600000"}), have: have(2, 1, map[string]string{"retention.ms": "3600000"})},
		{name: "fewer partitions", want: want(3, map[string]string{}), have: have(2, 1, map[string]string{}),
			partitions: 3, logs: []string{"owner-verify-1: partitions 2 -> 3"}},
		{name: "more partitions", want: want(2, map[string]string{}), have: have(3, 1, map[string]string{}),
			problems: []string{"owner-verify-1: has 3 partitions, wants 2: partitions never decrease (delete the topic, or set PARTITIONS=3)"}},
		{name: "replication factor", want: want(1, map[string]string{}), have: have(1, 2, map[string]string{}),
			problems: []string{"owner-verify-1: replication factor 2, wants 1: never changed"}},
		{name: "config differs", want: want(1, map[string]string{"retention.ms": "3600000"}), have: have(1, 1, map[string]string{"retention.ms": "1000"}),
			alter: []string{"set retention.ms=3600000"}, logs: []string{"owner-verify-1: set retention.ms=3600000 (was 1000)"}},
		{name: "config missing", want: want(1, map[string]string{"retention.ms": "-1"}), have: have(1, 1, map[string]string{}),
			alter: []string{"set retention.ms=-1"}, logs: []string{"owner-verify-1: set retention.ms=-1 (was unset)"}},
		{name: "override nobody wants", want: want(1, map[string]string{}), have: have(1, 1, map[string]string{"retention.bytes": "5"}),
			alter: []string{"delete retention.bytes"}, logs: []string{"owner-verify-1: removed retention.bytes (was 5)"}},
		{name: "every change at once, configs in name order", want: want(3, map[string]string{"retention.ms": "1", "cleanup.policy": "delete"}), have: have(2, 1, map[string]string{"segment.ms": "9", "retention.ms": "2"}),
			partitions: 3,
			alter:      []string{"set cleanup.policy=delete", "set retention.ms=1", "delete segment.ms"},
			logs:       []string{"owner-verify-1: partitions 2 -> 3", "owner-verify-1: set cleanup.policy=delete (was unset)", "owner-verify-1: set retention.ms=1 (was 2)", "owner-verify-1: removed segment.ms (was 9)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := diff("owner-verify-1", tc.want, tc.have)
			if p.Create != tc.create || p.Partitions != tc.partitions || !slices.Equal(alters(p), tc.alter) || !slices.Equal(p.Logs, tc.logs) || !slices.Equal(p.Problems, tc.problems) {
				t.Fatalf("got create=%v partitions=%d alter=%q logs=%q problems=%q", p.Create, p.Partitions, alters(p), p.Logs, p.Problems)
			}
		})
	}
}

// fakeOwner is an owner whose broker is have and whose apply fails with applyErr,
// after landing *landed of the plan's logs (all of them when applyErr is nil).
func fakeOwner(cfg Config, have *Actual, describeErr, applyErr *error, landed *int, applied *[]Plan) *owner {
	o := &owner{cfg: cfg, problem: cfg.Topic() + ": not reconciled yet"}
	o.describe = func(context.Context) (Actual, error) { return *have, *describeErr }
	o.apply = func(_ context.Context, p Plan) (int, error) {
		*applied = append(*applied, p)
		if *applyErr != nil {
			return *landed, *applyErr
		}
		return len(p.Logs), nil
	}
	return o
}

func TestOwnerPass(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() { log.SetOutput(os.Stderr); log.SetFlags(log.LstdFlags) }()

	cfg := want(2, map[string]string{})
	actual := have(3, 1, map[string]string{})
	var describeErr, applyErr error
	var landed int
	var applied []Plan
	o := fakeOwner(cfg, &actual, &describeErr, &applyErr, &landed, &applied)
	if o.health() == "" {
		t.Fatal("healthy before the first pass")
	}
	o.pass(context.Background())
	o.pass(context.Background())
	problem := "owner-verify-1: has 3 partitions, wants 2: partitions never decrease (delete the topic, or set PARTITIONS=3)"
	if o.health() != problem {
		t.Fatalf("health %q", o.health())
	}
	if got := strings.Count(logs.String(), problem); got != 1 {
		t.Fatalf("the problem was logged %d times, want once: %q", got, logs.String())
	}

	actual = have(2, 1, map[string]string{"retention.bytes": "5"})
	o.pass(context.Background())
	if o.health() != "" || !strings.Contains(logs.String(), "owner-verify-1: removed retention.bytes (was 5)\n") {
		t.Fatalf("health %q, logs %q", o.health(), logs.String())
	}

	applyErr = errors.New("alter configs: INVALID_CONFIG")
	logs.Reset()
	o.pass(context.Background())
	if o.health() != "owner-verify-1: alter configs: INVALID_CONFIG" || strings.Contains(logs.String(), "removed") {
		t.Fatalf("a failed apply: health %q, logs %q (want no change logged)", o.health(), logs.String())
	}

	applyErr, describeErr = nil, errors.New("unable to dial")
	o.pass(context.Background())
	if o.health() != "owner-verify-1: kafka: unable to dial" {
		t.Fatalf("health %q", o.health())
	}
}

// Kafka down at start, then back; then the topic deleted by hand while the
// owner runs: each pass recovers without a restart.
func TestOwnerRecovers(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	cfg := want(2, map[string]string{})
	actual := Actual{}
	describeErr, applyErr := error(errors.New("unable to dial")), error(nil)
	var landed int
	var applied []Plan
	o := fakeOwner(cfg, &actual, &describeErr, &applyErr, &landed, &applied)
	o.pass(context.Background())
	if o.health() != "owner-verify-1: kafka: unable to dial" || len(applied) != 0 {
		t.Fatalf("kafka down: health %q, applied %d plans", o.health(), len(applied))
	}
	describeErr = nil
	o.pass(context.Background())
	if o.health() != "" || len(applied) != 1 || !applied[0].Create {
		t.Fatalf("kafka back, topic missing: health %q, plans %+v", o.health(), applied)
	}
	actual = have(2, 1, map[string]string{})
	o.pass(context.Background())
	actual = Actual{} // deleted by hand
	o.pass(context.Background())
	if o.health() != "" || len(applied) != 3 || !applied[2].Create {
		t.Fatalf("topic deleted: health %q, plans %+v", o.health(), applied)
	}
}

// A raise of partitions lands, then the config alter fails: the partitions line
// is logged (it changed the topic), the config line is not (it did not), and
// the next pass would see the partitions already there.
func TestOwnerPartialApply(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() { log.SetOutput(os.Stderr); log.SetFlags(log.LstdFlags) }()

	cfg := want(3, map[string]string{"retention.ms": "1"})
	actual := have(2, 1, map[string]string{})
	var describeErr, applyErr error
	var landed int
	var applied []Plan
	o := fakeOwner(cfg, &actual, &describeErr, &applyErr, &landed, &applied)
	applyErr, landed = errors.New("alter configs: INVALID_CONFIG"), 1
	o.pass(context.Background())
	if !strings.Contains(logs.String(), "owner-verify-1: partitions 2 -> 3\n") || strings.Contains(logs.String(), "retention.ms") {
		t.Fatalf("logs %q, want the partitions line and not the config line", logs.String())
	}
	if o.health() != "owner-verify-1: alter configs: INVALID_CONFIG" {
		t.Fatalf("health %q", o.health())
	}
}

// An apply failure does not hide the plan's unfixable problems.
func TestOwnerApplyErrKeepsProblems(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	cfg := want(2, map[string]string{"retention.ms": "1"})
	actual := have(3, 1, map[string]string{})
	var describeErr, applyErr error
	var landed int
	var applied []Plan
	o := fakeOwner(cfg, &actual, &describeErr, &applyErr, &landed, &applied)
	applyErr = errors.New("alter configs: INVALID_CONFIG")
	o.pass(context.Background())
	for _, part := range []string{"owner-verify-1: alter configs: INVALID_CONFIG", "has 3 partitions, wants 2: partitions never decrease"} {
		if !strings.Contains(o.health(), part) {
			t.Fatalf("health %q lacks %q", o.health(), part)
		}
	}
	if !strings.Contains(o.health(), "; ") {
		t.Fatalf("health %q: want the parts joined with \"; \"", o.health())
	}
}

// Losing the create race (a Studio deploy made the topic first) is reported as
// such, not as a create, and is not a create failure.
func TestCreateErr(t *testing.T) {
	if err := createErr(nil); err != nil {
		t.Fatal(err)
	}
	if err := createErr(kerr.TopicAlreadyExists); err != errRaced {
		t.Fatalf("got %v, want errRaced", err)
	}
	if err := createErr(kerr.InvalidReplicationFactor); err == nil || !strings.HasPrefix(err.Error(), "create: ") {
		t.Fatalf("got %v", err)
	}
}

// Only configs set on the topic are owned: broker settings and defaults that
// describe also lists (min.insync.replicas from the broker, say) never count.
func TestTopicLevel(t *testing.T) {
	got := topicLevel([]kadm.Config{
		{Key: "retention.ms", Value: kadm.StringPtr("1000"), Source: kmsg.ConfigSourceDynamicTopicConfig},
		{Key: "min.insync.replicas", Value: kadm.StringPtr("1"), Source: kmsg.ConfigSourceStaticBrokerConfig},
		{Key: "cleanup.policy", Value: kadm.StringPtr("delete"), Source: kmsg.ConfigSourceDefaultConfig},
	})
	if !maps.Equal(got, map[string]string{"retention.ms": "1000"}) {
		t.Fatalf("got %v", got)
	}
}
