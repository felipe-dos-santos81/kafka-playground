package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// A lone topic validates for Deploy but has nothing to start; Deploy must say so
// before it touches Docker or Kafka (both nil here).
func TestDeployNothingToRun(t *testing.T) {
	e := &Engine{store: Store{dir: t.TempDir()}}
	f := Flow{ID: "0123abcd", Name: "lone", Nodes: []Node{
		node("topic-1", "topic", `{"name":"orders","partitions":3,"replication_factor":1}`),
	}}
	if err := e.store.Put(f); err != nil {
		t.Fatal(err)
	}
	var ps Problems
	if err := e.Deploy(context.Background(), f.ID); !errors.As(err, &ps) {
		t.Fatalf("want Problems, got %v", err)
	}
}

// A container-name clash is refused like any other problem, before Docker or
// Kafka (both nil here) are touched.
func TestDeployRefusesNameClash(t *testing.T) {
	e := &Engine{store: Store{dir: t.TempDir()}}
	f := clone(good)
	f.ID = "0123abcd"
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":2,"sink":{"kind":"log"}}`)
	f.Nodes = append(f.Nodes, node("consumer-1-2", "consumer", `{"group":"h","sink":{"kind":"log"}}`))
	f.Edges = append(f.Edges, edge("topic-1", "consumer-1-2"))
	if err := e.store.Put(f); err != nil {
		t.Fatal(err)
	}
	var ps Problems
	if err := e.Deploy(context.Background(), f.ID); !errors.As(err, &ps) || len(ps) != 1 || ps[0].Node != "consumer-1-2" {
		t.Fatalf("want one problem on consumer-1-2, got %v", err)
	}
}

func TestNodeStates(t *testing.T) {
	ctr := func(node string, instance int, state container.ContainerState) container.Summary {
		labels := map[string]string{labelFlow: "f", labelNode: node}
		if instance > 0 {
			labels[labelInstance] = strconv.Itoa(instance)
		}
		return container.Summary{Labels: labels, State: state}
	}
	f := clone(good)
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","instances":3,"sink":{"kind":"log"}}`)

	// Instance 1 runs, 2 exited, 3 was removed by hand; the producer has no container.
	got := nodeStates(f, []container.Summary{ctr("consumer-1", 1, container.StateRunning), ctr("consumer-1", 2, container.StateExited)})
	want := map[string]NodeState{
		"producer-1": {State: "missing"},
		"consumer-1": {State: "exited", Instances: []NodeState{{Instance: 1, State: "running"}, {Instance: 2, State: "exited"}, {Instance: 3, State: "missing"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("instances:\n got %+v\nwant %+v", got, want)
	}

	// Deployed with one consumer, file edited to three since: what runs is what counts.
	got = nodeStates(f, []container.Summary{ctr("producer-1", 0, container.StateRunning), ctr("consumer-1", 0, container.StateRunning)})
	if c := got["consumer-1"]; c.State != "running" || c.Instances != nil {
		t.Fatalf("deployed single, file says 3: want running with no instances, got %+v", c)
	}

	// Deployed with three, file edited to one since.
	f.Nodes[2].Data = json.RawMessage(`{"group":"g","sink":{"kind":"log"}}`)
	got = nodeStates(f, []container.Summary{ctr("consumer-1", 1, container.StateRunning), ctr("consumer-1", 2, container.StateRunning)})
	if c := got["consumer-1"]; c.State != "running" || len(c.Instances) != 2 {
		t.Fatalf("deployed 2, file says 1: want running with its 2 instances, got %+v", c)
	}
}

func TestPickInstance(t *testing.T) {
	single := NodeState{State: "running"}
	multi := NodeState{State: "exited", Instances: []NodeState{{Instance: 1, State: "running"}, {Instance: 2, State: "exited"}}}
	for _, c := range []struct {
		name   string
		ns     NodeState
		asked  int
		want   int
		errHas string // "" means no error
	}{
		{"single", single, 0, 0, ""},
		{"single is its own instance 1", single, 1, 0, ""},
		{"single asked for another instance", single, 2, 0, "no instance 2"},
		{"single exited", NodeState{State: "exited"}, 0, 0, "is exited"},
		{"multi defaults to its first", multi, 0, 1, ""},
		{"multi, a stopped instance", multi, 2, 0, "instance 2 is exited"},
		{"multi, no such instance", multi, 7, 0, "no instance 7"},
	} {
		got, err := pickInstance("consumer-1", c.ns, c.asked)
		if c.errHas == "" && (err != nil || got != c.want) {
			t.Errorf("%s: got %d, %v; want %d", c.name, got, err, c.want)
		}
		if c.errHas != "" && (err == nil || !errors.Is(err, ErrNotRunning) || !strings.Contains(err.Error(), c.errHas)) {
			t.Errorf("%s: want ErrNotRunning naming %q, got %v", c.name, c.errHas, err)
		}
	}
}

func TestApplyKafkaInstances(t *testing.T) {
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"consumer-1": {State: "running", Instances: []NodeState{{Instance: 1, State: "running"}, {Instance: 2, State: "running"}}},
	}}
	specs := []NodeSpec{
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: 1},
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: 2},
	}
	one := &kadm.DescribedGroupMember{ClientID: nodeRef{"f", "consumer-1", 1}.name()}
	two := &kadm.DescribedGroupMember{ClientID: nodeRef{"f", "consumer-1", 2}.name()}
	lags := kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": {
		0: {Topic: "orders", Partition: 0, Lag: 1, Member: two},
		1: {Topic: "orders", Partition: 1, Lag: 2, Member: one},
		2: {Topic: "orders", Partition: 2, Lag: 3, Member: two},
	}}}}
	applyKafka(&st, "f", specs, nil, lags, nil)
	c := st.Nodes["consumer-1"]
	if c.Lag == nil || *c.Lag != 6 || c.Assigned != nil {
		t.Fatalf("node: want the group's lag 6 and no node-level assignment, got %+v", c)
	}
	if a := c.Instances[0].Assigned; !reflect.DeepEqual(a, map[string][]int32{"orders": {1}}) {
		t.Fatalf("instance 1: want [1], got %v", a)
	}
	if a := c.Instances[1].Assigned; !reflect.DeepEqual(a, map[string][]int32{"orders": {0, 2}}) {
		t.Fatalf("instance 2: want [0 2], got %v", a)
	}
}

// The containers that run decide who holds what, not the file edited since the deploy.
func TestApplyKafkaFollowsWhatRuns(t *testing.T) {
	lags := func(ids ...string) kadm.DescribedGroupLags {
		m := map[int32]kadm.GroupMemberLag{}
		for p, id := range ids {
			m[int32(p)] = kadm.GroupMemberLag{Topic: "orders", Partition: int32(p), Member: &kadm.DescribedGroupMember{ClientID: id}}
		}
		return kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": m}}}
	}
	spec := func(i int) NodeSpec {
		return NodeSpec{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g", Instance: i}
	}

	// Deployed with one container, file edited to two instances since.
	st := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
	applyKafka(&st, "f", []NodeSpec{spec(1), spec(2)}, nil, lags(nodeRef{"f", "consumer-1", 0}.name()), nil)
	if a := st.Nodes["consumer-1"].Assigned; !reflect.DeepEqual(a, map[string][]int32{"orders": {0}}) {
		t.Fatalf("single container, file says 2: want [0] on the node, got %v", a)
	}

	// Deployed with three instances, file edited to one since.
	st = FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running", Instances: []NodeState{
		{Instance: 1, State: "running"}, {Instance: 2, State: "running"}, {Instance: 3, State: "running"}}}}}
	applyKafka(&st, "f", []NodeSpec{spec(0)}, nil, lags(nodeRef{"f", "consumer-1", 2}.name(), nodeRef{"f", "consumer-1", 3}.name(), nodeRef{"f", "consumer-1", 1}.name()), nil)
	c := st.Nodes["consumer-1"]
	if c.Assigned != nil {
		t.Fatalf("instances: want no node-level assignment, got %v", c.Assigned)
	}
	for i, want := range []int32{2, 0, 1} {
		if a := c.Instances[i].Assigned; !reflect.DeepEqual(a, map[string][]int32{"orders": {want}}) {
			t.Fatalf("instance %d: want [%d], got %v", i+1, want, a)
		}
	}
}

func TestApplyKafka(t *testing.T) {
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"producer-1": {State: "running", Total: 9},
		"consumer-1": {State: "running", Total: 4},
		"consumer-2": {State: "running"},
	}}
	specs := []NodeSpec{
		{Node: "producer-1", Type: "producer", Topic: "orders"},
		{Node: "consumer-1", Type: "consumer", Topic: "orders", Group: "g"},
		{Node: "consumer-2", Type: "consumer", Topic: "orders", Group: "g"},
	}
	topics := map[string]TopicData{"topic-1": {Name: "orders", Partitions: 2}, "topic-2": {Name: "gone", Partitions: 1}}
	one := &kadm.DescribedGroupMember{ClientID: nodeRef{"f", "consumer-1", 0}.name()}
	two := &kadm.DescribedGroupMember{ClientID: nodeRef{"f", "consumer-2", 0}.name()}
	lags := kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": {
		0: {Topic: "orders", Partition: 0, Lag: 2, Member: one},
		1: {Topic: "orders", Partition: 1, Lag: 3, Member: two},
		2: {Topic: "orders", Partition: 2, Lag: -1, Member: one, Err: errors.New("no end offset")},
	}}}}
	ends := kadm.ListedOffsets{
		"orders": {0: {Partition: 0, Offset: 10}, 1: {Partition: 1, Offset: 5}, 2: {Partition: 2, Offset: 7}},
		"gone":   {-1: {Partition: -1, Offset: -1, Err: kerr.UnknownTopicOrPartition}},
	}
	applyKafka(&st, "f", specs, topics, lags, ends)

	c1, c2 := st.Nodes["consumer-1"], st.Nodes["consumer-2"]
	if c1.Total != 4 || c1.Lag == nil || *c1.Lag != 5 || !reflect.DeepEqual(c1.Assigned, map[string][]int32{"orders": {0, 2}}) {
		t.Fatalf("consumer-1: want total 4, lag 5 (errored partition skipped), assigned [0 2]; got %+v lag %v", c1, c1.Lag)
	}
	if c2.Lag == nil || *c2.Lag != 5 || !reflect.DeepEqual(c2.Assigned, map[string][]int32{"orders": {1}}) {
		t.Fatalf("consumer-2: want the group's lag 5 and assigned [1]; got %+v", c2)
	}
	if p := st.Nodes["producer-1"]; p.Total != 9 || p.Lag != nil {
		t.Fatalf("producer-1 must be left as it was, got %+v", p)
	}
	t1 := st.Nodes["topic-1"]
	if t1.State != "ready" || t1.Partitions != 3 || t1.EndOffset != 22 || !strings.Contains(t1.Warning, "3 partitions") || !strings.Contains(t1.Warning, "asks for 2") {
		t.Fatalf("topic-1: want ready, 3 partitions, end 22 and a warning naming 3 and 2; got %+v", t1)
	}
	if t2 := st.Nodes["topic-2"]; t2.State != "missing" || t2.Partitions != 0 {
		t.Fatalf("topic-2: want missing, got %+v", t2)
	}

	// The broker did not answer: container states stay, nothing is invented.
	bare := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
	applyKafka(&bare, "f", specs, topics, nil, nil)
	if len(bare.Nodes) != 1 || bare.Nodes["consumer-1"].Lag != nil {
		t.Fatalf("no Kafka answers: want the snapshot unchanged, got %+v", bare.Nodes)
	}

	// A topic with a partition whose leader did not answer is left out, with no warning.
	topics["topic-3"] = TopicData{Name: "flaky", Partitions: 2}
	ends["flaky"] = map[int32]kadm.ListedOffset{0: {Partition: 0, Offset: 4}, 1: {Partition: 1, Offset: -1, Err: errors.New("leader not available")}}
	st = FlowState{Status: "running", Nodes: map[string]NodeState{}}
	applyKafka(&st, "f", specs, topics, nil, ends)
	if n, ok := st.Nodes["topic-3"]; ok {
		t.Fatalf("topic-3 has a failed partition: want it left out, got %+v", n)
	}
	if n := st.Nodes["topic-1"]; n.State != "ready" {
		t.Fatalf("topic-1 is unaffected by topic-3's failure, got %+v", n)
	}

	// A group with no commit yet shows no lag. Once it has one, a committed
	// partition counts from its commit; one without (kadm says At -1, with a lag
	// from the partition's start) counts from where auto_offset_reset starts: the
	// start for earliest, nothing for latest.
	uncommitted := kadm.GroupMemberLag{Topic: "orders", Partition: 0, Lag: 7, Commit: kadm.Offset{At: -1}}
	committed := kadm.GroupMemberLag{Topic: "orders", Partition: 1, Lag: 2, Commit: kadm.Offset{At: 3}}
	lagOf := func(n int64) *int64 { return &n }
	for _, c := range []struct {
		reset string
		lag   map[int32]kadm.GroupMemberLag
		want  *int64
	}{
		{"earliest", map[int32]kadm.GroupMemberLag{0: uncommitted}, nil},
		{"latest", map[int32]kadm.GroupMemberLag{0: uncommitted}, nil},
		{"earliest", map[int32]kadm.GroupMemberLag{0: uncommitted, 1: committed}, lagOf(9)},
		{"latest", map[int32]kadm.GroupMemberLag{0: uncommitted, 1: committed}, lagOf(2)},
	} {
		fresh := FlowState{Status: "running", Nodes: map[string]NodeState{"consumer-1": {State: "running"}}}
		spec := specs[1]
		spec.AutoOffsetReset = c.reset
		applyKafka(&fresh, "f", []NodeSpec{spec}, nil, kadm.DescribedGroupLags{"g": {Group: "g", Lag: kadm.GroupLag{"orders": c.lag}}}, nil)
		if got := fresh.Nodes["consumer-1"].Lag; (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
			t.Fatalf("%s lag over %v: got %v, want %v", c.reset, c.lag, got, c.want)
		}
	}

	// A non-nil but empty answer means the broker answered and knows no such topic
	// (a failed call is dropped by Snapshot, so it never reaches here as empty).
	empty := FlowState{Status: "running", Nodes: map[string]NodeState{}}
	applyKafka(&empty, "f", specs, topics, nil, kadm.ListedOffsets{})
	if n := empty.Nodes["topic-1"]; n.State != "missing" {
		t.Fatalf("empty end offsets: want topic-1 missing, got %+v", n)
	}
}

func TestWithRates(t *testing.T) {
	prev := FlowState{Nodes: map[string]NodeState{
		"steady":    {Boot: "a", Total: 10},
		"restarted": {Boot: "a", Total: 10},
		"fell":      {Boot: "a", Total: 50},
	}}
	cur := FlowState{Nodes: map[string]NodeState{
		"steady":    {Boot: "a", Total: 31},
		"restarted": {Boot: "b", Total: 3},
		"fell":      {Boot: "a", Total: 40},
		"new":       {Boot: "a", Total: 5},
		"topic-1":   {State: "ready", EndOffset: 99},
	}}
	if withRates(&cur, prev, 0); cur.Nodes["steady"].Rate != 0 {
		t.Fatalf("Δt 0 (a clock step): want no rate, got %v", cur.Nodes["steady"].Rate)
	}
	withRates(&cur, prev, 2)
	for node, want := range map[string]float64{"steady": 10.5, "restarted": 0, "fell": 0, "new": 0, "topic-1": 0} {
		if got := cur.Nodes[node].Rate; got != want {
			t.Errorf("%s: rate %v, want %v", node, got, want)
		}
	}

	// A node with instances: each instance against its own previous sample, the node their sum.
	prev = FlowState{Nodes: map[string]NodeState{"c": {Instances: []NodeState{{Instance: 1, Boot: "a", Total: 10}, {Instance: 2, Boot: "a", Total: 4}}}}}
	cur = FlowState{Nodes: map[string]NodeState{"c": {Total: 26, Instances: []NodeState{{Instance: 1, Boot: "a", Total: 20}, {Instance: 2, Boot: "b", Total: 1}, {Instance: 3, Boot: "a", Total: 5}}}}}
	withRates(&cur, prev, 2)
	c := cur.Nodes["c"]
	if c.Rate != 5 || c.Instances[0].Rate != 5 || c.Instances[1].Rate != 0 || c.Instances[2].Rate != 0 {
		t.Fatalf("want instance 1 at 5/s, the restarted and the new one without a rate, the node at 5/s; got %+v", c)
	}
}

func TestStreamTicks(t *testing.T) {
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	snap := func(context.Context) (FlowState, error) {
		calls++
		switch calls {
		case 1:
			return FlowState{Status: "running", Nodes: map[string]NodeState{"p": {State: "running", Boot: "a"}}}, nil
		case 2:
			return FlowState{}, errors.New("docker: down")
		case 3:
			return FlowState{Status: "running", Nodes: map[string]NodeState{"p": {State: "running", Boot: "a", Total: 5}}}, nil
		}
		cancel() // the browser went away
		return FlowState{}, context.Canceled
	}
	done := make(chan struct{})
	go func() {
		streamTicks(ctx, &out, func() error { return nil }, snap, time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the stream did not end with its context")
	}
	s := out.String()
	if strings.Count(s, "event: tick\ndata: {") != 2 ||
		!strings.Contains(s, "event: problem\ndata: {\"error\":\"docker: down\"}\n\n") ||
		strings.Count(s, `"rate":`) != 1 {
		t.Fatalf("want two ticks around a problem event, the second tick with a rate; got:\n%s", s)
	}
}

func TestApplySteps(t *testing.T) {
	specs := []NodeSpec{
		{Node: "consumer-1", Type: "consumer", Instance: 1, TransformNode: "transform-1"},
		{Node: "consumer-1", Type: "consumer", Instance: 2, TransformNode: "transform-1"},
		{Node: "consumer-2", Type: "consumer", TransformNode: "transform-2"},
		{Node: "consumer-3", Type: "consumer"},
		{Node: "consumer-4", Type: "consumer", Instance: 1, TransformNode: "transform-3"},
		{Node: "consumer-4", Type: "consumer", Instance: 2, TransformNode: "transform-3"},
		{Node: "consumer-5", Type: "consumer", TransformNode: "transform-4"},
		{Node: "consumer-6", Type: "consumer", TransformNode: "transform-5"},
	}
	st := FlowState{Status: "running", Nodes: map[string]NodeState{
		"consumer-1": {State: "exited", Instances: []NodeState{
			{Instance: 1, State: "running", Boot: "a", step: &tally{Total: 5, Errors: 1, LastError: "invalid operation"}},
			{Instance: 2, State: "exited"},
		}},
		"consumer-2": {State: "running", Boot: "c", step: &tally{Total: 3}},
		"consumer-3": {State: "running"},
		"consumer-4": {State: "running", Instances: []NodeState{
			{Instance: 1, State: "running", Boot: "a", step: &tally{Total: 5, Errors: 1, LastError: "first"}},
			{Instance: 2, State: "running", Boot: "b", step: &tally{Total: 3, LastError: "second"}},
		}},
		"consumer-5": {State: "running", Boot: "d", answered: true}, // answers, but no container reports transform-4
		"consumer-6": {State: "running"},                            // runs, but its /stats did not answer this tick
	}}
	applySteps(&st, specs)
	want := map[string]NodeState{
		"transform-1": {State: "exited", Total: 5, Errors: 1, LastError: "#1: invalid operation", Boot: "a"},
		"transform-2": {State: "running", Total: 3, Boot: "c"},
		"transform-3": {State: "running", Total: 8, Errors: 1, LastError: "#1: first", Boot: "a,b"},
		"transform-4": {State: "missing"},
		"transform-5": {State: "running"},
	}
	for id, w := range want {
		if got := st.Nodes[id]; !reflect.DeepEqual(got, w) {
			t.Errorf("%s:\n got %+v\nwant %+v", id, got, w)
		}
	}
	if len(st.Nodes) != 11 {
		t.Fatalf("only the five transform nodes are added, got %v", st.Nodes)
	}
}

func TestStreamTicksEnds(t *testing.T) {
	for _, c := range []struct {
		name  string
		snap  func(context.Context) (FlowState, error)
		flush func() error
		want  string // what was written before the stream ended
	}{
		{"a failed flush", func(context.Context) (FlowState, error) { return FlowState{Status: "stopped"}, nil },
			func() error { return errors.New("broken pipe") }, "event: tick\ndata: {\"status\":\"stopped\",\"nodes\":null}\n\n"},
		{"the flow deleted", func(context.Context) (FlowState, error) { return FlowState{}, ErrNotFound },
			func() error { return nil }, "event: gone\ndata: {}\n\n"},
	} {
		var out bytes.Buffer
		done := make(chan struct{})
		go func() {
			streamTicks(context.Background(), &out, c.flush, c.snap, time.Millisecond)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: the stream did not end", c.name)
		}
		if out.String() != c.want {
			t.Fatalf("%s: wrote %q, want %q", c.name, out.String(), c.want)
		}
	}
}

func TestWithStats(t *testing.T) {
	status := http.StatusOK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, `{"boot":"b","total":7,"errors":1,"lastError":"x","tailSeq":7}`)
	}))
	defer ts.Close()
	got := withStats(context.Background(), ts.URL, NodeState{State: "running"}, false)
	if got.Total != 7 || got.Errors != 1 || got.LastError != "x" || got.TailSeq != 7 || got.Boot != "b" || got.Warning != "" || !got.answered {
		t.Fatalf("a 200: want its counters, got %+v", got)
	}
	status = http.StatusInternalServerError
	got = withStats(context.Background(), ts.URL, NodeState{State: "running"}, false)
	if got.Total != 0 || got.LastError != "" || got.Warning != "stats: 500 Internal Server Error" || got.answered {
		t.Fatalf("a 500: want no numbers and the reason as a warning, got %+v", got)
	}
	if got = withStats(context.Background(), ts.URL, NodeState{State: "running"}, true); got.Warning != "" {
		t.Fatalf("a container younger than statsGrace: want nothing said yet, got %+v", got)
	}

	ns := NodeState{Instances: []NodeState{{Instance: 1, Total: 2}, {Instance: 2, Warning: "stats: timeout"}}}
	if ns.sumInstances(); ns.Total != 2 || ns.Warning != "#2: stats: timeout" {
		t.Fatalf("instances: want their sum and the first warning with its instance, got %+v", ns)
	}
}

func TestRewindTarget(t *testing.T) {
	r, err := rewindTarget(clone(good), "consumer-1", Earliest)
	if err != nil || r != (Rewound{Group: "orders-workers", Topic: "orders", To: Earliest}) {
		t.Fatalf("a wired consumer: want group orders-workers on orders, got %+v, %v", r, err)
	}
	unwired := clone(good)
	unwired.Edges = unwired.Edges[:1] // the consumer reads no topic
	for _, c := range []struct {
		name, node string
		to         RewindTo
		flow       Flow
		want       string // in the problem's message; "" means ErrNoNode
	}{
		{"bad to", "consumer-1", "middle", good, `to must be "earliest" or "latest"`},
		{"not a consumer", "producer-1", "earliest", good, "only a consumer has a group to rewind"},
		{"unwired", "consumer-1", "latest", unwired, "wire it to a topic first"},
		{"unknown node", "consumer-9", "earliest", good, ""},
	} {
		_, err := rewindTarget(clone(c.flow), c.node, c.to)
		var ps Problems
		switch {
		case c.want == "":
			if !errors.Is(err, ErrNoNode) {
				t.Errorf("%s: want ErrNoNode, got %v", c.name, err)
			}
		case !errors.As(err, &ps) || len(ps) != 1 || ps[0].Node != c.node || !strings.Contains(ps[0].Message, c.want):
			t.Errorf("%s: want one problem on %s containing %q, got %v", c.name, c.node, c.want, err)
		}
	}
}
