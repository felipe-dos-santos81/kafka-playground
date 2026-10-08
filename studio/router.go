// Routers: ordered rules over msg, the record's value (after its consumer's
// transform) decoded from JSON. A record goes to the topic of the first rule
// whose condition holds, else to the default topic, else nowhere. Validate
// compiles every rule on deploy; the consumer that runs a router compiles them
// again when it starts.
package main

import (
	"fmt"
	"sync/atomic"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// compileRule compiles a rule's condition in a transform's environment. It must
// yield true or false: a condition known not to (a string, a number) does not
// compile; with msg declared any, one of unknown type does, and a result that is
// not a boolean is an error when it runs.
func compileRule(src string) (*vm.Program, error) {
	return compileTransform(src, expr.AsBool())
}

// router is one consumer's compiled rules and its own counts. Each rule runs on
// its own VM (expr.Run), so the consumer's main loop and retry loop can route at
// once.
type router struct {
	rules     []*vm.Program
	topics    []string // rule i's topic
	def       string   // the default topic; "" drops what no rule matches
	counts    counters
	branches  []atomic.Int64 // records per rule, then the default's
	unmatched atomic.Int64   // records dropped: no rule matched, no default
}

// stepTally is what /stats reports for a step a consumer runs: a transform's
// tally, or a router's, which adds its branches and unmatched.
type stepTally struct {
	tally
	Branches  []int64 `json:"branches,omitempty"`  // a router's, per rule, then the default (0 without one)
	Unmatched int64   `json:"unmatched,omitempty"` // a router's dropped records: no rule matched, no default
}

// newRouter compiles routes for one consumer to run.
func newRouter(routes []Route, def string) (*router, error) {
	r := &router{def: def, branches: make([]atomic.Int64, len(routes)+1)}
	for i, rt := range routes {
		p, err := compileRule(rt.When)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
		r.rules = append(r.rules, p)
		r.topics = append(r.topics, rt.Topic)
	}
	return r, nil
}

// route picks a value's topic and counts the outcome. "" with no error drops the
// record (no rule matched, no default), which is not an error.
func (r *router) route(value []byte) (string, error) {
	r.counts.ok()
	topic, err := r.pick(value)
	switch {
	case err != nil:
		r.counts.fail(err)
	case topic == "":
		r.unmatched.Add(1)
	}
	return topic, err
}

// pick is route without the outcome's counting: the first rule that holds, else
// the default. A rule that fails stops it, so a broken rule never falls through.
func (r *router) pick(value []byte) (string, error) {
	msg, err := decodeMsg(value)
	if err != nil {
		return "", err
	}
	env := transformEnv{Msg: msg}
	for i, p := range r.rules {
		res, err := expr.Run(p, env)
		if err != nil {
			return "", fmt.Errorf("rule %d: %w", i+1, firstLine(err))
		}
		if res == true {
			r.branches[i].Add(1)
			return r.topics[i], nil
		}
	}
	if r.def != "" {
		r.branches[len(r.rules)].Add(1)
	}
	return r.def, nil
}

func (r *router) read() stepTally {
	t := stepTally{tally: r.counts.read(), Branches: make([]int64, len(r.branches)), Unmatched: r.unmatched.Load()}
	for i := range r.branches {
		t.Branches[i] = r.branches[i].Load()
	}
	return t
}
