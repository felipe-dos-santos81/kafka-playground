// The SSE stream of a flow's snapshots (spec 3.6): one loop per open stream, with
// rates computed against the stream's previous snapshot.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

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

// streamTicks is the body of GET /api/flows/{id}/events: every period it writes
// the flow's snapshot as an SSE `tick` event with rates against the previous
// tick (Δt between the two snapshots' starts); a failed snapshot is a `problem`
// event and the stream goes on. It returns when ctx ends (the browser went
// away), a flush fails, or the flow is gone: the browser's reconnect then gets
// a 404. Each open stream polls on its own: one tab, one loop.
func streamTicks(ctx context.Context, w io.Writer, flush func() error, snap func(context.Context) (FlowState, error), every time.Duration) {
	var prev FlowState
	last := time.Now()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		start := time.Now()
		st, err := snap(ctx)
		if ctx.Err() != nil || errors.Is(err, ErrNotFound) {
			return
		}
		if err != nil {
			b, _ := json.Marshal(map[string]string{"error": err.Error()})
			fmt.Fprintf(w, "event: problem\ndata: %s\n\n", b)
		} else {
			withRates(&st, prev, start.Sub(last).Seconds())
			prev, last = st, start
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
