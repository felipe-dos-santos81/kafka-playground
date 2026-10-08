// Retry and DLQ: where a consumer's failed record goes, the headers it carries,
// and how the retry loop (poll, in node.go) takes each record of the retry topic. A failure that may
// pass later (the sink, the forward) goes to the retry topic while tries remain,
// then to the DLQ; one that never will (the transform, the router) goes straight
// to the DLQ.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// failure is the step failure that ended a record's path.
type failure struct {
	err       error
	retryable bool // it may pass later: the sink or the forward, not the transform or the router
}

// fromRetry says whether r was read from c's retry topic, not from its input topic.
func (c *consumer) fromRetry(r *kgo.Record) bool {
	return c.spec.Retry != nil && r.Topic == c.spec.Retry.Topic
}

// sendOn sends r, whose path f ended, to the retry topic when f is retryable,
// retry is on and tries remain, else to the DLQ. What it sends is r as read (key
// and value, not the transformed value), so a retry runs r's whole path again,
// with failureHeaders. It reports whether r may be committed: a failed send is
// counted and logged and r still commits, unless the client was closed (Stop),
// which leaves r to be redelivered.
func (c *consumer) sendOn(ctx context.Context, r *kgo.Record, f failure) bool {
	tries := 1 // tries of r that failed, this one included
	if c.fromRetry(r) {
		n, _ := strconv.Atoi(header(r, headerAttempt))
		tries += n
	}
	to, kind, sent := c.spec.DLQ, "dlq", &c.deadLettered
	if spec := c.spec.Retry; f.retryable && spec != nil && tries <= spec.Attempts {
		to, kind, sent = spec.Topic, "retry", &c.retried
	}
	if err := c.write(ctx, &kgo.Record{Topic: to, Key: r.Key, Value: r.Value, Headers: failureHeaders(r, c.spec.Group, tries, f.err)}); err != nil {
		c.counts.fail(fmt.Errorf("%s: %w", kind, err))
		log.Printf("%s to %s: %v", kind, to, err)
		return !errors.Is(err, kgo.ErrClientClosed)
	}
	sent.Add(1)
	return true
}

// The headers a consumer sets on a record it sends to its retry topic or its DLQ.
const (
	headerGroup   = "studio-group"   // the consumer's group: a retry loop skips other groups' records
	headerAttempt = "studio-attempt" // tries of the record that failed so far
	headerError   = "studio-error"   // the last failure: its first line, at most errorHeaderMax bytes
	headerOrigin  = "studio-origin"  // where the record was first read: topic[partition]@offset
)

const errorHeaderMax = 1 << 10

// header is r's last value for key, "" without one.
func header(r *kgo.Record, key string) string {
	v := ""
	for _, h := range r.Headers {
		if h.Key == key {
			v = string(h.Value)
		}
	}
	return v
}

// failureHeaders are r's own headers with the four studio-* ones set for its
// tries'th failed try, err; any other header, studio-* or not, is kept.
// studio-origin keeps where r was first read. The error is cut at
// errorHeaderMax bytes, never inside a character.
func failureHeaders(r *kgo.Record, group string, tries int, err error) []kgo.RecordHeader {
	origin := header(r, headerOrigin)
	if origin == "" {
		origin = fmt.Sprintf("%s[%d]@%d", r.Topic, r.Partition, r.Offset)
	}
	msg := firstLine(err).Error()
	if len(msg) > errorHeaderMax {
		msg = strings.ToValidUTF8(msg[:errorHeaderMax], "") // drops a character the cut split
	}
	ours := []string{headerGroup, headerAttempt, headerError, headerOrigin}
	hs := slices.DeleteFunc(slices.Clone(r.Headers), func(h kgo.RecordHeader) bool { return slices.Contains(ours, h.Key) })
	return append(hs,
		kgo.RecordHeader{Key: headerGroup, Value: []byte(group)},
		kgo.RecordHeader{Key: headerAttempt, Value: []byte(strconv.Itoa(tries))},
		kgo.RecordHeader{Key: headerError, Value: []byte(msg)},
		kgo.RecordHeader{Key: headerOrigin, Value: []byte(origin)},
	)
}

// retryRecord takes one record of the retry topic. Another group's (a shared input
// topic) or one no consumer sent is only marked. Ours waits until it is due, the
// retry delay after it was written (at most the delay, for one dated in the
// future), then goes through handle. It reports whether r may be marked: false
// when Stop cut the wait (or closed the client), which leaves r for the next
// deploy to retry.
func (c *consumer) retryRecord(ctx context.Context, r *kgo.Record) bool {
	if header(r, headerGroup) != c.spec.Group {
		return true
	}
	delay := time.Duration(c.spec.Retry.DelayMS) * time.Millisecond
	if !wait(ctx, min(time.Until(r.Timestamp.Add(delay)), delay)) {
		return false
	}
	return c.handle(ctx, r)
}

// wait waits d (nothing when d ≤ 0) and reports whether it did: false when ctx
// ended first.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
