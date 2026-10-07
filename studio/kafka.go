// Kafka for the control plane: topic creation at deploy and the health check's
// ping. Node containers run their own clients (node.go).
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// createTopics creates every topic. It is idempotent: a topic that already
// exists counts as created, whatever its partition count (spec risk 7).
func createTopics(ctx context.Context, adm *kadm.Client, topics []TopicData) error {
	for _, t := range topics {
		rs, err := adm.CreateTopics(ctx, int32(t.Partitions), int16(t.ReplicationFactor), nil, t.Name)
		if err != nil {
			return err
		}
		if err := topicErr(rs); err != nil {
			return err
		}
	}
	return nil
}

// topicErr is the first per-topic error in rs other than TopicAlreadyExists.
func topicErr(rs kadm.CreateTopicResponses) error {
	for _, r := range rs {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

// rewindGroup commits, for every partition of r's topic, its start (Earliest) or
// end (Latest) offset as r's group's position, and says how many partitions it set.
func rewindGroup(ctx context.Context, adm *kadm.Client, r Rewound) (int, error) {
	list := adm.ListStartOffsets
	if r.To == Latest {
		list = adm.ListEndOffsets
	}
	listed, err := list(ctx, r.Topic)
	if err != nil {
		return 0, err
	}
	if err := listed.Error(); errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, ErrNoTopic
	} else if err != nil {
		return 0, err
	}
	if err := adm.CommitAllOffsets(ctx, r.Group, listed.Offsets()); err != nil {
		return 0, fmt.Errorf("commit %s: %w", r.Group, err)
	}
	return len(listed[r.Topic]), nil
}
