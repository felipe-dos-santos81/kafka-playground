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
