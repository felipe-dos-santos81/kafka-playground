package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestTopicErr(t *testing.T) {
	exists := kadm.CreateTopicResponse{Topic: "orders", Err: kerr.TopicAlreadyExists}
	created := kadm.CreateTopicResponse{Topic: "audit"}
	denied := kadm.CreateTopicResponse{Topic: "bad", Err: kerr.InvalidReplicationFactor}
	if err := topicErr(kadm.CreateTopicResponses{"orders": exists, "audit": created}); err != nil {
		t.Fatalf("an existing and a new topic: want nil, got %v", err)
	}
	err := topicErr(kadm.CreateTopicResponses{"orders": exists, "bad": denied})
	if !errors.Is(err, kerr.InvalidReplicationFactor) || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("want the bad topic's error, got %v", err)
	}
}
