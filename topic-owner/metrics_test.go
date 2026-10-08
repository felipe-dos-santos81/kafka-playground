package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testCollector(s topicState, err error, reconciled bool) *collector {
	cfg := Config{Base: "orders", Instance: 1, Role: RoleRetry}
	return &collector{cfg: cfg, reconciled: func() bool { return reconciled }, read: func(context.Context) (topicState, error) { return s, err }}
}

func TestCollector(t *testing.T) {
	s := topicState{
		partitions: []partitionState{
			{partition: 0, replicas: 1, isr: 1, start: 2, end: 10, size: 700},
			{partition: 1, replicas: 3, isr: 2, start: 0, end: 4, size: 300},
		},
		committed: map[string]map[int32]int64{
			"orders-1__redelivery": {0: 7},        // partition 1 never committed: no lag series for it
			"studio-g__retry":      {0: 12, 1: 4}, // past the end (a truncated log) counts as 0
		},
	}
	want := `
# HELP kafka_consumergroup_current_offset Offset the consumer group committed on the partition.
# TYPE kafka_consumergroup_current_offset gauge
kafka_consumergroup_current_offset{consumergroup="orders-1__redelivery",partition="0",topic="orders-1__retry"} 7
kafka_consumergroup_current_offset{consumergroup="studio-g__retry",partition="0",topic="orders-1__retry"} 12
kafka_consumergroup_current_offset{consumergroup="studio-g__retry",partition="1",topic="orders-1__retry"} 4
# HELP kafka_consumergroup_lag Log end offset minus the group's committed offset, for committed partitions.
# TYPE kafka_consumergroup_lag gauge
kafka_consumergroup_lag{consumergroup="orders-1__redelivery",partition="0",topic="orders-1__retry"} 3
kafka_consumergroup_lag{consumergroup="studio-g__retry",partition="0",topic="orders-1__retry"} 0
kafka_consumergroup_lag{consumergroup="studio-g__retry",partition="1",topic="orders-1__retry"} 0
# HELP kafka_topic_partition_current_offset Log end offset of the partition.
# TYPE kafka_topic_partition_current_offset gauge
kafka_topic_partition_current_offset{partition="0",topic="orders-1__retry"} 10
kafka_topic_partition_current_offset{partition="1",topic="orders-1__retry"} 4
# HELP kafka_topic_partition_oldest_offset Log start offset of the partition.
# TYPE kafka_topic_partition_oldest_offset gauge
kafka_topic_partition_oldest_offset{partition="0",topic="orders-1__retry"} 2
kafka_topic_partition_oldest_offset{partition="1",topic="orders-1__retry"} 0
# HELP kafka_topic_partition_under_replicated_partition 1 when the partition has fewer in-sync replicas than replicas.
# TYPE kafka_topic_partition_under_replicated_partition gauge
kafka_topic_partition_under_replicated_partition{partition="0",topic="orders-1__retry"} 0
kafka_topic_partition_under_replicated_partition{partition="1",topic="orders-1__retry"} 1
# HELP kafka_topic_partitions Number of partitions of the topic.
# TYPE kafka_topic_partitions gauge
kafka_topic_partitions{topic="orders-1__retry"} 2
# HELP topic_owner_info The topic this container owns, with its base, instance and role.
# TYPE topic_owner_info gauge
topic_owner_info{base="orders",role="retry",topic="orders-1__retry",topic_instance="1"} 1
# HELP topic_owner_kafka_up 1 when this scrape's admin calls to Kafka succeeded.
# TYPE topic_owner_kafka_up gauge
topic_owner_kafka_up{topic="orders-1__retry"} 1
# HELP topic_owner_partition_log_size_bytes Size of the partition's log segments, summed over replicas.
# TYPE topic_owner_partition_log_size_bytes gauge
topic_owner_partition_log_size_bytes{partition="0",topic="orders-1__retry"} 700
topic_owner_partition_log_size_bytes{partition="1",topic="orders-1__retry"} 300
# HELP topic_owner_reconciled 1 when the last reconcile left the topic in its desired state.
# TYPE topic_owner_reconciled gauge
topic_owner_reconciled{topic="orders-1__retry"} 1
`
	if err := testutil.CollectAndCompare(testCollector(s, nil, true), strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// A failed read still serves identity, health and whatever was read before the error.
func TestCollectorKafkaDown(t *testing.T) {
	s := topicState{partitions: []partitionState{{partition: 0, replicas: 1, isr: 1, start: -1, end: -1, size: -1}}}
	want := `
# HELP kafka_topic_partition_under_replicated_partition 1 when the partition has fewer in-sync replicas than replicas.
# TYPE kafka_topic_partition_under_replicated_partition gauge
kafka_topic_partition_under_replicated_partition{partition="0",topic="orders-1__retry"} 0
# HELP kafka_topic_partitions Number of partitions of the topic.
# TYPE kafka_topic_partitions gauge
kafka_topic_partitions{topic="orders-1__retry"} 1
# HELP topic_owner_info The topic this container owns, with its base, instance and role.
# TYPE topic_owner_info gauge
topic_owner_info{base="orders",role="retry",topic="orders-1__retry",topic_instance="1"} 1
# HELP topic_owner_kafka_up 1 when this scrape's admin calls to Kafka succeeded.
# TYPE topic_owner_kafka_up gauge
topic_owner_kafka_up{topic="orders-1__retry"} 0
# HELP topic_owner_reconciled 1 when the last reconcile left the topic in its desired state.
# TYPE topic_owner_reconciled gauge
topic_owner_reconciled{topic="orders-1__retry"} 0
`
	if err := testutil.CollectAndCompare(testCollector(s, errors.New("unable to dial"), false), strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// Before the first pass, or with the topic deleted, a scrape serves only
// identity and health: no partition series, nothing to panic on.
func TestCollectorNoTopic(t *testing.T) {
	if n := testutil.CollectAndCount(testCollector(topicState{}, errors.New("UNKNOWN_TOPIC_OR_PARTITION"), false)); n != 3 {
		t.Fatalf("%d series, want 3 (info, reconciled, kafka_up)", n)
	}
}
