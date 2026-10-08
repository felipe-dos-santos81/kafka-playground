package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
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
// identity and health: no partition series, nothing to panic on. Kafka
// answered, so kafka_up stays 1; reconciled is 0.
func TestCollectorNoTopic(t *testing.T) {
	c := testCollector(topicState{}, nil, false)
	if n := testutil.CollectAndCount(c); n != 3 {
		t.Fatalf("%d series, want 3 (info, reconciled, kafka_up)", n)
	}
	want := `
# HELP topic_owner_kafka_up 1 when this scrape's admin calls to Kafka succeeded.
# TYPE topic_owner_kafka_up gauge
topic_owner_kafka_up{topic="orders-1__retry"} 1
# HELP topic_owner_reconciled 1 when the last reconcile left the topic in its desired state.
# TYPE topic_owner_reconciled gauge
topic_owner_reconciled{topic="orders-1__retry"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "topic_owner_kafka_up", "topic_owner_reconciled"); err != nil {
		t.Fatal(err)
	}
}

// The DLQ's oldest record per partition is fetched once, and again only when
// the partition's start offset moves; an empty partition has none.
func TestOldestTimes(t *testing.T) {
	t0 := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	fetches := 0
	listed := kadm.ListedOffsets{"orders-1__dlq": {
		0: {Topic: "orders-1__dlq", Partition: 0, Offset: 0, Timestamp: t0.UnixMilli()},
		1: {Topic: "orders-1__dlq", Partition: 1, Offset: 0, Timestamp: -1}, // empty: the broker has no timestamp
	}}
	o := &oldestTimes{known: map[int32]oldestAt{}, fetch: func(context.Context) (kadm.ListedOffsets, error) {
		fetches++
		return listed, nil
	}}
	parts := []partitionState{{partition: 0, start: 0, end: 2}, {partition: 1, start: 0, end: 0}}
	for range 2 {
		times, err := o.get(context.Background(), "orders-1__dlq", parts)
		if err != nil || len(times) != 1 || !times[0].Equal(t0) {
			t.Fatalf("times %v, err %v; want partition 0 at %v only", times, err, t0)
		}
	}
	if fetches != 1 {
		t.Fatalf("%d fetches for an unchanged start, want 1", fetches)
	}

	// delete-records moved partition 0's start to 1: fetch again, the new oldest.
	listed["orders-1__dlq"][0] = kadm.ListedOffset{Topic: "orders-1__dlq", Partition: 0, Offset: 1, Timestamp: t0.Add(time.Minute).UnixMilli()}
	parts[0].start = 1
	times, err := o.get(context.Background(), "orders-1__dlq", parts)
	if err != nil || fetches != 2 || !times[0].Equal(t0.Add(time.Minute)) {
		t.Fatalf("times %v, err %v, %d fetches; want the record at offset 1, fetched once more", times, err, fetches)
	}

	// Everything replayed and deleted: no oldest record, no fetch.
	parts[0].start = 2
	if times, _ := o.get(context.Background(), "orders-1__dlq", parts); len(times) != 0 || fetches != 2 {
		t.Fatalf("times %v, %d fetches; want none, no fetch", times, fetches)
	}

	// The topic is deleted and recreated, so the partition was seen empty above
	// and a new record lands at the same start offset: its time is fetched, the
	// deleted record's is not served.
	listed["orders-1__dlq"][0] = kadm.ListedOffset{Topic: "orders-1__dlq", Partition: 0, Offset: 1, Timestamp: t0.Add(2 * time.Minute).UnixMilli()}
	parts[0].start, parts[0].end = 1, 2
	times, err = o.get(context.Background(), "orders-1__dlq", parts)
	if err != nil || fetches != 3 || !times[0].Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("times %v, err %v, %d fetches; want the new record, fetched once more", times, err, fetches)
	}

	// A failed fetch reports its error and serves nothing stale.
	parts[0].start, parts[0].end = 2, 3
	o.fetch = func(context.Context) (kadm.ListedOffsets, error) { return nil, errors.New("unable to dial") }
	if times, err := o.get(context.Background(), "orders-1__dlq", parts); err == nil || len(times) != 0 {
		t.Fatalf("times %v, err %v", times, err)
	}
}

// The DLQ role's collector serves the oldest record's timestamp in seconds.
func TestCollectorOldest(t *testing.T) {
	t0 := time.Date(2026, 10, 8, 14, 0, 0, 500_000_000, time.UTC)
	c := &collector{
		cfg:        Config{Base: "orders", Instance: 1, Role: RoleDLQ},
		reconciled: func() bool { return true },
		read: func(context.Context) (topicState, error) {
			return topicState{partitions: []partitionState{{partition: 0, replicas: 1, isr: 1, start: 4, end: 6, size: 100}}}, nil
		},
		oldest: &oldestTimes{known: map[int32]oldestAt{}, fetch: func(context.Context) (kadm.ListedOffsets, error) {
			return kadm.ListedOffsets{"orders-1__dlq": {0: {Topic: "orders-1__dlq", Partition: 0, Offset: 4, Timestamp: t0.UnixMilli()}}}, nil
		}},
	}
	want := `
# HELP topic_owner_oldest_message_timestamp_seconds Timestamp of the partition's oldest record (at its log start), for non-empty partitions. DLQ role only.
# TYPE topic_owner_oldest_message_timestamp_seconds gauge
topic_owner_oldest_message_timestamp_seconds{partition="0",topic="orders-1__dlq"} 1.7914680005e+09
# HELP topic_owner_kafka_up 1 when this scrape's admin calls to Kafka succeeded.
# TYPE topic_owner_kafka_up gauge
topic_owner_kafka_up{topic="orders-1__dlq"} 1
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "topic_owner_oldest_message_timestamp_seconds", "topic_owner_kafka_up"); err != nil {
		t.Fatal(err)
	}
}

// A scrape whose read fails leaves the oldest-record cache alone: the next
// good scrape serves the cached time without fetching again.
func TestCollectorOldestKeepsCacheOnFailedRead(t *testing.T) {
	t0 := time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)
	fetches := 0
	readErr := error(nil)
	c := &collector{
		cfg:        Config{Base: "orders", Instance: 1, Role: RoleDLQ},
		reconciled: func() bool { return true },
		read: func(context.Context) (topicState, error) {
			if readErr != nil {
				return topicState{}, readErr
			}
			return topicState{partitions: []partitionState{{partition: 0, replicas: 1, isr: 1, start: 4, end: 6, size: 100}}}, nil
		},
		oldest: &oldestTimes{known: map[int32]oldestAt{}, fetch: func(context.Context) (kadm.ListedOffsets, error) {
			fetches++
			return kadm.ListedOffsets{"orders-1__dlq": {0: {Topic: "orders-1__dlq", Partition: 0, Offset: 4, Timestamp: t0.UnixMilli()}}}, nil
		}},
	}
	testutil.CollectAndCount(c)
	readErr = errors.New("unable to dial")
	testutil.CollectAndCount(c)
	readErr = nil
	if n := testutil.CollectAndCount(c, "topic_owner_oldest_message_timestamp_seconds"); n != 1 || fetches != 1 {
		t.Fatalf("%d oldest series, %d fetches; want 1 series from the cache, 1 fetch in all", n, fetches)
	}
}
