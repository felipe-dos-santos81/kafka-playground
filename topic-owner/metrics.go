package main

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// scrapeDeadline bounds one scrape's admin calls; it fits Prometheus's 4 s scrape_timeout.
const scrapeDeadline = 3 * time.Second

// Series that mean exactly what kafka-exporter's mean keep its names and labels,
// so its dashboards work on them; the rest are topic_owner_*.
var (
	descInfo            = prometheus.NewDesc("topic_owner_info", "The topic this container owns, with its base, instance and role.", []string{"topic", "base", "topic_instance", "role"}, nil)
	descReconciled      = prometheus.NewDesc("topic_owner_reconciled", "1 when the last reconcile left the topic in its desired state.", []string{"topic"}, nil)
	descKafkaUp         = prometheus.NewDesc("topic_owner_kafka_up", "1 when this scrape's admin calls to Kafka succeeded.", []string{"topic"}, nil)
	descPartitions      = prometheus.NewDesc("kafka_topic_partitions", "Number of partitions of the topic.", []string{"topic"}, nil)
	descUnderReplicated = prometheus.NewDesc("kafka_topic_partition_under_replicated_partition", "1 when the partition has fewer in-sync replicas than replicas.", []string{"topic", "partition"}, nil)
	descEndOffset       = prometheus.NewDesc("kafka_topic_partition_current_offset", "Log end offset of the partition.", []string{"topic", "partition"}, nil)
	descStartOffset     = prometheus.NewDesc("kafka_topic_partition_oldest_offset", "Log start offset of the partition.", []string{"topic", "partition"}, nil)
	descLogSize         = prometheus.NewDesc("topic_owner_partition_log_size_bytes", "Size of the partition's log segments, summed over replicas.", []string{"topic", "partition"}, nil)
	descGroupOffset     = prometheus.NewDesc("kafka_consumergroup_current_offset", "Offset the consumer group committed on the partition.", []string{"consumergroup", "topic", "partition"}, nil)
	descGroupLag        = prometheus.NewDesc("kafka_consumergroup_lag", "Log end offset minus the group's committed offset, for committed partitions.", []string{"consumergroup", "topic", "partition"}, nil)
)

// partitionState is one partition as a scrape reads it.
type partitionState struct {
	partition     int32
	replicas, isr int
	start, end    int64 // log start and end offsets; -1 when unread
	size          int64 // bytes, summed over replicas; -1 when unread
}

// topicState is a topic as a scrape reads it: what was read before any error.
type topicState struct {
	partitions []partitionState           // in partition order
	committed  map[string]map[int32]int64 // group → partition → committed offset
}

// collector serves the topic's series at scrape time.
type collector struct {
	cfg        Config
	reconciled func() bool                               // whether the last reconcile left the topic in its desired state
	read       func(context.Context) (topicState, error) // the topic and its groups, as the broker has them
}

func newCollector(cfg Config, adm *kadm.Client, reconciled func() bool) *collector {
	return &collector{cfg: cfg, reconciled: reconciled, read: func(ctx context.Context) (topicState, error) {
		return readTopic(ctx, adm, cfg.Topic())
	}}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descInfo, descReconciled, descKafkaUp, descPartitions, descUnderReplicated, descEndOffset, descStartOffset, descLogSize, descGroupOffset, descGroupLag} {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	topic := c.cfg.Topic()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	gauge(descInfo, 1, topic, c.cfg.Base, strconv.Itoa(c.cfg.Instance), string(c.cfg.Role))
	gauge(descReconciled, boolValue(c.reconciled()), topic)
	ctx, cancel := context.WithTimeout(context.Background(), scrapeDeadline)
	defer cancel()
	s, err := c.read(ctx)
	gauge(descKafkaUp, boolValue(err == nil), topic)
	if len(s.partitions) > 0 {
		gauge(descPartitions, float64(len(s.partitions)), topic)
	}
	end := map[int32]int64{}
	for _, p := range s.partitions {
		part := strconv.Itoa(int(p.partition))
		gauge(descUnderReplicated, boolValue(p.isr < p.replicas), topic, part)
		if p.end >= 0 {
			end[p.partition] = p.end
			gauge(descEndOffset, float64(p.end), topic, part)
		}
		if p.start >= 0 {
			gauge(descStartOffset, float64(p.start), topic, part)
		}
		if p.size >= 0 {
			gauge(descLogSize, float64(p.size), topic, part)
		}
	}
	for group, parts := range s.committed {
		for p, at := range parts {
			part := strconv.Itoa(int(p))
			gauge(descGroupOffset, float64(at), group, topic, part)
			if e, ok := end[p]; ok {
				gauge(descGroupLag, float64(max(e-at, 0)), group, topic, part)
			}
		}
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// readTopic reads topic's partitions, offsets, log sizes and the committed
// offsets of every group with a commit on it. On an error it returns what it
// read before it.
func readTopic(ctx context.Context, adm *kadm.Client, topic string) (topicState, error) {
	var s topicState
	tds, err := adm.ListTopics(ctx, topic)
	if err == nil {
		err = tds[topic].Err
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return s, nil // Kafka answered: the topic is missing (topic_owner_reconciled says so)
	}
	if err != nil {
		return s, err
	}
	td := tds[topic]
	for _, pd := range td.Partitions.Sorted() {
		s.partitions = append(s.partitions, partitionState{partition: pd.Partition, replicas: len(pd.Replicas), isr: len(pd.ISR), start: -1, end: -1, size: -1})
	}
	at := func(p int32) *partitionState {
		i := slices.IndexFunc(s.partitions, func(ps partitionState) bool { return ps.partition == p })
		if i < 0 {
			return nil
		}
		return &s.partitions[i]
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	ends, err2 := adm.ListEndOffsets(ctx, topic)
	if err = errors.Join(err, err2); err != nil {
		return s, err
	}
	starts.Each(func(o kadm.ListedOffset) {
		if p := at(o.Partition); p != nil && o.Err == nil {
			p.start = o.Offset
		}
	})
	ends.Each(func(o kadm.ListedOffset) {
		if p := at(o.Partition); p != nil && o.Err == nil {
			p.end = o.Offset
		}
	})
	var set kadm.TopicsSet
	set.Add(topic, td.Partitions.Numbers()...)
	dirs, err := adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return s, err
	}
	for _, broker := range dirs {
		broker.EachPartition(func(d kadm.DescribedLogDirPartition) {
			if p := at(d.Partition); p != nil && d.Topic == topic && !d.IsFuture {
				p.size = max(p.size, 0) + d.Size
			}
		})
	}
	groups, err := adm.ListGroupsByType(ctx, []string{"classic", "consumer"})
	if err != nil {
		return s, err
	}
	s.committed = map[string]map[int32]int64{}
	if names := groups.Groups(); len(names) > 0 {
		for group, r := range adm.FetchManyOffsets(ctx, names...) {
			if errors.Is(r.Err, kerr.GroupIDNotFound) { // deleted since the list
				continue
			} else if r.Err != nil {
				err = errors.Join(err, r.Err)
				continue
			}
			for p, o := range r.Fetched[topic] {
				if o.Err == nil && o.At >= 0 {
					if s.committed[group] == nil {
						s.committed[group] = map[int32]int64{}
					}
					s.committed[group][p] = o.At
				}
			}
		}
	}
	return s, err
}
