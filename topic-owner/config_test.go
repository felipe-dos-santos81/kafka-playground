package main

import (
	"maps"
	"strings"
	"testing"
)

// env is a valid main-role environment with kv applied on top ("K=" sets K
// empty; "-K" removes K).
func env(kv ...string) []string {
	vars := map[string]string{"BASE_NAME": "orders", "INSTANCE": "1", "ROLE": "main", "KAFKA_BROKERS": "kafka:19092"}
	for _, s := range kv {
		if k, ok := strings.CutPrefix(s, "-"); ok {
			delete(vars, k)
			continue
		}
		k, v, _ := strings.Cut(s, "=")
		vars[k] = v
	}
	var out []string
	for k, v := range vars {
		out = append(out, k+"="+v)
	}
	return out
}

func TestLoadConfigRefuses(t *testing.T) {
	long := strings.Repeat("a", maxName-1) // with "-1" one over the limit
	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		{"dot", env("BASE_NAME=owner.verify"), `BASE_NAME "owner.verify" is not a base name: use letters, digits, _ and -, start with a letter or digit, no __, no -<digits> at the end`},
		{"double underscore", env("BASE_NAME=a__b"), `BASE_NAME "a__b" is not a base name`},
		{"ends in -digits", env("BASE_NAME=orders-2"), `BASE_NAME "orders-2" is not a base name`},
		{"leading underscore", env("BASE_NAME=_a"), `BASE_NAME "_a" is not a base name`},
		{"empty base", env("-BASE_NAME"), `BASE_NAME "" is not a base name`},
		{"leading zero", env("INSTANCE=01"), `INSTANCE "01" is not a positive integer without leading zeros`},
		{"zero", env("INSTANCE=0"), `INSTANCE "0" is not a positive integer without leading zeros`},
		{"too long", env("BASE_NAME=" + long), long + "-1 is too long: <base>-<instance> may have at most 242 characters"},
		{"unknown role", env("ROLE=x"), `ROLE "x" must be main, retry or dlq`},
		{"no brokers", env("-KAFKA_BROKERS"), "KAFKA_BROKERS is required"},
		{"bad partitions", env("PARTITIONS=0"), "PARTITIONS must be between 1 and 1048576"},
		{"replication factor", env("BASE_NAME=owner-verify", "REPLICATION_FACTOR=3"), "owner-verify-1: single-broker playground: replication_factor must be 1"},
		{"retry setting on main", env("MAX_ATTEMPTS=3"), "MAX_ATTEMPTS is only for ROLE=retry"},
		{"backoff on dlq", env("ROLE=dlq", "BACKOFF_MS=100"), "BACKOFF_MS is only for ROLE=retry"},
		{"attempts range", env("ROLE=retry", "MAX_ATTEMPTS=11"), "MAX_ATTEMPTS must be between 1 and 10"},
		{"backoff range", env("ROLE=retry", "BACKOFF_MS=99"), "BACKOFF_MS must be between 100 and 60000"},
		{"empty config name", env("TOPIC_CONFIG_=1"), "TOPIC_CONFIG_ names no topic config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadConfig(tc.env)
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error starting %q", err, tc.want)
			}
		})
	}
}

func TestLoadConfigAccepts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []string
		topic   string
		configs map[string]string
	}{
		{"main", env(), "orders-1", map[string]string{}},
		{"underscore and digits inside", env("BASE_NAME=orders_eu2", "INSTANCE=12"), "orders_eu2-12", map[string]string{}},
		{"retry", env("ROLE=retry"), "orders-1__retry", map[string]string{"message.timestamp.type": "LogAppendTime"}},
		{"dlq", env("ROLE=dlq"), "orders-1__dlq", map[string]string{"retention.ms": "-1"}},
		{"override", env("ROLE=dlq", "TOPIC_CONFIG_RETENTION_MS=3600000", "TOPIC_CONFIG_CLEANUP_POLICY=compact,delete"), "orders-1__dlq", map[string]string{"retention.ms": "3600000", "cleanup.policy": "compact,delete"}},
		{"empty removes a default", env("ROLE=dlq", "TOPIC_CONFIG_RETENTION_MS="), "orders-1__dlq", map[string]string{}},
		{"longest name", env("BASE_NAME=" + strings.Repeat("a", maxName-2)), strings.Repeat("a", maxName-2) + "-1", map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := loadConfig(tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Topic() != tc.topic || !maps.Equal(c.Configs, tc.configs) {
				t.Fatalf("got %s %v, want %s %v", c.Topic(), c.Configs, tc.topic, tc.configs)
			}
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	c, err := loadConfig(env("ROLE=retry", "KAFKA_BROKERS=a:1,b:2"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Partitions != 1 || c.ReplicationFactor != 1 || c.MaxAttempts != 3 || c.BackoffMS != 5000 || len(c.Brokers) != 2 {
		t.Fatalf("got %+v", c)
	}
}

// Every topic config name in Kafka 4.3.1 (TopicConfig in kafka-clients-4.3.1.jar)
// survives the trip to its variable and back.
func TestConfigEnvRoundTrip(t *testing.T) {
	for _, name := range []string{
		"cleanup.policy", "compression.gzip.level", "compression.type", "compression.zstd.level",
		"delete.retention.ms", "file.delete.delay.ms", "flush.messages", "flush.ms",
		"index.interval.bytes", "local.retention.bytes", "local.retention.ms", "max.compaction.lag.ms",
		"max.message.bytes", "message.downconversion.enable", "message.timestamp.after.max.ms",
		"message.timestamp.before.max.ms", "message.timestamp.type", "min.cleanable.dirty.ratio",
		"min.compaction.lag.ms", "min.insync.replicas", "remote.log.copy.disable",
		"remote.log.delete.on.disable", "remote.storage.enable", "retention.bytes", "retention.ms",
		"segment.bytes", "segment.index.bytes", "segment.jitter.ms", "segment.ms",
		"unclean.leader.election.enable",
	} {
		if got := configName(configEnv(name)); got != name {
			t.Errorf("%s -> %s -> %s", name, configEnv(name), got)
		}
	}
}
