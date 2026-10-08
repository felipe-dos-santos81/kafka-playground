# Topic containers M1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Three long-running topic-owner containers, `orders-1`, `orders-1__retry` and `orders-1__dlq`. Each creates its topic, keeps it in the desired state every 10 s, and serves `/healthz` and `/metrics`. Prometheus finds and scrapes them by label, and `make verify-topics` proves it all, ending in `TOPICS OK`.

**Architecture:** A new Go module `topic-owner/` builds one `scratch` image. The image runs as three compose services that differ only in `ROLE`.
- `config.go` reads the environment and enforces the name rules.
- `reconcile.go` holds a pure `diff` (desired against actual, giving a `Plan`) and the `owner` loop that applies plans and tracks health.
- `metrics.go` is a Prometheus collector that reads the topic at scrape time.
- `main.go` serves HTTP on `:9000`.

The I/O halves (`describe`, `apply`, `readTopic`) are thin kadm wrappers, proven end to end by `verify-topics`. Every pure part has a unit test. The redelivery worker (M2) and Grafana and the alert rules (M3) are not in this plan.

**Tech Stack:**
- Go 1.27.1, franz-go `kgo` v1.22.1 and `kadm` v1.19.0, `prometheus/client_golang` v1.24.1;
- Docker Compose, `prom/prometheus:v3.15.0`;
- GNU make 3.81 with BSD tools.

**Spec:** `docs/superpowers/specs/2026-10-08-topic-containers-design.md`. This plan is milestone M1 (spec §8). Spec §3.3 and §3.6 define the reconcile behaviour and the configuration, §4.1 the metrics, §5 the compose layout, §6 the failure modes, §7 the repository changes and §10 the verification.

**Provenance:** every Go file below was compiled, vetted, gofmt-checked and tested (13 tests pass). The image ran against a throwaway `apache/kafka:4.3.1`, and the compose, Prometheus and Makefile changes ran in a scratch copy of this repository: `make verify-topics` printed `TOPICS OK`, `make down` left no container, volume or network. Copy the code exactly.

## Global Constraints

- Nothing under `studio/` changes.
- Go module `kafka-playground/topic-owner`, `go 1.27.1`. Direct dependencies only `github.com/twmb/franz-go v1.22.1`, `github.com/twmb/franz-go/pkg/kadm v1.19.0`, `github.com/twmb/franz-go/pkg/kmsg v1.14.0` and `github.com/prometheus/client_golang v1.24.1`.
- Image `kafka-playground/topic-owner:0.1.0`, `build: ./topic-owner`, `pull_policy: build`; Dockerfile `golang:1.27.1-alpine` onto `scratch`, `USER 65534`, `EXPOSE 9000`, `ENTRYPOINT ["/topic-owner"]`.
- Prometheus `prom/prometheus:v3.15.0`, `127.0.0.1:9090`, `group_add: ["0"]`, socket mounted `:ro`, config `./prometheus:/etc/prometheus:ro`.
- Topic names: `<base>-<instance>`, `<base>-<instance>__retry`, `<base>-<instance>__dlq`.
  - Base `^[a-zA-Z0-9][a-zA-Z0-9_-]*$`, no `__`, not ending in `-<digits>`.
  - Instance `^[1-9][0-9]*$`.
  - `<base>-<instance>` at most 242 characters.
- Container name = compose service name = topic name.
- Labels `topic-owner.base`, `topic-owner.instance`, `topic-owner.role`; never `studio.flow`.
- `:9000` serves `GET /healthz` (200 `ok`, or 503 with the reason) and `GET /metrics`. `/topic-owner -healthcheck` prints the body and exits 0 only on 200.
- Reconcile every 10 s. Prometheus `scrape_interval` 5s, `scrape_timeout` 4s; the collector's deadline is 3 s.
- Role defaults: retry `message.timestamp.type=LogAppendTime`; dlq `retention.ms=-1`; main none.
- Log and fatal lines exactly as spec §3.6 (the verify target greps them):
  - `owner-verify-1: set retention.ms=3600000 (was 1000)`
  - `owner-verify-1: partitions 2 -> 3`
  - `has 3 partitions, wants 2: partitions never decrease`
  - `BASE_NAME "owner.verify" is not a base name`
  - `owner-verify-1: single-broker playground: replication_factor must be 1`
- M1 metric names (spec §4.1): `topic_owner_info`, `topic_owner_reconciled`, `topic_owner_kafka_up`, `kafka_topic_partitions`, `kafka_topic_partition_under_replicated_partition`, `kafka_topic_partition_current_offset`, `kafka_topic_partition_oldest_offset`, `topic_owner_partition_log_size_bytes`, `kafka_consumergroup_current_offset`, `kafka_consumergroup_lag`.
- `AGENTS.md` rules hold:
  - Makefile: real tabs, `## ` help, `# ── Section ──` rules, lower-case `arg ?= default`, user text through `$(call shq,$(value var))`, GNU make 3.81 with BSD tools.
  - Compose: `$$` for a shell `$` in compose strings; YAML merge is shallow, so merge `*after-kafka` back in.
  - Local only: every port on `127.0.0.1`.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Kafka cannot be reached when an owner starts, then comes back.** The owner becomes healthy without a restart (`TestOwnerRecovers`, Task 2).
2. **The topic is deleted by hand while its owner runs.** The next pass creates it again (`TestOwnerRecovers`, Task 2).
3. **A Studio deploy creates the topic between the owner's describe and its create.** Nothing is logged as created. `/healthz` says `created by someone else first; reconciling it on the next pass`, and the next pass reconciles the topic (`TestCreateErr`, Task 2).
4. **Broker-level configs that a topic describe also lists** (`min.insync.replicas=1` here) are not the topic's. They are never removed, so nothing flaps (`TestTopicLevel`, Task 2).
5. **A scrape before the first pass, or while the topic is missing.** `/metrics` still answers with identity and health only, and does not panic (`TestCollectorNoTopic`, Task 3).

---

### Task 1: Module and configuration

**Files:**
- Create: `topic-owner/go.mod`, `topic-owner/config.go`, `topic-owner/config_test.go`
- Modify: `.gitignore` (the "Go build and test output" block)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type Role string` with `RoleMain`, `RoleRetry`, `RoleDLQ`;
  - `const maxName = 242`, `const configPrefix = "TOPIC_CONFIG_"`;
  - `type Config struct { Base string; Instance int; Role Role; Brokers []string; Partitions, ReplicationFactor int; Configs map[string]string; MaxAttempts, BackoffMS int }`;
  - `func (c Config) Name() string` (`<base>-<instance>`) and `func (c Config) Topic() string` (the owned topic);
  - `func loadConfig(env []string) (Config, error)`. Its errors are the fatal lines without the `topic-owner: ` prefix;
  - `func configName(env string) string` and `func configEnv(name string) string`.

- [ ] **Step 1: Create the module**

`topic-owner/go.mod`:

```
module kafka-playground/topic-owner

go 1.27.1
```

- [ ] **Step 2: Write the failing test**

`topic-owner/config_test.go`:

```go
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
```

- [ ] **Step 3: Run it to see it fail**

Run: `cd topic-owner && go test ./...`
Expected: FAIL, with `undefined: loadConfig`, `undefined: maxName` and `undefined: configName`.

- [ ] **Step 4: Write the implementation**

`topic-owner/config.go`:

```go
package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Role is which of an instance's three topics a container owns.
type Role string

const (
	RoleMain  Role = "main"
	RoleRetry Role = "retry"
	RoleDLQ   Role = "dlq"
)

// maxName is the longest <base>-<instance>: <name>__retry must stay within
// Kafka's 249 characters (Studio's maxInputTopic).
const maxName = 249 - len("__retry")

// configPrefix starts the environment variable of a topic config:
// TOPIC_CONFIG_RETENTION_MS is retention.ms.
const configPrefix = "TOPIC_CONFIG_"

var (
	baseRe     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)
	digitsEnd  = regexp.MustCompile(`-[0-9]+$`)
	instanceRe = regexp.MustCompile(`^[1-9][0-9]*$`)
)

// roleDefaults are each role's topic configs before TOPIC_CONFIG_* overrides.
var roleDefaults = map[Role]map[string]string{
	RoleMain:  {},
	RoleRetry: {"message.timestamp.type": "LogAppendTime"}, // the backoff clock: the broker stamps every record
	RoleDLQ:   {"retention.ms": "-1"},                      // parked records stay until someone acts on them
}

// Config is a container's whole configuration, read from its environment.
type Config struct {
	Base              string
	Instance          int
	Role              Role
	Brokers           []string
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string // the desired topic-level configs
	MaxAttempts       int               // retry only
	BackoffMS         int               // retry only
}

// Name is <base>-<instance>, the main topic's name.
func (c Config) Name() string { return c.Base + "-" + strconv.Itoa(c.Instance) }

// Topic is the topic this container owns.
func (c Config) Topic() string {
	switch c.Role {
	case RoleRetry:
		return c.Name() + "__retry"
	case RoleDLQ:
		return c.Name() + "__dlq"
	}
	return c.Name()
}

// configName is the topic config a TOPIC_CONFIG_* variable sets.
func configName(env string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(env, configPrefix)), "_", ".")
}

// configEnv is the variable that sets topic config name; configName reverses it.
func configEnv(name string) string {
	return configPrefix + strings.ToUpper(strings.ReplaceAll(name, ".", "_"))
}

// loadConfig reads env (os.Environ's KEY=VALUE form). Its errors are the
// container's fatal lines, without the "topic-owner: " prefix.
func loadConfig(env []string) (Config, error) {
	vars := map[string]string{}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		vars[k] = v
	}
	c := Config{Base: vars["BASE_NAME"], Role: Role(vars["ROLE"])}
	if !baseRe.MatchString(c.Base) || strings.Contains(c.Base, "__") || digitsEnd.MatchString(c.Base) {
		return c, fmt.Errorf("BASE_NAME %q is not a base name: use letters, digits, _ and -, start with a letter or digit, no __, no -<digits> at the end", c.Base)
	}
	if !instanceRe.MatchString(vars["INSTANCE"]) {
		return c, fmt.Errorf("INSTANCE %q is not a positive integer without leading zeros", vars["INSTANCE"])
	}
	c.Instance, _ = strconv.Atoi(vars["INSTANCE"])
	if len(c.Name()) > maxName {
		return c, fmt.Errorf("%s is too long: <base>-<instance> may have at most %d characters", c.Name(), maxName)
	}
	defaults, ok := roleDefaults[c.Role]
	if !ok {
		return c, fmt.Errorf("ROLE %q must be main, retry or dlq", c.Role)
	}
	if vars["KAFKA_BROKERS"] == "" {
		return c, errors.New("KAFKA_BROKERS is required")
	}
	c.Brokers = strings.Split(vars["KAFKA_BROKERS"], ",")
	var err error
	if c.Partitions, err = intVar(vars, "PARTITIONS", 1, 1, 1<<20); err != nil {
		return c, err
	}
	if c.ReplicationFactor, err = intVar(vars, "REPLICATION_FACTOR", 1, 1, 1<<15); err != nil {
		return c, err
	}
	if c.ReplicationFactor != 1 {
		return c, fmt.Errorf("%s: single-broker playground: replication_factor must be 1", c.Topic())
	}
	for _, k := range []string{"MAX_ATTEMPTS", "BACKOFF_MS"} {
		if _, set := vars[k]; set && c.Role != RoleRetry {
			return c, fmt.Errorf("%s is only for ROLE=retry", k)
		}
	}
	if c.MaxAttempts, err = intVar(vars, "MAX_ATTEMPTS", 3, 1, 10); err != nil {
		return c, err
	}
	if c.BackoffMS, err = intVar(vars, "BACKOFF_MS", 5000, 100, 60000); err != nil {
		return c, err
	}
	c.Configs = map[string]string{}
	for k, v := range defaults {
		c.Configs[k] = v
	}
	for k, v := range vars {
		if !strings.HasPrefix(k, configPrefix) {
			continue
		}
		name := configName(k)
		if name == "" {
			return c, fmt.Errorf("%s names no topic config", k)
		}
		if v == "" {
			delete(c.Configs, name) // an empty value removes a role default
		} else {
			c.Configs[name] = v
		}
	}
	return c, nil
}

// intVar is vars[k] as an integer in [lo, hi], def when unset.
func intVar(vars map[string]string, k string, def, lo, hi int) (int, error) {
	s, set := vars[k]
	if !set {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s must be between %d and %d", k, lo, hi)
	}
	return n, nil
}
```

- [ ] **Step 5: Run the checks**

Run: `cd topic-owner && go vet ./... && test -z "$(gofmt -l .)" && go test ./...`
Expected: `ok  	kafka-playground/topic-owner`.

- [ ] **Step 6: Ignore the binary**

Apply to `.gitignore`:

```diff
--- a/.gitignore
+++ b/.gitignore
@@ -1,6 +1,7 @@
-# Go build and test output (go build in producer/ or studio/; go test -c, -coverprofile)
+# Go build and test output (go build in producer/, studio/ or topic-owner/; go test -c, -coverprofile)
 /producer/producer
 /studio/studio
+/topic-owner/topic-owner
 *.test
 *.out
 
```

- [ ] **Step 7: Commit**

```bash
git add topic-owner/go.mod topic-owner/config.go topic-owner/config_test.go .gitignore
git commit -m "topic-owner: module and configuration — name rules, role defaults, TOPIC_CONFIG_* variables

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Reconcile

**Files:**
- Create: `topic-owner/reconcile.go`, `topic-owner/reconcile_test.go`
- Modify: `topic-owner/go.mod`, `topic-owner/go.sum` (via `go get`)

**Interfaces:**
- Consumes: `Config`, `Config.Topic()`, `RoleMain` (Task 1).
- Produces:
  - `const reconcileEvery = 10 * time.Second`;
  - `type Actual struct { Exists bool; Partitions, ReplicationFactor int; Configs map[string]string }`;
  - `type Plan struct { Create bool; Partitions int; Alter []kadm.AlterConfig; Logs, Problems []string }`;
  - `func diff(topic string, want Config, have Actual) Plan`;
  - `type owner struct` with fields `cfg`, `describe func(context.Context) (Actual, error)` and `apply func(context.Context, Plan) error`;
  - `func newOwner(cfg Config, adm *kadm.Client) *owner`;
  - `func (o *owner) health() string`: `""` when the topic is in its desired state, else the reason;
  - `func (o *owner) run(ctx context.Context)` and `func (o *owner) pass(ctx context.Context)`;
  - `func describe(ctx, adm, topic) (Actual, error)`, `func apply(ctx, adm, cfg, p) error`, `func topicLevel([]kadm.Config) map[string]string`, `var errRaced error`, `func createErr(error) error`.

- [ ] **Step 1: Add the Kafka client modules**

Run: `cd topic-owner && go get github.com/twmb/franz-go@v1.22.1 github.com/twmb/franz-go/pkg/kadm@v1.19.0 github.com/twmb/franz-go/pkg/kmsg@v1.14.0`
Expected: `go: added github.com/twmb/franz-go v1.22.1`, plus `kadm v1.19.0` and `kmsg v1.14.0`.

- [ ] **Step 2: Write the failing test**

`topic-owner/reconcile_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func want(partitions int, configs map[string]string) Config {
	return Config{Base: "owner-verify", Instance: 1, Role: RoleMain, Partitions: partitions, ReplicationFactor: 1, Configs: configs}
}

func have(partitions, rf int, configs map[string]string) Actual {
	return Actual{Exists: true, Partitions: partitions, ReplicationFactor: rf, Configs: configs}
}

// alters renders p.Alter as "set k=v" and "delete k".
func alters(p Plan) []string {
	var out []string
	for _, a := range p.Alter {
		if a.Op == kadm.DeleteConfig {
			out = append(out, "delete "+a.Name)
		} else {
			out = append(out, "set "+a.Name+"="+*a.Value)
		}
	}
	return out
}

func TestDiff(t *testing.T) {
	for _, tc := range []struct {
		name       string
		want       Config
		have       Actual
		create     bool
		partitions int
		alter      []string
		logs       []string
		problems   []string
	}{
		{name: "missing", want: want(2, map[string]string{"retention.ms": "3600000"}), have: Actual{},
			create: true, logs: []string{"owner-verify-1: created with 2 partitions"}},
		{name: "in the desired state", want: want(2, map[string]string{"retention.ms": "3600000"}), have: have(2, 1, map[string]string{"retention.ms": "3600000"})},
		{name: "fewer partitions", want: want(3, map[string]string{}), have: have(2, 1, map[string]string{}),
			partitions: 3, logs: []string{"owner-verify-1: partitions 2 -> 3"}},
		{name: "more partitions", want: want(2, map[string]string{}), have: have(3, 1, map[string]string{}),
			problems: []string{"owner-verify-1: has 3 partitions, wants 2: partitions never decrease (delete the topic, or set PARTITIONS=3)"}},
		{name: "replication factor", want: want(1, map[string]string{}), have: have(1, 2, map[string]string{}),
			problems: []string{"owner-verify-1: replication factor 2, wants 1: never changed"}},
		{name: "config differs", want: want(1, map[string]string{"retention.ms": "3600000"}), have: have(1, 1, map[string]string{"retention.ms": "1000"}),
			alter: []string{"set retention.ms=3600000"}, logs: []string{"owner-verify-1: set retention.ms=3600000 (was 1000)"}},
		{name: "config missing", want: want(1, map[string]string{"retention.ms": "-1"}), have: have(1, 1, map[string]string{}),
			alter: []string{"set retention.ms=-1"}, logs: []string{"owner-verify-1: set retention.ms=-1 (was unset)"}},
		{name: "override nobody wants", want: want(1, map[string]string{}), have: have(1, 1, map[string]string{"retention.bytes": "5"}),
			alter: []string{"delete retention.bytes"}, logs: []string{"owner-verify-1: removed retention.bytes (was 5)"}},
		{name: "every change at once, configs in name order", want: want(3, map[string]string{"retention.ms": "1", "cleanup.policy": "delete"}), have: have(2, 1, map[string]string{"segment.ms": "9", "retention.ms": "2"}),
			partitions: 3,
			alter:      []string{"set cleanup.policy=delete", "set retention.ms=1", "delete segment.ms"},
			logs:       []string{"owner-verify-1: partitions 2 -> 3", "owner-verify-1: set cleanup.policy=delete (was unset)", "owner-verify-1: set retention.ms=1 (was 2)", "owner-verify-1: removed segment.ms (was 9)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := diff("owner-verify-1", tc.want, tc.have)
			if p.Create != tc.create || p.Partitions != tc.partitions || !slices.Equal(alters(p), tc.alter) || !slices.Equal(p.Logs, tc.logs) || !slices.Equal(p.Problems, tc.problems) {
				t.Fatalf("got create=%v partitions=%d alter=%q logs=%q problems=%q", p.Create, p.Partitions, alters(p), p.Logs, p.Problems)
			}
		})
	}
}

// fakeOwner is an owner whose broker is have and whose apply fails with applyErr.
func fakeOwner(cfg Config, have *Actual, describeErr, applyErr *error, applied *[]Plan) *owner {
	o := &owner{cfg: cfg, problem: cfg.Topic() + ": not reconciled yet"}
	o.describe = func(context.Context) (Actual, error) { return *have, *describeErr }
	o.apply = func(_ context.Context, p Plan) error {
		*applied = append(*applied, p)
		return *applyErr
	}
	return o
}

func TestOwnerPass(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() { log.SetOutput(os.Stderr); log.SetFlags(log.LstdFlags) }()

	cfg := want(2, map[string]string{})
	actual := have(3, 1, map[string]string{})
	var describeErr, applyErr error
	var applied []Plan
	o := fakeOwner(cfg, &actual, &describeErr, &applyErr, &applied)
	if o.health() == "" {
		t.Fatal("healthy before the first pass")
	}
	o.pass(context.Background())
	o.pass(context.Background())
	problem := "owner-verify-1: has 3 partitions, wants 2: partitions never decrease (delete the topic, or set PARTITIONS=3)"
	if o.health() != problem {
		t.Fatalf("health %q", o.health())
	}
	if got := strings.Count(logs.String(), problem); got != 1 {
		t.Fatalf("the problem was logged %d times, want once: %q", got, logs.String())
	}

	actual = have(2, 1, map[string]string{"retention.bytes": "5"})
	o.pass(context.Background())
	if o.health() != "" || !strings.Contains(logs.String(), "owner-verify-1: removed retention.bytes (was 5)\n") {
		t.Fatalf("health %q, logs %q", o.health(), logs.String())
	}

	applyErr = errors.New("alter configs: INVALID_CONFIG")
	logs.Reset()
	o.pass(context.Background())
	if o.health() != "owner-verify-1: alter configs: INVALID_CONFIG" || strings.Contains(logs.String(), "removed") {
		t.Fatalf("a failed apply: health %q, logs %q (want no change logged)", o.health(), logs.String())
	}

	applyErr, describeErr = nil, errors.New("unable to dial")
	o.pass(context.Background())
	if o.health() != "owner-verify-1: kafka: unable to dial" {
		t.Fatalf("health %q", o.health())
	}
}

// Kafka down at start, then back; then the topic deleted by hand while the
// owner runs: each pass recovers without a restart.
func TestOwnerRecovers(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	cfg := want(2, map[string]string{})
	actual := Actual{}
	describeErr, applyErr := error(errors.New("unable to dial")), error(nil)
	var applied []Plan
	o := fakeOwner(cfg, &actual, &describeErr, &applyErr, &applied)
	o.pass(context.Background())
	if o.health() != "owner-verify-1: kafka: unable to dial" || len(applied) != 0 {
		t.Fatalf("kafka down: health %q, applied %d plans", o.health(), len(applied))
	}
	describeErr = nil
	o.pass(context.Background())
	if o.health() != "" || len(applied) != 1 || !applied[0].Create {
		t.Fatalf("kafka back, topic missing: health %q, plans %+v", o.health(), applied)
	}
	actual = have(2, 1, map[string]string{})
	o.pass(context.Background())
	actual = Actual{} // deleted by hand
	o.pass(context.Background())
	if o.health() != "" || len(applied) != 3 || !applied[2].Create {
		t.Fatalf("topic deleted: health %q, plans %+v", o.health(), applied)
	}
}

// Losing the create race (a Studio deploy made the topic first) is reported as
// such, not as a create, and is not a create failure.
func TestCreateErr(t *testing.T) {
	if err := createErr(nil); err != nil {
		t.Fatal(err)
	}
	if err := createErr(kerr.TopicAlreadyExists); err != errRaced {
		t.Fatalf("got %v, want errRaced", err)
	}
	if err := createErr(kerr.InvalidReplicationFactor); err == nil || !strings.HasPrefix(err.Error(), "create: ") {
		t.Fatalf("got %v", err)
	}
}

// Only configs set on the topic are owned: broker settings and defaults that
// describe also lists (min.insync.replicas from the broker, say) never count.
func TestTopicLevel(t *testing.T) {
	got := topicLevel([]kadm.Config{
		{Key: "retention.ms", Value: kadm.StringPtr("1000"), Source: kmsg.ConfigSourceDynamicTopicConfig},
		{Key: "min.insync.replicas", Value: kadm.StringPtr("1"), Source: kmsg.ConfigSourceStaticBrokerConfig},
		{Key: "cleanup.policy", Value: kadm.StringPtr("delete"), Source: kmsg.ConfigSourceDefaultConfig},
	})
	if !maps.Equal(got, map[string]string{"retention.ms": "1000"}) {
		t.Fatalf("got %v", got)
	}
}
```

- [ ] **Step 3: Run it to see it fail**

Run: `cd topic-owner && go test ./...`
Expected: FAIL, with `undefined: Actual`, `undefined: diff` and `undefined: owner`.

- [ ] **Step 4: Write the implementation**

`topic-owner/reconcile.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// reconcileEvery is how often a container compares its topic with the desired state.
const reconcileEvery = 10 * time.Second

// Actual is a topic as the broker has it.
type Actual struct {
	Exists            bool
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string // topic-level overrides only (source DYNAMIC_TOPIC_CONFIG)
}

// Plan is what one reconcile pass does to bring a topic to its desired state.
type Plan struct {
	Create     bool               // the topic is missing: create it with the desired partitions and configs
	Partitions int                // > 0: raise the partition count to this
	Alter      []kadm.AlterConfig // incremental config changes, in name order
	Logs       []string           // one line per change, logged once it is applied
	Problems   []string           // differences no pass can fix
}

// diff is the plan that takes have to want for topic. Broker and default
// configs are never touched; topic-level overrides want does not name are removed.
func diff(topic string, want Config, have Actual) Plan {
	var p Plan
	if !have.Exists {
		p.Create = true
		p.Logs = append(p.Logs, fmt.Sprintf("%s: created with %d partitions", topic, want.Partitions))
		return p
	}
	switch {
	case have.Partitions < want.Partitions:
		p.Partitions = want.Partitions
		p.Logs = append(p.Logs, fmt.Sprintf("%s: partitions %d -> %d", topic, have.Partitions, want.Partitions))
	case have.Partitions > want.Partitions:
		p.Problems = append(p.Problems, fmt.Sprintf("%s: has %d partitions, wants %d: partitions never decrease (delete the topic, or set PARTITIONS=%d)", topic, have.Partitions, want.Partitions, have.Partitions))
	}
	if have.ReplicationFactor != want.ReplicationFactor {
		p.Problems = append(p.Problems, fmt.Sprintf("%s: replication factor %d, wants %d: never changed", topic, have.ReplicationFactor, want.ReplicationFactor))
	}
	names := map[string]bool{}
	for k := range want.Configs {
		names[k] = true
	}
	for k := range have.Configs {
		names[k] = true
	}
	for _, k := range slices.Sorted(maps.Keys(names)) {
		v, wanted := want.Configs[k]
		was, set := have.Configs[k]
		switch {
		case wanted && !set:
			p.Alter = append(p.Alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(v)})
			p.Logs = append(p.Logs, fmt.Sprintf("%s: set %s=%s (was unset)", topic, k, v))
		case wanted && was != v:
			p.Alter = append(p.Alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(v)})
			p.Logs = append(p.Logs, fmt.Sprintf("%s: set %s=%s (was %s)", topic, k, v, was))
		case !wanted:
			p.Alter = append(p.Alter, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
			p.Logs = append(p.Logs, fmt.Sprintf("%s: removed %s (was %s)", topic, k, was))
		}
	}
	return p
}

// owner keeps one topic in its desired state and remembers whether the last
// pass left it there.
type owner struct {
	cfg      Config
	describe func(context.Context) (Actual, error) // the topic as the broker has it
	apply    func(context.Context, Plan) error     // carries out a plan

	mu      sync.Mutex
	problem string // why the topic is not in its desired state; "" when it is
}

func newOwner(cfg Config, adm *kadm.Client) *owner {
	o := &owner{cfg: cfg, problem: cfg.Topic() + ": not reconciled yet"}
	o.describe = func(ctx context.Context) (Actual, error) { return describe(ctx, adm, cfg.Topic()) }
	o.apply = func(ctx context.Context, p Plan) error { return apply(ctx, adm, cfg, p) }
	return o
}

// health is "" when the last pass left the topic in its desired state, else why not.
func (o *owner) health() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.problem
}

// run reconciles at once, then every reconcileEvery, until ctx ends.
func (o *owner) run(ctx context.Context) {
	t := time.NewTicker(reconcileEvery)
	defer t.Stop()
	for {
		o.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// pass is one reconcile: describe, diff, apply. It logs each applied change,
// and a problem only when it differs from the last one.
func (o *owner) pass(ctx context.Context) {
	topic := o.cfg.Topic()
	problem := ""
	if have, err := o.describe(ctx); err != nil {
		problem = fmt.Sprintf("%s: kafka: %v", topic, err)
	} else {
		p := diff(topic, o.cfg, have)
		if err := o.apply(ctx, p); err != nil {
			problem = fmt.Sprintf("%s: %v", topic, err)
		} else {
			for _, l := range p.Logs {
				log.Print(l)
			}
			problem = strings.Join(p.Problems, "; ")
		}
	}
	o.mu.Lock()
	changed := problem != o.problem
	o.problem = problem
	o.mu.Unlock()
	if changed && problem != "" {
		log.Print(problem)
	}
}

// describe reads topic's partitions, replication factor and topic-level configs.
func describe(ctx context.Context, adm *kadm.Client, topic string) (Actual, error) {
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return Actual{}, err
	}
	td := tds[topic]
	if errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
		return Actual{}, nil
	} else if td.Err != nil {
		return Actual{}, td.Err
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return Actual{}, err
	}
	rc, err := rcs.On(topic, nil)
	if err == nil {
		err = rc.Err
	}
	if err != nil {
		return Actual{}, err
	}
	return Actual{Exists: true, Partitions: len(td.Partitions), ReplicationFactor: td.Partitions.NumReplicas(), Configs: topicLevel(rc.Configs)}, nil
}

// topicLevel is the configs set on the topic itself (source DYNAMIC_TOPIC_CONFIG):
// the ones an owner manages. Broker settings and defaults are not the topic's.
func topicLevel(configs []kadm.Config) map[string]string {
	out := map[string]string{}
	for _, c := range configs {
		if c.Source == kmsg.ConfigSourceDynamicTopicConfig {
			out[c.Key] = c.MaybeValue()
		}
	}
	return out
}

// errRaced is apply's answer when the topic appeared between describe and
// create (a Studio deploy, another owner): nothing was created, and the next
// pass reconciles the topic it finds.
var errRaced = errors.New("created by someone else first; reconciling it on the next pass")

// createErr is CreateTopic's error as apply reports it.
func createErr(err error) error {
	switch {
	case errors.Is(err, kerr.TopicAlreadyExists):
		return errRaced
	case err != nil:
		return fmt.Errorf("create: %w", err)
	}
	return nil
}

// apply carries out p on cfg's topic.
func apply(ctx context.Context, adm *kadm.Client, cfg Config, p Plan) error {
	topic := cfg.Topic()
	if p.Create {
		configs := map[string]*string{}
		for k, v := range cfg.Configs {
			configs[k] = kadm.StringPtr(v)
		}
		_, err := adm.CreateTopic(ctx, int32(cfg.Partitions), int16(cfg.ReplicationFactor), configs, topic)
		return createErr(err)
	}
	if p.Partitions > 0 {
		rs, err := adm.UpdatePartitions(ctx, p.Partitions, topic)
		if err == nil {
			err = rs.Error()
		}
		if err != nil {
			return fmt.Errorf("partitions: %w", err)
		}
	}
	if len(p.Alter) > 0 {
		rs, err := adm.AlterTopicConfigs(ctx, p.Alter, topic)
		if err == nil {
			for _, r := range rs {
				if r.Err != nil {
					err = fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
				}
			}
		}
		if err != nil {
			return fmt.Errorf("alter configs: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 5: Run the checks**

Run: `cd topic-owner && go mod tidy && go vet ./... && test -z "$(gofmt -l .)" && go test ./...`
Expected: `ok  	kafka-playground/topic-owner`.

- [ ] **Step 6: Commit**

```bash
git add topic-owner/go.mod topic-owner/go.sum topic-owner/reconcile.go topic-owner/reconcile_test.go
git commit -m "topic-owner: reconcile — desired-state diff (create, partitions up only, configs set and removed), owner loop with health, a lost create race reported as such

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Metrics, HTTP and the image

**Files:**
- Create: `topic-owner/metrics.go`, `topic-owner/metrics_test.go`, `topic-owner/main.go`, `topic-owner/main_test.go`, `topic-owner/Dockerfile`
- Modify: `topic-owner/go.mod`, `topic-owner/go.sum`, `Makefile` (the `test` target)

**Interfaces:**
- Consumes: `Config`, `loadConfig`, `RoleRetry` (Task 1); `newOwner`, `(*owner).health`, `(*owner).run` (Task 2).
- Produces:
  - `type partitionState`, `type topicState`;
  - `type collector struct { cfg Config; reconciled func() bool; read func(context.Context) (topicState, error) }`;
  - `func newCollector(cfg Config, adm *kadm.Client, reconciled func() bool) *collector`;
  - `func readTopic(ctx, adm, topic) (topicState, error)`;
  - `func routes(health func() string, reg *prometheus.Registry) http.Handler`;
  - `func healthcheck(url string) int`;
  - the binary `/topic-owner`, with `-healthcheck`.

- [ ] **Step 1: Add client_golang**

Run: `cd topic-owner && go get github.com/prometheus/client_golang@v1.24.1`
Expected: `go: added github.com/prometheus/client_golang v1.24.1`.

- [ ] **Step 2: Write the failing tests**

`topic-owner/metrics_test.go`:

```go
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
```

`topic-owner/main_test.go`:

```go
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRoutes(t *testing.T) {
	problem := "owner-verify-1: has 3 partitions, wants 2: partitions never decrease (delete the topic, or set PARTITIONS=3)"
	reg := prometheus.NewRegistry()
	reg.MustRegister(testCollector(topicState{}, nil, true))
	srv := httptest.NewServer(routes(func() string { return problem }, reg))
	defer srv.Close()

	get := func(path string) (int, string) {
		r, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r.StatusCode, string(b)
	}
	if code, body := get("/healthz"); code != 503 || strings.TrimSpace(body) != problem {
		t.Fatalf("unhealthy /healthz: %d %q", code, body)
	}
	if code, body := get("/metrics"); code != 200 || !strings.Contains(body, `topic_owner_info{base="orders",role="retry",topic="orders-1__retry",topic_instance="1"} 1`) {
		t.Fatalf("/metrics: %d %q", code, body)
	}
	if code := healthcheckQuiet(t, srv.URL+"/healthz"); code != 1 {
		t.Fatalf("healthcheck of an unhealthy owner exited %d, want 1", code)
	}

	problem = ""
	if code, body := get("/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("healthy /healthz: %d %q", code, body)
	}
	if code := healthcheckQuiet(t, srv.URL+"/healthz"); code != 0 {
		t.Fatalf("healthcheck of a healthy owner exited %d, want 0", code)
	}
}

// healthcheckQuiet runs healthcheck with its output discarded.
func healthcheckQuiet(t *testing.T, url string) int {
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)
	defer func() { os.Stdout = stdout }()
	return healthcheck(url)
}
```

- [ ] **Step 3: Run them to see them fail**

Run: `cd topic-owner && go test ./...`
Expected: FAIL, with `undefined: topicState`, `undefined: collector` and `undefined: routes`.

- [ ] **Step 4: Write the implementation**

`topic-owner/metrics.go`:

```go
package main

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
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
	groups, err := adm.ListGroups(ctx)
	if err != nil {
		return s, err
	}
	s.committed = map[string]map[int32]int64{}
	if names := groups.Groups(); len(names) > 0 {
		for group, r := range adm.FetchManyOffsets(ctx, names...) {
			if r.Err != nil {
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
```

`topic-owner/main.go`:

```go
// Topic owner: one long-running container per topic. ROLE=main owns
// <base>-<instance>, retry owns <base>-<instance>__retry, dlq owns
// <base>-<instance>__dlq. Each creates its topic, keeps it in the desired state
// (reconcile.go) and serves /healthz and /metrics (metrics.go) on :9000.
// `topic-owner -healthcheck` is the compose healthcheck (the scratch image has
// no curl); it prints why the topic is not in its desired state.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const addr = ":9000"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck("http://localhost" + addr + "/healthz"))
	}
	cfg, err := loadConfig(os.Environ())
	if err != nil {
		log.Fatalf("topic-owner: %v", err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		log.Fatalf("topic-owner: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	o := newOwner(cfg, adm)
	reg := prometheus.NewRegistry()
	reg.MustRegister(newCollector(cfg, adm, func() bool { return o.health() == "" }))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go o.run(ctx)
	srv := &http.Server{Addr: addr, Handler: routes(o.health, reg)}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Printf("%s: owner (role %s) on %s", cfg.Topic(), cfg.Role, addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("topic-owner: %v", err)
	}
}

// routes serves GET /healthz (200 "ok", or 503 and why not) and GET /metrics.
func routes(health func() string, reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if problem := health(); problem != "" {
			http.Error(w, problem, http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// healthcheck fetches url and prints its body; its result is the exit code:
// 0 on 200, else 1. Docker keeps the output in .State.Health.Log.
func healthcheck(url string) int {
	c := http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(url)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	defer r.Body.Close()
	io.Copy(os.Stdout, r.Body)
	if r.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
```

- [ ] **Step 5: Run the checks**

Run: `cd topic-owner && go mod tidy && go vet ./... && test -z "$(gofmt -l .)" && go test -v ./... 2>&1 | grep -E '^(--- |ok)'`
Expected: 13 `--- PASS` lines and `ok  	kafka-playground/topic-owner`.

`go.mod`'s first `require` block then lists exactly:
- `github.com/prometheus/client_golang v1.24.1`
- `github.com/twmb/franz-go v1.22.1`
- `github.com/twmb/franz-go/pkg/kadm v1.19.0`
- `github.com/twmb/franz-go/pkg/kmsg v1.14.0`

- [ ] **Step 6: Write the Dockerfile and build it**

`topic-owner/Dockerfile`:

```dockerfile
FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /topic-owner .

FROM scratch
COPY --from=build /topic-owner /topic-owner
USER 65534
EXPOSE 9000
ENTRYPOINT ["/topic-owner"]
```

Run: `docker build -q -t kafka-playground/topic-owner:0.1.0 topic-owner && docker run --rm -e BASE_NAME=owner.verify kafka-playground/topic-owner:0.1.0; echo "rc=$?"`

Expected: a `sha256:` line, then a timestamped line ending in
`topic-owner: BASE_NAME "owner.verify" is not a base name: use letters, digits, _ and -, start with a letter or digit, no __, no -<digits> at the end`,
then `rc=1`.

- [ ] **Step 7: Add the module to `make test`**

In `Makefile`'s `test` recipe, after the `cd studio && …` line, add this line. It starts with a real tab:

```make
	cd topic-owner && go vet ./... && test -z "$$(gofmt -l . | tee /dev/stderr)" && go test ./...
```

Run: `make test`
Expected: it ends without error. The output includes `ok  	kafka-playground/topic-owner`.

- [ ] **Step 8: Commit**

```bash
git add topic-owner Makefile
git commit -m "topic-owner: /metrics (topic, offsets, log size, group lag, under kafka-exporter's names where they mean the same), /healthz and -healthcheck, scratch image; make test runs its tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: The stack — owners for `orders-1`, Prometheus, Makefile targets, `verify-topics`

**Files:**
- Modify: `docker-compose.yml`, `Makefile`
- Create: `prometheus/prometheus.yml`

**Interfaces:**
- Consumes:
  - the image from Task 3;
  - the log and health lines from Task 2: `<topic>: set retention.ms=3600000 (was 1000)`, `<topic>: partitions 2 -> 3`, `has 3 partitions, wants 2: partitions never decrease`;
  - the fatal lines from Task 1;
  - the metrics `kafka_topic_partitions` and `topic_owner_info` from Task 3.
- Produces:
  - compose services `orders-1`, `orders-1__retry`, `orders-1__dlq` and `prometheus`, and the anchor `x-topic-owner`;
  - Prometheus job `topic-owners`, whose `instance` label is the container name;
  - make targets `owners`, `query` (`q=`), `kcat` (`args=`, default `-L`) and `verify-topics`;
  - `down` with `-v` and the one-off removal;
  - `verify` running `verify-topics`;
  - the variable `PROMETHEUS_URL`.

- [ ] **Step 1: Add the services**

Apply to `docker-compose.yml`:

```diff
--- a/docker-compose.yml
+++ b/docker-compose.yml
@@ -1,5 +1,6 @@
 # Local Kafka playground: one KRaft broker, one-shot topic jobs, kcat consumers,
-# a small producer page and Redpanda Console. No auth, no TLS, nothing persisted.
+# a small producer page, Redpanda Console, Pipeline Studio, topic owners and
+# Prometheus. No auth, no TLS, nothing persisted.
 # Containers reach the broker at kafka:19092; the host at localhost:9092.
 
 # One-shot job: create a topic, exit 0 (also when it already exists).
@@ -35,6 +36,22 @@
         -u -f 'partition=%p offset=%o key=%k value=%s\n' \
         "$${TOPIC_NAME:?TOPIC_NAME is required}"
 
+# Topic owner: one long-running container per topic (topic-owner/, Go).
+# ROLE=main owns <base>-<instance>; retry owns <base>-<instance>__retry; dlq
+# owns <base>-<instance>__dlq. Each creates its topic, keeps it in the desired
+# state (every 10 s) and serves /healthz and /metrics on :9000 inside the
+# network. Declare one block of three per instance, after `producer` (they use
+# its *bootstrap anchor); container name = service name = topic name.
+x-topic-owner: &topic-owner
+  build: ./topic-owner
+  image: kafka-playground/topic-owner:0.1.0
+  pull_policy: build
+  depends_on: *after-kafka
+  healthcheck:
+    test: ["CMD", "/topic-owner", "-healthcheck"]
+    interval: 2s
+    retries: 30
+
 services:
   kafka:
     image: apache/kafka:4.3.1
@@ -150,3 +167,50 @@
       test: ["CMD", "wget", "-q", "-O", "/dev/null", "http://localhost:8080/admin/health"]
       interval: 2s
       retries: 30
+
+  # Topic owners for orders-1, its retry topic and its DLQ (README, "Topic owners").
+  orders-1:
+    <<: *topic-owner
+    container_name: orders-1
+    labels: &orders-1-labels
+      topic-owner.base: orders
+      topic-owner.instance: "1"
+      topic-owner.role: main
+    environment: &orders-1-env
+      KAFKA_BROKERS: *bootstrap
+      BASE_NAME: orders
+      INSTANCE: 1
+      ROLE: main
+      PARTITIONS: 3
+
+  # Produces to orders-1 and orders-1__dlq (from M2 on), so it starts after both.
+  orders-1__retry:
+    <<: *topic-owner
+    container_name: orders-1__retry
+    depends_on:
+      <<: *after-kafka
+      orders-1: {condition: service_healthy}
+      orders-1__dlq: {condition: service_healthy}
+    labels: {<<: *orders-1-labels, topic-owner.role: retry}
+    environment: {<<: *orders-1-env, ROLE: retry, MAX_ATTEMPTS: 3}
+
+  orders-1__dlq:
+    <<: *topic-owner
+    container_name: orders-1__dlq
+    labels: {<<: *orders-1-labels, topic-owner.role: dlq}
+    environment: {<<: *orders-1-env, ROLE: dlq}
+
+  # Prometheus: scrapes every topic owner, found by its labels through the
+  # Docker socket (no target list): http://localhost:9090
+  prometheus:
+    image: prom/prometheus:v3.15.0
+    group_add: ["0"]    # Docker Desktop shows the socket as root:root 0660 inside containers
+    ports:
+      - "127.0.0.1:9090:9090"
+    volumes:
+      - /var/run/docker.sock:/var/run/docker.sock:ro
+      - ./prometheus:/etc/prometheus:ro
+    healthcheck:
+      test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1:9090/-/ready"]
+      interval: 2s
+      retries: 30
```

The `x-topic-owner` anchor comes after `x-consumer`, because it uses `*after-kafka`. The services come last, because they use `*bootstrap`, defined in `producer`.

- [ ] **Step 2: Write the Prometheus configuration**

`prometheus/prometheus.yml`:

```yaml
# Prometheus for the playground: scrapes every topic-owner container, found
# through the Docker socket by its topic-owner.role label (no target list).
global:
  scrape_interval: 5s
  scrape_timeout: 4s
  evaluation_interval: 5s

scrape_configs:
  - job_name: topic-owners
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
        port: 9000
        filters: [{name: label, values: [topic-owner.role]}]
    relabel_configs:
      # instance is the container name (= the topic), not its IP
      - source_labels: [__meta_docker_container_name]
        regex: '/(.*)'
        target_label: instance
```

Run: `docker compose config --quiet && echo ok`
Expected: `ok`.

- [ ] **Step 3: Add the Makefile targets**

Apply to `Makefile` (recipe lines start with real tabs):

```diff
--- a/Makefile
+++ b/Makefile
@@ -1,5 +1,5 @@
 # Kafka Playground: a local Kafka sandbox (KRaft broker, topic jobs, kcat consumers,
-# producer page, Redpanda Console, Pipeline Studio).
+# producer page, Redpanda Console, Pipeline Studio, topic owners, Prometheus).
 # Typical use: up → produce → logs → scale → groups → down. verify checks it all end to end.
 SERVICE = Kafka Playground
 
@@ -8,6 +8,7 @@
 PRODUCER_URL ?= http://localhost:8081
 CONSOLE_URL ?= http://localhost:8080
 STUDIO_URL ?= http://localhost:8082
+PROMETHEUS_URL ?= http://localhost:9090
 KAFKA_BIN = $(COMPOSE) exec -T kafka /opt/kafka/bin
 BOOTSTRAP = --bootstrap-server localhost:19092
 topic ?= orders
@@ -15,11 +16,13 @@
 value ?=
 n ?= 3
 svc ?= orders-workers orders-audit
+q ?=
+args ?= -L
 
 # Single-quote $(1) for the shell; fed $(value var), quotes, spaces and $ pass through untouched.
 shq = '$(subst ','\'',$(1))'
 
-.PHONY: help up down ps logs topics groups nodes produce scale verify verify-studio verify-ui test
+.PHONY: help up down ps logs topics groups nodes owners query produce kcat scale verify verify-studio verify-ui verify-topics test
 
 # ── Environment ──────────────────────────────────────────────────────────────
 
@@ -34,11 +37,14 @@
 
 up: ## [STEP 1] Start everything and wait until it is healthy
 	$(COMPOSE) up -d --wait
-	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Studio: $(STUDIO_URL)   Broker from the host: localhost:9092"
+	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Studio: $(STUDIO_URL)   Prometheus: $(PROMETHEUS_URL)   Broker from the host: localhost:9092"
 
-down: ## Remove every container, Studio nodes first (deletes topics and messages)
-	@ids=$$(docker ps -aq -f label=studio.flow); [ -z "$$ids" ] || docker rm -f $$ids >/dev/null
-	$(COMPOSE) down --remove-orphans
+# Studio nodes and topic-owner one-offs (docker compose run) are not services:
+# remove them first, or the network cannot go. -v removes the anonymous volumes
+# the kafka and prometheus images declare.
+down: ## Remove every container and volume, Studio nodes first (deletes topics, messages and metrics)
+	@ids=$$(docker ps -aq -f label=studio.flow; docker ps -aq -f label=topic-owner.role -f label=com.docker.compose.oneoff=True); [ -z "$$ids" ] || docker rm -f $$ids >/dev/null
+	$(COMPOSE) down -v --remove-orphans
 
 ps: ## Show every container, including exited topic jobs
 	$(COMPOSE) ps -a
@@ -57,6 +63,13 @@
 nodes: ## List Studio node containers (one per producer and consumer instance)
 	docker ps -a -f label=studio.flow
 
+owners: ## List topic-owner containers (main, retry and dlq per topic) and their health
+	docker ps -a -f label=topic-owner.role
+
+query: ## Ask Prometheus an instant PromQL query (usage: make query q='kafka_topic_partitions{topic="orders-1"}')
+	@$(if $(value q),true,{ echo "q is required, e.g. make query q='kafka_topic_partitions'"; exit 1; })
+	@curl -sS --fail-with-body $(PROMETHEUS_URL)/api/v1/query --data-urlencode $(call shq,query=$(value q)); echo
+
 # ── Produce ──────────────────────────────────────────────────────────────────
 
 produce: ## [STEP 2] Produce one JSON record via the producer page (usage: make produce value='{"id":1}' [topic=orders] [key=k1])
@@ -66,6 +79,10 @@
 		$(if $(value key),--url-query $(call shq,key=$(value key))) \
 		--data-binary $(call shq,$(value value))
 
+# kcat from the stack's pinned image, on the compose network; sh -c splits args into words.
+kcat: ## Run kcat against the broker (usage: make kcat args='-C -t orders-1 -o beginning -e -J'; default -L)
+	@$(COMPOSE) run --rm -T --no-deps --entrypoint sh orders-audit -c $(call shq,exec kcat -b kafka:19092 $(value args))
+
 # ── Scale ────────────────────────────────────────────────────────────────────
 
 scale: ## [STEP 4] Set the number of orders-workers group members (usage: make scale n=3)
@@ -226,12 +243,54 @@
 	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$qid || { echo "STUDIO FAILED: delete the retry flow"; exit 1; }; \
 	echo "STUDIO OK ($$id)"
 
+# Runs owners of its own base, owner-verify, as one-offs of the orders-1 service
+# (same image, network and healthcheck; labels and environment overridden).
+verify-topics: up ## Check the topic owners end to end: refusals, reconcile, metrics in Prometheus; cleans up its containers and topics
+	@trap 'docker rm -f owner-verify-refused owner-verify-1 owner-verify-1__retry owner-verify-1__dlq >/dev/null 2>&1; $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --delete --topic "owner-verify-1(__retry|__dlq)?" >/dev/null 2>&1; $(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --delete --group owner-verify-1__redelivery >/dev/null 2>&1' EXIT; \
+	own() { n=$$1 r=$$2 p=$$3; shift 3; docker rm -f "$$n" >/dev/null 2>&1; \
+		out=$$($(COMPOSE) run -d --no-deps --name "$$n" -l topic-owner.base=owner-verify -l topic-owner.instance=1 -l topic-owner.role="$$r" \
+			-e BASE_NAME=owner-verify -e INSTANCE=1 -e ROLE="$$r" -e PARTITIONS="$$p" "$$@" orders-1 2>&1) || { echo "TOPICS FAILED: start $$n: $$out"; return 1; }; }; \
+	healthlog() { docker inspect -f '{{range .State.Health.Log}}{{.Output}}{{end}}' "$$1" 2>/dev/null; }; \
+	healthy() { for i in $$(seq 30); do [ "$$(docker inspect -f '{{.State.Health.Status}}' "$$1" 2>/dev/null)" = healthy ] && return 0; sleep 1; done; echo "TOPICS FAILED: $$1 never became healthy: $$(healthlog $$1)"; return 1; }; \
+	desc() { $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --describe --topic "$$1" 2>/dev/null | head -1; }; \
+	refused() { out=$$($(COMPOSE) run --rm --no-deps --name owner-verify-refused "$$@" orders-1 2>&1) && { echo "TOPICS FAILED: $$* was accepted"; return 1; }; echo "$$out"; }; \
+	out=$$(refused -e BASE_NAME=owner.verify) || exit 1; \
+	echo "$$out" | grep -q 'BASE_NAME "owner.verify" is not a base name' || { echo "TOPICS FAILED: BASE_NAME owner.verify: $$out"; exit 1; }; \
+	out=$$(refused -e BASE_NAME=owner-verify -e REPLICATION_FACTOR=3) || exit 1; \
+	echo "$$out" | grep -q 'owner-verify-1: single-broker playground: replication_factor must be 1' || { echo "TOPICS FAILED: REPLICATION_FACTOR=3: $$out"; exit 1; }; \
+	echo "topics refused: a base name with a dot, replication factor 3"; \
+	own owner-verify-1 main 2 -e TOPIC_CONFIG_RETENTION_MS=3600000 && healthy owner-verify-1 || exit 1; \
+	d=$$(desc owner-verify-1); \
+	echo "$$d" | grep -q 'PartitionCount: 2' && echo "$$d" | grep -q 'retention.ms=3600000' || { echo "TOPICS FAILED: want 2 partitions and retention.ms=3600000: $$d"; exit 1; }; \
+	$(KAFKA_BIN)/kafka-configs.sh $(BOOTSTRAP) --alter --entity-type topics --entity-name owner-verify-1 --add-config retention.ms=1000 >/dev/null || { echo "TOPICS FAILED: alter retention.ms"; exit 1; }; \
+	for i in $$(seq 15); do \
+		docker logs owner-verify-1 2>&1 | grep -q 'owner-verify-1: set retention.ms=3600000 (was 1000)' && break; \
+		[ "$$i" = 15 ] && { echo "TOPICS FAILED: retention.ms=1000 was not reverted: $$(desc owner-verify-1)"; exit 1; }; sleep 1; \
+	done; \
+	echo "topics reconcile: $$(desc owner-verify-1)"; \
+	own owner-verify-1 main 3 -e TOPIC_CONFIG_RETENTION_MS=3600000 && healthy owner-verify-1 || exit 1; \
+	docker logs owner-verify-1 2>&1 | grep -q 'owner-verify-1: partitions 2 -> 3' || { echo "TOPICS FAILED: partitions not raised: $$(docker logs owner-verify-1 2>&1)"; exit 1; }; \
+	own owner-verify-1 main 2 -e TOPIC_CONFIG_RETENTION_MS=3600000 || exit 1; \
+	for i in $$(seq 15); do \
+		healthlog owner-verify-1 | grep -q 'has 3 partitions, wants 2: partitions never decrease' && break; \
+		[ "$$i" = 15 ] && { echo "TOPICS FAILED: a decrease to 2 partitions was not refused: $$(healthlog owner-verify-1)"; exit 1; }; sleep 1; \
+	done; \
+	echo "topics partitions: raised 2 -> 3, a decrease refused"; \
+	own owner-verify-1 main 3 -e TOPIC_CONFIG_RETENTION_MS=3600000 && healthy owner-verify-1 || exit 1; \
+	q() { curl -sS $(PROMETHEUS_URL)/api/v1/query --data-urlencode "query=$$1" | grep -q "\"value\":\[[0-9.]*,\"$$2\"\]"; }; \
+	for i in $$(seq 30); do \
+		q 'up{job="topic-owners",instance="owner-verify-1"}' 1 && q 'kafka_topic_partitions{topic="owner-verify-1"}' 3 && q 'topic_owner_info{topic="owner-verify-1",role="main"}' 1 && break; \
+		[ "$$i" = 30 ] && { echo "TOPICS FAILED: Prometheus never scraped owner-verify-1 with 3 partitions: $$(curl -sS $(PROMETHEUS_URL)/api/v1/query --data-urlencode 'query={topic="owner-verify-1"}')"; exit 1; }; sleep 1; \
+	done; \
+	echo "topics prometheus: owner-verify-1 up, 3 partitions"; \
+	echo "TOPICS OK"
+
 verify-ui: up studio/ui/.chromium ## Check the studio UI in Chromium (Playwright); installs Chromium once
 	@cd studio/ui && STUDIO_URL=$(STUDIO_URL) KAFKA_BOOTSTRAP=$(lastword $(BOOTSTRAP)) npx playwright test && echo "UI OK"
 
 # Waits until both groups have committed past the record (so it can no longer be
 # redelivered), then counts it in the logs: exactly once per group.
-verify: up verify-studio verify-ui ## Full check: studio API, studio UI, then one record seen once per consumer group
+verify: up verify-studio verify-ui verify-topics ## Full check: studio API, studio UI, topic owners, then one record seen once per consumer group
 	@id="verify-$$(date +%s)"; \
 	sent=$$($(MAKE) --no-print-directory produce key="$$id" value="{\"id\":\"$$id\"}") || exit 1; \
 	partition=$$(echo "$$sent" | sed 's/.*"partition":\([0-9]*\).*/\1/'); \
```

What the patch does:
- `down` removes Studio nodes and topic-owner one-offs, then runs `docker compose down -v --remove-orphans`. `docker compose down` does not remove `docker compose run` containers, and `-v` removes the anonymous volumes declared by the `apache/kafka` and `prom/prometheus` images.
- `verify-topics` runs owners of its own base, `owner-verify`, as one-offs of the `orders-1` service. They get the same image, network and healthcheck, with labels and environment overridden. Its exit trap removes its containers, its topics `owner-verify-1(__retry|__dlq)?` and the group `owner-verify-1__redelivery` (made from M2 on).

Run: `make help | grep -E 'owners|query|kcat|verify-topics|^.*down'`
Expected: the four new targets and the new `down` help text.

- [ ] **Step 4: Run the new check end to end**

Run: `make down && make verify-topics`
Expected: after the stack starts, these lines (the topic id varies):

```
topics refused: a base name with a dot, replication factor 3
topics reconcile: Topic: owner-verify-1	TopicId: …	PartitionCount: 2	ReplicationFactor: 1	Configs: min.insync.replicas=1,retention.ms=3600000
topics partitions: raised 2 -> 3, a decrease refused
topics prometheus: owner-verify-1 up, 3 partitions
TOPICS OK
```

- [ ] **Step 5: Try the new targets**

Run each command; you should see:

- `make owners`: `orders-1`, `orders-1__retry` and `orders-1__dlq`, each `(healthy)`. No `owner-verify` container.
- `make query q='kafka_topic_partitions{topic=~"orders-1.*"}'`: JSON with three results, each with value `"3"` and `instance` equal to its `topic`.
- `echo '{"id":1}' | make kcat args='-P -t orders-1 -k a -H studio-attempt=1 -H "studio-error=sink: http 503"' && make kcat args='-C -t orders-1 -o beginning -e -J'`: the record, with `"headers":["studio-attempt","1","studio-error","sink: http 503"]`.
- `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic 'orders-1.*' | grep PartitionCount`: three topics, `PartitionCount: 3`. The `__retry` topic's `Configs` include `message.timestamp.type=LogAppendTime`, and the `__dlq` topic's include `retention.ms=-1`.
- `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list | grep -c owner-verify`: `0`. The trap deleted its topics.

- [ ] **Step 6: Check that `make down` leaves nothing**

Run: `make down && docker ps -aq -f label=topic-owner.role | wc -l && docker volume ls -q -f label=com.docker.compose.project=kafka-playground | wc -l`
Expected: `0` and `0`.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml prometheus/prometheus.yml Makefile
git commit -m "topic owners in the stack: orders-1 and its __retry and __dlq owned by long-running containers, Prometheus finding them by label; make owners, query, kcat, verify-topics (TOPICS OK); make down removes owner one-offs and volumes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Documentation and the full check

**Files:**
- Modify: `README.md`, `AGENTS.md`, `docs/superpowers/specs/2026-10-08-topic-containers-design.md` (Status line)

**Interfaces:**
- Consumes: everything above. The docs quote the targets, variables, metric names and paths exactly as Tasks 1–4 made them.
- Produces: the README section `## Topic owners` (later milestones extend it), and the `AGENTS.md` layout and rules lines.

- [ ] **Step 1: README**

Apply to `README.md`:

```diff
--- a/README.md
+++ b/README.md
@@ -2,7 +2,7 @@
 
 A local Kafka sandbox. Produce JSON from a browser and watch partitions, consumer groups and fan-out. Pipeline Studio adds a canvas where you draw producer → topic → consumer flows and run them.
 
-Everything is local: no auth, no TLS, every port on `127.0.0.1`. `make down` deletes the Kafka data; Studio flows are files in `flows/` and stay.
+Everything is local: no auth, no TLS, every port on `127.0.0.1`. `make down` deletes the Kafka data and the metrics; Studio flows are files in `flows/` and stay.
 
 | Service | What it is | Where |
 |---|---|---|
@@ -12,6 +12,8 @@
 | `orders-audit` | kcat consumer in its own group on `orders` | `make logs svc=orders-audit` |
 | `producer` | producer page (`producer/`, Go) | http://localhost:8081 |
 | `studio` | Pipeline Studio (`studio/`, Go + React Flow) | http://localhost:8082 |
+| `orders-1`, `orders-1__retry`, `orders-1__dlq` | topic owners: each creates its topic and keeps it in the desired state (`topic-owner/`, Go) | `make owners` |
+| `prometheus` | Prometheus: scrapes the topic owners | http://localhost:9090 |
 | `console` | Redpanda Console: topics, messages, groups | http://localhost:8080 |
 
 ## Quick start
@@ -74,6 +76,91 @@
 
 Consumers with the same `GROUP_ID` split the partitions. Without one, each container gets every record. Add members with `deploy.replicas: N` or `docker compose up -d --scale <service>=N`.
 
+## Topic owners
+
+`orders-1`, `orders-1__retry` and `orders-1__dlq` are long-running containers, one per topic (`topic-owner/`, Go). Each one owns its topic:
+
+- It creates the topic. Then, every 10 s, it puts the topic back in its desired state: it sets missing configs, sets changed ones back, removes topic-level overrides nobody asked for, and raises partitions.
+- It never lowers partitions or changes the replication factor. Such a difference makes the container unhealthy and says why: `docker inspect --format '{{json .State.Health.Log}}' orders-1`, or `make logs svc=orders-1`.
+- `make up` waits until every topic is in its desired state. So `depends_on: {orders-1: {condition: service_healthy}}` guarantees a consumer its topic, as a finished topic job does.
+- A config changed by hand, in Console or with `kafka-configs.sh`, is set back. To change one, change the container's environment and recreate it.
+- It serves `/metrics` for its topic on port 9000 inside the network. Prometheus (http://localhost:9090) finds the containers by their labels, with no target list.
+- `make owners` lists them. `make query q='kafka_topic_partitions'` asks Prometheus. `make kcat args='-C -t orders-1 -o beginning -e -J'` runs kcat on the compose network (default `-L`).
+
+The retry container owns its topic like the other two. The redelivery worker that will read it is a later milestone.
+
+### Configuration
+
+| Variable | Default | |
+|---|---|---|
+| `BASE_NAME`, `INSTANCE`, `ROLE` | required | The topic is `<base>-<instance>`, plus `__retry` for `ROLE: retry` or `__dlq` for `ROLE: dlq`. |
+| `KAFKA_BROKERS` | required | `*bootstrap` |
+| `PARTITIONS` | `1` | Raised on an existing topic, never lowered. |
+| `REPLICATION_FACTOR` | `1` | Anything else is refused: there is one broker. |
+| `TOPIC_CONFIG_<NAME>` | role defaults | `TOPIC_CONFIG_RETENTION_MS: 3600000` sets `retention.ms`. An empty value removes a role default. |
+| `MAX_ATTEMPTS`, `BACKOFF_MS` | `3`, `5000` | Retry role only (1–10 and 100–60000). |
+
+Role defaults: the retry topic gets `message.timestamp.type=LogAppendTime`, so the broker stamps each record. The DLQ gets `retention.ms=-1`, so parked records stay.
+
+A base name has letters, digits, `_` and `-`. It starts with a letter or digit, has no `__`, and does not end in `-<digits>`. `<base>-<instance>` has at most 242 characters. A `.` is not allowed: Kafka treats `a.b` and `a_b` as the same name in its metrics, and every retry and DLQ name contains `_`.
+
+### Add an instance
+
+Copy the three `orders-1` services with new anchors and numbers:
+
+```yaml
+  orders-2:
+    <<: *topic-owner
+    container_name: orders-2
+    labels: &orders-2-labels
+      topic-owner.base: orders
+      topic-owner.instance: "2"
+      topic-owner.role: main
+    environment: &orders-2-env
+      KAFKA_BROKERS: *bootstrap
+      BASE_NAME: orders
+      INSTANCE: 2
+      ROLE: main
+      PARTITIONS: 6
+
+  orders-2__retry:
+    <<: *topic-owner
+    container_name: orders-2__retry
+    depends_on:
+      <<: *after-kafka
+      orders-2: {condition: service_healthy}
+      orders-2__dlq: {condition: service_healthy}
+    labels: {<<: *orders-2-labels, topic-owner.role: retry}
+    environment: {<<: *orders-2-env, ROLE: retry, MAX_ATTEMPTS: 5}
+
+  orders-2__dlq:
+    <<: *topic-owner
+    container_name: orders-2__dlq
+    labels: {<<: *orders-2-labels, topic-owner.role: dlq}
+    environment: {<<: *orders-2-env, ROLE: dlq}
+```
+
+Instances share nothing: `orders-2` has its own topics and settings. Prometheus finds the new containers by their labels.
+
+### Metrics
+
+Every series has a `topic` label. Where a series means what a [kafka-exporter](https://github.com/danielqsj/kafka_exporter) series means, it has the same name and labels, so dashboards made for kafka-exporter work.
+
+| Metric | |
+|---|---|
+| `kafka_topic_partitions` | partitions |
+| `kafka_topic_partition_under_replicated_partition` | 1 when a partition has fewer in-sync replicas than replicas (always 0 on one broker) |
+| `kafka_topic_partition_current_offset`, `kafka_topic_partition_oldest_offset` | log end and start offsets |
+| `kafka_consumergroup_current_offset`, `kafka_consumergroup_lag` | each group's committed offset and lag, for partitions it has committed |
+| `topic_owner_partition_log_size_bytes` | log size |
+| `topic_owner_info` | `base`, `topic_instance` and `role` (Prometheus reserves `instance`, which is the container name) |
+| `topic_owner_reconciled` | 1 when the topic is in its desired state |
+| `topic_owner_kafka_up` | 1 when the last scrape reached Kafka |
+
+Messages in per second: `sum by (topic) (rate(kafka_topic_partition_current_offset[1m]))`. Bytes in and out per topic are not exported. Only the broker's JMX has them, and the JMX agent would need a jar and a change to the `kafka` service.
+
+Prometheus reads the Docker socket with `group_add: ["0"]`, because Docker Desktop shows the socket as `root:root 0660` inside containers. On native Linux, use the host's docker gid instead. The socket gives root on the host, which is one more reason everything stays on `127.0.0.1`.
+
 ## Pipeline Studio
 
 Open http://localhost:8082. Drag nodes from the palette, wire them, edit the selected node on the right, then Save and Deploy.
```

- [ ] **Step 2: AGENTS.md**

Apply to `AGENTS.md`:

```diff
--- a/AGENTS.md
+++ b/AGENTS.md
@@ -1,22 +1,24 @@
 # AGENTS.md
 
-Local Kafka playground, run with Docker Compose. Usage is in README.md. Pipeline Studio's design is `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` (the authority; §7 lists the milestones), with one implementation plan per milestone in `docs/superpowers/plans/`. Its UI tests are designed in `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`.
+Local Kafka playground, run with Docker Compose. Usage is in README.md. Pipeline Studio's design is `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` (the authority; §7 lists the milestones), with one implementation plan per milestone in `docs/superpowers/plans/`. Its UI tests are designed in `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`. The topic owners are designed in `docs/superpowers/specs/2026-10-08-topic-containers-design.md`, with one plan per milestone.
 
 ## Layout
 
-- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console.
+- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console, topic owners (`x-topic-owner`, one block of three services per instance), Prometheus.
 - `producer/`: producer page. Go (franz-go), `main.go` plus embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
 - `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `stream.go`, `docker.go`, `kafka.go`, `transform.go`, `router.go`) and node runner (`node.go`, with its retry loop and failure path in `retry.go`, run as `studio node` in one container per producer, consumer and consumer instance), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
 - `studio/ui/e2e/`: the UI tests (Playwright, `playwright.config.ts` beside them): `flows.ts` (the API and flow parts), `locators.ts`, `studio.ts` (the fixture), and one `*.spec.ts` per area. `make verify-ui` runs them against the running stack.
+- `topic-owner/`: topic owner. Go (franz-go, client_golang): `config.go` (environment, name rules, role defaults), `reconcile.go` (the desired-state diff and its loop), `metrics.go` (`/metrics`), `main.go` (`/healthz`, `-healthcheck`); multi-stage `Dockerfile` onto `scratch`. It runs as one container per topic.
+- `prometheus/`: `prometheus.yml`, bind-mounted read-only into the `prometheus` service.
 - `flows/`: Studio flow files, bind-mounted into the `studio` container; `flows/0a1b2c3d.json` is the example.
-- `Makefile`: day-to-day commands; `make test` runs the static checks and unit tests, `make verify` the end-to-end test (`verify-studio` deletes its flows and its `studio-verify…` topics when it exits; name a new test topic in that trap too).
+- `Makefile`: day-to-day commands; `make test` runs the static checks and unit tests, `make verify` the end-to-end test (`verify-studio` deletes its flows and its `studio-verify…` topics when it exits, and `verify-topics` its `owner-verify…` containers, topics and group; name a new test topic or group in that trap too).
 
 ## Check your change
 
 ```sh
 make test                  # go vet, gofmt, go test, UI build (tsc), UI tests type-check, compose config
-make down && make verify   # always: ends with STUDIO OK, UI OK and VERIFY OK (the first run downloads Chromium)
-make down                  # then `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing
+make down && make verify   # always: ends with STUDIO OK, UI OK, TOPICS OK and VERIFY OK (the first run downloads Chromium)
+make down                  # then `docker ps -aq -f label=studio.flow`, `docker ps -aq -f label=topic-owner.role` and `git status --short flows` print nothing
 ```
 
 ## Rules
@@ -36,6 +38,8 @@
 - Studio consumers need their own groups: franz-go and the kcat consumers' librdkafka share no assignor, so the broker refuses a mixed group.
 - One `.gitignore`, at the root; don't add nested ones (a nested `dist` rule would hide `studio/ui/dist/.gitkeep`).
 - The UI tests (`studio/ui/e2e/`, Playwright, `make verify-ui`) find elements by role, label and text, and by `data-testid` where there is none (`node-<id>`, `runtime-<id>`, `tail`). A change to the UI's visible text, roles or those ids runs `make verify-ui`; the top-bar messages they check are quoted in the Studio spec or the UI tests spec (§5), so rewording one goes through the spec. Everything they make is named `studio-ui-…`, a namespace the suite owns: each test removes every such flow, and the run removes every such topic and group after the last test. Don't give anything else that prefix.
-- Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). Keep that line in `down`; use `make nodes` to see them.
+- Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). So does a topic owner started with `docker compose run` (labels `topic-owner.role` and `com.docker.compose.oneoff=True`), which `docker compose down` leaves behind. Keep that line in `down`; use `make nodes` to see them. `down` passes `-v`: the `kafka` and `prometheus` images declare anonymous volumes that would otherwise stay.
+- A topic owner's service, `container_name` and topic are the same string: `<base>-<instance>`, plus `__retry` or `__dlq` (the suffixes Studio uses). The name rules live in `topic-owner/config.go`, on top of Studio's `topicNameRe` and `maxInputTopic`. Its labels are `topic-owner.base`, `topic-owner.instance` and `topic-owner.role`, never `studio.flow`. Prometheus finds owners by `topic-owner.role`, so a new instance needs no Prometheus change: add one block of three services with their own anchors (README, "Add an instance").
+- A topic owner's series that mean what kafka-exporter's mean keep its names and labels; the rest are `topic_owner_*`. `verify-topics` greps metric names and log lines; renaming one changes it too. `verify-topics` owns the `owner-verify` prefix: don't give anything else that name.
 - Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
 - Keep README.md, and the spec when behaviour departs from it, in sync with any behaviour change.
```

- [ ] **Step 3: The spec's status**

In `docs/superpowers/specs/2026-10-08-topic-containers-design.md`, replace the Status paragraph (its first three lines after the title) with:

```markdown
Status: approved on 2026-10-08. M1 is built, from
`docs/superpowers/plans/2026-10-08-topic-containers-m1.md`; M2 and M3 follow,
one plan each.
```

- [ ] **Step 4: Run the full check**

Run: `make test && make down && make verify`
Expected: `make test` passes, and `make verify` prints `STUDIO OK (…)`, `… passed`, `UI OK`, `TOPICS OK`, then `VERIFY OK`.

- [ ] **Step 5: Leave a clean machine**

Run: `make down && docker ps -aq -f label=studio.flow && docker ps -aq -f label=topic-owner.role && git status --short flows`
Expected: no output after `make down`'s own lines.

- [ ] **Step 6: Commit**

```bash
git add README.md AGENTS.md docs/superpowers/specs/2026-10-08-topic-containers-design.md
git commit -m "docs: topic owners — README section (what they own, configuration, adding an instance, metrics, the socket), AGENTS layout and rules, spec status M1 built

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
