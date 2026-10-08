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
