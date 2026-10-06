# Kafka playground — design

Date: 2026-10-06

## Goal

A laptop-only Kafka sandbox started by one `docker compose up`, for watching
partitions, consumer groups and fan-out behave while producing JSON messages
from a browser.

Success criteria:

- `docker compose up` brings everything up with no manual steps; nothing
  uses a `latest` tag.
- Two topics exist after startup; re-running a topic job exits 0.
- Three consumers run: two share a group on a 3-partition topic (each gets a
  subset of partitions), one has its own group (sees every message).
  `docker compose up --scale` adds members and triggers a visible rebalance.
- A web page on a host port produces a record (topic, optional key, JSON
  value) and reports the partition and offset, or the error.
- The broker is reachable from containers (`kafka:19092`) and from the host
  (`localhost:9092`).

Non-goals: auth, TLS, persistence across `docker compose down`, schema
registry, multi-broker.

## Component decisions

No custom code. Every component is an existing image.

| Component | Chosen | Rejected |
|---|---|---|
| Broker | `apache/kafka:4.3.1` — official, KRaft via `KAFKA_*` env, ships CLI scripts used for the healthcheck and topic jobs. 4.4.0 is still RC. | `apache/kafka-native` (no CLI scripts, would need a second image anyway); `bitnami/kafka` (free tag catalog gutted in 2025); `confluentinc/cp-kafka` (bigger, Confluent licence). |
| Topic provisioner | `kafka-topics.sh --create --if-not-exists` from the broker image, wrapped in a compose YAML anchor. Idempotent by flag, env defaults via bash. | Custom code (nothing to add). |
| Consumer | `confluentinc/cp-kcat:8.2.4` — kcat with current librdkafka, image rebuilt Sep 2026, `-G` mode prints partition assignments on every rebalance. | `edenhill/kcat:1.7.1` (image frozen Jan 2022, librdkafka 1.8.x, Kafka 4.x compatibility unverified); `kafka-console-consumer.sh` (~300 MB JVM per consumer, slow start, no rebalance output). |
| Producer page | `redpandadata/console:v3.12.0` — Go binary, produce form at `/topics/<topic>/produce-record` with key, value and partition selector, backend `POST /api/topics-records` returns partition and offset. Producing is a Community feature; no licence needed. | Kafbat UI 1.5.0 (produce API returns 204, no partition/offset); AKHQ 0.28.0 (JVM, no JSON validation); Confluent REST Proxy 8.3.2 (returns offsets but still needs custom HTML, nginx, CORS and a ~1 GB JVM); custom Go page (would fit exactly, but user chose Console). |

Known gap accepted by the user: Console selects the topic by navigating to
it rather than a dropdown on the form, and client-side JSON validation and
the on-screen partition/offset display are to be confirmed during
verification, not guaranteed.

## Files

```
docker-compose.yml
Makefile
README.md
docs/superpowers/specs/2026-10-06-kafka-playground-design.md
```

## docker-compose.yml

### Broker `kafka`

- `image: apache/kafka:4.3.1`, `ports: ["9092:9092"]`, no volume (ephemeral).
- Env:
  - `KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`
  - `KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093`
  - `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`
  - `KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:19092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093`
  - `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:9092`
  - `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT`
  - `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT`
  - `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`,
    `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`,
    `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`,
    `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1`
  - `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0`
  - `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (topics exist only via the
    provisioner, so the job is meaningful and Console's topic list is real)
- Healthcheck: `kafka-topics.sh --bootstrap-server localhost:19092 --list`,
  interval 5s, timeout 10s, retries 20, start_period 15s.

### Topic jobs — anchor `x-topic`

```yaml
x-topic: &topic
  image: apache/kafka:4.3.1
  depends_on:
    kafka: { condition: service_healthy }
  command:
    - bash
    - -ec
    - |
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
        --create --if-not-exists \
        --topic "$${TOPIC_NAME:?TOPIC_NAME is required}" \
        --partitions "$${PARTITIONS:-1}" \
        --replication-factor "$${REPLICATION_FACTOR:-1}"
```

Example services: `topic-orders` (`PARTITIONS: 3`) and `topic-events`
(defaults). Adding a topic = one more service with `<<: *topic` and an
`environment` block.

### Consumers — anchor `x-consumer`

```yaml
x-consumer: &consumer
  image: confluentinc/cp-kcat:8.2.4
  depends_on:
    kafka: { condition: service_healthy }
  entrypoint:
    - sh
    - -ec
    - |
      exec kcat -b kafka:19092 \
        -G "$${GROUP_ID:-$$HOSTNAME}" \
        -X auto.offset.reset="$${AUTO_OFFSET_RESET:-earliest}" \
        -u -f 'partition=%p offset=%o key=%k value=%s\n' \
        "$${TOPIC_NAME:?TOPIC_NAME is required}"
```

- `GROUP_ID` defaults to the container hostname, which is unique per
  container, so a service without `GROUP_ID` is its own group.
- Each consumer service overrides `depends_on` with
  `topic-<name>: { condition: service_completed_successfully }`. Reason:
  librdkafka re-checks a missing subscribed topic only every 5 minutes, so a
  consumer that starts before the topic exists would sit idle.
- Example services:
  - `orders-workers`: `TOPIC_NAME=orders`, `GROUP_ID=orders-workers`,
    `deploy.replicas: 2` — shared group, load balancing, scalable with
    `--scale orders-workers=N`.
  - `orders-audit`: `TOPIC_NAME=orders`, no `GROUP_ID` — fan-out.
- Output: one line per record, `partition=N offset=N key=K value=V`, plus
  kcat's `% Group ... rebalanced ... assigned: orders [0], orders [1]` lines on
  stderr, both visible in `docker compose logs -f`.

### Producer page `console`

- `image: redpandadata/console:v3.12.0`, `ports: ["8080:8080"]`,
  `environment: KAFKA_BROKERS=kafka:19092`,
  `depends_on: kafka: { condition: service_healthy }`.
- UI: `http://localhost:8080/topics/<topic>/produce-record`.
- Backend used by that form: `POST /api/topics-records`, body
  `{"topicNames":["orders"],"compressionType":0,"useTransactions":false,
  "records":[{"key":<base64>,"value":<base64>,"headers":[],"partitionId":-1}]}`,
  200 response with partition and offset per record.

## Makefile

Style copied from the user's reference Makefile: `SERVICE` variable, a `help`
target that greps `## ` comments, `.PHONY`, `# ── Section ──` rules, lower-case
`arg ?= default` variables passed as `make target arg=val`, `@echo` progress
lines. Targets:

| Target | Does |
|---|---|
| `help` | print targets (default) |
| `up` | `docker compose up -d --wait`, print URLs |
| `down` | `docker compose down --remove-orphans` |
| `ps` | `docker compose ps -a` |
| `logs [svc=...]` | follow logs, default `orders-workers orders-audit` |
| `topics` | `kafka-topics.sh --describe` inside the broker |
| `groups` | `kafka-consumer-groups.sh --describe --all-groups` inside the broker |
| `produce value='{...}' [topic=orders] [key=k]` | one record via `POST /api/topics-records` (base64 key/value, `partitionId: -1`) |
| `scale n=3` | `docker compose up -d --scale orders-workers=N orders-workers` |
| `verify` | `up`, produce a unique record, assert `orders-workers` logged it once and `orders-audit` once |

`verify` is the one runnable end-to-end check that ships with the repo.

## README.md

Sections, in order: what you get (one table of services and ports), start
(`make up`, URLs, `make help`), add a topic (copy the three-line service), add
a consumer (copy a service; note the `depends_on` on the topic job and the
`GROUP_ID` semantics), walkthrough (produce from the Console form, watch
`make logs`, what to expect for shared vs separate groups, same key → same
partition, `make scale n=3`, `make groups`), produce from the command line
(`make produce` and the raw `curl`), connect from the host (`localhost:9092`),
reset (`make down`), verify (`make verify`).

## Verification plan

1. `docker compose up -d --wait`; `docker compose ps` shows `kafka` healthy,
   both topic jobs exited 0, two `orders-workers` replicas, `orders-audit`
   and `console` running.
2. `docker compose up topic-orders` a second time exits 0 (idempotent).
3. Consumer logs show each `orders-workers` replica assigned a disjoint
   subset of partitions 0–2 and `orders-audit` assigned all three.
4. `curl` the Console backend with a keyed JSON record; response has
   partition and offset.
5. Logs show that record exactly once across the two workers and once in
   `orders-audit`, as `partition=.. offset=.. key=.. value=..`.
6. `docker compose up -d --scale orders-workers=3`; rebalance lines show
   three members with one partition each.
7. Host reachability: a metadata request to `localhost:9092` from the host
   (host `kcat` if installed, otherwise report what was checked).
8. UI check of the produce form (topic navigation, JSON editor behaviour,
   partition/offset display) with browser automation if available; otherwise
   report the API-level verification and state that the on-screen display
   was not observed.

## Risks and fallbacks

- `cp-kcat` entrypoint or shell differs from assumption → adjust the
  `entrypoint` list; worst case a 3-line Dockerfile `FROM cp-kcat` with the
  script copied in (user allowed custom Dockerfiles).
- kcat `-f` ignored in `-G` mode → use `-J` (JSON output, includes
  partition, offset, key, payload).
- Console does not show partition/offset in the UI → report as a gap; the
  backend response does include them.
- Console's keyless records may stick to one partition (franz-go sticky
  partitioner); the form's partition selector and keys still demonstrate
  balancing. Document in README.
