# Kafka Playground Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A local Kafka sandbox started by one `docker compose up`: KRaft broker, idempotent topic jobs, kcat consumers that show group balancing and fan-out, and Redpanda Console as the producer web page.

**Architecture:** One `docker-compose.yml` and no custom code. Two YAML anchors (`x-topic`, `x-consumer`) wrap existing images with a few lines of shell so topic jobs and consumers can be declared repeatedly with env vars. A `Makefile` wraps the day-to-day commands and ships the one end-to-end check (`make verify`). A `README.md` explains start, extend, walkthrough.

**Tech Stack:** Docker 29.8 / Compose v5.5 on macOS; `apache/kafka:4.3.1` (broker + CLI scripts); `confluentinc/cp-kcat:8.2.4` (consumers); `redpandadata/console:v3.12.0` (producer UI); GNU make; curl; base64.

**Spec:** `docs/superpowers/specs/2026-10-06-kafka-playground-design.md`

## Global Constraints

- Images pinned exactly: `apache/kafka:4.3.1`, `confluentinc/cp-kcat:8.2.4`, `redpandadata/console:v3.12.0`. No `latest` anywhere.
- No authentication, no TLS. Local only.
- Everything starts with `docker compose up`; no manual steps.
- Broker addresses: `kafka:19092` from containers, `localhost:9092` from the host. Console on host port `8080`.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`: topics exist only via topic jobs.
- Only three deliverable files: `docker-compose.yml`, `Makefile`, `README.md`. No custom code, no Dockerfiles unless a documented fallback in the spec is triggered.
- Host is macOS: BSD `base64` (no `-w0`), no `timeout` binary, GNU make 3.81+ from Xcode CLT.
- Makefile style (from the user's reference): `SERVICE` variable, `help` greps `## ` comments, `.PHONY`, `# ── Section ──` rules, lower-case `arg ?= default` variables, `@echo` progress lines, recipes indented with real tab characters.
- Compose interpolation: inside compose `command`/`entrypoint` strings, a literal shell `$` must be written `$$`.
- Every commit message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

1. A topic job declared without `TOPIC_NAME` (or with it empty) must exit non-zero with `TOPIC_NAME is required`, never create a topic with an empty name. Pinned in Task 2, Step 7.
2. A client on the host connecting to `localhost:9092` must receive `localhost:9092` as the advertised broker address, not `kafka:19092`, or every host client would fail after the first metadata request. Pinned in Task 1, Step 6.
3. A consumer started with `AUTO_OFFSET_RESET=latest` must not replay records that already exist in the topic. Pinned in Task 3, Step 7.
4. Producing to a topic that does not exist must return an error promptly and never report a partition/offset (auto-create is off). Pinned in Task 4, Step 8.
5. `make produce` with a JSON value containing spaces, double quotes and colons must deliver the exact bytes; the consumer must print the value verbatim. Pinned in Task 5, Step 6.

---

### Task 1: Broker service with healthcheck

**Files:**
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing.
- Produces: service name `kafka`; internal bootstrap `kafka:19092`; host bootstrap `localhost:9092`; healthcheck that later services gate on with `depends_on: kafka: condition: service_healthy`.

- [ ] **Step 1: Run the check that must fail before the file exists**

Run: `cd /Users/felipe.dos.santos/code/mine/kafka-playground && docker compose config --services`
Expected: error containing `no configuration file provided`.

- [ ] **Step 2: Create `docker-compose.yml` with the broker only**

```yaml
# Local Kafka playground: one KRaft broker, one-shot topic jobs, kcat consumers,
# Redpanda Console as the producer page. No auth, no TLS, nothing persisted.
# Containers reach the broker at kafka:19092; the host at localhost:9092.

services:
  kafka:
    image: apache/kafka:4.3.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:19092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 15s
```

- [ ] **Step 3: Validate the file**

Run: `docker compose config --quiet && echo VALID`
Expected: `VALID`.

- [ ] **Step 4: Start the broker and wait for healthy**

Run: `docker compose up -d --wait kafka && docker compose ps --format 'table {{.Service}}\t{{.Status}}'`
Expected: a row `kafka   Up N seconds (healthy)` within about 40 s. If it never turns healthy, run `docker compose logs kafka | tail -30` and `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list` to see the actual error.

- [ ] **Step 5: Confirm the host port is open**

Run: `nc -z localhost 9092 && echo OPEN`
Expected: `OPEN`.

- [ ] **Step 6: Confirm the advertised host listener (Review Focus 2)**

Run (uses the consumer image as a throwaway client; `host.docker.internal` is how a container reaches the Mac host):
```sh
docker run --rm --entrypoint kcat confluentinc/cp-kcat:8.2.4 -L -b host.docker.internal:9092
```
Expected output contains `broker 1 at localhost:9092`. If `kcat` is installed on the host, also run `kcat -L -b localhost:9092` and expect the same line.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml
git commit -m "Add single-node KRaft broker with healthcheck

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Topic provisioner anchor and the two example topics

**Files:**
- Modify: `docker-compose.yml` (add `x-topic` above `services:`, add two services under `services:`)

**Interfaces:**
- Consumes: service `kafka` healthy, bootstrap `kafka:19092`.
- Produces: anchor `*topic`; env contract `TOPIC_NAME` (required), `PARTITIONS` (default 1), `REPLICATION_FACTOR` (default 1); services `topic-orders` (3 partitions) and `topic-events` (1 partition) that exit 0, usable as `depends_on: <job>: condition: service_completed_successfully`.

- [ ] **Step 1: Run the check that must fail before the services exist**

Run: `docker compose config --services | grep -c '^topic-'`
Expected: `0`.

- [ ] **Step 2: Add the anchor and the two jobs**

Insert this block between the header comment and `services:`:

```yaml
# One-shot job: create a topic, exit 0 (also when it already exists).
# Declare it as many times as you need topics; only `environment` changes.
x-topic: &topic
  image: apache/kafka:4.3.1
  depends_on:
    kafka:
      condition: service_healthy
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

Append under `services:` (after `kafka`):

```yaml
  topic-orders:
    <<: *topic
    environment:
      TOPIC_NAME: orders
      PARTITIONS: 3

  topic-events:
    <<: *topic
    environment:
      TOPIC_NAME: events
```

- [ ] **Step 3: Validate and run both jobs**

Run:
```sh
docker compose config --quiet && echo VALID
docker compose up -d topic-orders topic-events && docker compose wait topic-orders topic-events; echo "wait exit=$?"
docker compose ps -a --format 'table {{.Service}}\t{{.Status}}' | grep topic-
```
Expected: `VALID`, `wait exit=0`, and both rows show `Exited (0)`.

- [ ] **Step 4: Check the logs of the first run**

Run: `docker compose logs --no-log-prefix topic-orders topic-events`
Expected: `Created topic orders.` and `Created topic events.` (order may vary), no stack traces.

- [ ] **Step 5: Re-run one job to prove idempotency**

Run: `docker compose up -d topic-orders && docker compose wait topic-orders; echo "wait exit=$?"`
Expected: `wait exit=0`. No new `Created topic` line in `docker compose logs --no-log-prefix topic-orders`.

- [ ] **Step 6: Check the partition count**

Run: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic orders | head -1`
Expected: a line containing `Topic: orders`, `PartitionCount: 3`, `ReplicationFactor: 1`.

- [ ] **Step 7: Missing `TOPIC_NAME` fails fast (Review Focus 1)**

Run: `docker compose run --rm --no-deps -e TOPIC_NAME= topic-orders; echo "exit=$?"`
Expected: stderr contains `TOPIC_NAME is required`, then `exit=1`. Then confirm no empty-named topic appeared: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list` prints exactly `events` and `orders`.

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml
git commit -m "Add idempotent topic jobs for orders and events

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Consumer anchor and the three example consumers

**Files:**
- Modify: `docker-compose.yml` (add `x-consumer` after `x-topic`, add two services under `services:`)

**Interfaces:**
- Consumes: service `kafka`, jobs `topic-orders` / `topic-events`.
- Produces: anchor `*consumer`; env contract `TOPIC_NAME` (required), `GROUP_ID` (default: container hostname, unique per container), `AUTO_OFFSET_RESET` (default `earliest`); services `orders-workers` (2 replicas, group `orders-workers`) and `orders-audit` (own group). Log line format `partition=<n> offset=<n> key=<k> value=<v>`; kcat prints `% Group <g> rebalanced ... assigned: orders [0], ...` on stderr.

- [ ] **Step 1: Run the check that must fail before the services exist**

Run: `docker compose config --services | grep -c '^orders-'`
Expected: `0`.

- [ ] **Step 2: Add the anchor and the two consumer services**

Insert after the `x-topic` block (before `services:`):

```yaml
# Consumer: subscribe to one topic, print every record to stdout.
# GROUP_ID defaults to the container hostname, so a consumer without one is
# its own group (fan-out); consumers sharing a GROUP_ID split the partitions.
x-consumer: &consumer
  image: confluentinc/cp-kcat:8.2.4
  depends_on:
    kafka:
      condition: service_healthy
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

Append under `services:` (after `topic-events`):

```yaml
  # Two members of one group on a 3-partition topic: each gets a share.
  # `docker compose up -d --scale orders-workers=3` adds a member.
  orders-workers:
    <<: *consumer
    depends_on:
      topic-orders:
        condition: service_completed_successfully
    deploy:
      replicas: 2
    environment:
      TOPIC_NAME: orders
      GROUP_ID: orders-workers

  # Own group (no GROUP_ID): receives every record regardless of the workers.
  orders-audit:
    <<: *consumer
    depends_on:
      topic-orders:
        condition: service_completed_successfully
    environment:
      TOPIC_NAME: orders
```

- [ ] **Step 3: Validate and start the consumers**

Run:
```sh
docker compose config --quiet && echo VALID
docker compose up -d orders-workers orders-audit && sleep 8
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' | grep orders-
```
Expected: `VALID`; three rows `Up`: two for service `orders-workers`, one for `orders-audit`. If a container is restarting or exited, `docker compose logs orders-workers` shows the kcat error; the likely causes are the image lacking `sh` (then use `bash` in the anchor's `entrypoint`) or a typo in the `-X` option.

- [ ] **Step 4: Check partition assignment lines**

Run:
```sh
docker compose logs --no-log-prefix orders-workers | grep rebalanced
docker compose logs --no-log-prefix orders-audit | grep rebalanced
```
Expected: for the workers, two lines whose `assigned:` lists are disjoint and together cover `orders [0]`, `orders [1]`, `orders [2]` (for example one line `assigned: orders [0], orders [1]` and one `assigned: orders [2]`). For the audit consumer, one line listing all three partitions.

- [ ] **Step 5: Produce three keyed records from inside the broker and watch them arrive**

Run:
```sh
docker compose exec -T kafka bash -c 'printf "%s\n" "a|{\"n\":1}" "b|{\"n\":2}" "c|{\"n\":3}" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic orders --property parse.key=true --property key.separator="|"'
sleep 3
docker compose logs --no-log-prefix orders-workers | grep '^partition='
docker compose logs --no-log-prefix orders-audit | grep '^partition='
```
Expected: three lines from the workers in total (each record printed by exactly one replica) and three lines from the audit consumer, formatted like `partition=1 offset=0 key=a value={"n":1}`.

- [ ] **Step 6: Confirm the shared group really split the work**

Run: `docker compose logs orders-workers | grep '^kafka-playground-orders-workers' | grep 'partition=' | cut -d'|' -f1 | sort | uniq -c`
Expected: if the three keys hashed to more than one partition, more than one container name appears. If all three keys landed in one partition only one container prints, which is still correct; in that case produce three more records with keys `d`, `e`, `f` using the Step 5 command and re-check.

- [ ] **Step 7: `AUTO_OFFSET_RESET=latest` does not replay (Review Focus 3)**

Run:
```sh
cid=$(docker compose run -d --no-deps -e TOPIC_NAME=orders -e AUTO_OFFSET_RESET=latest orders-audit)
sleep 6
echo "records replayed: $(docker logs "$cid" 2>&1 | grep -c '^partition=')"
echo "joined: $(docker logs "$cid" 2>&1 | grep -c rebalanced)"
docker rm -f "$cid" >/dev/null
```
Expected: `records replayed: 0` and `joined: 1`.

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml
git commit -m "Add kcat consumers: shared-group workers and separate-group audit

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Redpanda Console as the producer page

**Files:**
- Modify: `docker-compose.yml` (add service `console`)

**Interfaces:**
- Consumes: service `kafka`, bootstrap `kafka:19092`.
- Produces: service `console` on `http://localhost:8080`; `GET /admin/health`; `GET /api/topics`; `POST /api/topics-records` with body `{"topicNames":["<topic>"],"compressionType":0,"useTransactions":false,"records":[{"key":<base64 or null>,"value":"<base64>","headers":[],"partitionId":-1}]}` returning a 200 JSON body that reports the partition and offset per record.

- [ ] **Step 1: Run the check that must fail before the service exists**

Run: `curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/admin/health`
Expected: `000` (connection refused).

- [ ] **Step 2: Add the service**

Append under `services:` (after `orders-audit`):

```yaml
  # Producer web page: http://localhost:8080 -> Topics -> orders -> Produce record.
  console:
    image: redpandadata/console:v3.12.0
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_BROKERS: kafka:19092
```

- [ ] **Step 3: Start it and check health**

Run:
```sh
docker compose config --quiet && echo VALID
docker compose up -d console && sleep 5
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/admin/health
```
Expected: `VALID` and `200`. If not 200 after 20 s, `docker compose logs console | tail -20` shows why (a fatal line about the Kafka connection means the `KAFKA_BROKERS` value or the broker's `PLAINTEXT` listener is wrong).

- [ ] **Step 4: Topic list comes from the broker**

Run: `curl -s localhost:8080/api/topics | grep -o '"topicName":"[^"]*"' | sort`
Expected: `"topicName":"events"` and `"topicName":"orders"`. Internal topics such as `__consumer_offsets` may also appear; that is fine.

- [ ] **Step 5: Produce through the backend**

Run:
```sh
curl -sS -X POST localhost:8080/api/topics-records -H 'Content-Type: application/json' \
  -d "{\"topicNames\":[\"orders\"],\"compressionType\":0,\"useTransactions\":false,\"records\":[{\"key\":\"$(printf k1 | base64)\",\"value\":\"$(printf '{"hello":"console"}' | base64)\",\"headers\":[],\"partitionId\":-1}]}"
echo
```
Expected: HTTP 200 with a JSON body that names the partition and offset of the record (field names like `partitionId` and `offset`). Copy the exact body into your notes; Task 6 quotes it in the README.

- [ ] **Step 6: The consumers received it**

Run: `sleep 3; docker compose logs --no-log-prefix orders-workers orders-audit | grep -c 'key=k1 value={"hello":"console"}'`
Expected: `2` (once from one worker replica, once from the audit consumer).

- [ ] **Step 7: Produce from the page URL exists**

Run: `curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/topics/orders/produce-record`
Expected: `200` (the single-page app serves its HTML for that route).

- [ ] **Step 8: Unknown topic returns an error, no offset (Review Focus 4)**

Run:
```sh
curl -s -m 20 -w '\nHTTP %{http_code}\n' -X POST localhost:8080/api/topics-records -H 'Content-Type: application/json' \
  -d '{"topicNames":["nope"],"compressionType":0,"useTransactions":false,"records":[{"key":null,"value":"e30=","headers":[],"partitionId":-1}]}'
```
Expected: the response arrives within 20 s and either the status is not 200 or the body carries an error message for the record; in no case does the body report a non-negative `offset` for topic `nope`. Confirm no topic was created: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list` still prints only `events` and `orders`.

- [ ] **Step 9: Commit**

```bash
git add docker-compose.yml
git commit -m "Add Redpanda Console as the producer page

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Makefile

**Files:**
- Create: `Makefile`

**Interfaces:**
- Consumes: compose services `kafka`, `topic-orders`, `topic-events`, `orders-workers`, `orders-audit`, `console`; Console endpoint `POST /api/topics-records`; consumer log format `key=<k> value=<v>`.
- Produces: targets `help`, `up`, `down`, `ps`, `logs`, `topics`, `groups`, `produce`, `scale`, `verify`; variables `topic` (default `orders`), `key` (default empty), `value` (required by `produce`), `n` (default 3), `svc` (default `orders-workers orders-audit`), `CONSOLE_URL` (default `http://localhost:8080`).

- [ ] **Step 1: Run the check that must fail before the file exists**

Run: `make help`
Expected: `make: *** No rule to make target 'help'.  Stop.` or `No targets specified and no makefile found`.

- [ ] **Step 2: Create `Makefile`**

Recipe lines start with a real tab character, not spaces.

```makefile
# Makefile for Kafka Playground — local KRaft broker, topic jobs, kcat consumers, Redpanda Console
# Typical flow: up → produce → logs → scale → groups → down
SERVICE = Kafka Playground

# Variables
COMPOSE = docker compose
CONSOLE_URL ?= http://localhost:8080
topic ?= orders
key ?=
n ?= 3
svc ?= orders-workers orders-audit

.PHONY: help up down ps logs topics groups produce scale verify

# ── Environment ──────────────────────────────────────────────────────────────

help: ## Print this help message
	@printf '\033[01;32m${SERVICE} — Local Kafka sandbox\033[00;37m\n\n'
	@printf "\033[33mUsage:\033[0m\n  make [target] [arg=\"val\"...]\n\n\033[33mTargets:\033[0m\n"
	@grep -E '^[-a-zA-Z0-9_\.\/]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
		{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ── Stack ────────────────────────────────────────────────────────────────────

up: ## [STEP 1] Start the whole stack and wait until every service is healthy or running
	$(COMPOSE) up -d --wait
	@echo "Console: $(CONSOLE_URL)   Broker from the host: localhost:9092"

down: ## Stop and remove every container (topics and messages are lost)
	$(COMPOSE) down --remove-orphans
	@echo "Stack removed."

ps: ## Show every container, including exited topic jobs
	$(COMPOSE) ps -a

logs: ## [STEP 3] Follow consumer logs (usage: make logs [svc="orders-audit"]; default: all example consumers)
	$(COMPOSE) logs -f $(svc)

# ── Inspect ──────────────────────────────────────────────────────────────────

topics: ## Describe every topic: partitions, leaders, replicas
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe

groups: ## [STEP 5] Describe every consumer group: members, assigned partitions, lag
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --all-groups

# ── Produce ──────────────────────────────────────────────────────────────────

# ponytail: value is single-quote wrapped; a value containing ' needs the raw curl in the README.
produce: ## [STEP 2] Produce one JSON record via the Console backend (usage: make produce value='{"id":1}' [topic=orders] [key=k1])
	@test -n '$(value)' || { echo "value is required, e.g. make produce value='{\"id\":1}'"; exit 1; }
	@if [ -n '$(key)' ]; then key="\"$$(printf '%s' '$(key)' | base64 | tr -d '\n')\""; else key=null; fi; \
	value=$$(printf '%s' '$(value)' | base64 | tr -d '\n'); \
	curl -sS -X POST $(CONSOLE_URL)/api/topics-records -H 'Content-Type: application/json' \
		-d "{\"topicNames\":[\"$(topic)\"],\"compressionType\":0,\"useTransactions\":false,\"records\":[{\"key\":$$key,\"value\":\"$$value\",\"headers\":[],\"partitionId\":-1}]}"; \
	echo

# ── Scale ────────────────────────────────────────────────────────────────────

scale: ## [STEP 4] Set the number of orders-workers group members (usage: make scale n=3)
	$(COMPOSE) up -d --scale orders-workers=$(n) orders-workers
	@echo "orders-workers now has $(n) member(s); watch the rebalance with: make logs svc=orders-workers"

# ── Verify ───────────────────────────────────────────────────────────────────

verify: up ## End-to-end check: produce a unique record, assert the worker group logged it once and the audit group once
	@id="verify-$$(date +%s)"; \
	$(MAKE) --no-print-directory produce topic=orders key="$$id" value="{\"id\":\"$$id\"}" >/dev/null; \
	sleep 3; \
	w=$$($(COMPOSE) logs --no-log-prefix orders-workers | grep -c "key=$$id "); \
	a=$$($(COMPOSE) logs --no-log-prefix orders-audit | grep -c "key=$$id "); \
	echo "orders-workers saw it $$w time(s), orders-audit saw it $$a time(s)"; \
	if [ "$$w" = 1 ] && [ "$$a" = 1 ]; then echo "VERIFY OK"; else echo "VERIFY FAILED"; exit 1; fi
```

- [ ] **Step 3: `help` lists every target**

Run: `make help`
Expected: a green header, then ten lines, one per target, each with its `##` description; no `Makefile:NN: *** missing separator` error (that error means a recipe line is indented with spaces instead of a tab).

- [ ] **Step 4: `up` brings up the full stack**

Run: `make up; echo "exit=$?"`
Expected: `exit=0` and the `Console: http://localhost:8080 ...` line. If `docker compose up -d --wait` instead fails with a message about `topic-orders` or `topic-events` having exited, change the `up` recipe to the two lines `$(COMPOSE) up -d` and `$(COMPOSE) wait topic-orders topic-events` and re-run.

- [ ] **Step 5: `ps`, `topics`, `groups` run**

Run: `make ps && make topics && make groups`
Expected: `ps` shows `kafka` healthy, the two topic jobs `Exited (0)`, two `orders-workers`, `orders-audit` and `console` up; `topics` shows `orders` with 3 partitions and `events` with 1; `groups` shows group `orders-workers` with two members covering partitions 0–2, and one more group named after the audit container's hostname.

- [ ] **Step 6: `produce` preserves quotes, spaces and colons (Review Focus 5)**

Run:
```sh
make produce value='{"a": "b c", "n": 1}' key=q1
sleep 3
docker compose logs --no-log-prefix orders-audit | grep -F 'key=q1 value={"a": "b c", "n": 1}'
```
Expected: the first command prints a JSON body with partition and offset; the last prints exactly one line ending in `key=q1 value={"a": "b c", "n": 1}`.

- [ ] **Step 7: `produce` argument handling**

Run:
```sh
make produce; echo "exit=$?"
make produce topic=events value='{}'
cid=$(docker compose run -d --no-deps -e TOPIC_NAME=events -e GROUP_ID=events-check orders-audit)
sleep 6; docker logs "$cid" 2>&1 | grep '^partition='
docker rm -f "$cid" >/dev/null
```
Expected: first prints `value is required, ...` and `exit=1`; second prints a body with partition `0`; the temporary events consumer prints `partition=0 offset=0 key= value={}` before it is removed.

- [ ] **Step 8: `verify` passes**

Run: `make verify`
Expected: ends with `orders-workers saw it 1 time(s), orders-audit saw it 1 time(s)` and `VERIFY OK`, exit 0.

- [ ] **Step 9: `scale` triggers a visible rebalance**

Run:
```sh
make scale n=3 && sleep 6
docker compose logs --no-log-prefix orders-workers | grep rebalanced | tail -3
make scale n=2
```
Expected: the three most recent rebalance lines show three different member ids, each assigned exactly one of `orders [0]`, `orders [1]`, `orders [2]`. The last command returns the group to two members.

- [ ] **Step 10: Commit**

```bash
git add Makefile
git commit -m "Add Makefile: up/down/logs/produce/scale/verify

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: README

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: everything above. The produce response body noted in Task 4 Step 5.
- Produces: user documentation; nothing else depends on it.

- [ ] **Step 1: Write `README.md`**

```markdown
# Kafka playground

A local Kafka sandbox for watching partitions, consumer groups and fan-out while you produce JSON messages from a browser. One `docker compose up`, no custom code: every component is an existing, pinned image.

| Service | Image | Role | Where |
|---|---|---|---|
| `kafka` | `apache/kafka:4.3.1` | single-node KRaft broker | `localhost:9092` from the host, `kafka:19092` from containers |
| `topic-orders`, `topic-events` | `apache/kafka:4.3.1` | one-shot jobs: create a topic, exit 0 | `make ps` |
| `orders-workers` (2 replicas) | `confluentinc/cp-kcat:8.2.4` | consumers sharing group `orders-workers` on `orders` (3 partitions) | `make logs svc=orders-workers` |
| `orders-audit` | `confluentinc/cp-kcat:8.2.4` | consumer in its own group on `orders` | `make logs svc=orders-audit` |
| `console` | `redpandadata/console:v3.12.0` | web UI: browse topics, produce records | http://localhost:8080 |

Topic auto-creation is off: a topic exists only if a topic job created it. Nothing is persisted; `make down` wipes everything.

## Start

```sh
make up        # docker compose up -d --wait
make ps
```

Open http://localhost:8080. `make help` lists every target.

## Add a topic

Copy one of the topic jobs in `docker-compose.yml`:

```yaml
  topic-payments:
    <<: *topic
    environment:
      TOPIC_NAME: payments      # required
      PARTITIONS: 6             # optional, default 1
      REPLICATION_FACTOR: 1     # optional, default 1 (single broker: keep 1)
```

Re-running a job is safe: it exits 0 if the topic already exists.

## Add a consumer

Copy a consumer service. `depends_on` points at the topic's job so the consumer starts after the topic exists.

```yaml
  payments-worker:
    <<: *consumer
    depends_on:
      topic-payments:
        condition: service_completed_successfully
    environment:
      TOPIC_NAME: payments            # required
      GROUP_ID: payments-workers      # optional; default: unique per container
      AUTO_OFFSET_RESET: earliest     # optional; earliest (default) or latest
```

- Same `GROUP_ID` across containers: the group shares the topic's partitions (load balancing). More members than partitions leaves members idle.
- No `GROUP_ID`: each container is its own group and receives every record (fan-out).
- `deploy.replicas: N` in the service, or `make scale n=N` for `orders-workers`, adds members to a group.

Each record prints as `partition=0 offset=12 key=k1 value={"id":1}`. On every group change kcat also prints `% Group ... rebalanced ... assigned: orders [0], orders [1]`.

## Walkthrough

1. `make logs` in one terminal.
2. Open http://localhost:8080/topics/orders/produce-record. Enter key `k1`, paste `{"id": 1, "status": "new"}` as the value and click Produce. Console reports the partition and offset of the new record.
3. In the logs the record appears once in `orders-workers` (one of the two replicas) and once in `orders-audit`.
4. Produce again with the same key: same partition, offset + 1. Use another key, or pick a partition in the form, to see the other worker replica print it.
5. `make scale n=3`: the rebalance lines show three members with one partition each. `make scale n=2` goes back.
6. `make groups` shows each group's members, assigned partitions and lag.

## Produce from the command line

This goes through the Console backend, exactly what the form does:

```sh
make produce value='{"id": 2}' key=k2             # topic defaults to orders
make produce topic=events value='{"type": "ping"}'
```

The raw request, if you need it elsewhere (key and value are base64):

```sh
curl -sS -X POST localhost:8080/api/topics-records -H 'Content-Type: application/json' \
  -d "{\"topicNames\":[\"orders\"],\"compressionType\":0,\"useTransactions\":false,\"records\":[{\"key\":\"$(printf k1 | base64)\",\"value\":\"$(printf '{"id":1}' | base64)\",\"headers\":[],\"partitionId\":-1}]}"
```

The response names the partition and offset of each record, or the error.

## Connect from the host

Any Kafka client on your machine: bootstrap server `localhost:9092`, no auth, no TLS.

```sh
kcat -L -b localhost:9092                                                                 # if kcat is installed
docker run --rm --entrypoint kcat confluentinc/cp-kcat:8.2.4 -L -b host.docker.internal:9092   # otherwise
```

## Verify end to end

```sh
make verify
```

Produces a unique record and asserts that the worker group logged it exactly once and the audit group exactly once.

## Reset

```sh
make down      # docker compose down: removes containers; topics and messages are gone
```
```

- [ ] **Step 2: Replace the response sentence with the real body**

In "Produce from the command line", after `The response names the partition and offset of each record, or the error.`, add a fenced block containing the exact JSON body recorded in Task 4 Step 5 (with the offset you got). If the body in Task 4 used different field names than `partitionId`/`offset`, use the real ones.

- [ ] **Step 3: Run the README top to bottom on a fresh stack**

Run, in order: `make down`, `make up`, `make ps`, `make produce value='{"id": 2}' key=k2`, `make produce topic=events value='{"type": "ping"}'`, the raw `curl` block, `make scale n=3`, `make scale n=2`, `make groups`, `make verify`, the `docker run --rm --entrypoint kcat ...` line, `make down`.
Expected: every command exits 0 and its output matches the README's description of it. Fix any README sentence that does not match what you saw.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "Add README: start, extend, walkthrough, verify

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Final verification from scratch and report

**Files:**
- Modify: `README.md` (only if the UI observation in Step 3 contradicts the walkthrough wording)

**Interfaces:**
- Consumes: everything.
- Produces: the verification report for the user.

- [ ] **Step 1: Cold start and the automated check**

Run:
```sh
make down
docker compose up -d --wait; echo "up exit=$?"
make ps
make verify
```
Expected: `up exit=0`; `ps` shows `kafka` healthy, both topic jobs `Exited (0)`, two `orders-workers`, `orders-audit`, `console`; `VERIFY OK`.

- [ ] **Step 2: Idempotency and scale, once more on the cold stack**

Run:
```sh
docker compose up -d topic-orders topic-events && docker compose wait topic-orders topic-events; echo "rerun exit=$?"
make scale n=3 && sleep 6 && docker compose logs --no-log-prefix orders-workers | grep rebalanced | tail -3
make scale n=2
```
Expected: `rerun exit=0`; three rebalance lines with one partition each.

- [ ] **Step 3: Produce from the web page itself**

If a browser automation skill is available in the session (`anthropic-skills:chrome-browser` or `anthropic-skills:built-in-browser`), open `http://localhost:8080/topics/orders/produce-record`, set key `ui-1`, value `{"from": "ui"}`, click Produce, and take a screenshot of the result. Then enter the value `{` (invalid JSON) and record whether the page blocks or warns before sending. Then run `docker compose logs --no-log-prefix orders-workers orders-audit | grep -c 'key=ui-1 value={"from": "ui"}'` and expect `2`.
If no browser automation is available, skip this step and say so in the report: the UI result display and the client-side JSON validation were not observed; the backend call was verified in Task 4.

- [ ] **Step 4: Reconcile the README with what the UI showed**

If Step 3 ran and Console displayed the partition and offset, leave the walkthrough as is. If it displayed something else (for example only a success toast), rewrite walkthrough item 2's last sentence to describe exactly what appeared, and add one sentence to "Produce from the command line" noting that the backend response is where the partition and offset are guaranteed. If the invalid-JSON test showed no client-side validation, add to the walkthrough: `Console does not validate JSON before sending; an invalid value is produced as plain text.` Commit any change:

```bash
git add README.md
git commit -m "Align README walkthrough with observed Console behaviour

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

- [ ] **Step 5: Tear down and write the report**

Run: `make down`
Then report to the user, in this order: what was verified (cold start, idempotent jobs, shared-group split, fan-out, scale rebalance, host reachability, backend produce with partition/offset, consumers receiving it), what could not be verified (UI display and JSON validation, if Step 3 was skipped), and the per-component reuse evaluation summary from the spec.
