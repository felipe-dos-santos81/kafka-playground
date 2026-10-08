# Kafka playground

A local Kafka sandbox. Produce JSON from a browser and watch partitions, consumer groups and fan-out. Pipeline Studio adds a canvas where you draw producer → topic → consumer flows and run them.

Everything is local: no auth, no TLS, every port on `127.0.0.1`. `make down` deletes the Kafka data, the metrics and Grafana's state; Studio flows are files in `flows/` and stay.

| Service | What it is | Where |
|---|---|---|
| `kafka` | single-node KRaft broker (`apache/kafka:4.3.1`) | `localhost:9092` (host), `kafka:19092` (containers) |
| `topic-orders`, `topic-events` | one-shot jobs that create a topic and exit | `make ps` |
| `orders-workers` ×2 | kcat consumers sharing group `orders-workers` on `orders` (3 partitions) | `make logs svc=orders-workers` |
| `orders-audit` | kcat consumer in its own group on `orders` | `make logs svc=orders-audit` |
| `producer` | producer page (`producer/`, Go) | http://localhost:8081 |
| `studio` | Pipeline Studio (`studio/`, Go + React Flow) | http://localhost:8082 |
| `orders-1`, `orders-1__retry`, `orders-1__dlq` | topic owners: each creates its topic and keeps it in the desired state (`topic-owner/`, Go) | `make owners` |
| `prometheus` | Prometheus: scrapes the topic owners and evaluates their alert rules | http://localhost:9090 |
| `grafana` | Grafana: the topic-owners dashboard | http://localhost:3000/d/topic-owners |
| `console` | Redpanda Console: topics, messages, groups | http://localhost:8080 |

## Quick start

```sh
make up        # start everything, wait until healthy
make verify    # end-to-end check; ends with VERIFY OK
make test      # static checks and unit tests, no Docker needed
make help      # every target
make down      # remove everything
```

## Walkthrough

1. `make logs` in one terminal.
2. On http://localhost:8081 pick `orders`, key `k1`, value `{"id": 1}`, Send. The page shows partition and offset.
3. One `orders-workers` replica prints it (shared group: load balancing) and `orders-audit` prints it too (own group: fan-out): `partition=0 offset=0 key=k1 value={"id": 1}`.
4. Same key again: same partition, next offset. Other keys spread out; keyless records may cluster (sticky partitioner).
5. Value `{`: the page refuses it before sending.
6. `make scale n=3`: kcat logs a rebalance and each member gets one partition. `make up` resets to 2.
7. `make groups` shows members, partitions and lag.

## Produce from the command line

```sh
make produce value='{"id": 2}' key=k2              # topic defaults to orders
make produce topic=events value='{"type": "ping"}'
curl -X POST 'localhost:8081/api/produce?topic=orders&key=k1' --data-binary '{"id": 1}'
```

Success: `{"topic":"orders","partition":0,"offset":0}`. Failure: non-2xx with `{"error":"..."}` (invalid JSON → 400, unknown topic → 502); `make produce` exits non-zero.

## Add a topic

```yaml
  topic-payments:
    <<: *topic
    environment:
      TOPIC_NAME: payments      # required
      PARTITIONS: 6             # default 1
      REPLICATION_FACTOR: 1     # default 1
```

Also add `topic-payments: {condition: service_completed_successfully}` under `producer.depends_on`. The producer page then lists the topic, and `make up` (`--wait`) accepts the finished job. A job does nothing if its topic exists, so to change `PARTITIONS` later, run `make down` first.

## Add a consumer

```yaml
  payments-worker:
    <<: *consumer
    depends_on:
      <<: *after-kafka
      topic-payments:
        condition: service_completed_successfully
    environment:
      TOPIC_NAME: payments            # required
      GROUP_ID: payments-workers      # default: the container hostname (its own group)
      AUTO_OFFSET_RESET: earliest     # or latest
```

Consumers with the same `GROUP_ID` split the partitions. Without one, each container gets every record. Add members with `deploy.replicas: N` or `docker compose up -d --scale <service>=N`.

## Topic owners

`orders-1`, `orders-1__retry` and `orders-1__dlq` are long-running containers, one per topic (`topic-owner/`, Go). Each one owns its topic:

- It creates the topic. Then, every 10 s, it sets missing or changed configs, removes topic-level overrides nobody asked for, and raises partitions.
- It never lowers partitions or changes the replication factor. Instead, the container turns unhealthy and says why: `docker inspect --format '{{json .State.Health.Log}}' orders-1`, or `make logs svc=orders-1`.
- `make up` waits until every topic is in its desired state, so `depends_on: {orders-1: {condition: service_healthy}}` guarantees a consumer its topic.
- It sets back a config changed by hand (in Console or with `kafka-configs.sh`). To change a config, change the container's environment and recreate it.
- It serves `/metrics` on port 9000 inside the network. Prometheus finds the containers by their labels, with no target list.
- `make owners` lists them, `make query q='kafka_topic_partitions'` asks Prometheus, and `make kcat args='-C -t orders-1 -o beginning -e -J'` runs kcat on the compose network (default `-L`).

The retry container also runs the redelivery worker (below).

### Configuration

| Variable | Default | |
|---|---|---|
| `BASE_NAME`, `INSTANCE`, `ROLE` | required | The topic is `<base>-<instance>`, plus `__retry` for `ROLE: retry` or `__dlq` for `ROLE: dlq`. |
| `KAFKA_BROKERS` | required | `*bootstrap` |
| `PARTITIONS` | `1` | Raised on an existing topic, never lowered. |
| `REPLICATION_FACTOR` | `1` | Anything else is refused: there is one broker. |
| `TOPIC_CONFIG_<NAME>` | role defaults | `TOPIC_CONFIG_RETENTION_MS: 3600000` sets `retention.ms`. An empty value removes a role default. |
| `MAX_ATTEMPTS`, `BACKOFF_MS` | `3`, `5000` | Retry role only, refused on the others: the tries before the DLQ (1–10), and the backoff in ms for a record without `studio-backoff-ms` (100–60000). |

Role defaults: the retry topic gets `message.timestamp.type=LogAppendTime`, so the broker stamps each record. The DLQ gets `retention.ms=-1`, so parked records stay.

A base name has letters, digits, `_` and `-`. It starts with a letter or digit, has no `__`, and does not end in `-<digits>`. `<base>-<instance>` has at most 242 characters. A `.` is not allowed: Kafka treats `a.b` and `a_b` as the same name in its metrics, and every retry and DLQ name contains `_`.

### Add an instance

Copy the three `orders-1` services with new anchors and numbers. Paste them after the `orders-1` block (after the `producer` service): they use the `*bootstrap` anchor defined in `producer`, and above it compose fails on an unknown alias.

```yaml
  orders-2:
    <<: *topic-owner
    container_name: orders-2
    labels: &orders-2-labels
      topic-owner.base: orders
      topic-owner.instance: "2"
      topic-owner.role: main
    environment: &orders-2-env
      KAFKA_BROKERS: *bootstrap
      BASE_NAME: orders
      INSTANCE: 2
      ROLE: main
      PARTITIONS: 6

  orders-2__retry:
    <<: *topic-owner
    container_name: orders-2__retry
    depends_on:
      <<: *after-kafka
      orders-2: {condition: service_healthy}
      orders-2__dlq: {condition: service_healthy}
    labels: {<<: *orders-2-labels, topic-owner.role: retry}
    environment: {<<: *orders-2-env, ROLE: retry, MAX_ATTEMPTS: 5}

  orders-2__dlq:
    <<: *topic-owner
    container_name: orders-2__dlq
    labels: {<<: *orders-2-labels, topic-owner.role: dlq}
    environment: {<<: *orders-2-env, ROLE: dlq}
```

Instances share nothing: `orders-2` has its own topics and settings. Prometheus finds the new containers on its own.

### Retry and the DLQ

A consumer of `orders-1` that fails a record parks it in `orders-1__retry`, then commits the original. It sends the record's own key and value, and the record's own headers with these set:

| Header | Set by | Value | What the worker does with it |
|---|---|---|---|
| `studio-group` | Studio consumers only | the consumer's group | Skips the record: that Studio consumer's own retry loop handles it. If that loop no longer runs (Retry turned off, the flow deleted), the record stays in `orders-1__retry`. Never set it from another client. |
| `studio-attempt` | the publisher, on every failure | failed tries so far: `1`, `2`, … | Missing counts as 1. Not a positive integer: DLQ. At `MAX_ATTEMPTS` or above: DLQ. |
| `studio-backoff-ms` | a publisher other than Studio, on every failure | `0`–`3600000`, counted from the record's time in `orders-1__retry` | Missing: `BACKOFF_MS`. Anything else: DLQ. |
| `studio-error` | the publisher | the error's first line, at most 1 KiB | Replaces it only for a bad header: `retry: bad header studio-backoff-ms "soon"`. |
| `studio-origin` | the first publisher, once | `topic[partition]@offset` where the record was first read | Sets it to the record's place in `orders-1__retry` when missing. |
| `studio-first-failure` | the first publisher, once; Studio does not write it | RFC 3339 UTC, `2026-10-08T14:00:00.000Z` | Sets it to the record's time in `orders-1__retry` when missing. |

A Studio consumer with Retry writes the first four (`studio-group`, `studio-attempt`, `studio-error`, `studio-origin`) to the same topics, so Console, the DLQ and a Studio tail read both alike. Studio never writes `studio-backoff-ms` or `studio-first-failure`: other publishers do.

The retry container runs the redelivery worker, in group `orders-1__redelivery` (franz-go only: never point `kcat -G` at it, and never give a Studio consumer that group). For each record without `studio-group`, in partition order:

- A record whose `studio-attempt` has reached `MAX_ATTEMPTS`, or whose header is bad, goes to `orders-1__dlq` at once, without waiting for its backoff.
- Any other record waits until its time plus its backoff, then goes back to `orders-1`, with its key, value and headers. The retry topic's `LogAppendTime` makes that time the broker's clock.
- A partition is handled in order. While a record waits, the records behind it on its partition wait too, so a long backoff holds up shorter ones, and even a record bound for the DLQ; more partitions on `orders-1__retry` reduce that.
- It commits a record only after the broker acknowledges the send. A crash in between sends it twice: delivery is at-least-once.

What a consumer of `orders-1` sees:

- Every group on `orders-1` gets the redelivered record, including groups that never failed it, such as an `orders-audit`-style fan-out group. A Studio consumer with Retry avoids that with its own loop.
- A redelivered record has `studio-origin`, and `studio-attempt` if its publisher set one; an original has neither. To act on a record once, dedupe on `studio-origin` when present, else on the record's own `topic[partition]@offset`.
- Key order is not kept: the record comes back after records sent while it waited.

`MAX_ATTEMPTS` counts tries, the first included: with 3, a record is tried 3 times. Studio's `attempts` counts retries after the first try, so Studio's `attempts: 3` is `MAX_ATTEMPTS: 4`.

A Studio consumer with Retry that fails a record the worker redelivered parks it again with its own `studio-group` and restarts `studio-attempt` at 1. From then on its own loop retries it, so a record can be tried more than `MAX_ATTEMPTS` times in all.

Park a record by hand, as a failing consumer would:

```sh
echo '{"id":42}' | make kcat args='-P -t orders-1__retry -k order-42 -H studio-attempt=1 -H studio-backoff-ms=5000 -H "studio-error=sink: http 503"'
make kcat args='-C -t orders-1 -o -1 -e -J'   # about 5 s later: order-42, with studio-origin and studio-first-failure added
```

Read headers with `-J`. kcat's `%h` joins them with commas and does not escape a comma inside `studio-error`.

#### Inspect and replay the DLQ

- Console (http://localhost:8080) → Topics → `orders-1__dlq` shows each record with its headers. `make kcat args='-C -t orders-1__dlq -o beginning -e -J'` prints them.
- To replay, copy key and value back to `orders-1`. The headers stay behind, so each record starts over with fresh attempts:

  ```sh
  make kcat args='-C -t orders-1__dlq -o beginning -e -f "%k\t%s\n"' | make kcat args='-P -t orders-1 -K "\t"'
  ```

- Kafka cannot delete one record. After a full replay, move the DLQ's start past what you replayed, with one entry per partition (`-1` is the end):

  ```sh
  docker compose exec -T kafka /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:19092 --offset-json-file /dev/stdin <<'EOF'
  {"partitions":[{"topic":"orders-1__dlq","partition":0,"offset":-1},{"topic":"orders-1__dlq","partition":1,"offset":-1},{"topic":"orders-1__dlq","partition":2,"offset":-1}],"version":1}
  EOF
  ```

### Metrics

Every series has a `topic` label. A series that means what a [kafka-exporter](https://github.com/danielqsj/kafka_exporter) series means has its name and labels, so kafka-exporter queries work.

| Metric | |
|---|---|
| `kafka_topic_partitions` | partitions |
| `kafka_topic_partition_under_replicated_partition` | 1 when a partition has fewer in-sync replicas than replicas (always 0 on one broker) |
| `kafka_topic_partition_current_offset`, `kafka_topic_partition_oldest_offset` | log end and start offsets |
| `kafka_consumergroup_current_offset`, `kafka_consumergroup_lag` | each group's committed offset and lag, for partitions it has committed |
| `topic_owner_partition_log_size_bytes` | log size |
| `topic_owner_info` | `base`, `topic_instance` and `role` (Prometheus reserves `instance`, which is the container name) |
| `topic_owner_reconciled` | 1 when the topic is in its desired state |
| `topic_owner_kafka_up` | 1 when the last scrape's admin calls succeeded (a missing topic shows as `topic_owner_reconciled` 0, not here) |
| `topic_owner_redeliveries_total` | retry role: records sent back to the main topic |
| `topic_owner_dead_lettered_total` | retry role: records moved to the DLQ, by `reason` (`attempts`, `bad_header`) |
| `topic_owner_skipped_total` | retry role: records left to a Studio retry loop |
| `topic_owner_backoff_seconds` | retry role: the backoff each redelivered record asked for (a histogram) |
| `topic_owner_oldest_message_timestamp_seconds` | DLQ role: the time of each non-empty partition's oldest record |

Some queries:

- Messages in per second: `sum by (topic) (rate(kafka_topic_partition_current_offset[1m]))`.
- Records waiting in a retry topic: `sum by (topic) (kafka_consumergroup_lag{consumergroup=~".+__redelivery"})`.
- Records parked in a DLQ: `sum by (topic) (kafka_topic_partition_current_offset{topic=~".+__dlq"} - kafka_topic_partition_oldest_offset{topic=~".+__dlq"})`.
- Age of the oldest parked record, in seconds: `time() - min by (topic) (topic_owner_oldest_message_timestamp_seconds)`.

### Dashboard and alerts

Grafana at http://localhost:3000/d/topic-owners shows the `Topic owners` dashboard; anyone can open it as admin, without logging in. Pick `base` and `topic_instance` at the top (both default to All). It shows:

- the alerts firing now;
- **main:** messages in per second, partitions, log size, lag per group;
- **retry:** waiting, redeliveries per second, moves to the DLQ per second by reason, backoff p50 and p95, skipped per second;
- **dlq:** parked records and the age of the oldest one.

The dashboard is `grafana/dashboards/topic-owners.json`, and its datasource is in `grafana/provisioning/`. Grafana does not save UI edits: edit the JSON, and Grafana reloads it within 10 s.

Prometheus evaluates `prometheus/rules.yml` every 15 s. Firing alerts show at http://localhost:9090/alerts and on the dashboard. There is no Alertmanager, so nothing is sent anywhere.

| Alert | Fires when |
|---|---|
| `TopicConsumerLagHigh` | a group's lag on a main topic stays above 100 for 2 minutes |
| `TopicDLQGrowing` | a record was parked in a DLQ in the last 10 minutes, and the DLQ's owner runs |
| `TopicRetryWaiting` | records wait in a retry topic for 5 minutes: the worker is down, or a Studio retry loop stopped |
| `TopicOwnerUnhealthy` | for 1 minute: a running owner's scrapes fail, its topic is not in its desired state, or an owner seen in the last hour is gone (stopped or crashed). Prometheus labels a `docker compose run` one-off, such as `make verify`'s, `oneoff="true"`; it may go away without an alert |

Bytes in and out per topic are not exported: only the broker's JMX has them, and the JMX agent needs a jar and a change to the `kafka` service.

Prometheus reads the Docker socket with `group_add: ["0"]`, because Docker Desktop shows the socket as `root:root 0660` inside containers; on native Linux, use the host's docker gid. The socket gives root on the host, one more reason everything stays on `127.0.0.1`.

## Pipeline Studio

Open http://localhost:8082. Drag nodes from the palette, wire them, edit the selected node on the right, then Save and Deploy.

Try it: wire a Producer with source `timer` to a Topic and a Consumer, then Save and Deploy. The numbers on every node update once a second. Select the consumer to see its tail. Stop removes the containers. `flows/0a1b2c3d.json` is a larger example: a timer into `orders`, read by a consumer with two instances that forwards to `orders-archive`.

### Flows

- Allowed edges: Producer → Topic, Topic → Consumer, and from a Consumer: → Topic (a forward), → Transform → Topic, → Router → Topics, or → Transform → Router → Topics. The editor refuses other edges, and the server rejects them on save.
- Each flow is a file, `flows/<id>.json`: React Flow's nodes and edges, plus `id`, `name` and `viewport`. You can edit, copy or commit these files. The studio logs and skips a file that does not parse. It fills in missing node fields with their defaults when you open a flow; the next save writes them.
- Deploy refuses (422, naming the node) a forward that loops back to a topic it reads, and node ids whose container names clash (a consumer `consumer-1` with two instances next to a node `consumer-1-2`).

### Nodes

#### Producer

- Sends by hand (Send in its tail, or the webhook `curl` line in the Inspector) or on a timer (`interval_ms`, at least 10).
- Key and value are templates with `{{.Seq}}`, `{{.Now}}` and `{{.Rand}}`.
- `curl -X POST 'localhost:8082/api/flows/<id>/nodes/producer-1/send?key=k1' --data '{"id": 1}'` sends that body as the value.
- A template that fails, or renders no JSON, is a producer error: counted, logged by a timer, and a 500 from `send`.

#### Topic

- Created on deploy. A topic that already exists is used as it is, with a warning if its partition count differs.

#### Consumer

- Settings: a group, `earliest` or `latest`, a sink, and an optional forward to a topic (with the same key).
- The sink is `log` (its tail and `docker logs`) or `http` (POST each value as JSON within 5 s; any answer other than 2xx is an error).
- Sink and forward are independent. Without a DLQ, a failure is counted and logged, not retried, and the record still commits. So a failed forward is lost (at-most-once).
- `instances` (1–10) runs that many containers in the group. They split the partitions.
- A consumer that stops without leaving its group (killed, or Docker restarted) stays in the group for 10 s. A redeploy right after it can wait that long.
- Studio consumers (franz-go) cannot share a group with the compose kcat consumers (librdkafka). Their assignors differ, so the broker refuses the join with `INCONSISTENT_GROUP_PROTOCOL`, shown as the node's errors. Give them their own groups.

#### Transform

- An [expr-lang](https://expr-lang.org) expression over `msg`, the record's value decoded from JSON, e.g. `{id: msg.id, total: msg.qty * msg.price}`.
- Runs in its consumer's containers, after the sink and before the forward. Its result is forwarded with the record's key. `nil` drops the record, so it can filter: `msg.qty > 0 ? msg : nil`.
- Deploy refuses an expression that does not compile (422, naming the node).
- A value that is not JSON, a failing expression (a missing field gives `invalid operation: <nil> * <nil>`), or a result that JSON cannot hold is an error on the Transform node and on its consumer (`transform: …`). That record is not forwarded.
- Integers pass through exactly, also above 2^53. Other numbers become float64.
- The node shows its consumer's state and its own counts. A Transform added after deploy shows `missing` until a container reports it.

#### Router

- Ordered rules. Each rule is a condition over `msg` that yields true or false (e.g. `msg.total > 100`) and a topic the router is wired to. A default topic is optional.
- Runs in its consumer's containers, after the Transform if there is one. A record goes, with its key, to the topic of the first rule that holds, else to the default. Without a default, the record is dropped and counted as `unmatched`.
- Wiring the router to a topic adds a rule for it to fill in, unless a rule or the default already sends there. So an edge that you delete and draw again finds its rule.
- Deploy refuses (422, naming the node and the rule): an empty condition, one that does not compile or cannot be a boolean, a rule or default whose topic has no edge, and an edge that no rule or default uses.
- A value that is not JSON, or a condition that fails (`rule 1: invalid operation: <nil> > int`), is an error on the Router node and on its consumer (`router: …`). That record is not forwarded, not even to the default.
- Its edges show `#1`, `#2` and `default`, with each one's count while the flow runs (`#1 · 80`).

#### Retry and DLQ

Set these in a consumer's "On failure" group in the Inspector.

- The topics take their names from the topic the consumer reads: `orders-1` gives `orders-1__retry` and `orders-1__dlq`. Deploy creates them with the same number of partitions.
- With **DLQ** on, a record's first failure ends its path:
  - A failure that may pass later (the http sink, the forward) sends the record to the retry topic if **Retry** is on and tries remain, else to the DLQ.
  - A transform or router failure sends it straight to the DLQ.
- The record goes as read (key and value), with headers `studio-group`, `studio-attempt` (failed tries so far), `studio-error` and `studio-origin` (`topic[partition]@offset`).
- A second client in the same container, in group `<group>__retry`, reads the retry topic. It waits until each record is `delay_ms` (100–60000) old, then runs it through the whole path again, for up to `attempts` (1–10) retries after the first try. It skips records of other groups.
- A retry runs the sink again, even if the sink succeeded before: delivery is at-least-once.
- Deploy refuses Retry without DLQ.
- The consumer's line shows `retried` and `dlq` (from 0 when they are on) and `waiting` (records still in the retry topic).
- To watch the DLQ, draw a topic named `<input>__dlq` and wire a log consumer to it. Its tail shows each record's headers.
- Records can still be lost or left behind:
  - If the write to the retry topic or the DLQ fails (other than at Stop), the record is counted, logged and committed: it is lost.
  - If you turn Retry off or rename the group, records still in `<input>__retry` stay there, because no loop reads them. Draw that topic to see them.

#### Flows that feed each other

- Point a consumer's http sink at another flow's producer `send` URL. Node containers reach the studio as `http://studio:8082`.

### Running

- Deploy starts one container per producer and per consumer instance, named `studio-<flow>-<node>` (`…-<i>` for an instance) and labelled `studio.flow`. `make nodes` lists them. `docker logs`, `docker stop` and `docker rm -f` work on them; the node then shows `exited` or `missing`.
- Stop removes the containers. Topics and committed offsets stay, so a redeployed consumer continues where its group stopped.
- Rewind: a stopped flow's consumer can rewind its group from the Inspector ("Rewind group": to earliest or to latest). The next deploy then reads from the start of the topic, or only new records, whatever `auto.offset.reset` says.
  - API: `curl -X POST localhost:8082/api/flows/<id>/nodes/consumer-1/rewind --data '{"to":"earliest"}'`.
  - It answers 409 while the flow runs (the broker cannot move a group that has members) and when the topic was never deployed.
  - The Inspector's buttons wait until you save, because a rewind uses the saved group and topic.
- Live numbers: `GET /api/flows/<id>/events` (server-sent events) sends a snapshot once a second:
  - every node: record count, rate and errors (the last error as a tooltip);
  - a consumer: its lag and the partitions it holds (per instance; `2/3 running` when some are down);
  - a topic: its partitions and end offset.
  - `GET /api/flows/<id>/state` returns the same snapshot without rates. The flow list refreshes every 5 s.
- If the stream reports a problem or drops (for example, the studio restarts), the numbers grey out and the top bar says `live numbers paused: <reason>` until the next tick. A flow deleted elsewhere closes and says so.
- A node container that does not answer `/stats` shows the reason instead of its numbers, once the container is 5 s old.
- Lag shows once the group has committed. Before that (for example, a `latest` consumer before its first record) there is none. After that, a partition without a commit counts from where `auto_offset_reset` starts.
- The tail shows a node's last 100 records. It fetches when a newer record arrives (the snapshot's `tailSeq` moves), follows the newest record only while you are scrolled to the bottom, and starts over when the container restarts. A consumer with instances has an instance picker (`…/tail?instance=<i>`).
- A consumer commits only the records it handled. On Stop it finishes the batch in hand (up to 3 s), so it skips nothing (at-least-once). `docker rm -f` skips that step, and the uncommitted records come again on the next deploy.
- Node containers outlive the studio: `docker compose restart studio` keeps flows running. They are not compose services, so stop the stack with `make down`, which removes them first.

### API, security, development

- The API is under `/api` and always answers JSON: an unknown path is 404, a wrong method is 405, and errors are `{"error": "..."}`.
- Browsers can write only from the studio's own page: a cross-site POST, PUT or DELETE gets 403. curl and the node containers are not affected.
- `GET /api/health` reports the Docker Engine version (through the mounted `/var/run/docker.sock`) and whether the broker answers. That socket gives root access to the host, which is one more reason the studio stays on `127.0.0.1`.
- Native Linux: the setup assumes Docker Desktop. On Linux the socket is `root:docker 0660`, so `/api/health` stays 503 and `make up` fails. Add `user: "0"` to the `studio` service, or `group_add` the host's docker gid and make `./flows` writable by that user.
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and sends `/api` to the running `studio` container. For Go changes, run `make up` (it rebuilds the image). `make test` runs the static checks and unit tests.
- UI tests: `make verify-ui` runs Playwright in Chromium against the running stack: the editor, the node types, and the live view through a studio restart. `make verify` runs them too. The first run downloads Chromium (about 150 MB). A failed test leaves a trace and a screenshot in `studio/ui/test-results/` (open one with `npx playwright show-trace <file>`).

## Connect from the host

Any client: bootstrap `localhost:9092`, e.g. `kcat -L -b localhost:9092`.

## License

MIT, see [LICENSE](LICENSE).
