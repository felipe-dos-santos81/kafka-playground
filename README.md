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
make up        # docker compose up -d, then waits (up to 60 s) until Console is healthy and every consumer has partitions
make ps
```

`docker compose up -d` alone also works; `make up` additionally waits until the stack is usable. Open http://localhost:8080. `make help` lists every target.

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
      kafka:
        condition: service_healthy
      topic-payments:
        condition: service_completed_successfully
    environment:
      TOPIC_NAME: payments            # required
      GROUP_ID: payments-workers      # optional; default: unique per container
      AUTO_OFFSET_RESET: earliest     # optional; earliest (default) or latest
```

Both dependencies are needed: overriding `depends_on` replaces the one inherited from `*consumer`, so the broker dependency must be repeated.

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

```json
{"records":[{"topicName":"orders","partitionId":2,"offset":1}]}
```

The HTTP status is 200 even when a record fails; check each record's `error` field. For a missing topic:

```json
{"records":[{"topicName":"nope","partitionId":-1,"offset":0,"error":"UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition."}]}
```

## Connect from the host

Any Kafka client on your machine: bootstrap server `localhost:9092`, no auth, no TLS.

```sh
kcat -L -b localhost:9092                                                                 # if kcat is installed
docker run --rm --entrypoint kcat confluentinc/cp-kcat:8.2.4 -L -b host.docker.internal:9092   # otherwise
```

## Reset

```sh
make down      # docker compose down: removes containers; topics and messages are gone
```

## Verify end to end

```sh
make verify
```

Produces a unique record and asserts that the worker group logged it exactly once and the audit group exactly once.
