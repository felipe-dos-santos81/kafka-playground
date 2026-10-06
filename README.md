# Kafka playground

A local Kafka sandbox for watching partitions, consumer groups and fan-out while you produce JSON from a browser. No auth, no TLS, nothing persisted; everything binds to `127.0.0.1`.

| Service | What it is | Where |
|---|---|---|
| `kafka` | single-node KRaft broker (`apache/kafka:4.3.1`) | `localhost:9092` (host), `kafka:19092` (containers) |
| `topic-orders`, `topic-events` | one-shot jobs that create a topic and exit 0 | `make ps` |
| `orders-workers` ×2 | kcat consumers sharing group `orders-workers` on `orders` (3 partitions) | `make logs svc=orders-workers` |
| `orders-audit` | kcat consumer in its own group on `orders` | `make logs svc=orders-audit` |
| `producer` | producer page built from `producer/` (Go) | http://localhost:8081 |
| `console` | Redpanda Console, for browsing topics, messages and groups | http://localhost:8080 |

## Quick start

```sh
make up        # = docker compose up -d --wait
make verify    # produce one record, assert each consumer group got it exactly once
make help      # every target
make down      # remove everything
```

## Walkthrough

1. `make logs` in one terminal.
2. On http://localhost:8081 pick `orders`, key `k1`, value `{"id": 1}`, Send. The page shows the partition and offset.
3. The record is printed once by one `orders-workers` replica (shared group: load balancing) and once by `orders-audit` (own group: fan-out), as `partition=0 offset=0 key=k1 value={"id": 1}`.
4. Same key again: same partition, next offset. Other keys spread across partitions; keyless records may cluster in one partition (sticky partitioner).
5. Value `{`: the page refuses it before sending.
6. `make scale n=3`: kcat logs `% Group ... rebalanced ... assigned: ...` and each member gets one partition. `make up` resets to 2.
7. `make groups` shows members, assigned partitions and lag.

## Produce from the command line

```sh
make produce value='{"id": 2}' key=k2              # topic defaults to orders
make produce topic=events value='{"type": "ping"}'
curl -X POST 'localhost:8081/api/produce?topic=orders&key=k1' --data-binary '{"id": 1}'
```

Success: `{"topic":"orders","partition":0,"offset":0}`. Failure: non-2xx and `{"error":"..."}` (invalid JSON → 400, unknown topic → 502); `make produce` then exits non-zero.

## Add a topic

```yaml
  topic-payments:
    <<: *topic
    environment:
      TOPIC_NAME: payments      # required
      PARTITIONS: 6             # default 1
      REPLICATION_FACTOR: 1     # default 1
```

Also add `topic-payments: {condition: service_completed_successfully}` under `producer.depends_on`: the dropdown then lists it at first load, and `--wait` accepts the finished job. Re-running a job is a no-op if the topic exists, so changing `PARTITIONS` later needs `make down`.

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

Consumers sharing a `GROUP_ID` split the partitions; without one, each container gets every record. Add members with `deploy.replicas: N` or `docker compose up -d --scale <service>=N`.

## Connect from the host

Any client: bootstrap `localhost:9092`, e.g. `kcat -L -b localhost:9092`.

## License

MIT, see [LICENSE](LICENSE).
