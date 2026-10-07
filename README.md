# Kafka playground

A local Kafka sandbox: produce JSON from a browser and watch partitions, consumer groups and fan-out. Pipeline Studio adds a canvas for drawing producer → topic → consumer flows. No auth, no TLS, everything on `127.0.0.1`; Kafka data is lost on `make down`, Studio flows are files in `flows/`.

| Service | What it is | Where |
|---|---|---|
| `kafka` | single-node KRaft broker (`apache/kafka:4.3.1`) | `localhost:9092` (host), `kafka:19092` (containers) |
| `topic-orders`, `topic-events` | one-shot jobs that create a topic and exit | `make ps` |
| `orders-workers` ×2 | kcat consumers sharing group `orders-workers` on `orders` (3 partitions) | `make logs svc=orders-workers` |
| `orders-audit` | kcat consumer in its own group on `orders` | `make logs svc=orders-audit` |
| `producer` | producer page (`producer/`, Go) | http://localhost:8081 |
| `studio` | Pipeline Studio (`studio/`, Go + React Flow) | http://localhost:8082 |
| `console` | Redpanda Console: topics, messages, groups | http://localhost:8080 |

## Quick start

```sh
make up        # start everything, wait until healthy
make verify    # end-to-end check; ends with VERIFY OK
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

Also add `topic-payments: {condition: service_completed_successfully}` under `producer.depends_on`, so the dropdown lists it and `--wait` accepts the finished job. Re-running a job is a no-op if the topic exists; changing `PARTITIONS` later needs `make down`.

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

## Pipeline Studio

On http://localhost:8082: drag Producer, Topic and Consumer nodes from the palette, wire them, edit the selected node on the right, Save.

- Allowed edges: Producer → Topic, Topic → Consumer, Consumer → Topic. Consumer → Transform → Topic is reserved for a later milestone: a Transform node in a flow file shows and edits, but the palette doesn't offer it yet. The editor refuses other wires; the server rejects them on save.
- Opening a flow file that lacks some node fields fills in the defaults and marks the flow unsaved; the file changes only when you Save.
- Each flow is `flows/<id>.json`: React Flow's nodes and edges plus `id`, `name` and `viewport`. Edit, copy or commit them; a file that does not parse is skipped and logged. `flows/0a1b2c3d.json` is an example.
- Deploy is not built yet. `GET /api/health` reports the Docker Engine version reachable through the mounted `/var/run/docker.sock`; deploy will run each node as a container. That socket is root-equivalent on the host, another reason the studio stays on `127.0.0.1`.
- Native Linux: the setup assumes Docker Desktop. There the socket is `root:docker 0660`, so `/api/health` stays 503 and `make up` fails. Add `user: "0"` to the `studio` service (the socket already grants root), or `group_add` the host's docker gid and make `./flows` writable by that user.
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the running `studio` container. Go changes need `make up` (rebuilds the image).

## Connect from the host

Any client: bootstrap `localhost:9092`, e.g. `kcat -L -b localhost:9092`.

## License

MIT, see [LICENSE](LICENSE).
