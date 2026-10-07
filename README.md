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

On http://localhost:8082: drag Producer, Topic and Consumer nodes from the palette, wire them, edit the selected node on the right, Save, Deploy.

Try it: a Producer with source `timer`, a Topic, a Consumer; wire them, Save, Deploy. Every node's numbers move once a second; select the consumer to watch its tail. Stop removes the containers.

- Allowed edges: Producer → Topic, Topic → Consumer, Consumer → Topic. Consumer → Transform → Topic is reserved for a later milestone: a Transform node in a flow file shows and edits, but the palette doesn't offer it yet. The editor refuses other wires; the server rejects them on save.
- Opening a flow file that lacks some node fields fills in the defaults and marks the flow unsaved; the file changes only when you Save.
- Each flow is `flows/<id>.json`: React Flow's nodes and edges (each edge with a unique `id`) plus `id`, `name` and `viewport`. Edit, copy or commit them; a file that does not parse is skipped and logged. `flows/0a1b2c3d.json` is an example: a timer producing to `orders` once a second, read by a two-instance consumer that forwards to `orders-archive`. The flow list shows each flow's state, re-read every 5 s.
- **Deploy** runs the saved flow: it creates the topics (a topic that exists is used as it is) and starts one container per producer and consumer, named `studio-<flow>-<node>` and labelled `studio.flow`. `make nodes` lists them; `docker logs`, `docker stop` and `docker rm -f` work on them, and the node's badge turns `exited` or `missing`. **Stop** removes them. Topics and committed offsets stay, so a redeployed consumer carries on where its group left off.
- Select a deployed producer or consumer to open its tail: its last 100 records, fetched whenever the node's count moves and started over when its container restarts. A producer's tail has **Send**, which renders its key and value templates. From the command line, `curl -X POST 'localhost:8082/api/flows/<id>/nodes/producer-1/send?key=k1' --data '{"id": 1}'` sends that JSON body as the value.
- Producers send by hand or on a timer (`interval_ms`, at least 10, rendering the key and value templates with `{{.Seq}}`, `{{.Now}}` and `{{.Rand}}`). The Inspector shows a producer's webhook: a `curl` line for `…/nodes/<node>/send`. Node containers reach the studio as `http://studio:8082`.
- A consumer's sink is `log` (its tail and `docker logs`) or `http`, which POSTs each value as JSON within 5 s; any answer but 2xx counts as an error. Wire a consumer to a topic and it forwards every record there with the same key. A failed sink or forward is counted and logged, not retried, and the offset still commits: that record is not forwarded (at-most-once). A deploy refuses a flow whose forwards loop back to a topic they read from (directly or through other consumers), since records would circulate forever.
- `instances` (1–10) runs that many containers of a consumer, `studio-<flow>-<node>-<i>`, in its group; they split the topic's partitions. The node shows how many run (`2/3 running`) and which partitions each holds; the tail drawer picks an instance (`…/tail?instance=<i>`). A deploy refuses node ids whose container names would clash (a consumer `consumer-1` with two instances next to a node `consumer-1-2`), naming the node (422).
- Flows feed each other through an http sink pointed at another flow's producer `send` URL, as `make verify` does with two flows. A deploy still refuses transforms (M5), naming the node.
- Studio consumers (franz-go, cooperative-sticky assignor) cannot share a group with the compose kcat consumers (librdkafka, range/roundrobin): the broker refuses the join with `INCONSISTENT_GROUP_PROTOCOL`, which shows as the node's errors. Give studio consumers their own groups.
- While a flow runs, every node shows live numbers, streamed once a second from `GET /api/flows/<id>/events` (server-sent events): producers and consumers their record count, rate and errors (the last error as a tooltip); consumers their group's lag and the partitions they hold; topics their partition count and end offset, with a warning when an existing topic has a different partition count from the flow's. `GET /api/flows/<id>/state` returns the same snapshot without rates.
- Node containers outlive the studio: `docker compose restart studio` keeps flows running. They are not compose services, so stop the stack with `make down`, which removes them first; `docker compose down` alone cannot remove the network while they are attached.
- A consumer commits only the records it handled. On Stop it finishes the batch in hand (up to 3 s) and commits those, so nothing is skipped (at-least-once). `docker rm -f` skips that, so its uncommitted records are delivered again on the next deploy.
- The API lives under `/api` and always answers JSON: an unknown path is 404, a wrong method 405, errors are `{"error": "..."}`. Browsers may only write from the studio's own page: a cross-site POST, PUT or DELETE gets 403. curl and the node containers are not affected.
- `GET /api/health` reports the Docker Engine version reachable through the mounted `/var/run/docker.sock` and whether the broker answers. That socket is root-equivalent on the host, another reason the studio stays on `127.0.0.1`.
- Native Linux: the setup assumes Docker Desktop. There the socket is `root:docker 0660`, so `/api/health` stays 503 and `make up` fails. Add `user: "0"` to the `studio` service (the socket already grants root), or `group_add` the host's docker gid and make `./flows` writable by that user.
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the running `studio` container. Go changes need `make up` (rebuilds the image).

## Connect from the host

Any client: bootstrap `localhost:9092`, e.g. `kcat -L -b localhost:9092`.

## License

MIT, see [LICENSE](LICENSE).
