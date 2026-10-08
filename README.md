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

On http://localhost:8082: drag nodes from the palette, wire them, edit the selected one on the right, Save, Deploy.

Try it: a Producer with source `timer`, a Topic and a Consumer; wire them, Save, Deploy. Every node's numbers move once a second; select the consumer to watch its tail. Stop removes the containers. `flows/0a1b2c3d.json` is a bigger example: a timer into `orders`, read by a two-instance consumer that forwards to `orders-archive`.

### Flows

- Edges: Producer → Topic, Topic → Consumer, then Consumer → Topic (a forward), Consumer → Transform → Topic, Consumer → Router → Topics, or Consumer → Transform → Router → Topics. The editor refuses other wires and the server rejects them on save.
- Each flow is a file, `flows/<id>.json`: React Flow's nodes and edges plus `id`, `name` and `viewport`. Edit, copy or commit them; a file that doesn't parse is skipped and logged, and missing node fields are filled in on open (the flow shows unsaved).
- A deploy also refuses forwards that loop back to a topic they read (records would circulate forever) and node ids whose container names would clash (a consumer `consumer-1` with two instances next to a node `consumer-1-2`), naming the node (422).

### Nodes

- **Producer**: sends by hand (Send in its tail, or the webhook `curl` line in the Inspector) or on a timer (`interval_ms`, at least 10). Key and value are templates with `{{.Seq}}`, `{{.Now}}` and `{{.Rand}}`. `curl -X POST 'localhost:8082/api/flows/<id>/nodes/producer-1/send?key=k1' --data '{"id": 1}'` sends that body as the value. Templates that fail or render no JSON are the producer's error: counted, logged by a timer, and a 500 for a template `send`.
- **Topic**: created on deploy; one that exists is used as it is, with a warning when its partition count differs.
- **Consumer**: a group, `earliest` or `latest`, a sink, and optionally a forward to a topic with the same key. The sink is `log` (its tail and `docker logs`) or `http` (POST each value as JSON within 5 s; any answer but 2xx is an error). Sink and forward are independent; a failure is counted and logged, not retried, and the record still commits, so a failed forward is lost (at-most-once). `instances` (1–10) runs that many containers in the group, splitting the partitions.
- **Transform**: an [expr-lang](https://expr-lang.org) expression over `msg`, the record's value decoded from JSON, e.g. `{id: msg.id, total: msg.qty * msg.price}`. It runs in its consumer's containers, after the sink and before the forward: its result is forwarded with the record's key, and `nil` drops the record (a filter: `msg.qty > 0 ? msg : nil`). A deploy refuses an expression that does not compile (422, naming the node). A value that is not JSON, a failing expression (a missing field gives `invalid operation: <nil> * <nil>`) or a result JSON cannot hold is counted on the Transform node and as an error of its consumer (`transform: …`), and that record is not forwarded. Integers pass through exact (an id above 2^53 too); other numbers are float64. The node shows its consumer's state (`missing` until a container reports it, for a Transform added after deploy) and its own counts.
- **Router**: ordered rules, each a condition over `msg` (an expr-lang expression that yields true or false, e.g. `msg.total > 100`) and one of the topics the router is wired to, plus an optional default topic. It runs in its consumer's containers, after the Transform if there is one: a record goes, with its key, to the topic of the first rule that holds, else to the default; with no default it is dropped and counted as `unmatched`. Wiring the router to a topic adds a rule for it, to fill in, unless a rule or the default already sends there (so an edge deleted and drawn again finds its rule). A deploy refuses a condition that is empty, does not compile or cannot be a boolean, a rule or default whose topic the router has no edge to, and an edge no rule or default uses (422, naming the node and the rule). A value that is not JSON or a condition that fails (`rule 1: invalid operation: <nil> > int`) is counted on the Router node and as an error of its consumer (`router: …`), and that record is not forwarded, not even to the default. Its edges are labelled `#1`, `#2`, `default`, with each one's count while the flow runs (`#1 · 80`).
- Flows feed each other through an http sink pointed at another flow's producer `send` URL; node containers reach the studio as `http://studio:8082`.
- Studio consumers (franz-go) cannot share a group with the compose kcat consumers (librdkafka): their assignors differ, so the broker refuses the join with `INCONSISTENT_GROUP_PROTOCOL`, shown as the node's errors. Give them their own groups.

### Running

- Deploy starts one container per producer and consumer (one per instance), named `studio-<flow>-<node>` (`…-<i>` for an instance) and labelled `studio.flow`. `make nodes` lists them; `docker logs`, `docker stop` and `docker rm -f` work on them, and the node turns `exited` or `missing`. Stop removes them; topics and committed offsets stay, so a redeployed consumer carries on where its group left off.
- A stopped flow's consumer can rewind its group from the Inspector ("Rewind group": to earliest or to latest): the next deploy reads the topic from its start, or only what arrives after, whatever `auto.offset.reset` says. `curl -X POST localhost:8082/api/flows/<id>/nodes/consumer-1/rewind --data '{"to":"earliest"}'` does the same. A running flow answers 409 (the broker refuses to move a group with members), and so does one whose topic was never deployed. The Inspector's buttons also wait for unsaved edits to be saved, since a rewind uses the saved group and topic.
- Live numbers stream once a second from `GET /api/flows/<id>/events` (server-sent events): record count, rate and errors (the last error as a tooltip); a consumer's lag and the partitions it holds (per instance, with `2/3 running` when some are down); a topic's partitions and end offset. `GET /api/flows/<id>/state` returns the same snapshot without rates. The flow list refreshes every 5 s.
- When the stream reports a problem or drops (the studio restarting), the numbers grey out and the top bar says `live numbers paused: <reason>` until the next tick. A flow deleted elsewhere closes, saying so. A node whose container does not answer `/stats` shows why instead of its numbers, once the container is 5 s old. A consumer's lag shows once its group has committed: until then (a `latest` consumer before its first record) there is none, and after it a partition with no commit yet counts from where `auto_offset_reset` starts.
- The tail shows a node's last 100 records: it fetches when a newer one arrives (the snapshot's `tailSeq` moves), keeps the newest in view only while scrolled to the bottom, and starts over when the container restarts; a consumer with instances has an instance picker (`…/tail?instance=<i>`).
- A consumer commits only the records it handled. On Stop it finishes the batch in hand (up to 3 s), so nothing is skipped (at-least-once); `docker rm -f` skips that, and its uncommitted records come again on the next deploy.
- Node containers outlive the studio: `docker compose restart studio` keeps flows running. They are not compose services, so stop the stack with `make down`, which removes them first.

### API, security, development

- The API lives under `/api` and always answers JSON: an unknown path is 404, a wrong method 405, errors are `{"error": "..."}`. Browsers may only write from the studio's own page: a cross-site POST, PUT or DELETE gets 403. curl and the node containers are not affected.
- `GET /api/health` reports the Docker Engine version, reached through the mounted `/var/run/docker.sock`, and whether the broker answers. That socket is root-equivalent on the host, another reason the studio stays on `127.0.0.1`.
- Native Linux: the setup assumes Docker Desktop. On Linux the socket is `root:docker 0660`, so `/api/health` stays 503 and `make up` fails: add `user: "0"` to the `studio` service, or `group_add` the host's docker gid and make `./flows` writable by that user.
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the running `studio` container. Go changes need `make up` (it rebuilds the image). `make test` runs the static checks and unit tests.
- UI tests: `make verify-ui` drives the studio's page in Chromium (Playwright) against the running stack: building a flow in the editor, the node types, and the live view through a studio restart. `make verify` runs it too. The first run downloads Chromium (about 150 MB); a failed test leaves a trace and a screenshot in `studio/ui/test-results/` (`npx playwright show-trace <file>`).

## Connect from the host

Any client: bootstrap `localhost:9092`, e.g. `kcat -L -b localhost:9092`.

## License

MIT, see [LICENSE](LICENSE).
