# AGENTS.md

Local Kafka playground, run with Docker Compose. Usage is in README.md. Pipeline Studio's design is `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` (the authority; §7 lists the milestones), with one implementation plan per milestone in `docs/superpowers/plans/`. Its UI tests are designed in `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`. The topic owners are designed in `docs/superpowers/specs/2026-10-08-topic-containers-design.md`, with one plan per milestone.

## Layout

- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console, topic owners (`x-topic-owner`, one block of three services per instance), Prometheus, Grafana.
- `producer/`: producer page. Go (franz-go), `main.go` plus embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
- `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `stream.go`, `docker.go`, `kafka.go`, `transform.go`, `router.go`) and node runner (`node.go`, with its retry loop and failure path in `retry.go`, run as `studio node` in one container per producer, consumer and consumer instance), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
- `studio/ui/e2e/`: the UI tests (Playwright, `playwright.config.ts` beside them): `flows.ts` (the API and flow parts), `locators.ts`, `studio.ts` (the fixture), and one `*.spec.ts` per area. `make verify-ui` runs them against the running stack.
- `topic-owner/`: topic owner. Go (franz-go, client_golang): `config.go` (environment, name rules, role defaults), `reconcile.go` (the desired-state diff and its loop), `metrics.go` (`/metrics`), `redeliver.go` (the retry role's redelivery worker), `main.go` (`/healthz`, `-healthcheck`); multi-stage `Dockerfile` onto `scratch`. It runs as one container per topic.
- `prometheus/`: `prometheus.yml` and `rules.yml` (the alert rules), bind-mounted read-only into the `prometheus` service.
- `grafana/`: `provisioning/` (the Prometheus datasource and the dashboard provider) and `dashboards/topic-owners.json`, bind-mounted read-only into the `grafana` service.
- `flows/`: Studio flow files, bind-mounted into the `studio` container; `flows/0a1b2c3d.json` is the example.
- `Makefile`: day-to-day commands; `make test` runs the static checks and unit tests, `make verify` the end-to-end test (`verify-studio` deletes its flows and its `studio-verify…` topics when it exits, and `verify-topics` its `owner-verify…` containers, topics and group; name a new test topic or group in that trap too).

## Check your change

```sh
make test                  # go vet, gofmt, go test, UI build (tsc), UI tests type-check, compose config
make down && make verify   # always: ends with STUDIO OK, UI OK, TOPICS OK and VERIFY OK (the first run downloads Chromium)
make down                  # then `docker ps -aq -f label=studio.flow`, `docker ps -aq -f label=topic-owner.role`, `docker volume ls -q -f label=com.docker.compose.project=kafka-playground` and `git status --short flows` print nothing
```

## Rules

- Pin every image to an exact version; never `latest`. The producer and studio images are local: keep `pull_policy: build`.
- Keep it local-only: no auth, no TLS, ports bound to `127.0.0.1`, Console analytics off.
- Prefer existing images and config over new code.
- Inside compose `command`/`entrypoint` strings, write a shell `$` as `$$`.
- YAML merge (`<<:`) is shallow: a service that overrides `depends_on` must merge `*after-kafka` back in.
- Every new topic job also goes under `producer.depends_on`, or `docker compose up --wait` fails on the exited job.
- `studio/flow.go` defines node ids, node data fields and the allowed-edge table; `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` mirror them. Change all three together. A new data field gets its default in `schema.ts`'s `defaultData`; `fillDefaults` adds it to older files when they open, and that fill is not an unsaved edit. Every other validation rule lives only in `flow.go`: the server is the authority.
- The live snapshot's JSON is `NodeState` in `studio/engine.go`, mirrored by `NodeRuntime` in `studio/ui/src/flow/api.ts`; change both. A consumer with instances lists them in `instances`; code walks a node's containers through `NodeState.containers()` (UI: `containersOf`) instead of branching on single vs instances. `verify-studio` greps that JSON and relies on its field order (`lag` before `assigned`, `instances` last, node ids sorted).
- A rewind's answer is `Rewound` in `studio/engine.go`, and its direction `RewindTo` in `studio/flow.go` (with the rest of the validation, in `rewindTarget`); `studio/ui/src/flow/api.ts` mirrors both. Change them together.
- A node container is named by `nodeRef` (`studio/resolve.go`): `studio-<flow>-<node>`, plus `-<i>` for a consumer instance. Its stop budgets in `node.go` derive from `stopGraceSeconds` (`docker.go`), the grace `make down` and Stop give it; keep them inside it.
- A transform has no container: `Resolve` gives its expression and node id to the upstream consumer's `NodeSpec`, the consumer reports its counts in `/stats` `step`, and `applySteps` puts them on the Transform node. Expressions compile in `Validate` (deploy) and again in the consumer, through `compileTransform` (`transform.go`). A router is the same: its rules go to the consumer's `NodeSpec` (`Routes`, `RouteDefault`, `RouterNode`), its counts arrive in `/stats` `route`, and `applySteps` puts them on the Router node; conditions compile through `compileRule` (`router.go`).
- Retry and DLQ are consumer settings, not nodes: `ConsumerData.Retry`/`DLQ` (`flow.go`) are copied to the consumer's `NodeSpec.Retry`/`DLQ` by `Resolve`; code derives the names it needs, `<input>__retry`, `<input>__dlq` and group `<group>__retry` (`retryTopic`, `dlqTopic`, `retryGroup`). The retry loop is a second client in the consumer's container, polled by the same `consumer.poll` as the main loop but taking records with `retryRecord` (`retry.go`). Each loop tells `handle` where its record came from (`retried`). Both call `handle` at once, so the transform and the router keep no shared VM (`expr.Run` per record); the `studio-group` header keeps a shared retry topic's groups apart. Its counts arrive in `/stats` `retried`/`dlq`, reported only by a consumer running with retry or a DLQ; `waiting` is the retry group's lag (`groupLag`).
- Studio consumers need their own groups: franz-go and the kcat consumers' librdkafka share no assignor, so the broker refuses a mixed group.
- One `.gitignore`, at the root; don't add nested ones (a nested `dist` rule would hide `studio/ui/dist/.gitkeep`).
- The UI tests (`studio/ui/e2e/`, Playwright, `make verify-ui`) find elements by role, label and text, and by `data-testid` where there is none (`node-<id>`, `runtime-<id>`, `tail`). A change to the UI's visible text, roles or those ids runs `make verify-ui`; the top-bar messages they check are quoted in the Studio spec or the UI tests spec (§5), so rewording one goes through the spec. Everything they make is named `studio-ui-…`, a namespace the suite owns: each test removes every such flow, and the run removes every such topic and group after the last test. Don't give anything else that prefix.
- Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). So does a topic owner started with `docker compose run` (labels `topic-owner.role` and `com.docker.compose.oneoff=True`), which `docker compose down` leaves behind. Keep that line in `down`; use `make nodes` to see them. `down` passes `-v`: the `kafka` and `prometheus` images declare anonymous volumes that would otherwise stay.
- A topic owner's service, `container_name` and topic are the same string: `<base>-<instance>`, plus `__retry` or `__dlq` (the suffixes Studio uses). The name rules live in `topic-owner/config.go`, on top of Studio's `topicNameRe` and `maxInputTopic`. Its labels are `topic-owner.base`, `topic-owner.instance` and `topic-owner.role`, never `studio.flow`. Prometheus finds owners by `topic-owner.role`, so a new instance needs no Prometheus change: add one block of three services with their own anchors (README, "Add an instance").
- The redelivery worker's header contract is Studio's (`studio/retry.go`) plus `studio-backoff-ms` and `studio-first-failure` (`topic-owner/redeliver.go`). A change to a header's name or format changes both files, the README table and both specs together. The worker owns the records without `studio-group`; its group `<base>-<instance>__redelivery` is franz-go only.
- A topic owner's series that mean what kafka-exporter's mean keep its names and labels; the rest are `topic_owner_*`. `prometheus/rules.yml`, `grafana/dashboards/topic-owners.json` and `verify-topics` quote metric names (`verify-topics` also log lines, the alert `TopicDLQGrowing` and Grafana's `Successfully queried the Prometheus API`); renaming one changes all three. Grafana does not save UI edits to the provisioned dashboard: change it in its JSON, and check every query against a running stack. `verify-topics` owns the `owner-verify` prefix: don't give anything else that name.
- Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
- Keep README.md, and the spec when behaviour departs from it, in sync with any behaviour change.

## Unit testing: write fewer, better tests

Every test is code someone has to read, maintain, and wait on in CI. A test earns its place only if it can fail for a reason no other test already covers. Optimize for distinct behaviors verified, not for test count or coverage numbers.

### Before writing any test

1. Read the existing tests for the code you're touching. If a behavior is already covered, do not cover it again. Extend or adjust the existing test instead of adding a new one beside it.
2. List the distinct behaviors you need to verify (one line each). If two items on the list would fail for the same underlying bug, merge them.
3. For each remaining item, ask: "If I deleted this test, what bug could slip through that the other tests would miss?" If you can't name one, don't write it.

### What counts as redundant

- Multiple inputs from the same equivalence class (e.g. testing 3, 5, and 7 when any positive integer exercises the same path). Pick one representative plus the boundaries.
- The same logic tested at several layers. Test it once at the lowest layer that owns it; higher layers only test their own wiring and logic.
- Tests that differ only in input and expected values. Collapse them into one table-driven/parameterized test.
- A new test that is a strict subset of an existing, broader one.

### What not to test at all

- Trivial code with no logic: getters, setters, plain constructors, constants, simple delegation.
- The language, standard library, framework, or third-party dependencies.
- Implementation details: private helpers, internal call order, or mock interactions that merely restate the implementation. Test observable behavior through the public interface.
- Scenarios that the type system or compiler already makes impossible.

### What you should still test

- Each distinct branch or behavior of the public contract, once.
- Boundaries and edge cases (empty, zero, max, nil/null, off-by-one).
- Error paths that have distinct handling.
- A regression test for each bug you fix, targeting that specific bug.

### When changing existing code

- Update the tests that cover the changed behavior rather than adding parallel ones. Delete tests made obsolete by the change.
- Don't add tests for code you didn't change unless asked.

### Reporting

When you finish, state briefly which behaviors you tested and any you deliberately skipped as redundant or trivial, so the reviewer can disagree if needed.
