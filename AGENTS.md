# AGENTS.md

Local Kafka playground, run with Docker Compose. Usage is in README.md. Pipeline Studio's design is `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` (the authority; §7 lists the milestones), with one implementation plan per milestone in `docs/superpowers/plans/`. Its UI tests are designed in `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`.

## Layout

- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console.
- `producer/`: producer page. Go (franz-go), `main.go` plus embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
- `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `stream.go`, `docker.go`, `kafka.go`, `transform.go`) and node runner (`node.go`, run as `studio node` in one container per producer, consumer and consumer instance), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
- `studio/ui/e2e/`: the UI tests (Playwright, `playwright.config.ts` beside them): `flows.ts` (the API and flow parts), `locators.ts`, `studio.ts` (the fixture), and one `*.spec.ts` per area. `make verify-ui` runs them against the running stack.
- `flows/`: Studio flow files, bind-mounted into the `studio` container; `flows/0a1b2c3d.json` is the example.
- `Makefile`: day-to-day commands; `make test` runs the static checks and unit tests, `make verify` the end-to-end test (`verify-studio` deletes its flows and its `studio-verify…` topics when it exits; name a new test topic in that trap too).

## Check your change

```sh
make test                  # go vet, gofmt, go test, UI build (tsc), UI tests type-check, compose config
make down && make verify   # always: ends with STUDIO OK, UI OK and VERIFY OK (the first run downloads Chromium)
make down                  # then `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing
```

## Rules

- Pin every image to an exact version; never `latest`. The producer and studio images are local: keep `pull_policy: build`.
- Keep it local-only: no auth, no TLS, ports bound to `127.0.0.1`, Console analytics off.
- Prefer existing images and config over new code.
- Inside compose `command`/`entrypoint` strings, write a shell `$` as `$$`.
- YAML merge (`<<:`) is shallow: a service that overrides `depends_on` must merge `*after-kafka` back in.
- Every new topic job also goes under `producer.depends_on`, or `docker compose up --wait` fails on the exited job.
- `studio/flow.go` defines node ids, node data fields and the allowed-edge table; `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` mirror them. Change all three together. Every other validation rule lives only in `flow.go`: the server is the authority.
- The live snapshot's JSON is `NodeState` in `studio/engine.go`, mirrored by `NodeRuntime` in `studio/ui/src/flow/api.ts`; change both. A consumer with instances lists them in `instances`; code walks a node's containers through `NodeState.containers()` (UI: `containersOf`) instead of branching on single vs instances. `verify-studio` greps that JSON and relies on its field order (`lag` before `assigned`, `instances` last, node ids sorted).
- A node container is named by `nodeRef` (`studio/resolve.go`): `studio-<flow>-<node>`, plus `-<i>` for a consumer instance. Its stop budgets in `node.go` derive from `stopGraceSeconds` (`docker.go`), the grace `make down` and Stop give it; keep them inside it.
- A transform has no container: `Resolve` gives its expression and node id to the upstream consumer's `NodeSpec`, the consumer reports its counts in `/stats` `step`, and `applySteps` puts them on the Transform node. Expressions compile in `Validate` (deploy) and again in the consumer, through `compileTransform` (`transform.go`).
- Studio consumers need their own groups: franz-go and the kcat consumers' librdkafka share no assignor, so the broker refuses a mixed group.
- One `.gitignore`, at the root; don't add nested ones (a nested `dist` rule would hide `studio/ui/dist/.gitkeep`).
- The UI tests (`studio/ui/e2e/`, Playwright, `make verify-ui`) find elements by role and text, and by `data-testid` where there is none (`node-<id>`, `runtime-<id>`, `tail`). A change to the UI's visible text, roles or those ids runs `make verify-ui`; the top-bar messages they check are quoted in the Studio spec or the UI tests spec (§5), so rewording one goes through the spec. Each test removes the flows it made; the topics and groups they named go after the last test, by exact name and only if they start `studio-ui-`.
- Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). Keep that line in `down`; use `make nodes` to see them.
- Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
- Keep README.md, and the spec when behaviour departs from it, in sync with any behaviour change.
