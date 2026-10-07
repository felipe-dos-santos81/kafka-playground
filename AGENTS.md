# AGENTS.md

Local Kafka playground, run with Docker Compose. Usage is in README.md; designs and plans are in `docs/superpowers/`.

## Layout

- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console.
- `producer/`: producer page. Go (franz-go), `main.go` plus embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
- `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `docker.go`, `kafka.go`) and node runner (`node.go`, run as `studio node` in one container per producer and consumer), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
- `flows/`: Studio flow files, bind-mounted into the `studio` container.
- `Makefile`: day-to-day commands; `make verify` is the end-to-end test.

## Check your change

```sh
(cd producer && go vet ./... && gofmt -l .)                  # producer Go; gofmt prints nothing
(cd studio && go vet ./... && gofmt -l . && go test ./...)   # studio Go; gofmt prints nothing
(cd studio/ui && npm run build)                              # studio UI; runs tsc
docker compose config --quiet                                # compose
make down && make verify                                     # always: ends with STUDIO OK and VERIFY OK
make down
```

## Rules

- Pin every image to an exact version; never `latest`. The producer and studio images are local: keep `pull_policy: build`.
- Keep it local-only: no auth, no TLS, ports bound to `127.0.0.1`, Console analytics off.
- Prefer existing images and config over new code.
- Inside compose `command`/`entrypoint` strings, write a shell `$` as `$$`.
- YAML merge (`<<:`) is shallow: a service that overrides `depends_on` must merge `*after-kafka` back in.
- Every new topic job also goes under `producer.depends_on`, or `docker compose up --wait` fails on the exited job.
- `studio/flow.go` defines node ids, node data fields and the allowed-edge table; `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` mirror them. Change all three together. Every other validation rule lives only in `flow.go`: the server is the authority.
- The live snapshot's JSON is `NodeState` in `studio/engine.go`, mirrored by `NodeRuntime` in `studio/ui/src/flow/api.ts`; change both. `verify-studio` greps that JSON and relies on its field order (`lag` before `assigned`, node ids sorted).
- One `.gitignore`, at the root; don't add nested ones (a nested `dist` rule would hide `studio/ui/dist/.gitkeep`).
- Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). Keep that line in `down`; use `make nodes` to see them.
- Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
- Keep README.md in sync with any behaviour change.
