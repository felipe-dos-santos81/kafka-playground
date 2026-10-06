# AGENTS.md

Local Kafka playground, run with Docker Compose. See README.md for usage; the design is in `docs/superpowers/specs/`.

## Layout

- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Console.
- `producer/`: the only custom code. Go (franz-go), one `main.go` plus an embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
- `Makefile`: day-to-day commands; `make verify` is the end-to-end test.

## Check your change

```sh
(cd producer && go vet ./... && gofmt -l .)   # after Go changes; gofmt must print nothing
docker compose config --quiet                  # after compose changes
make down && make verify                       # always: must end with VERIFY OK
make down
```

## Rules

- Pin every image to an exact version; never `latest`. The producer image is local: keep `pull_policy: build`.
- Keep it local-only: no auth, no TLS, ports bound to `127.0.0.1`, Console analytics off.
- Prefer existing images and config over new code.
- Inside compose `command`/`entrypoint` strings, write a shell `$` as `$$`.
- YAML merge (`<<:`) is shallow: a service that overrides `depends_on` must merge `*after-kafka` back in.
- Every new topic job also goes under `producer.depends_on`, or `docker compose up --wait` fails on the exited job.
- Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
- Keep README.md in sync with any behaviour change.
