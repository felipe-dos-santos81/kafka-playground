# Makefile for Kafka Playground — local Kafka sandbox
# KRaft broker, topic jobs, kcat consumers, producer page, Redpanda Console, Pipeline Studio.
# Typical flow: up → produce → logs → scale → groups → down; verify checks it all end to end.
SERVICE = Kafka Playground

# Variables
COMPOSE = docker compose
PRODUCER_URL ?= http://localhost:8081
CONSOLE_URL ?= http://localhost:8080
STUDIO_URL ?= http://localhost:8082
KAFKA_BIN = $(COMPOSE) exec -T kafka /opt/kafka/bin
BOOTSTRAP = --bootstrap-server localhost:19092
topic ?= orders
key ?=
value ?=
n ?= 3
svc ?= orders-workers orders-audit

# Single-quote $(1) for the shell; fed $(value var), quotes, spaces and $ pass through untouched.
shq = '$(subst ','\'',$(1))'

.PHONY: help up down ps logs topics groups nodes produce scale verify verify-studio test

# ── Environment ──────────────────────────────────────────────────────────────

help: ## Print this help message
	@printf '\033[01;32m${SERVICE} — Local Kafka sandbox\033[00;37m\n\n'
	@printf "\033[33mUsage:\033[0m\n  make [target] [arg=\"val\"...]\n\n\033[33mTargets:\033[0m\n"
	@grep -E '^[-a-zA-Z0-9_\.\/]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
		{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ── Stack ────────────────────────────────────────────────────────────────────

up: ## [STEP 1] Start everything and wait until it is healthy
	$(COMPOSE) up -d --wait
	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Studio: $(STUDIO_URL)   Broker from the host: localhost:9092"

down: ## Remove every container, Studio node containers first (topics and messages are lost)
	@ids=$$(docker ps -aq -f label=studio.flow); [ -z "$$ids" ] || docker rm -f $$ids >/dev/null
	$(COMPOSE) down --remove-orphans

ps: ## Show every container, including exited topic jobs
	$(COMPOSE) ps -a

logs: ## [STEP 3] Follow consumer logs (usage: make logs [svc=orders-audit])
	$(COMPOSE) logs -f $(svc)

# ── Inspect ──────────────────────────────────────────────────────────────────

topics: ## Describe every topic: partitions, leaders, replicas
	$(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --describe

groups: ## [STEP 5] Describe every consumer group: members, partitions, lag
	$(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --describe --all-groups

nodes: ## List Studio node containers: one per deployed producer, consumer and consumer instance
	docker ps -a -f label=studio.flow

# ── Produce ──────────────────────────────────────────────────────────────────

produce: ## [STEP 2] Produce one JSON record via the producer page (usage: make produce value='{"id":1}' [topic=orders] [key=k1])
	@$(if $(value value),true,{ echo "value is required, e.g. make produce value='{\"id\":1}'"; exit 1; })
	@curl -sS --fail-with-body -X POST $(PRODUCER_URL)/api/produce \
		--url-query $(call shq,topic=$(value topic)) \
		$(if $(value key),--url-query $(call shq,key=$(value key))) \
		--data-binary $(call shq,$(value value))

# ── Scale ────────────────────────────────────────────────────────────────────

scale: ## [STEP 4] Set the number of orders-workers group members (usage: make scale n=3)
	$(COMPOSE) up -d --scale orders-workers=$(n) orders-workers

# ── Verify ───────────────────────────────────────────────────────────────────

verify-studio: up ## Check the studio end to end: save rules, deploy, send, tail, lag, live ticks, chained flows, instances, stop, delete
	@health=$$(curl -sS --fail-with-body $(STUDIO_URL)/api/health) || { echo "STUDIO FAILED: health: $$health"; exit 1; }; \
	echo "studio health: $$health"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows -H 'Sec-Fetch-Site: cross-site' --data '{"name":"x"}'); \
	[ "$$code" = 403 ] || { echo "STUDIO FAILED: cross-site write accepted ($$code)"; exit 1; }; \
	flow3() { printf '{"name":"%s","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":%s},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"%s","partitions":%s,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":%s}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"}]}' "$$1" "$$2" "$$3" "$$4" "$$5"; }; \
	manual='{"source":"manual","key":"","value":"{}"}'; \
	flow=$$(flow3 verify "$$manual" studio-verify 1 '{"group":"studio-verify","auto_offset_reset":"earliest","sink":{"kind":"log"}}'); \
	create() { out=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$1") || { echo "STUDIO FAILED: create $$2: $$out" >&2; return 1; }; echo "$$out" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'; }; \
	flows=; trap 'for f in $$flows; do curl -sS -X DELETE $(STUDIO_URL)/api/flows/$$f >/dev/null 2>&1; done; docker rm -f studio-$$id-consumer-1 >/dev/null 2>&1' EXIT; \
	id=$$(create "$$flow" "the verify flow") || exit 1; flows="$$id"; \
	nodes() { docker ps -aq -f label=studio.flow=$$id | wc -l | tr -d ' '; }; \
	[ -f "flows/$$id.json" ] || { echo "STUDIO FAILED: flows/$$id.json not written"; exit 1; }; \
	bad=$$(echo "$$flow" | sed 's/"source":"producer-1","target":"topic-1"/"source":"topic-1","target":"producer-1"/'); \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X PUT $(STUDIO_URL)/api/flows/$$id -H 'Content-Type: application/json' --data "$$bad"); \
	[ "$$code" = 422 ] || { echo "STUDIO FAILED: bad edge accepted ($$code)"; exit 1; }; \
	curl -sS --fail-with-body $(STUDIO_URL)/api/flows/$$id | grep -q '"name":"verify"' || { echo "STUDIO FAILED: round trip"; exit 1; }; \
	docker create --name studio-$$id-consumer-1 $$(docker inspect -f '{{.Config.Image}}' $$($(COMPOSE) ps -q studio)) >/dev/null; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows/$$id/deploy); \
	docker rm studio-$$id-consumer-1 >/dev/null; \
	[ "$$code" = 502 ] && [ "$$(nodes)" = 0 ] || { echo "STUDIO FAILED: deploy into a taken name: $$code with $$(nodes) containers left (want 502, 0)"; exit 1; }; \
	deployed=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/deploy) || { echo "STUDIO FAILED: deploy: $$deployed"; exit 1; }; \
	[ "$$(nodes)" = 2 ] || { echo "STUDIO FAILED: $$(nodes) node containers after deploy, want 2"; exit 1; }; \
	rec="verify-$$(date +%s)"; \
	sent=$$(curl -sS --fail-with-body -X POST "$(STUDIO_URL)/api/flows/$$id/nodes/producer-1/send?key=$$rec" --data "{\"id\":\"$$rec\"}") || { echo "STUDIO FAILED: send: $$sent"; exit 1; }; \
	echo "studio sent $$rec: $$sent"; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/nodes/producer-1/send >/dev/null || { echo "STUDIO FAILED: template send"; exit 1; }; \
	for i in $$(seq 30); do \
		curl -sS "$(STUDIO_URL)/api/flows/$$id/nodes/consumer-1/tail?since=0" | grep -q "\"key\":\"$$rec\"" && break; \
		[ "$$i" = 30 ] && { echo "STUDIO FAILED: the consumer tail never showed $$rec"; exit 1; }; sleep 1; \
	done; \
	for i in $$(seq 20); do \
		curl -sS "$(STUDIO_URL)/api/flows/$$id/state" | grep -q '"consumer-1":{[^}]*"lag":0[,}]' && break; \
		[ "$$i" = 20 ] && { echo "STUDIO FAILED: consumer lag never reached 0: $$(curl -sS $(STUDIO_URL)/api/flows/$$id/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio state: $$(curl -sS $(STUDIO_URL)/api/flows/$$id/state)"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X POST $(STUDIO_URL)/api/flows/$$id/deploy); \
	[ "$$code" = 409 ] && [ "$$(nodes)" = 2 ] || { echo "STUDIO FAILED: second deploy: $$code with $$(nodes) containers (want 409, 2)"; exit 1; }; \
	$(COMPOSE) restart studio >/dev/null 2>&1 && $(COMPOSE) up -d --wait studio >/dev/null 2>&1 || { echo "STUDIO FAILED: restart"; exit 1; }; \
	curl -sS --fail-with-body $(STUDIO_URL)/api/flows/$$id/state | grep -q '"status":"running"' || { echo "STUDIO FAILED: flow not running after a studio restart"; exit 1; }; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/stop >/dev/null || { echo "STUDIO FAILED: stop"; exit 1; }; \
	[ "$$(nodes)" = 0 ] || { echo "STUDIO FAILED: $$(nodes) node containers left after stop"; exit 1; }; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$id/nodes/consumer-1/tail?since=0"); \
	[ "$$code" = 409 ] || { echo "STUDIO FAILED: tail of a stopped flow: $$code, want 409"; exit 1; }; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$id/deploy >/dev/null || { echo "STUDIO FAILED: redeploy"; exit 1; }; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$id || { echo "STUDIO FAILED: delete"; exit 1; }; \
	[ "$$(nodes)" = 0 ] && [ ! -f "flows/$$id.json" ] || { echo "STUDIO FAILED: delete left $$(nodes) containers or flows/$$id.json"; exit 1; }; \
	tflow=$$(flow3 verify-timer '{"source":"timer","interval_ms":100,"key":"","value":"{\"n\": {{.Seq}}}"}' studio-verify-timer 1 '{"group":"studio-verify-timer","auto_offset_reset":"latest","sink":{"kind":"log"}}'); \
	tid=$$(create "$$tflow" "the timer flow") || exit 1; flows="$$flows $$tid"; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$tid/deploy >/dev/null || { echo "STUDIO FAILED: timer deploy"; exit 1; }; \
	tick=$$(curl -sN --max-time 20 $(STUDIO_URL)/api/flows/$$tid/events | grep -m1 '"consumer-1":{[^}]*"rate":.*"producer-1":{[^}]*"rate":'); \
	[ -n "$$tick" ] || { echo "STUDIO FAILED: no tick with consumer and producer rates for the timer flow"; exit 1; }; \
	echo "studio tick: $$tick"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$tid || { echo "STUDIO FAILED: delete the timer flow"; exit 1; }; \
	a='{"name":"verify-chain-a","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":'"$$manual"'},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"studio-verify-a","partitions":1,"replication_factor":1}},{"id":"consumer-1","type":"consumer","position":{"x":400,"y":0},"data":{"group":"studio-verify-a","auto_offset_reset":"earliest","sink":{"kind":"log"}}},{"id":"topic-2","type":"topic","position":{"x":600,"y":0},"data":{"name":"studio-verify-a-out","partitions":1,"replication_factor":1}},{"id":"consumer-2","type":"consumer","position":{"x":800,"y":0},"data":{"group":"studio-verify-a-out","auto_offset_reset":"earliest","sink":{"kind":"log"}}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"},{"id":"e2","source":"topic-1","target":"consumer-1"},{"id":"e3","source":"consumer-1","target":"topic-2"},{"id":"e4","source":"topic-2","target":"consumer-2"}]}'; \
	aid=$$(create "$$a" "chain flow A") || exit 1; flows="$$flows $$aid"; \
	b=$$(flow3 verify-chain-b "$$manual" studio-verify-b 1 '{"group":"studio-verify-b","auto_offset_reset":"earliest","sink":{"kind":"http","url":"http://studio:8082/api/flows/'"$$aid"'/nodes/producer-1/send"}}'); \
	bid=$$(create "$$b" "chain flow B") || exit 1; flows="$$flows $$bid"; \
	for f in $$aid $$bid; do curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$f/deploy >/dev/null || { echo "STUDIO FAILED: deploy chain flow $$f"; exit 1; }; done; \
	rec="chain-$$(date +%s)"; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$bid/nodes/producer-1/send --data "{\"id\":\"$$rec\"}" >/dev/null || { echo "STUDIO FAILED: send to chain flow B"; exit 1; }; \
	for i in $$(seq 30); do \
		curl -sS "$(STUDIO_URL)/api/flows/$$aid/nodes/consumer-2/tail?since=0" | grep -q "$$rec" && break; \
		[ "$$i" = 30 ] && { echo "STUDIO FAILED: $$rec never reached flow A's last consumer; flow B: $$(curl -sS $(STUDIO_URL)/api/flows/$$bid/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio chain: $$rec went through flow B's http sink into flow A and was forwarded"; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$bid && curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$aid || { echo "STUDIO FAILED: delete the chain flows"; exit 1; }; \
	c=$$(flow3 verify-instances "$$manual" studio-verify-instances 3 '{"group":"studio-verify-instances","auto_offset_reset":"earliest","instances":3,"sink":{"kind":"log"}}'); \
	cid=$$(create "$$c" "the instances flow") || exit 1; flows="$$flows $$cid"; \
	curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows/$$cid/deploy >/dev/null || { echo "STUDIO FAILED: deploy the instances flow"; exit 1; }; \
	[ "$$(docker ps -q -f label=studio.flow=$$cid -f label=studio.node=consumer-1 | wc -l | tr -d ' ')" = 3 ] || { echo "STUDIO FAILED: want 3 consumer-1 containers"; exit 1; }; \
	for i in $$(seq 45); do \
		[ "$$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state | grep -o '"assigned":{"studio-verify-instances":\[[0-2]\]}' | wc -l | tr -d ' ')" = 3 ] && break; \
		[ "$$i" = 45 ] && { echo "STUDIO FAILED: the 3 instances never held one partition each: $$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state)"; exit 1; }; sleep 1; \
	done; \
	echo "studio instances: $$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state)"; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$cid/nodes/consumer-1/tail?since=0&instance=3"); \
	[ "$$code" = 200 ] || { echo "STUDIO FAILED: tail of instance 3: $$code, want 200"; exit 1; }; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$cid/nodes/consumer-1/tail?since=0&instance=4"); \
	[ "$$code" = 409 ] || { echo "STUDIO FAILED: tail of a fourth instance: $$code, want 409"; exit 1; }; \
	docker rm -f studio-$$cid-consumer-1-2 >/dev/null; \
	curl -sS $(STUDIO_URL)/api/flows/$$cid/state | grep -q '"consumer-1":{"state":"missing",.*"instance":2,"state":"missing"' || { echo "STUDIO FAILED: a removed instance does not show as missing: $$(curl -sS $(STUDIO_URL)/api/flows/$$cid/state)"; exit 1; }; \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' "$(STUDIO_URL)/api/flows/$$cid/nodes/consumer-1/tail?since=0&instance=2"); \
	[ "$$code" = 409 ] || { echo "STUDIO FAILED: tail of a removed instance: $$code, want 409"; exit 1; }; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$cid || { echo "STUDIO FAILED: delete the instances flow"; exit 1; }; \
	echo "STUDIO OK ($$id)"

# Waits until both groups have committed past the record (so it can no longer be
# redelivered), then counts it in the logs: exactly once per group.
verify: up verify-studio ## End-to-end check: studio API, then one record seen exactly once per consumer group
	@id="verify-$$(date +%s)"; \
	sent=$$($(MAKE) --no-print-directory produce key="$$id" value="{\"id\":\"$$id\"}") || exit 1; \
	partition=$$(echo "$$sent" | sed 's/.*"partition":\([0-9]*\).*/\1/'); \
	offset=$$(echo "$$sent" | sed 's/.*"offset":\([0-9]*\).*/\1/'); \
	audit_group=$$($(COMPOSE) ps -q orders-audit | cut -c1-12); \
	echo "Produced $$id to orders[$$partition] offset $$offset; waiting for both groups to commit..."; \
	for i in $$(seq 20); do \
		$(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --describe --group orders-workers --group $$audit_group 2>/dev/null | \
			awk -v p="$$partition" -v o="$$offset" '$$2 == "orders" && $$3 == p && $$4 != "-" && $$4 + 0 > o { n++ } END { exit n < 2 }' && break; \
		[ "$$i" = 20 ] && { echo "VERIFY FAILED: offsets not committed"; exit 1; }; sleep 1; \
	done; \
	seen() { $(COMPOSE) logs --since 5m --no-log-prefix "$$1" | grep -c "key=$$id "; }; \
	workers=$$(seen orders-workers); audit=$$(seen orders-audit); \
	echo "orders-workers: $$workers, orders-audit: $$audit"; \
	if [ "$$workers" = 1 ] && [ "$$audit" = 1 ]; then echo "VERIFY OK"; else echo "VERIFY FAILED"; exit 1; fi

# ── Development ──────────────────────────────────────────────────────────────

# gofmt -l lists unformatted files; tee shows them, and a non-empty list fails the step.
test: ## Static checks and unit tests, no running stack needed: go vet, gofmt, go test, UI build (tsc), compose config
	cd producer && go vet ./... && test -z "$$(gofmt -l . | tee /dev/stderr)"
	cd studio && go vet ./... && test -z "$$(gofmt -l . | tee /dev/stderr)" && go test ./...
	cd studio/ui && { [ -d node_modules ] || npm ci; } && npm run build
	$(COMPOSE) config --quiet
