# Kafka Playground: KRaft broker, topic jobs, kcat consumers, producer page, Redpanda Console, Pipeline Studio
# Typical flow: up → produce → logs → scale → groups → down
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

.PHONY: help up down ps logs topics groups produce scale verify verify-studio

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

down: ## Remove every container (topics and messages are lost)
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

# ── Studio ───────────────────────────────────────────────────────────────────

verify-studio: up ## Check the studio API: health, create, reject a bad edge, read back, delete
	@health=$$(curl -sS --fail-with-body $(STUDIO_URL)/api/health) || { echo "STUDIO FAILED: health: $$health"; exit 1; }; \
	echo "studio health: $$health"; \
	flow='{"name":"verify","nodes":[{"id":"producer-1","type":"producer","position":{"x":0,"y":0},"data":{"source":"manual","key":"","value":"{}"}},{"id":"topic-1","type":"topic","position":{"x":200,"y":0},"data":{"name":"verify","partitions":1,"replication_factor":1}}],"edges":[{"id":"e1","source":"producer-1","target":"topic-1"}]}'; \
	created=$$(curl -sS --fail-with-body -X POST $(STUDIO_URL)/api/flows -H 'Content-Type: application/json' --data "$$flow") || { echo "STUDIO FAILED: create: $$created"; exit 1; }; \
	id=$$(echo "$$created" | sed 's/^{"id":"\([0-9a-f]\{8\}\)".*/\1/'); \
	[ -f "flows/$$id.json" ] || { echo "STUDIO FAILED: flows/$$id.json not written ($$created)"; exit 1; }; \
	bad=$$(echo "$$flow" | sed 's/"source":"producer-1","target":"topic-1"/"source":"topic-1","target":"producer-1"/'); \
	code=$$(curl -sS -o /dev/null -w '%{http_code}' -X PUT $(STUDIO_URL)/api/flows/$$id -H 'Content-Type: application/json' --data "$$bad"); \
	[ "$$code" = 422 ] || { echo "STUDIO FAILED: bad edge accepted ($$code)"; exit 1; }; \
	curl -sS --fail-with-body $(STUDIO_URL)/api/flows/$$id | grep -q '"name":"verify"' || { echo "STUDIO FAILED: round trip"; exit 1; }; \
	curl -sS --fail -X DELETE $(STUDIO_URL)/api/flows/$$id || { echo "STUDIO FAILED: delete"; exit 1; }; \
	[ ! -f "flows/$$id.json" ] || { echo "STUDIO FAILED: flows/$$id.json still exists"; exit 1; }; \
	echo "STUDIO OK ($$id)"

# ── Verify ───────────────────────────────────────────────────────────────────

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
