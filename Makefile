# Makefile for Kafka Playground — KRaft broker, topic jobs, kcat consumers, producer page, Redpanda Console
# Typical flow: up → produce → logs → scale → groups → down
SERVICE = Kafka Playground

# Variables
COMPOSE = docker compose
PRODUCER_URL ?= http://localhost:8081
CONSOLE_URL ?= http://localhost:8080
KAFKA_BIN = $(COMPOSE) exec -T kafka /opt/kafka/bin
BOOTSTRAP = --bootstrap-server localhost:19092
topic ?= orders
key ?=
value ?=
n ?= 3
svc ?= orders-workers orders-audit

# Single-quote $(1) for the shell; fed $(value var), quotes, spaces and $ pass through untouched.
shq = '$(subst ','\'',$(1))'

.PHONY: help up down ps logs topics groups produce scale verify

# ── Environment ──────────────────────────────────────────────────────────────

help: ## Print this help message
	@printf '\033[01;32m${SERVICE} — Local Kafka sandbox\033[00;37m\n\n'
	@printf "\033[33mUsage:\033[0m\n  make [target] [arg=\"val\"...]\n\n\033[33mTargets:\033[0m\n"
	@grep -E '^[-a-zA-Z0-9_\.\/]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
		{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ── Stack ────────────────────────────────────────────────────────────────────

up: ## [STEP 1] Start everything; returns when the broker and both pages are healthy
	$(COMPOSE) up -d --wait
	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Broker from the host: localhost:9092"

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

# ── Verify ───────────────────────────────────────────────────────────────────

# Waits until both groups have committed past the record (so it can no longer be
# redelivered), then counts it in the logs: exactly once per group.
verify: up ## End-to-end check: one record, seen once by the worker group and once by the audit group
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
