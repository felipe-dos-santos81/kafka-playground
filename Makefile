# Makefile for Kafka Playground — local KRaft broker, topic jobs, kcat consumers, Redpanda Console
# Typical flow: up → produce → logs → scale → groups → down
SERVICE = Kafka Playground

# Variables
COMPOSE = docker compose
CONSOLE_URL ?= http://localhost:8080
topic ?= orders
key ?=
n ?= 3
svc ?= orders-workers orders-audit

.PHONY: help up down ps logs topics groups produce scale verify

# ── Environment ──────────────────────────────────────────────────────────────

help: ## Print this help message
	@printf '\033[01;32m${SERVICE} — Local Kafka sandbox\033[00;37m\n\n'
	@printf "\033[33mUsage:\033[0m\n  make [target] [arg=\"val\"...]\n\n\033[33mTargets:\033[0m\n"
	@grep -E '^[-a-zA-Z0-9_\.\/]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
		{printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ── Stack ────────────────────────────────────────────────────────────────────

up: ## [STEP 1] Start the whole stack (topic jobs run to completion first, then consumers start)
	$(COMPOSE) up -d
	@echo "Waiting for Console and for every consumer to get partitions..."; \
	for i in $$(seq 60); do \
		h=$$(curl -s -o /dev/null -w '%{http_code}' $(CONSOLE_URL)/admin/health); \
		c=$$($(COMPOSE) logs --no-log-prefix orders-workers orders-audit | grep -c 'assigned: orders'); \
		[ "$$h" = 200 ] && [ "$$c" -ge 3 ] && exit 0; sleep 1; \
	done; echo "stack not ready after 60s (console=$$h, assignments=$$c)"; exit 1
	@echo "Console: $(CONSOLE_URL)   Broker from the host: localhost:9092"

down: ## Stop and remove every container (topics and messages are lost)
	$(COMPOSE) down --remove-orphans
	@echo "Stack removed."

ps: ## Show every container, including exited topic jobs
	$(COMPOSE) ps -a

logs: ## [STEP 3] Follow consumer logs (usage: make logs [svc="orders-audit"]; default: all example consumers)
	$(COMPOSE) logs -f $(svc)

# ── Inspect ──────────────────────────────────────────────────────────────────

topics: ## Describe every topic: partitions, leaders, replicas
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe

groups: ## [STEP 5] Describe every consumer group: members, assigned partitions, lag
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --all-groups

# ── Produce ──────────────────────────────────────────────────────────────────

# ponytail: value is single-quote wrapped; a value containing ' needs the raw curl in the README.
produce: ## [STEP 2] Produce one JSON record via the Console backend (usage: make produce value='{"id":1}' [topic=orders] [key=k1])
	@test -n '$(value)' || { echo "value is required, e.g. make produce value='{\"id\":1}'"; exit 1; }
	@if [ -n '$(key)' ]; then key="\"$$(printf '%s' '$(key)' | base64 | tr -d '\n')\""; else key=null; fi; \
	value=$$(printf '%s' '$(value)' | base64 | tr -d '\n'); \
	curl -sS -X POST $(CONSOLE_URL)/api/topics-records -H 'Content-Type: application/json' \
		-d "{\"topicNames\":[\"$(topic)\"],\"compressionType\":0,\"useTransactions\":false,\"records\":[{\"key\":$$key,\"value\":\"$$value\",\"headers\":[],\"partitionId\":-1}]}"; \
	echo

# ── Scale ────────────────────────────────────────────────────────────────────

scale: ## [STEP 4] Set the number of orders-workers group members (usage: make scale n=3)
	$(COMPOSE) up -d --scale orders-workers=$(n) orders-workers
	@echo "orders-workers now has $(n) member(s); watch the rebalance with: make logs svc=orders-workers"

# ── Verify ───────────────────────────────────────────────────────────────────

verify: up ## End-to-end check: produce a unique record, assert the worker group logged it once and the audit group once
	@id="verify-$$(date +%s)"; \
	$(MAKE) --no-print-directory produce topic=orders key="$$id" value="{\"id\":\"$$id\"}" >/dev/null; \
	sleep 3; \
	w=$$($(COMPOSE) logs --no-log-prefix orders-workers | grep -c "key=$$id "); \
	a=$$($(COMPOSE) logs --no-log-prefix orders-audit | grep -c "key=$$id "); \
	echo "orders-workers saw it $$w time(s), orders-audit saw it $$a time(s)"; \
	if [ "$$w" = 1 ] && [ "$$a" = 1 ]; then echo "VERIFY OK"; else echo "VERIFY FAILED"; exit 1; fi
