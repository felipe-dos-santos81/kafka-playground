# Topic containers M3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prometheus evaluates four alert rules on the topic owners' series, and Grafana shows them on a provisioned dashboard. `make verify-topics` checks both in a new step 6 and still ends in `TOPICS OK`.

**Architecture:**
- **`prometheus/rules.yml`** (new): one rule group, `topic-owners`, with the spec's four alerts. `prometheus.yml` loads it. There is no Alertmanager: firing alerts show at http://localhost:9090/alerts and in the dashboard's table.
- **`grafana`** (new service, `grafana/grafana:13.2.3`): anonymous admin, no phone-home, no volume. Files provision its datasource (`uid: prometheus`) and one dashboard (`uid: topic-owners`), and Grafana does not save UI edits.
- **`verify-topics` step 6** runs `promtool` inside the running Prometheus, then checks that the four rules are healthy and that `TopicDLQGrowing` fires for the DLQ that step 5 filled. Then it checks Grafana's health, the dashboard and the datasource.

No Go code changes.

**Tech Stack:** Prometheus v3.15.0 (rule files, `promtool`), Grafana 13.2.3 (file provisioning, classic dashboard JSON), Docker Compose, GNU make 3.81 with BSD tools.

**Spec:** `docs/superpowers/specs/2026-10-08-topic-containers-design.md`. This plan is milestone M3 (§8). The behaviour is in §4.2 (the dashboard's expressions and the alert rules), §5.3 (the `grafana` service), §5.4 (`prometheus.yml` and the Grafana provisioning files), §7 (repository changes and `AGENTS.md` lines) and §10 (`verify-topics` step 6).

**Provenance:** every file and patch below was built and tested before this plan was written, in a scratch copy of this repository.
- **Grafana 13.2.3:**
  - `make up` brought the stack up healthy, Grafana included, with no `level=error` line in Grafana's log.
  - An anonymous request created a folder (`"canAdmin":true`). A recreated container no longer had it, so nothing persists.
- **The dashboard:**
  - Every query, with the variables filled the way Grafana fills them, returned series for `orders-1` after one record was parked and one redelivered. The exception is lag per group: nothing consumes `orders-1`.
  - A Chromium screenshot showed the alert table, the three rows and their series.
- **The alerts:** `TopicDLQGrowing` fired for `orders-1__dlq` (the M3 demo).
- **The checks:**
  - `make verify-topics` printed `TOPICS OK` in 50 s, including step 6.
  - A broken rule made `promtool check rules` fail, and a wrong dashboard uid made Grafana answer 404.
  - `make test` passed. `make down && make verify` printed `TOPICS OK` and `VERIFY OK`, and `make down` left no container, volume or network.
- **Copy the code exactly.**

**Rulings (where this plan departs from the spec's text; Task 4 brings the spec in line):**
- **Two provisioning mounts.** The spec mounts all of `./grafana/provisioning`, which hides the image's empty `plugins/` and `alerting/` directories, and Grafana logs `level=error` for each. The service mounts `datasources/` and `dashboards/` separately.
- **The dashboard's expressions** are §4.2's, plus one matcher per panel that picks the role's topic from the variables, `topic=~"${base}-${topic_instance}__retry"`. Without it, the variables would change nothing. The braces are needed, because `$topic_instance__retry` names another variable. Moves to the DLQ are summed `by (topic, reason)`, as §5.4's panel list asks ("by reason").
- **The alert table sits above the three rows**, not in a fourth row. The M3 demo expects "three rows".
- **The rules carry no annotations.** The spec defines none. Step 6's grep accepts any annotations, so adding some later does not break it.

## Global Constraints

- Nothing under `studio/`, `topic-owner/`, `producer/` or `flows/` changes. The only new image is `grafana/grafana:13.2.3`. Prometheus stays `prom/prometheus:v3.15.0`.
- Grafana binds `127.0.0.1:3000:3000`. It runs anonymous `Admin` with the login form off, and these settings: `GF_ANALYTICS_REPORTING_ENABLED`, `GF_ANALYTICS_CHECK_FOR_UPDATES`, `GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES`, `GF_ANALYTICS_FEEDBACK_LINKS_ENABLED` and `GF_NEWS_NEWS_FEED_ENABLED` all `"false"`, and `GF_PLUGINS_PREINSTALL_DISABLED` `"true"`. It has no volume.
- Names, exactly:
  - the rule group `topic-owners`;
  - the alerts `TopicConsumerLagHigh`, `TopicDLQGrowing`, `TopicRetryWaiting` and `TopicOwnerUnhealthy`;
  - the datasource uid `prometheus`;
  - the dashboard uid `topic-owners`, titled `Topic owners`;
  - the dashboard provider path `/var/lib/grafana-dashboards`, with `allowUiUpdates: false`;
  - `rule_files: [/etc/prometheus/rules.yml]`.
- Alert expressions and `for` durations, word for word from spec §4.2:
  - `TopicConsumerLagHigh`: `sum by (consumergroup, topic) (kafka_consumergroup_lag{topic!~".+__(retry|dlq)"}) > 100`, for 2m;
  - `TopicDLQGrowing`: `sum by (topic) (increase(kafka_topic_partition_current_offset{topic=~".+__dlq"}[10m])) > 0`, no `for`;
  - `TopicRetryWaiting`: `sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~".+__retry"}) > 0`, for 5m;
  - `TopicOwnerUnhealthy`: `up{job="topic-owners"} == 0 or topic_owner_reconciled == 0`, for 1m.
- `verify-topics` step 6 prints `topics alerts: 4 rules, TopicDLQGrowing firing for owner-verify-1__dlq; Grafana: dashboard topic-owners, datasource healthy` before `TOPICS OK`.
- `AGENTS.md` rules hold:
  - Makefile: real tabs, `## ` help, `$$` for a shell `$`, GNU make 3.81 with BSD tools;
  - pinned images; local-only (`127.0.0.1`);
  - `verify-topics` owns the `owner-verify` prefix;
  - README, `AGENTS.md` and the spec change with the behaviour.
- AGENTS.md's "Unit testing: write fewer, better tests" applies. M3 changes no Go code and adds no unit test. Its checks are `verify-topics` step 6, Task 1's `promtool` runs and Task 2's one-off query check. Do not add tests beyond them. Each report states which behaviours were checked, and which checks were skipped and why.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- **A dashboard variable name glued to a suffix.** `$topic_instance__retry` interpolates as an unknown variable, and the retry and DLQ rows go empty without an error. Every variable in the JSON is written `${…}`. Task 2's query check fills the variables as Grafana does and fails on a query that returns no series.
- **Grafana starting before Prometheus is ready.** If it does, the datasource shows errors at first. `depends_on: {prometheus: {condition: service_healthy}}` prevents it, and step 6 checks the datasource's health through Grafana (Task 3).
- **An invalid rule file.** Prometheus refuses to start with one, so `make up` hangs waiting for it to become healthy. Task 1 runs `promtool` on the files before `make up`. Step 6 runs it again inside the running container, and Task 3's mutation check proves step 6 fails on a broken rule.
- **`TopicDLQGrowing` that keeps firing.** It fires for 10 minutes after the last parked record, also after `verify-topics` deleted its DLQ, because `increase()` still sees the samples in its window. That is correct for "parked in the last 10 minutes", and the README and spec say so (Task 4), so a reader is not surprised by an `owner-verify-1__dlq` alert after `make verify`.
- **Edits made in Grafana's UI.** With `allowUiUpdates: false`, Grafana refuses to save them, and `make down` would drop them anyway. The README says to edit the JSON (Task 4).

---

### Task 1: Alert rules

**Files:**
- Create: `prometheus/rules.yml`
- Modify: `prometheus/prometheus.yml`, `docker-compose.yml` (the `prometheus` service's comment)

**Interfaces:**
- Consumes: the series M1 and M2 export, `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `topic_owner_reconciled` and `up{job="topic-owners"}`, with their `topic` and `consumergroup` labels.
- Produces: the rule group `topic-owners` with the four alerts. Prometheus loads it from `/etc/prometheus/rules.yml`, which `./prometheus` is already mounted over. Task 2's alert table and Task 3's step 6 read the alerts.

- [ ] **Step 1: Check that the rules are missing**

Run:

```sh
docker run --rm --entrypoint promtool -v "$PWD/prometheus:/etc/prometheus:ro" prom/prometheus:v3.15.0 check rules /etc/prometheus/rules.yml
```

Expected: exit 1, with `promtool: error: path '/etc/prometheus/rules.yml' does not exist`.

- [ ] **Step 2: Write the rules**

Create `prometheus/rules.yml`:

```yaml
# Alerts on the topic owners' series (spec §4.2). The role is in the topic's
# name (__retry, __dlq), so the rules select by topic. No Alertmanager: firing
# alerts show at http://localhost:9090/alerts and on the Grafana dashboard.
# verify-topics greps the alert names.
groups:
  - name: topic-owners
    rules:
      - alert: TopicConsumerLagHigh # a consumer of a main topic falls behind
        expr: sum by (consumergroup, topic) (kafka_consumergroup_lag{topic!~".+__(retry|dlq)"}) > 100
        for: 2m
      - alert: TopicDLQGrowing # any newly parked record is news in a playground
        expr: sum by (topic) (increase(kafka_topic_partition_current_offset{topic=~".+__dlq"}[10m])) > 0
      - alert: TopicRetryWaiting # the worker is down, or a Studio retry loop stopped
        expr: sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~".+__retry"}) > 0
        for: 5m
      - alert: TopicOwnerUnhealthy # a container is down, or its topic drifted beyond repair
        expr: up{job="topic-owners"} == 0 or topic_owner_reconciled == 0
        for: 1m
```

- [ ] **Step 3: Load them**

Apply to `prometheus/prometheus.yml`:

```diff
--- a/prometheus/prometheus.yml
+++ b/prometheus/prometheus.yml
@@ -1,9 +1,11 @@
 # Prometheus for the playground: scrapes every topic-owner container, found
-# through the Docker socket by its topic-owner.role label (no target list).
+# through the Docker socket by its topic-owner.role label (no target list), and
+# evaluates the alert rules in rules.yml.
 global:
   scrape_interval: 5s
   scrape_timeout: 4s
   evaluation_interval: 5s
+rule_files: [/etc/prometheus/rules.yml]
 
 scrape_configs:
   - job_name: topic-owners
```

Apply to `docker-compose.yml`, for the `prometheus` service's comment only. The `grafana` service comes in Task 2.

```diff
--- a/docker-compose.yml
+++ b/docker-compose.yml
@@ -201,7 +201,8 @@
     environment: {<<: *orders-1-env, ROLE: dlq}
 
   # Prometheus: scrapes every topic owner, found by its labels through the
-  # Docker socket (no target list): http://localhost:9090
+  # Docker socket (no target list), and evaluates prometheus/rules.yml:
+  # http://localhost:9090 (alerts at /alerts)
   prometheus:
     image: prom/prometheus:v3.15.0
     group_add: ["0"]    # Docker Desktop shows the socket as root:root 0660 inside containers
```

- [ ] **Step 4: Check the files with promtool**

Run:

```sh
docker run --rm --entrypoint promtool -v "$PWD/prometheus:/etc/prometheus:ro" prom/prometheus:v3.15.0 check config /etc/prometheus/prometheus.yml
```

Expected: exit 0, with `SUCCESS: 1 rule files found`, `is valid prometheus config file syntax` and `SUCCESS: 4 rules found`. `check config` also checks the rule files it names. It does not open the Docker socket, so this one-off needs no socket mount.

- [ ] **Step 5: Check that the running Prometheus evaluates them**

Run:

```sh
make up
curl -sS 'http://localhost:9090/api/v1/rules?type=alert' | grep -oE '"name":"Topic[A-Za-z]+"|"health":"[a-z]+"'
```

Expected: the four names, each followed by `"health":"ok"`. If it says `"health":"unknown"`, wait 5 s, because the rules have not been evaluated yet.

- [ ] **Step 6: Commit**

```bash
git add prometheus/rules.yml prometheus/prometheus.yml docker-compose.yml
git commit -m "topic owners M3: alert rules — TopicConsumerLagHigh, TopicDLQGrowing, TopicRetryWaiting, TopicOwnerUnhealthy, loaded by Prometheus

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Grafana and the dashboard

**Files:**
- Create: `grafana/provisioning/datasources/prometheus.yml`, `grafana/provisioning/dashboards/topic-owners.yml`, `grafana/dashboards/topic-owners.json`
- Modify: `docker-compose.yml` (the header comment and the new `grafana` service)

**Interfaces:**
- Consumes:
  - the `prometheus` service and its healthcheck;
  - the series of M1 and M2 and their labels (`topic`, `consumergroup`, `reason`, `le`, and `base` and `topic_instance` on `topic_owner_info`);
  - Task 1's alerts, through `ALERTS`.
- Produces: Grafana at http://localhost:3000. It has the datasource uid `prometheus` and the dashboard uid `topic-owners`, which Task 3's step 6 checks.

- [ ] **Step 1: Check that Grafana is missing**

Run: `curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:3000/api/health`

Expected: `000` with `Failed to connect`.

- [ ] **Step 2: Provision the datasource and the dashboard**

Create `grafana/provisioning/datasources/prometheus.yml`:

```yaml
# The playground's Prometheus, the dashboard's only datasource. Not editable in the UI.
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
    jsonData:
      timeInterval: 5s # Prometheus's scrape_interval
```

Create `grafana/provisioning/dashboards/topic-owners.yml`:

```yaml
# Loads grafana/dashboards/*.json (mounted at /var/lib/grafana-dashboards).
# Edit the files, not the UI: allowUiUpdates is off.
apiVersion: 1
providers:
  - name: topic-owners
    type: file
    allowUiUpdates: false
    options:
      path: /var/lib/grafana-dashboards
```

Create `grafana/dashboards/topic-owners.json`:

```json
{
  "uid": "topic-owners",
  "title": "Topic owners",
  "tags": [
    "kafka",
    "topic-owners"
  ],
  "description": "The topic owners' series (topic-owner/): one row per role. Provisioned from grafana/dashboards/topic-owners.json.",
  "schemaVersion": 41,
  "version": 1,
  "refresh": "5s",
  "time": {
    "from": "now-30m",
    "to": "now"
  },
  "templating": {
    "list": [
      {
        "name": "base",
        "type": "query",
        "datasource": {
          "type": "prometheus",
          "uid": "prometheus"
        },
        "query": "label_values(topic_owner_info, base)",
        "definition": "label_values(topic_owner_info, base)",
        "refresh": 2,
        "multi": true,
        "includeAll": true,
        "sort": 1,
        "current": {
          "text": [
            "All"
          ],
          "value": [
            "$__all"
          ]
        }
      },
      {
        "name": "topic_instance",
        "type": "query",
        "datasource": {
          "type": "prometheus",
          "uid": "prometheus"
        },
        "query": "label_values(topic_owner_info{base=~\"$base\"}, topic_instance)",
        "definition": "label_values(topic_owner_info{base=~\"$base\"}, topic_instance)",
        "refresh": 2,
        "multi": true,
        "includeAll": true,
        "sort": 1,
        "current": {
          "text": [
            "All"
          ],
          "value": [
            "$__all"
          ]
        }
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "type": "table",
      "title": "Firing alerts",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "description": "Prometheus's firing alerts (rules in prometheus/rules.yml); also at http://localhost:9090/alerts.",
      "gridPos": {
        "h": 6,
        "w": 24,
        "x": 0,
        "y": 0
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "ALERTS{alertstate=\"firing\"}",
          "instant": true,
          "range": false,
          "format": "table"
        }
      ],
      "transformations": [
        {
          "id": "organize",
          "options": {
            "excludeByName": {
              "Time": true,
              "Value": true,
              "__name__": true,
              "alertstate": true
            }
          }
        }
      ]
    },
    {
      "id": 2,
      "type": "row",
      "title": "main: ${base}-${topic_instance}",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 6
      },
      "panels": []
    },
    {
      "id": 3,
      "type": "timeseries",
      "title": "Messages in per second",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 0,
        "y": 7
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~\"${base}-${topic_instance}\"}[1m]))",
          "legendFormat": "{{topic}}"
        }
      ],
      "description": "rate() over the log end offset, a gauge as in kafka-exporter; Prometheus notes that it is not a counter.",
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      }
    },
    {
      "id": 4,
      "type": "stat",
      "title": "Partitions",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 4,
        "x": 8,
        "y": 7
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "kafka_topic_partitions{topic=~\"${base}-${topic_instance}\"}",
          "legendFormat": "{{topic}}"
        }
      ]
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Log size",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 6,
        "x": 12,
        "y": 7
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "sum by (topic) (topic_owner_partition_log_size_bytes{topic=~\"${base}-${topic_instance}\"})",
          "legendFormat": "{{topic}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bytes"
        },
        "overrides": []
      }
    },
    {
      "id": 6,
      "type": "timeseries",
      "title": "Lag per group",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 6,
        "x": 18,
        "y": 7
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~\"${base}-${topic_instance}\"})",
          "legendFormat": "{{consumergroup}} on {{topic}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      }
    },
    {
      "id": 7,
      "type": "row",
      "title": "retry: ${base}-${topic_instance}__retry",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 15
      },
      "panels": []
    },
    {
      "id": 8,
      "type": "timeseries",
      "title": "Waiting",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 5,
        "x": 0,
        "y": 16
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "sum by (topic) (kafka_consumergroup_lag{consumergroup=~\".+__redelivery\",topic=~\"${base}-${topic_instance}__retry\"})",
          "legendFormat": "{{topic}}"
        }
      ],
      "description": "Records the redelivery worker has not handled yet (its group's lag).",
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      }
    },
    {
      "id": 9,
      "type": "timeseries",
      "title": "Redeliveries per second",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 5,
        "x": 5,
        "y": 16
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "rate(topic_owner_redeliveries_total{topic=~\"${base}-${topic_instance}__retry\"}[5m])",
          "legendFormat": "{{topic}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      }
    },
    {
      "id": 10,
      "type": "timeseries",
      "title": "Moves to the DLQ per second",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 5,
        "x": 10,
        "y": 16
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "sum by (topic, reason) (rate(topic_owner_dead_lettered_total{topic=~\"${base}-${topic_instance}__retry\"}[5m]))",
          "legendFormat": "{{topic}} {{reason}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      }
    },
    {
      "id": 11,
      "type": "timeseries",
      "title": "Backoff p50 and p95",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 5,
        "x": 15,
        "y": 16
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "histogram_quantile(0.5, sum by (le, topic) (rate(topic_owner_backoff_seconds_bucket{topic=~\"${base}-${topic_instance}__retry\"}[5m])))",
          "legendFormat": "{{topic}} p50"
        },
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "B",
          "expr": "histogram_quantile(0.95, sum by (le, topic) (rate(topic_owner_backoff_seconds_bucket{topic=~\"${base}-${topic_instance}__retry\"}[5m])))",
          "legendFormat": "{{topic}} p95"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      }
    },
    {
      "id": 12,
      "type": "timeseries",
      "title": "Skipped per second",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 4,
        "x": 20,
        "y": 16
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "rate(topic_owner_skipped_total{topic=~\"${base}-${topic_instance}__retry\"}[5m])",
          "legendFormat": "{{topic}}"
        }
      ],
      "description": "Records with a studio-group header, left to that Studio consumer's retry loop.",
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      }
    },
    {
      "id": 13,
      "type": "row",
      "title": "dlq: ${base}-${topic_instance}__dlq",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 24
      },
      "panels": []
    },
    {
      "id": 14,
      "type": "timeseries",
      "title": "Parked",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 25
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "sum by (topic) (kafka_topic_partition_current_offset{topic=~\"${base}-${topic_instance}__dlq\"} - kafka_topic_partition_oldest_offset{topic=~\"${base}-${topic_instance}__dlq\"})",
          "legendFormat": "{{topic}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      }
    },
    {
      "id": 15,
      "type": "timeseries",
      "title": "Age of the oldest parked record",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 25
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "refId": "A",
          "expr": "time() - min by (topic) (topic_owner_oldest_message_timestamp_seconds{topic=~\"${base}-${topic_instance}__dlq\"})",
          "legendFormat": "{{topic}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      }
    }
  ]
}
```

- [ ] **Step 3: Add the service**

Apply to `docker-compose.yml`:

```diff
--- a/docker-compose.yml
+++ b/docker-compose.yml
@@ -1,6 +1,6 @@
 # Local Kafka playground: one KRaft broker, one-shot topic jobs, kcat consumers,
-# a small producer page, Redpanda Console, Pipeline Studio, topic owners and
-# Prometheus. No auth, no TLS, nothing persisted.
+# a small producer page, Redpanda Console, Pipeline Studio, topic owners,
+# Prometheus and Grafana. No auth, no TLS, nothing persisted.
 # Containers reach the broker at kafka:19092; the host at localhost:9092.
 
 # One-shot job: create a topic, exit 0 (also when it already exists).
@@ -215,3 +215,32 @@
       test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1:9090/-/ready"]
       interval: 2s
       retries: 30
+
+  # Grafana: the topic-owners dashboard, provisioned from files; anonymous
+  # admin, nothing phones home. http://localhost:3000
+  grafana:
+    image: grafana/grafana:13.2.3
+    depends_on:
+      prometheus: {condition: service_healthy}
+    ports:
+      - "127.0.0.1:3000:3000"
+    volumes:
+      # the two subdirectories, not the whole tree: the image's other provisioning
+      # directories must stay, or Grafana logs an error for each one it cannot open
+      - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro
+      - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro
+      - ./grafana/dashboards:/var/lib/grafana-dashboards:ro
+    environment:
+      GF_AUTH_ANONYMOUS_ENABLED: "true"
+      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
+      GF_AUTH_DISABLE_LOGIN_FORM: "true"
+      GF_ANALYTICS_REPORTING_ENABLED: "false"
+      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
+      GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES: "false"
+      GF_ANALYTICS_FEEDBACK_LINKS_ENABLED: "false"
+      GF_NEWS_NEWS_FEED_ENABLED: "false"
+      GF_PLUGINS_PREINSTALL_DISABLED: "true"
+    healthcheck:
+      test: ["CMD", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:3000/api/health"]
+      interval: 2s
+      retries: 30
```

- [ ] **Step 4: Start it and check it**

Run:

```sh
docker compose config --quiet && make up
curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:3000/api/dashboards/uid/topic-owners
curl -sS http://localhost:3000/api/datasources/uid/prometheus/health; echo
docker compose logs grafana 2>&1 | grep -c 'level=error'
```

Expected:
- `make up` lists `grafana` as `Healthy`. Its URL line gains `Grafana:` only in Task 3.
- `200`.
- JSON with `"message":"Successfully queried the Prometheus API."`.
- `0`.

- [ ] **Step 5: Check every dashboard query against Prometheus**

First give the retry and DLQ rows something to show:
- park one record that goes straight to the DLQ (`studio-attempt=3`, and `orders-1__retry` has `MAX_ATTEMPTS: 3`);
- park one that is redelivered after 0.5 s.

Then run each query with `base=orders` and `topic_instance=1`, filled in the way Grafana fills a multi-value variable (`(orders)`):

```sh
echo '{"id":"demo"}' | make kcat args='-P -t orders-1__retry -k demo -H studio-attempt=3'
echo '{"id":"r"}' | make kcat args='-P -t orders-1__retry -k r -H studio-attempt=1 -H studio-backoff-ms=500'
sleep 15
python3 - grafana/dashboards/topic-owners.json <<'EOF'
import json, sys, urllib.parse, urllib.request
empty = []
for p in json.load(open(sys.argv[1]))["panels"]:
    for t in p.get("targets", []):
        q = t["expr"].replace("${base}", "(orders)").replace("${topic_instance}", "(1)")
        n = len(json.load(urllib.request.urlopen("http://localhost:9090/api/v1/query?" + urllib.parse.urlencode({"query": q})))["data"]["result"])
        print(f'{p["title"]:34} {t["refId"]} {n}')
        if n == 0 and p["title"] != "Lag per group":
            empty.append(p["title"])
sys.exit(f"no series: {empty}" if empty else 0)
EOF
```

Expected: 13 lines, and every count except `Lag per group` is at least 1. Lag per group is 0 because nothing consumes `orders-1`. `Firing alerts` counts `TopicDLQGrowing` for `orders-1__dlq`, so http://localhost:9090/alerts shows it too. The script exits 0.

Open http://localhost:3000/d/topic-owners?var-base=orders&var-topic_instance=1. It shows the alert table, the rows `main: orders-1`, `retry: orders-1__retry` and `dlq: orders-1__dlq`, and series in every panel but lag per group.

- [ ] **Step 6: Commit**

```bash
git add grafana docker-compose.yml
git commit -m "topic owners M3: Grafana — provisioned Prometheus datasource and the topic-owners dashboard (firing alerts, then a row per role)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: `verify-topics` step 6

**Files:**
- Modify: `Makefile`: the header comment, `GRAFANA_URL`, the `up` line, and `verify-topics` (its help text and step 6)

**Interfaces:**
- Consumes:
  - Task 1's rules, and Task 2's Grafana, datasource and dashboard;
  - `verify-topics` step 5, which leaves two records parked in `owner-verify-1__dlq` after Prometheus has a sample of the empty DLQ, so `increase()` has a base;
  - `PROMETHEUS_URL` and `COMPOSE`.
- Produces: `GRAFANA_URL ?= http://localhost:3000`. Step 6 prints `topics alerts: 4 rules, TopicDLQGrowing firing for owner-verify-1__dlq; Grafana: dashboard topic-owners, datasource healthy` before `TOPICS OK`.

- [ ] **Step 1: Check that step 6 is missing**

Run: `make verify-topics 2>&1 | tail -2`

Expected: `topics worker: …` then `TOPICS OK`, with no `topics alerts:` line.

- [ ] **Step 2: Add step 6**

Apply to `Makefile`. The recipe lines start with a tab.

```diff
--- a/Makefile
+++ b/Makefile
@@ -1,5 +1,5 @@
 # Kafka Playground: a local Kafka sandbox (KRaft broker, topic jobs, kcat consumers,
-# producer page, Redpanda Console, Pipeline Studio, topic owners, Prometheus).
+# producer page, Redpanda Console, Pipeline Studio, topic owners, Prometheus, Grafana).
 # Typical use: up → produce → logs → scale → groups → down. verify checks it all end to end.
 SERVICE = Kafka Playground
 
@@ -9,6 +9,7 @@
 CONSOLE_URL ?= http://localhost:8080
 STUDIO_URL ?= http://localhost:8082
 PROMETHEUS_URL ?= http://localhost:9090
+GRAFANA_URL ?= http://localhost:3000
 KAFKA_BIN = $(COMPOSE) exec -T kafka /opt/kafka/bin
 BOOTSTRAP = --bootstrap-server localhost:19092
 topic ?= orders
@@ -40,7 +41,7 @@
 
 up: ## [STEP 1] Start everything and wait until it is healthy
 	$(COMPOSE) up -d --wait
-	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Studio: $(STUDIO_URL)   Prometheus: $(PROMETHEUS_URL)   Broker from the host: localhost:9092"
+	@echo "Producer page: $(PRODUCER_URL)   Console: $(CONSOLE_URL)   Studio: $(STUDIO_URL)   Prometheus: $(PROMETHEUS_URL)   Grafana: $(GRAFANA_URL)/d/topic-owners   Broker from the host: localhost:9092"
 
 # Studio nodes and topic-owner one-offs (docker compose run) are not services:
 # remove them first, or the network cannot go. -v removes the anonymous volumes
@@ -248,7 +249,7 @@
 
 # Runs owners of its own base, owner-verify, as one-offs of the orders-1 service
 # (same image, network and healthcheck; labels and environment overridden).
-verify-topics: up ## Check the topic owners end to end: refusals, reconcile, metrics, the redelivery worker and the DLQ; cleans up its containers, topics and group
+verify-topics: up ## Check the topic owners end to end: refusals, reconcile, metrics, the redelivery worker and the DLQ, alerts and Grafana; cleans up its containers, topics and group
 	@trap 'docker rm -f owner-verify-refused owner-verify-1 owner-verify-1__retry owner-verify-1__dlq >/dev/null 2>&1; $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --delete --topic "owner-verify-1(__retry|__dlq)?" >/dev/null 2>&1; $(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --delete --group owner-verify-1__redelivery >/dev/null 2>&1' EXIT; \
 	own() { n=$$1 r=$$2 p=$$3; shift 3; docker rm -f "$$n" >/dev/null 2>&1; \
 		out=$$($(COMPOSE) run -d --no-deps --name "$$n" -l topic-owner.base=owner-verify -l topic-owner.instance=1 -l topic-owner.role="$$r" \
@@ -315,6 +316,20 @@
 		{ echo "TOPICS FAILED: want a back on owner-verify-1 with its studio-origin, b and d (bad header) on the DLQ: main $$main dlq $$dlq"; exit 1; }; \
 	echo "$$main$$dlq" | grep -q '"key":"c"' && { echo "TOPICS FAILED: c (studio-group set) was forwarded: $$main $$dlq"; exit 1; }; \
 	echo "topics worker: a redelivered, b and d dead-lettered, c left to its Studio loop, 2 parked"; \
+	out=$$($(COMPOSE) exec -T prometheus promtool check config /etc/prometheus/prometheus.yml 2>&1) || { echo "TOPICS FAILED: promtool check config: $$out"; exit 1; }; \
+	out=$$($(COMPOSE) exec -T prometheus promtool check rules /etc/prometheus/rules.yml 2>&1) || { echo "TOPICS FAILED: promtool check rules: $$out"; exit 1; }; \
+	for i in $$(seq 30); do \
+		rules=$$(curl -sS $(PROMETHEUS_URL)/api/v1/rules?type=alert); \
+		[ "$$(echo "$$rules" | grep -o '"health":"ok"' | wc -l | tr -d ' ')" = 4 ] && \
+			curl -sS $(PROMETHEUS_URL)/api/v1/alerts | grep -q '"alertname":"TopicDLQGrowing","topic":"owner-verify-1__dlq"},"annotations":{[^}]*},"state":"firing"' && break; \
+		[ "$$i" = 30 ] && { echo "TOPICS FAILED: want 4 healthy alert rules and TopicDLQGrowing firing for owner-verify-1__dlq: rules $$rules alerts $$(curl -sS $(PROMETHEUS_URL)/api/v1/alerts)"; exit 1; }; sleep 1; \
+	done; \
+	for p in /api/health /api/dashboards/uid/topic-owners; do \
+		curl -sSf -o /dev/null $(GRAFANA_URL)$$p || { echo "TOPICS FAILED: Grafana $$p"; exit 1; }; \
+	done; \
+	out=$$(curl -sS $(GRAFANA_URL)/api/datasources/uid/prometheus/health); \
+	echo "$$out" | grep -q 'Successfully queried the Prometheus API' || { echo "TOPICS FAILED: Grafana's Prometheus datasource: $$out"; exit 1; }; \
+	echo "topics alerts: 4 rules, TopicDLQGrowing firing for owner-verify-1__dlq; Grafana: dashboard topic-owners, datasource healthy"; \
 	echo "TOPICS OK"
 
 verify-ui: up studio/ui/.chromium ## Check the studio UI in Chromium (Playwright); installs Chromium once
```

- [ ] **Step 3: Run it**

Run: `make verify-topics 2>&1 | tail -3`

Expected:

```
topics worker: a redelivered, b and d dead-lettered, c left to its Studio loop, 2 parked
topics alerts: 4 rules, TopicDLQGrowing firing for owner-verify-1__dlq; Grafana: dashboard topic-owners, datasource healthy
TOPICS OK
```

- [ ] **Step 4: Check that step 6 fails on a broken rule**

Run:

```sh
cp prometheus/rules.yml /tmp/rules.yml.bak
sed -i '' 's/> 100/>> 100/' prometheus/rules.yml
make verify-topics 2>&1 | tail -3
cp /tmp/rules.yml.bak prometheus/rules.yml && git diff --exit-code prometheus/rules.yml
```

Expected: `TOPICS FAILED: promtool check rules:` with `could not parse expression`, then a clean `git diff`. The running Prometheus keeps the rules it loaded at start, so only `promtool` sees the change.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "topic owners M3: verify-topics step 6 — promtool, the four rules healthy, TopicDLQGrowing firing for owner-verify-1__dlq, Grafana's dashboard and datasource; make up prints the Grafana URL

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Documentation and the full check

**Files:**
- Modify: `README.md`, `AGENTS.md`, `docs/superpowers/specs/2026-10-08-topic-containers-design.md`

**Interfaces:**
- Consumes: Tasks 1–3 as built: the alert names and durations, the dashboard's rows and panels, the URLs, and the two provisioning mounts.
- Produces: the docs match the behaviour. The spec's status says M3 is built.

- [ ] **Step 1: README**

Apply to `README.md`:

```diff
--- a/README.md
+++ b/README.md
@@ -2,7 +2,7 @@
 
 A local Kafka sandbox. Produce JSON from a browser and watch partitions, consumer groups and fan-out. Pipeline Studio adds a canvas where you draw producer → topic → consumer flows and run them.
 
-Everything is local: no auth, no TLS, every port on `127.0.0.1`. `make down` deletes the Kafka data and the metrics; Studio flows are files in `flows/` and stay.
+Everything is local: no auth, no TLS, every port on `127.0.0.1`. `make down` deletes the Kafka data, the metrics and Grafana's state; Studio flows are files in `flows/` and stay.
 
 | Service | What it is | Where |
 |---|---|---|
@@ -13,7 +13,8 @@
 | `producer` | producer page (`producer/`, Go) | http://localhost:8081 |
 | `studio` | Pipeline Studio (`studio/`, Go + React Flow) | http://localhost:8082 |
 | `orders-1`, `orders-1__retry`, `orders-1__dlq` | topic owners: each creates its topic and keeps it in the desired state (`topic-owner/`, Go) | `make owners` |
-| `prometheus` | Prometheus: scrapes the topic owners | http://localhost:9090 |
+| `prometheus` | Prometheus: scrapes the topic owners and evaluates their alert rules | http://localhost:9090 |
+| `grafana` | Grafana: the topic-owners dashboard | http://localhost:3000/d/topic-owners |
 | `console` | Redpanda Console: topics, messages, groups | http://localhost:8080 |
 
 ## Quick start
@@ -84,7 +85,7 @@
 - It never lowers partitions or changes the replication factor. Such a difference makes the container unhealthy and says why: `docker inspect --format '{{json .State.Health.Log}}' orders-1`, or `make logs svc=orders-1`.
 - `make up` waits until every topic is in its desired state. So `depends_on: {orders-1: {condition: service_healthy}}` guarantees a consumer its topic, as a finished topic job does.
 - A config changed by hand, in Console or with `kafka-configs.sh`, is set back. To change one, change the container's environment and recreate it.
-- It serves `/metrics` for its topic on port 9000 inside the network. Prometheus (http://localhost:9090) finds the containers by their labels, with no target list.
+- It serves `/metrics` for its topic on port 9000 inside the network. Prometheus (http://localhost:9090) finds the containers by their labels, with no target list. Grafana shows them (below).
 - `make owners` lists them. `make query q='kafka_topic_partitions'` asks Prometheus. `make kcat args='-C -t orders-1 -o beginning -e -J'` runs kcat on the compose network (default `-L`).
 
 The retry container also runs the redelivery worker (below).
@@ -227,6 +228,26 @@
 - Records parked in a DLQ: `sum by (topic) (kafka_topic_partition_current_offset{topic=~".+__dlq"} - kafka_topic_partition_oldest_offset{topic=~".+__dlq"})`.
 - Age of the oldest parked record, in seconds: `time() - min by (topic) (topic_owner_oldest_message_timestamp_seconds)`.
 
+### Dashboard and alerts
+
+Grafana at http://localhost:3000/d/topic-owners shows the `Topic owners` dashboard. Anyone can open it, as admin, without logging in. Choose `base` and `topic_instance` at the top (both default to All). It has:
+
+- a table of the alerts firing now;
+- **main:** messages in per second, partitions, log size, lag per group;
+- **retry:** waiting, redeliveries per second, moves to the DLQ per second by reason, backoff p50 and p95, skipped per second;
+- **dlq:** parked records, and the age of the oldest one.
+
+The dashboard and its datasource are files: `grafana/dashboards/topic-owners.json` and `grafana/provisioning/`. Grafana does not save changes made in the UI, and `make down` removes everything it stored. To change a panel, edit the JSON. Grafana reloads it within 10 s.
+
+Prometheus evaluates the alert rules in `prometheus/rules.yml` every 5 s. Firing alerts show at http://localhost:9090/alerts and in the dashboard's table. There is no Alertmanager, so nothing is sent anywhere.
+
+| Alert | Fires when |
+|---|---|
+| `TopicConsumerLagHigh` | a group's lag on a main topic stays above 100 for 2 minutes |
+| `TopicDLQGrowing` | a record was parked in a DLQ in the last 10 minutes; it keeps firing for 10 minutes after the last one, also after the topic is gone (`make verify` leaves one for `owner-verify-1__dlq`) |
+| `TopicRetryWaiting` | records wait in a retry topic for 5 minutes: the worker is down, or a Studio retry loop stopped |
+| `TopicOwnerUnhealthy` | for 1 minute, a topic owner is not scraped, or its topic is not in its desired state |
+
 Bytes in and out per topic are not exported. Only the broker's JMX has them, and the JMX agent would need a jar and a change to the `kafka` service.
 
 Prometheus reads the Docker socket with `group_add: ["0"]`, because Docker Desktop shows the socket as `root:root 0660` inside containers. On native Linux, use the host's docker gid instead. The socket gives root on the host, which is one more reason everything stays on `127.0.0.1`.
```

- [ ] **Step 2: AGENTS.md**

Apply to `AGENTS.md`:

```diff
--- a/AGENTS.md
+++ b/AGENTS.md
@@ -4,12 +4,13 @@
 
 ## Layout
 
-- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console, topic owners (`x-topic-owner`, one block of three services per instance), Prometheus.
+- `docker-compose.yml`: broker, topic jobs (`x-topic` anchor), kcat consumers (`x-consumer`), producer page, Pipeline Studio, Console, topic owners (`x-topic-owner`, one block of three services per instance), Prometheus, Grafana.
 - `producer/`: producer page. Go (franz-go), `main.go` plus embedded `index.html`, multi-stage `Dockerfile` onto `scratch`.
 - `studio/`: Pipeline Studio. Go control plane (`main.go`, `api.go`, `flow.go`, `store.go`, `resolve.go`, `engine.go`, `stream.go`, `docker.go`, `kafka.go`, `transform.go`, `router.go`) and node runner (`node.go`, with its retry loop and failure path in `retry.go`, run as `studio node` in one container per producer, consumer and consumer instance), embedding the React Flow UI built from `studio/ui/` (`go:embed all:ui/dist`; keep `ui/dist/.gitkeep`).
 - `studio/ui/e2e/`: the UI tests (Playwright, `playwright.config.ts` beside them): `flows.ts` (the API and flow parts), `locators.ts`, `studio.ts` (the fixture), and one `*.spec.ts` per area. `make verify-ui` runs them against the running stack.
 - `topic-owner/`: topic owner. Go (franz-go, client_golang): `config.go` (environment, name rules, role defaults), `reconcile.go` (the desired-state diff and its loop), `metrics.go` (`/metrics`), `redeliver.go` (the retry role's redelivery worker), `main.go` (`/healthz`, `-healthcheck`); multi-stage `Dockerfile` onto `scratch`. It runs as one container per topic.
-- `prometheus/`: `prometheus.yml`, bind-mounted read-only into the `prometheus` service.
+- `prometheus/`: `prometheus.yml` and `rules.yml` (the alert rules), bind-mounted read-only into the `prometheus` service.
+- `grafana/`: `provisioning/` (the Prometheus datasource and the dashboard provider) and `dashboards/topic-owners.json`, bind-mounted read-only into the `grafana` service.
 - `flows/`: Studio flow files, bind-mounted into the `studio` container; `flows/0a1b2c3d.json` is the example.
 - `Makefile`: day-to-day commands; `make test` runs the static checks and unit tests, `make verify` the end-to-end test (`verify-studio` deletes its flows and its `studio-verify…` topics when it exits, and `verify-topics` its `owner-verify…` containers, topics and group; name a new test topic or group in that trap too).
 
@@ -41,7 +42,7 @@
 - Studio node containers are not compose services: they carry the `studio.flow` label, and `make down` removes them before `docker compose down` (the network cannot go while they are attached). So does a topic owner started with `docker compose run` (labels `topic-owner.role` and `com.docker.compose.oneoff=True`), which `docker compose down` leaves behind. Keep that line in `down`; use `make nodes` to see them. `down` passes `-v`: the `kafka` and `prometheus` images declare anonymous volumes that would otherwise stay.
 - A topic owner's service, `container_name` and topic are the same string: `<base>-<instance>`, plus `__retry` or `__dlq` (the suffixes Studio uses). The name rules live in `topic-owner/config.go`, on top of Studio's `topicNameRe` and `maxInputTopic`. Its labels are `topic-owner.base`, `topic-owner.instance` and `topic-owner.role`, never `studio.flow`. Prometheus finds owners by `topic-owner.role`, so a new instance needs no Prometheus change: add one block of three services with their own anchors (README, "Add an instance").
 - The redelivery worker's header contract is Studio's (`studio/retry.go`) plus `studio-backoff-ms` and `studio-first-failure` (`topic-owner/redeliver.go`). A change to a header's name or format changes both files, the README table and both specs together. The worker owns the records without `studio-group`; its group `<base>-<instance>__redelivery` is franz-go only.
-- A topic owner's series that mean what kafka-exporter's mean keep its names and labels; the rest are `topic_owner_*`. `verify-topics` greps metric names and log lines; renaming one changes it too. `verify-topics` owns the `owner-verify` prefix: don't give anything else that name.
+- A topic owner's series that mean what kafka-exporter's mean keep its names and labels; the rest are `topic_owner_*`. `prometheus/rules.yml`, `grafana/dashboards/topic-owners.json` and `verify-topics` quote metric names (`verify-topics` also log lines and the alert `TopicDLQGrowing`); renaming one changes all three. Grafana does not save UI edits (`allowUiUpdates: false`): change the dashboard in its JSON, and check every query against a running stack. `verify-topics` owns the `owner-verify` prefix: don't give anything else that name.
 - Makefile: GNU make 3.81 on macOS with BSD tools (no `timeout`, no `base64 -w0`, no `sed -i` without `''`). Recipes use real tabs. Follow the existing style: `SERVICE`, `## ` help comments, `# ── Section ──` rules, lower-case `arg ?= default`. Pass user text to the shell as `$(call shq,$(value var))`.
 - Keep README.md, and the spec when behaviour departs from it, in sync with any behaviour change.
 
```

- [ ] **Step 3: The spec**

Apply to `docs/superpowers/specs/2026-10-08-topic-containers-design.md`:

```diff
--- a/docs/superpowers/specs/2026-10-08-topic-containers-design.md
+++ b/docs/superpowers/specs/2026-10-08-topic-containers-design.md
@@ -1,8 +1,9 @@
 # Topic containers — design
 
-Status: approved on 2026-10-08. M1 and M2 are built, from
-`docs/superpowers/plans/2026-10-08-topic-containers-m1.md` and
-`docs/superpowers/plans/2026-10-08-topic-containers-m2.md`; M3 follows.
+Status: approved on 2026-10-08. M1, M2 and M3 are built, from
+`docs/superpowers/plans/2026-10-08-topic-containers-m1.md`,
+`docs/superpowers/plans/2026-10-08-topic-containers-m2.md` and
+`docs/superpowers/plans/2026-10-08-topic-containers-m3.md`.
 
 A topic deployed as three long-running containers on the playground's broker:
 `orders-1`, `orders-1__retry` and `orders-1__dlq`. Each container owns one
@@ -723,6 +724,10 @@
 | `TopicRetryWaiting` | `sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~".+__retry"}) > 0` | 5m | records sit in a retry topic longer than any sensible backoff: the worker is down, or a Studio loop stopped (3.5) |
 | `TopicOwnerUnhealthy` | `up{job="topic-owners"} == 0 or topic_owner_reconciled == 0` | 1m | a container is down or its topic drifted beyond repair |
 
+`TopicDLQGrowing` keeps firing for 10 minutes after the last parked record,
+also after its topic is deleted: `increase()` still sees the samples inside
+its window (M3). `make verify` leaves it firing for `owner-verify-1__dlq`.
+
 No Alertmanager. On a laptop there is nothing to route to, and the alerts page
 (http://localhost:9090/alerts) plus a dashboard panel on `ALERTS` show the
 state. Adding one later is an `alerting:` block in `prometheus.yml` and a
@@ -876,7 +881,8 @@
     ports:
       - "127.0.0.1:3000:3000"
     volumes:
-      - ./grafana/provisioning:/etc/grafana/provisioning:ro
+      - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro
+      - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro
       - ./grafana/dashboards:/var/lib/grafana-dashboards:ro
     environment:
       GF_AUTH_ANONYMOUS_ENABLED: "true"
@@ -914,6 +920,10 @@
 
 - **No volume.** It declares none (lab), so its SQLite database lives in the
   container layer and goes with it.
+- **Two provisioning mounts.** Mounting all of `/etc/grafana/provisioning`
+  hides the image's empty `plugins/` and `alerting/` directories, and Grafana
+  logs an error for each (M3). The service mounts `datasources/` and
+  `dashboards/` only.
 - **Phone-home.** `GF_PLUGINS_PREINSTALL_DISABLED` stops Grafana 13 from
   downloading about 18 plugins from grafana.com at start (lab). The bundled
   Prometheus datasource still works.
@@ -968,13 +978,16 @@
   `path: /var/lib/grafana-dashboards` and `allowUiUpdates: false`.
 - **`grafana/dashboards/topic-owners.json`:** uid `topic-owners`, title
   `Topic owners`. It has variables `base` and `topic_instance`, from
-  `label_values(topic_owner_info, …)`, and a row per role:
+  `label_values(topic_owner_info, …)` (multi-value, default All). A table of
+  firing `ALERTS` sits on top, then a row per role:
   - **main:** messages in/s, partitions, log size, lag per group;
   - **retry:** waiting, redeliveries/s, moves to the DLQ/s by reason, backoff p50 and p95, skipped/s;
-  - **dlq:** parked, age of the oldest record;
-  - **all:** a table of firing `ALERTS`.
+  - **dlq:** parked, age of the oldest record.
 
-  The section 4.2 expressions, word for word.
+  The section 4.2 expressions, word for word, plus one matcher that picks the
+  role's topic from the variables, `topic=~"${base}-${topic_instance}__retry"`
+  (braces, because `$topic_instance__retry` would name another variable).
+  Moves to the DLQ are summed `by (topic, reason)`.
 
 Alerts stay in Prometheus rule files, not Grafana alert provisioning. A
 Prometheus rule is three lines, while Grafana's provisioned rule is a query
@@ -1267,6 +1280,9 @@
 6. **Grafana anonymous `Admin`** is deprecated in 13 (it logs a warning) but
    works. If a later Grafana drops it, use `Viewer` plus
    `GF_USERS_VIEWERS_CAN_EDIT=true` for Explore; that pairing is unverified.
+   **Checked in M3, on 13.2.3:** the warning reads `auth.anonymous.org_role is
+   deprecated, only viewer role is supported`, yet an anonymous request created
+   a folder (`"canAdmin":true`). Admin still works.
 7. **Bytes in and out.** If they are wanted later, the exact change is:
    - a custom broker image or a bind-mounted `jmx_prometheus_javaagent-1.7.0.jar`
      (GitHub asset; sha256
```

- [ ] **Step 4: The full check**

Run:

```sh
make test
make down && make verify
make down
docker ps -aq -f label=studio.flow; docker ps -aq -f label=topic-owner.role; git status --short flows
docker volume ls -q -f label=com.docker.compose.project=kafka-playground
```

Expected:
- `make test` exits 0.
- `make verify` ends with `STUDIO OK`, `UI OK`, the six `topics …` lines, `TOPICS OK` and `VERIFY OK`.
- After `make down`, the last two commands print nothing.

- [ ] **Step 5: Commit**

```bash
git add README.md AGENTS.md docs/superpowers/specs/2026-10-08-topic-containers-design.md
git commit -m "docs: topic owners M3 — README's dashboard and alerts, AGENTS layout and rules for grafana/ and rules.yml, spec status and M3 findings

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
