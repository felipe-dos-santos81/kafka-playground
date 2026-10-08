# Pipeline Studio Router node — design

Status: approved in brainstorming on 2026-10-07; built from
`docs/superpowers/plans/2026-10-07-studio-router.md`. The Studio spec
(`docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`) stays the
authority for everything this does not change; this document adds a node type
to it and, once built, its sections are updated to match (§9 below).

## 1. Goal

Let a flow send each record to one of several topics by its content. A Router
sits after a consumer (or after its Transform) and forwards each record to the
topic of the first rule whose condition holds, to a default topic, or nowhere.

```
consumer-1 ─▶ transform-1 ─▶ router-1 ─┬─▶ big-orders     #1 when: msg.total > 100
                                        ├─▶ eu-orders      #2 when: msg.region == "EU"
                                        └─▶ other-orders   default
```

Agreed in brainstorming:

- **Ordered rules, first match wins.** Not one expression returning a topic name,
  not a fan-out to every match.
- **Transform, then Router.** A consumer may run consumer → router or
  consumer → transform → router; the router sees the transformed value. No
  router before a transform, no transform per branch.
- **Rules live on the Router node** (approach A), not on edges: the flow file's
  edges stay `{id, source, target}`.

Out of scope: fan-out to several topics, a transform per branch, routers in
series, per-branch keys or headers, a router outside a consumer.

## 2. Data model

A new node type `router`:

```json
{ "id": "router-1", "type": "router", "position": { "x": 0, "y": 0 },
  "data": { "rules": [ { "when": "msg.total > 100", "to": "topic-2" },
                       { "when": "msg.region == \"EU\"", "to": "topic-3" } ],
            "default": "topic-4" } }
```

- `rules`: ordered; `when` is an expr-lang condition over `msg` that yields a
  boolean; `to` is the id of a topic node.
- `default`: a topic node id, or absent/empty for none.
- `studio/flow.go` (node types, data structs, `allowedEdges`),
  `studio/ui/src/flow/schema.ts` and `studio/ui/src/nodes/types.ts` gain the type
  together (AGENTS.md).

Edge table additions: `consumer → router`, `transform → router`,
`router → topic`. The existing pairs stay.

## 3. Validation (`studio/flow.go`)

- **Save:** like every node's, only the node and edge checks (ids, type,
  allowed pairs); a half-built router saves, and its data is checked on deploy.
- **Deploy**, each a 422 naming the router (and the rule, as `rule <i>: …`):
  - exactly one edge in, from a consumer or a transform; at least one edge out
    (`a router needs at least one edge to a topic`);
  - at least one rule;
  - every `when` is non-empty and compiles as a boolean (`expr.AsBool()`, in
    the same environment as a Transform: `msg` declared `any`); a compile error
    keeps its first line, e.g. `rule 2: when: unexpected token`;
  - every rule's `to`, and `default` when set, is a topic node the router has an
    edge to; a rule with no topic picked is refused as `rule <i>: pick a topic`;
  - every edge out of the router is some rule's `to` or the default (a dangling
    branch is refused, not ignored);
  - a transform before the router keeps its existing rule (one edge in, one out);
  - the forward-loop check follows the router's edges like any other, so a
    route back to the consumer's own topic is refused.

## 4. Resolve and the node runner

`Resolve` folds the router into its consumer, as it folds a Transform: the
consumer's `NodeSpec` gains `Routes []Route{When, Topic}` (rule order, topic
names), `RouteDefault` (a topic name or empty) and `RouterNode` (the node id
its counts are reported under). `Forward` stays empty when a router decides.

Per record in the consumer (`node.go`): tail → http sink → transform (if any) →
router → forward.

- `msg` is the value (after the transform) decoded from JSON exactly as the
  Transform decodes it: integers exact, other numbers float64.
- Rules are tried in order; the first `when` that is true picks its topic.
  None true: the default topic, or the record is dropped and counted as
  unmatched (not an error).
- A value that is not JSON, or a runtime error in a condition (`nil > 100`), is a
  router error, counted on the Router node and as its consumer's error
  (`router: …`, as a transform's failure is): the record is not forwarded, later
  rules are not tried, and the record still commits (at-most-once, like a failed
  transform or forward).
- The forward keeps the record's key and carries the (transformed) value; a
  failed forward counts on the consumer, as today.
- The consumer compiles the rules when it starts and reuses one VM, as the
  Transform does.

## 5. Counts and the snapshot

- A consumer with a router reports, in `/stats`,
  `route: {total, errors, lastError, branches, unmatched}`: `total` every record
  the router got, `branches` one count per rule plus one for the default (always
  present, 0 without a default) of the records each picked, whether or not the
  forward then succeeded (a failed forward counts on the consumer), `unmatched`
  the dropped ones (left out at 0). A transform's `step` is the same shape
  without the two router fields.
- The snapshot gives the Router node its consumer's state and these counts,
  summed over the consumer's instances (the last error prefixed `#<i>: ` from an
  instance, like every node), and a `boot` joining theirs, as for a Transform.
- `NodeState` (`engine.go`) and `NodeRuntime` (`api.ts`) gain `branches`
  (array) and `unmatched` together, as the last fields before `instances`, so
  `verify-studio`'s field-order greps keep working.

## 6. UI

- The palette offers Router. Its node summary: `3 rules · default other-orders`
  or `3 rules · no default` (topic names, not ids).
- Each edge out of a router carries a label derived from the router's data,
  never saved: `#1`, `#2`, `default` (joined, `#1, default`, when several send to
  one topic); while the flow runs, with the branch count: `#1 · 80`, `default · 2`.
- Drawing an edge from a router to a topic adds a rule `{when: "", to: <topic>}`,
  unless a rule or the default already sends there; Deploy refuses the empty
  condition until it is filled in. Deleting such an edge keeps the rule; the
  Inspector marks it "not wired" (and Deploy refuses it), and drawing the edge
  again wires it back rather than adding a second rule.
- Inspector (router): per rule a "When" textarea and a "Topic" select of the
  topics the router is wired to (by name), with move up, move down and remove;
  then "Add rule" and a "Default" select ("none: drop the record" or a wired
  topic). Hint:
  "First match wins. Without a default, records no rule matches are dropped and
  counted." Every control has a label tied to it.
- The Router node's runtime line: `120 msgs · 3.0/s · 2 errors · 5 unmatched`;
  its last error is the node's tooltip.

## 7. Tests

- **Go:**
  - `flow_test`: one case per §3 rule (wiring, empty or non-boolean `when`, a
    `to` with no edge, an edge with no rule, a loop through a router), and Save
    accepting a half-built router;
  - `resolve_test`: consumer → transform → router folds into one spec with
    `Routes`, `RouteDefault` and `RouterNode`;
  - `node_test`: first match wins; default; unmatched dropped and counted; a
    failing condition stops routing that record; the key is kept and the value
    is the transformed one;
  - `engine_test`: branch counts and `unmatched` summed across instances.
- **`verify-studio`:** a flow producer (manual) → topic → consumer → router
  (`msg.total > 100` → big, default → other) → two topics → two consumers; a big
  and a small record each reach their own consumer; the Router node shows
  `"total":2` and `"branches":[1,1]`; a router whose rule does not compile is a
  422 on deploy.
- **UI suite** (`studio/ui/e2e/`): building a router flow in the editor (drag
  from the palette, wire router → two topics, edges add rules, Deploy refuses
  the empty conditions, fill and save, deploy); and, on an API-created router
  flow, the edge labels while running after two sends from the producer's
  drawer: `#1 · 1` and `default · 1`.

## 8. Errors at a glance

| Condition | Result |
|---|---|
| a `when` that does not compile or is not boolean | 422 on deploy, naming the router and rule |
| a `to`/default with no edge, or an edge with no rule | 422 on deploy |
| value not JSON, or a condition fails at run time | router error; record not forwarded; commits |
| no rule matches, no default | dropped, counted as unmatched |
| forward fails | consumer error (as today); commits |

## 9. Documentation

- `README.md`: a Router bullet under Nodes (rules, default, unmatched, the
  labels on its edges).
- The Studio spec: §3.4 (the per-record path), §3.6 (the router's counts in the
  snapshot), §4.2 (node data), §4.3 (edge table), and a §7 "Router" milestone
  recording what was built as decided.
- `AGENTS.md`: the router, like the transform, runs inside its consumer; its
  counts arrive as `/stats` `route` and are put on the Router node by the
  snapshot.
- The UI tests spec (`2026-10-07-studio-ui-tests-design.md`) §5: the two new
  tests.

## 10. Assumptions

1. `expr.AsBool()` refuses at compile time an expression whose type is known not
   to be boolean, and with `msg` declared `any`, an expression of unknown type
   compiles and is checked at run time (a non-boolean result is then a router
   error). The plan confirms both through Context7 (`/expr-lang/expr`) and a
   probe.
2. React Flow's built-in edge `label` is enough for the branch labels; no custom
   edge component is needed.
