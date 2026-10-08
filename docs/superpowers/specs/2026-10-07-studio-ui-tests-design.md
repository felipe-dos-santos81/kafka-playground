# Pipeline Studio UI tests — design

Status: approved in brainstorming on 2026-10-07; implementation plan to follow
in `docs/superpowers/plans/`.

## 1. Goal and scope

Pipeline Studio (M1–M6, `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md`)
has Go unit tests and an API-level end-to-end check (`make verify-studio`), but
nothing exercises the React UI. Four M6 behaviours have only been checked by
reading the code. This adds a browser test suite that drives the real UI
against the running stack, and puts it in `make verify`.

In scope: the editor (palette, wiring, Inspector, Save, Deploy, Stop, the flow
list), node types and the tail drawer (Transform, instances), and the M6 live
view (paused and recovered, a deleted flow, a node warning, tail scroll).

Out of scope: visual regression, other browsers than Chromium, component unit
tests, CI. The suite runs locally, like everything in this repository.

## 2. Constraints

- `@playwright/test` pinned exactly at `1.63.0` (the release current on
  2026-10-07) as a dev dependency of `studio/ui`; Chromium only, installed with
  `npx playwright install chromium`. `@types/node` is declared at `24.19.1`,
  the version vite already installs, so the tests' type-check does not rely on
  a transitive package. No other new dependency.
- The suite runs on the host, against the stack `make up` starts, at
  `STUDIO_URL` (default `http://localhost:8082`). Tests that stop containers
  call `docker` on the host.
- No change to Go code, the API, the images or `docker-compose.yml`. The UI
  changes by the `data-testid` attributes in §4, and by two fixes the tests
  surfaced (§9): the Inspector's labels are tied to their controls, and a new
  flow's canvas no longer zooms in on its first node.
- Local-only, as the rest of the repository: no auth, nothing leaves the host.
- Makefile style as in `AGENTS.md` (GNU make 3.81, BSD tools, real tabs, `##`
  help, `$$`).

## 3. Harness

```
make verify-ui
  └─ npm ci and Chromium, each only when the lockfile changed
     └─ STUDIO_URL=… KAFKA_BOOTSTRAP=… npx playwright test   (host, one worker)
          ├─ chromium → $STUDIO_URL
          ├─ Playwright request → $STUDIO_URL/api   (set up and clean up flows)
          └─ execFileSync docker …              (stop/start studio, SIGSTOP/CONT a node,
                                                 delete topics and groups)
```

- `studio/ui/playwright.config.ts`: `testDir: 'e2e'`, `workers: 1`,
  `timeout: 60_000`, `reporter: 'list'`,
  `use: { baseURL, trace: 'retain-on-failure', screenshot: 'only-on-failure' }`,
  `outputDir: 'test-results'`, one project, Chromium.
- `studio/ui/e2e/flows.ts`: `/api` calls (create, deploy, delete, look up)
  through one Playwright `request` client on the config's `baseURL`, and the
  parts flows are built from, typed by the app's own `FlowFile`. `studio/ui/e2e/locators.ts`: where things are on the page.
  `studio/ui/e2e/studio.ts`: the fixtures (`test.extend`): unique names
  `studio-ui-<test>-<timestamp>` for flows, topics and groups; docker commands
  through `execFileSync` (a node's container is found by its `studio.flow` and
  `studio.node` labels, not by rebuilding its name); wait for `/api/health`
  200; the teardown (§6). It re-exports the other two, so a test imports from
  one place.
- `studio/ui/e2e/*.spec.ts`: the tests (§5). The app's build type-checks them
  too, in the same compiler run (`tsc -b . e2e && vite build`), so `make test`
  catches a broken test without a browser; Vite bundles only `src/`, so the
  studio image is unchanged.
- Makefile: `verify-ui: up ## …` runs the above and prints `UI OK`; `verify: up
  verify-studio verify-ui` (API check, then browser suite, then the kcat check);
  `test` builds the UI as before. A `studio/ui/node_modules` target runs `npm
  ci` whenever `package-lock.json` is newer than the last install, so a checkout
  made before Playwright was added installs it; a `studio/ui/.chromium` stamp
  runs `npx playwright install chromium` only when the lockfile changed (a new
  Playwright wants its own Chromium). `KAFKA_BOOTSTRAP` is the Makefile's
  `BOOTSTRAP` address.
- `.gitignore`: `studio/ui/test-results/`, `studio/ui/playwright-report/`,
  `studio/ui/.chromium`, `*.tsbuildinfo` (the compiler's cache).

## 4. Locating elements

Accessible locators first: buttons and inputs by role and name
(`getByRole('button', { name: 'Deploy' })`), Inspector fields by their label
(`getByLabel('Group id', { exact: true })`), messages by text. A `data-testid`
only where the UI has no accessible name:

| `data-testid` | Element | File |
|---|---|---|
| `node-<id>` | a node's frame (`.node`) | `nodes/StudioNodes.tsx` |
| `runtime-<id>` | a node's runtime line | `nodes/StudioNodes.tsx` |
| `tail` | the tail drawer: its records, its instance picker and Send, and the box that scrolls | `TailDrawer.tsx` |

React Flow's handles are reached inside a node's frame by React Flow's own
classes (`.react-flow__handle.source`, `.react-flow__handle.target`). Node ids
are the ones the canvas assigns on drop (`producer-1`, …) or the ones the API
fixture gives. No page-object layer.

## 5. Tests

Sixteen tests (the last five added since, below), run one at a time; each flow, topic and group has a unique name and
is removed afterwards (§6).

### `e2e/editor.spec.ts` — the editor, flows built through the UI

1. **Build, run, stop.** New flow; drag Producer, Topic and Consumer from the
   palette; set the producer to `timer` at 200 ms, name the topic, set the
   consumer's group; wire Producer → Topic → Consumer. Save: the button reads
   `Saved`. Deploy: the status reads `running` and the consumer's runtime line
   shows a count that grows between two reads. Select the consumer: the tail
   drawer lists records. Stop: the status reads `stopped`, and
   `docker ps -q -f label=studio.flow=<id>` prints nothing.
2. **Refused wire.** Dragging from a Producer's output handle to a Consumer's
   input (an edge the table refuses) creates no edge; the same gesture from the
   Producer to a Topic then draws one, so the drag itself is known to work.
3. **Deploy errors in the top bar.** Deploying a flow whose consumer has no
   incoming edge shows the 422 message in the top bar, naming the node; the
   canvas keeps the flow.
4. **Unsaved changes.** Renaming the flow enables Save; opening another flow
   asks to discard: dismissed, the edited flow stays open; accepted, the other
   opens.
5. **Flow list refresh.** A flow deployed through the API (another tab) shows
   `running` in the list within 11 s (two of its 5 s re-reads), without a reload.

### `e2e/nodes.spec.ts` — node types and the drawer, flows created through the API

6. **Transform.** A flow `producer → topic → consumer → transform → topic`,
   opened in the editor, deploys; two records sent from the producer's drawer
   (one with `qty` and `price`, one without — set through the producer's value
   template by the API fixture) make the Transform node show `2 msgs` and
   `1 errors`, its last error as the node's tooltip. A transform whose
   expression does not compile: Deploy shows the 422 naming the node.
7. **Instances.** A consumer with `instances: 3` on a 3-partition topic shows
   `3/3 running` and one partition per instance in its runtime line; the
   drawer's instance picker offers instances 1–3, and picking one shows that
   instance's records.

### `e2e/live.spec.ts` — the M6 live view

8. **Studio restart.** With a running flow open, `docker compose stop studio`:
   within 3 s the top bar shows `live numbers paused:` and the canvas has the
   `paused` class. `docker compose start studio`: within 15 s the message is
   gone and a count moves again, with no reload.
9. **Deleted elsewhere.** Deleting the open flow through the API: within 3 s
   the top bar shows `the open flow was deleted`, the canvas is empty, and the
   flow list no longer has it.
10. **Node warning.** 6 s after the deploy (past the 5 s start-up grace),
    `docker kill -s STOP` the consumer's container: within 3 s its runtime line
    shows `stats:` while the producer's keeps its numbers. `docker kill -s
    CONT`: the warning goes.
11. **Tail scroll.** A timer at 250 ms fills the drawer. Scrolled to the top, the
    first visible record stays the same while new ones arrive; scrolled back to
    the bottom, the newest record comes into view.

### Added since

12. **Rewind** (`nodes.spec.ts`). On a deployed flow the consumer's "to earliest"
    is disabled and says to stop the flow; after Stop it rewinds and shows
    `<group> on <topic>: 1 partition rewound to earliest`.
13. **Router in the editor** (`editor.spec.ts`). On an API-created flow shown
    unfitted (one column, room for the router), drop a Router from the palette
    and wire consumer → router → two topics: the Inspector shows two rules
    with those topics, the node reads `2 rules · no default`. Deleting the edge to
    the second topic marks rule 2 "Not wired"; drawing it again wires it back,
    still two rules. Save and Deploy:
    the top bar shows `422: router-1: rule 1: when is required`. Fill rule 1,
    remove rule 2, make its topic the default: the edges read `#1` and
    `default`; Save and Deploy run the flow.
14. **Router counts** (`nodes.spec.ts`). An API-created router flow
    (`msg.total > 100` to one topic, default to another) deployed; two sends
    from the producer's drawer, one big and one small: the router shows
    `2 msgs` and its edges `#1 · 1` and `default · 1`.
15. **Retry and DLQ settings** (`nodes.spec.ts`). The consumer's "On failure"
    group shows `<topic>__retry` and `<topic>__dlq`; ticking Retry shows
    Attempts 3 and Delay (ms) 5000; Save and Deploy: the top bar shows
    `422: consumer-1: retry needs a DLQ: records go there once their attempts run out`.
16. **Retry, then DLQ** (`nodes.spec.ts`). A consumer whose http sink always
    fails, with `retry {attempts: 1, delay_ms: 1000}` and a DLQ: before any send
    its runtime line shows `0 retried` and `0 dlq`; one send shows `1 retried`, then `1 dlq` and `0 waiting` on its runtime line, and its tail
    shows `studio-attempt: 1` on the retried record.

## 6. Cleanup, failures, timing

- `studio-ui-` is a namespace the suite owns: every flow, topic and group a
  test makes starts with it, and nothing else does.
- Each test's fixture teardown runs even when the test fails, and each step
  runs even when an earlier one failed: if the studio was stopped, `docker
  compose start studio` and wait up to 45 s for `/api/health` 200; then, for
  every `studio-ui-` flow, `docker kill -s CONT` its containers (a paused one
  would make its removal wait) and delete it through `/api` (which removes its
  containers). One worker runs the tests in turn, so those are this test's
  flows, plus any an interrupted earlier run left. The fixture has its own
  120 s budget, apart from the test's.
- After the last test, a worker-scoped fixture lists the broker's topics and
  consumer groups and deletes every `studio-ui-` one: a listing and a delete per
  Kafka tool (each starts a JVM in the broker), not a delete per test. A topic
  name is escaped, since `--topic` takes a regex, so it matches only itself; a
  shared topic such as `orders` is never touched.
- Waits use `expect(…).toHaveText/toBeVisible`, `expect.poll` or `toPass`
  with explicit timeouts. The only fixed wait is test 10's 6 s.
- One worker: test 8 stops the studio every other test talks to, and its
  teardown waits for health, so the next test starts against a healthy studio.
- A failure keeps a trace and a screenshot in `studio/ui/test-results/`;
  `make verify-ui` exits non-zero, so `make verify` stops before the kcat
  check.
- Expected run time: 2–3 minutes. `make verify` grows by that much.

## 7. Documentation

- `AGENTS.md`: `make verify-ui` in "Check your change" (the one-time Chromium
  download noted), and a rule: a change to the UI's visible text, roles or
  `data-testid`s runs `make verify-ui`; the top-bar messages the tests check are
  quoted in the Studio spec or the UI tests spec (§5), so wording changes go
  through the spec.
- `README.md`: the development section names `make verify-ui`.

## 8. Assumptions

1. Playwright's `dragTo` drives the palette's HTML5 drag and drop and React
   Flow's handle-to-handle connection in Chromium; the plan confirms both
   through Context7 (`/microsoft/playwright`) and a throwaway run, and falls back
   to `page.mouse` moves where `dragTo` does not.
2. The page stays loaded while the studio is stopped (test 8): the browser
   already has the bundle, and only the event stream drops.
3. Chromium's `EventSource` reconnects by itself after a dropped connection,
   which is what the UI's "connection lost, reconnecting" path relies on.

## 9. Built as decided in its plan

- `dragTo` drives both the palette's HTML5 drop and React Flow's handle-to-handle
  connection in Chromium; no `page.mouse` fallback was needed (checked live).
- A new flow's canvas used to zoom in on, and centre, the first node dropped
  on it (React Flow's `fitView` on an empty canvas). `Canvas.tsx` now fits only
  a flow opened with nodes, so the editor test drops its nodes where it means
  to.
- The Inspector's labels were not tied to their controls. Each now names its
  control (`htmlFor` and `id`), which screen readers use too, so the tests find
  a field with `getByLabel`.
- `data-testid="tail"` marks the whole drawer, not only its list: the instance
  picker, Send and the scrolling box live there too.
- Cleanup goes by the `studio-ui-` namespace rather than by remembering names:
  the fixture keeps no record of what a test made, and a run also clears what an
  interrupted one left. Two runs of the suite at once would remove each other's
  flows; one worker and one run at a time is the design.
- `e2e/tsconfig.json` uses Node's types from `@types/node`, declared at the
  version vite already installs (`24.19.1`).
- The tail-scroll test's timer runs at 250 ms: the drawer keeps 100 records, and a faster timer passes that cap during the check (100 ms gives only about 10 s), so the first record would change while scrolled up. At 250 ms the cap arrives about 25 s after start, and more than 20 records still arrive within the 20 s poll.
- The fixture has its own 120 s timeout, so its teardown does not share the test's budget and a 45 s wait for a restarted studio still leaves time to clean up. The instances test sets its own 120 s: three containers start and the group rebalances before it can check one partition each.
