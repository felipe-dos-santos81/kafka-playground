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
  `npx playwright install chromium`. No other new dependency.
- The suite runs on the host, against the stack `make up` starts, at
  `STUDIO_URL` (default `http://localhost:8082`). Tests that stop containers
  call `docker` on the host.
- No change to Go code, the API, the images or `docker-compose.yml`. The UI
  changes only by the `data-testid` attributes in §4.
- Local-only, as the rest of the repository: no auth, nothing leaves the host.
- Makefile style as in `AGENTS.md` (GNU make 3.81, BSD tools, real tabs, `##`
  help, `$$`).

## 3. Harness

```
make verify-ui
  └─ cd studio/ui; npm ci if needed; npx playwright install chromium
     └─ STUDIO_URL=… npx playwright test        (host, one worker)
          ├─ chromium → $STUDIO_URL
          ├─ fetch → $STUDIO_URL/api            (set up and clean up flows)
          └─ execFileSync docker …              (stop/start studio, SIGSTOP/CONT a node,
                                                 delete topics and groups)
```

- `studio/ui/playwright.config.ts`: `testDir: 'e2e'`, `workers: 1`,
  `fullyParallel: false`, `retries: 0`, `timeout: 60_000`, `reporter: 'list'`,
  `use: { baseURL, trace: 'retain-on-failure', screenshot: 'only-on-failure' }`,
  `outputDir: 'test-results'`, one project, Chromium.
- `studio/ui/e2e/studio.ts`: the shared fixture (`test.extend`) and helpers:
  create, deploy and delete flows through `/api`; unique names
  `studio-ui-<test>-<timestamp>` for flows, topics and groups; docker commands
  through `execFileSync`; wait for `/api/health` 200.
- `studio/ui/e2e/*.spec.ts`: the tests (§5). `studio/ui/e2e/tsconfig.json`
  lets `make test` type-check them (`tsc -p e2e`) without a browser; the app's
  build (`tsc && vite build`) does not include `e2e/`, so the studio image is
  unchanged.
- Makefile: `verify-ui: up ## …` runs the above; `verify: up verify-studio
  verify-ui` (API check, then browser suite, then the kcat check); `test` gains
  `tsc -p e2e`.
- `.gitignore`: `studio/ui/test-results/`, `studio/ui/playwright-report/`.

## 4. Locating elements

Accessible locators first: buttons and inputs by role and name
(`getByRole('button', { name: 'Deploy' })`), messages by text. A
`data-testid` only where the UI has no accessible name:

| `data-testid` | Element | File |
|---|---|---|
| `node-<id>` | a node's frame (`.node`) | `nodes/StudioNodes.tsx` |
| `runtime-<id>` | a node's runtime line | `nodes/StudioNodes.tsx` |
| `tail` | the tail drawer's record list | `TailDrawer.tsx` |

React Flow's handles are reached inside a node's frame by React Flow's own
classes (`.react-flow__handle.source`, `.react-flow__handle.target`). Node ids
are the ones the canvas assigns on drop (`producer-1`, …) or the ones the API
fixture gives. No page-object layer.

## 5. Tests

Eleven tests, run one at a time; each flow, topic and group has a unique name and
is removed afterwards (§6).

### `e2e/editor.spec.ts` — the editor, flows built through the UI

1. **Build, run, stop.** New flow; drag Producer, Topic and Consumer from the
   palette; set the producer to `timer` at 200 ms, name the topic, set the
   consumer's group; wire Producer → Topic → Consumer. Save: the button reads
   `Saved`. Deploy: the status reads `running` and the consumer's runtime line
   shows a count that grows between two reads. Select the consumer: the tail
   drawer lists records. Stop: the status reads `stopped`, and
   `docker ps -q -f label=studio.flow=<id>` prints nothing.
2. **Refused wire.** Dragging from a Consumer's source handle to a Producer
   creates no edge.
3. **Deploy errors in the top bar.** Deploying a flow whose consumer has no
   incoming edge shows the 422 message in the top bar, naming the node; the
   canvas keeps the flow.
4. **Unsaved changes.** Renaming the flow enables Save; opening another flow
   asks to discard: dismissed, the edited flow stays open; accepted, the other
   opens.
5. **Flow list refresh.** A flow deployed through the API (another tab) shows
   `running` in the list within 6 s, without a reload.

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
11. **Tail scroll.** A timer at 50 ms fills the drawer. Scrolled to the top, the
    first visible record stays the same while new ones arrive; scrolled back to
    the bottom, the newest record comes into view.

## 6. Cleanup, failures, timing

- The fixture's teardown runs even when a test fails, in this order:
  `docker kill -s CONT` every container the test stopped (errors ignored); if
  the studio was stopped, `docker compose start studio` and wait for
  `/api/health` 200; delete the test's flows through `/api` (which removes
  their containers); delete the test's topics and consumer groups by exact
  name (`kafka-topics.sh --delete`, `kafka-consumer-groups.sh --delete`). No
  prefix matching: only names the test created.
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
  quoted in the Studio spec, so wording changes go through the spec.
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
