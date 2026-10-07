# Pipeline Studio UI Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Playwright suite that drives the studio's real UI in Chromium against the running stack: the editor, the node types and the M6 live view. It runs as `make verify-ui` and inside `make verify`.

**Architecture:** `@playwright/test` lives in `studio/ui` and runs on the host with one worker. A shared fixture, `e2e/studio.ts`:
- creates flows through `/api` with unique `studio-ui-…` names;
- runs `docker` for the tests that stop the studio or pause a node;
- in teardown, even after a failure: resumes paused containers, restarts the studio, deletes the test's flows, then deletes exactly the topics and groups those flows name.

Tests find elements by role and text, plus three `data-testid`s where the UI has no accessible name.

**Tech Stack:** `@playwright/test` 1.63.0 with Chromium. The APIs were checked through Context7 (`/microsoft/playwright/v1.63.0`: `test.extend` fixtures with teardown after `use`, `locator.dragTo` with `targetPosition`, `expect.poll`). The app's stack is React 19, `@xyflow/react` 12.12.0, Vite 8. Node's `child_process` runs `docker`.

**Spec:** `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md` (all sections). Earlier context: `docs/superpowers/specs/2026-10-06-pipeline-studio-design.md` §7 M6. The plan starts from `main` at `2e8c38f`.

**Verified before writing:** the whole plan was applied in a throwaway worktree and run against a live stack:
- `make test` passed.
- `make down && make verify` ended with `STUDIO OK`, `11 passed (1.1m)`, `UI OK` and `VERIFY OK`.
- A second `make verify-ui` gave `11 passed (1.2m)`.
- Afterwards no `studio-ui` flow, container, topic or group remained.

## Global Constraints

- `@playwright/test` pinned exactly at `1.63.0` as a dev dependency of `studio/ui`; Chromium only (`npx playwright install chromium`). No other new dependency.
- The suite runs on the host against the stack `make up` starts, at `STUDIO_URL` (default `http://localhost:8082`).
- No change to Go code, the API, the images or `docker-compose.yml`. The UI changes only by the `data-testid`s `node-<id>`, `runtime-<id>` and `tail`.
- Tests run one at a time (`workers: 1`). Waits use web-first assertions or `expect.poll` with explicit timeouts. The only fixed wait is the node-warning test's 6 s.
- Every flow, topic and group a test makes is named `studio-ui-<label>-<time>` and removed by exact name; never by prefix.
- Makefile: GNU make 3.81, BSD tools, recipe lines start with a real tab, shell `$` written `$$`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Keep README.md, AGENTS.md and the specs in sync.

## Review Focus

1. **A test that fails while the studio is stopped.** The studio must be running again for the next test and for the user. The fixture restarts it and waits for `/api/health`. Pinned in Task 3, Step 4, where `curl` checks health right after the live tests.
2. **Leftovers.** A run, passing or failing, must leave no `studio-ui-…` flow, container, topic or group. Pinned by the cleanup check after every run in Tasks 1–3.
3. **The user's own things are never touched.** The example flow `0a1b2c3d` and the topics `orders` and `events` are still there after a run. Pinned in the same cleanup checks.
4. **A wiring drag that lands on the wrong element must fail loudly, not pass.** The editor tests assert the edge count, and a drop on the canvas controls times out. Pinned in Task 1, Steps 4 and 6.
5. **Timing on a slow machine.** The suite must pass twice in a row: no test may depend on luck with ticks or rebalances. Pinned in Task 3, Step 5.

## Decisions (Task 3 writes them into the UI tests spec, §9)

1. **`dragTo` is enough.** It drives both the palette's HTML5 drop and React Flow's handle-to-handle connection in Chromium, so no `page.mouse` fallback is needed.
2. **The editor test works around the first-node zoom.** A new flow's canvas zooms in on, and centres, the first node it gets. So the test drops the producer first, then the topic and consumer into free corners away from React Flow's controls, then presses the "fit view" control.
3. **Inspector fields are found by their label text.** The labels are not tied to their controls, so a field is the control right after its label text (`label:text-is("Group id") + *`). No UI change is needed.
4. **`data-testid="tail"` marks the whole drawer**, not only its list: the instance picker, Send and the scrolling box live there too.
5. **The teardown deletes exactly what the test's flows name.** It reads each flow back through `/api`, deletes the topics and groups the file names, and retries the group delete once after 3 s, because a consumer may still be leaving.
6. **Node's types come from `@types/node`, which `npm ci` already installs** (vite brings it in).

## File map

| File | Responsibility | Task |
|---|---|---|
| `studio/ui/package.json`, `package-lock.json` | `@playwright/test` 1.63.0 | 1 |
| `studio/ui/playwright.config.ts` | one worker, Chromium, traces on failure | 1 |
| `studio/ui/e2e/tsconfig.json` | type-check the tests in `make test` | 1 |
| `studio/ui/e2e/studio.ts` | fixture, API and docker helpers, locators | 1 |
| `studio/ui/e2e/editor.spec.ts` | 5 editor tests | 1 |
| `studio/ui/src/nodes/StudioNodes.tsx`, `TailDrawer.tsx` | `data-testid`s | 1 |
| `Makefile`, `.gitignore` | `verify-ui`, `make test` type-check, ignore test output | 1 (`verify` dependency in 3) |
| `studio/ui/e2e/nodes.spec.ts` | Transform and instances | 2 |
| `studio/ui/e2e/live.spec.ts` | the four M6 checks | 3 |
| `AGENTS.md`, `README.md`, UI tests spec | docs | 3 |

---

### Task 1: Harness, `data-testid`s and the editor tests

**Files:**
- Create: `studio/ui/playwright.config.ts`, `studio/ui/e2e/tsconfig.json`, `studio/ui/e2e/studio.ts`, `studio/ui/e2e/editor.spec.ts`
- Modify: `studio/ui/package.json`, `studio/ui/package-lock.json` (through npm), `studio/ui/src/nodes/StudioNodes.tsx`, `studio/ui/src/TailDrawer.tsx`, `Makefile`, `.gitignore`

**Interfaces:**
- Consumes: the UI as it is on `main`. The banner header holds the flow name textbox, Save, Deploy, Stop and the status. The flow list's New button opens a `window.prompt`. The palette items are `.palette-item.<type>`, and the canvas assigns node ids `<type>-<n>` on drop. React Flow's controls include a "fit view" button. The API is `/api/flows` (GET list, POST create, GET, DELETE), `/api/flows/{id}/deploy` and `/api/health`.
- Produces, from `e2e/studio.ts`:
  - **Fixture:** `test` (with the `studio` fixture) and `expect`.
  - **API:** `api(method, path, body?)`.
  - **Flow builders:** `node(id, type, x, data)`, `edge(source, target)`, `manual`, `timer(ms)`, `topic(name, partitions?)`, `consumer(group, more?)`, `chain(name, p, t, c)`.
  - **Locators:** `topBar(page)`, `nodeOf(page, id)`, `runtimeOf(page, id)`, `tail(page)`, `paletteItem(page, type)`, `field(page, label)`.
  - **Actions:** `connect(page, from, to)`, `msgs(line)`.
  - **The `studio` fixture's methods:** `unique(label)`, `create(flow)`, `deploy(id)`, `remove(id)`, `idOf(name)`, `open(page, name)`, `stopStudio()`, `startStudio()`, `pause(container)`, `resume(container)`.
  - Tasks 2 and 3 import these from `./studio`.

- [ ] **Step 1: Add Playwright**

Run: `cd studio/ui && npm install --save-dev --save-exact @playwright/test@1.63.0 && npx playwright install chromium`
Expected: `studio/ui/package.json`'s `devDependencies` gains `"@playwright/test": "1.63.0"`, and `npx playwright --version` prints `Version 1.63.0`.

- [ ] **Step 2: The harness**

Create `studio/ui/playwright.config.ts`:

```ts
// Browser tests of the studio's UI against the running stack (`make verify-ui`).
// One worker: a test that stops the studio or pauses a node container must not
// share them with another test.
import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: 'e2e',
  workers: 1,
  fullyParallel: false,
  retries: 0,
  timeout: 60_000,
  reporter: 'list',
  outputDir: 'test-results',
  use: {
    baseURL: process.env.STUDIO_URL ?? 'http://localhost:8082',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
```

Create `studio/ui/e2e/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "es2023",
    "lib": ["ES2023", "DOM"],
    "module": "esnext",
    "moduleResolution": "bundler",
    "types": ["node"],
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true
  },
  "include": [".", "../playwright.config.ts"]
}
```

Create `studio/ui/e2e/studio.ts`:

```ts
// The UI tests' fixture and helpers. Flows are set up and removed through the
// studio's API; tests that stop the studio or pause a node call docker on the
// host. Every name a test makes is unique (studio-ui-<label>-<time>), and the
// fixture removes what the test made, by exact name, even when the test fails.
import { test as base, expect, type Locator, type Page } from '@playwright/test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const STUDIO_URL = process.env.STUDIO_URL ?? 'http://localhost:8082'
const COMPOSE_FILE = fileURLToPath(new URL('../../../docker-compose.yml', import.meta.url))

export type FlowNode = { id: string; type: string; position: { x: number; y: number }; data: Record<string, unknown> }
export type FlowEdge = { id: string; source: string; target: string }
export type Flow = { name: string; nodes: FlowNode[]; edges: FlowEdge[] }

const docker = (...args: string[]) => execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
const compose = (...args: string[]) => docker('compose', '-f', COMPOSE_FILE, ...args)
const kafka = (tool: string, ...args: string[]) =>
  compose('exec', '-T', 'kafka', `/opt/kafka/bin/${tool}`, '--bootstrap-server', 'localhost:19092', ...args)

export async function api<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(STUDIO_URL + path, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text}`)
  return (text ? JSON.parse(text) : undefined) as T
}

// A flow's parts, laid out left to right.
export const node = (id: string, type: string, x: number, data: Record<string, unknown>): FlowNode => ({
  id,
  type,
  position: { x, y: 0 },
  data,
})
export const edge = (source: string, target: string): FlowEdge => ({ id: `${source}-${target}`, source, target })
export const manual = { source: 'manual', key: '', value: '{"id": {{.Seq}}}' }
export const timer = (ms: number) => ({ source: 'timer', interval_ms: ms, key: '{{.Seq}}', value: '{"id": {{.Seq}}}' })
export const topic = (name: string, partitions = 1) => ({ name, partitions, replication_factor: 1 })
export const consumer = (group: string, more: Record<string, unknown> = {}) => ({
  group,
  auto_offset_reset: 'earliest',
  sink: { kind: 'log' },
  ...more,
})

// chain is the flow producer-1 → topic-1 → consumer-1.
export function chain(name: string, p: Record<string, unknown>, t: Record<string, unknown>, c: Record<string, unknown>): Flow {
  return {
    name,
    nodes: [node('producer-1', 'producer', 0, p), node('topic-1', 'topic', 250, t), node('consumer-1', 'consumer', 500, c)],
    edges: [edge('producer-1', 'topic-1'), edge('topic-1', 'consumer-1')],
  }
}

type Studio = {
  unique(label: string): string // a name to use for a flow, a topic or a group
  create(flow: Flow): Promise<string> // the new flow's id
  deploy(id: string): Promise<void>
  remove(id: string): Promise<void>
  idOf(name: string): Promise<string>
  open(page: Page, name: string): Promise<void>
  stopStudio(): void
  startStudio(): Promise<void>
  pause(container: string): void
  resume(container: string): void
}

async function healthy() {
  await expect
    .poll(() => fetch(`${STUDIO_URL}/api/health`).then((r) => r.status, () => 0), { timeout: 60_000 })
    .toBe(200)
}

export const test = base.extend<{ studio: Studio }>({
  studio: async ({}, use) => {
    const names = new Set<string>()
    const paused = new Set<string>()
    let stopped = false
    const studio: Studio = {
      unique(label) {
        const name = `studio-ui-${label}-${Date.now()}`
        names.add(name)
        return name
      },
      async create(flow) {
        names.add(flow.name)
        return (await api<{ id: string }>('POST', '/api/flows', flow)).id
      },
      async deploy(id) {
        await api('POST', `/api/flows/${id}/deploy`)
      },
      async remove(id) {
        await api('DELETE', `/api/flows/${id}`)
      },
      async idOf(name) {
        const f = (await api<{ id: string; name: string }[]>('GET', '/api/flows')).find((f) => f.name === name)
        if (!f) throw new Error(`no flow named ${name}`)
        return f.id
      },
      async open(page, name) {
        await page.goto('/')
        await page.getByRole('listitem').filter({ hasText: name }).getByText(name).click()
        await expect(page.getByRole('banner').getByRole('textbox')).toHaveValue(name)
      },
      stopStudio() {
        stopped = true
        compose('stop', 'studio')
      },
      async startStudio() {
        compose('start', 'studio')
        await healthy()
        stopped = false
      },
      pause(container) {
        paused.add(container)
        docker('kill', '-s', 'STOP', container)
      },
      resume(container) {
        docker('kill', '-s', 'CONT', container)
        paused.delete(container)
      },
    }
    await use(studio)

    // Teardown, even after a failure: resume, restart, then remove what the test made.
    for (const c of paused) {
      try {
        docker('kill', '-s', 'CONT', c)
      } catch {
        // gone already
      }
    }
    if (stopped) await studio.startStudio()
    const topics: string[] = []
    const groups: string[] = []
    for (const f of await api<{ id: string; name: string }[]>('GET', '/api/flows')) {
      if (!names.has(f.name)) continue
      const file = await api<Flow>('GET', `/api/flows/${f.id}`)
      for (const n of file.nodes) {
        if (n.type === 'topic' && n.data.name) topics.push(String(n.data.name))
        if (n.type === 'consumer' && n.data.group) groups.push(String(n.data.group))
      }
      await studio.remove(f.id) // stops its containers first
    }
    try {
      if (topics.length) kafka('kafka-topics.sh', '--delete', '--topic', topics.join('|'))
    } catch {
      // a topic the test never deployed does not exist
    }
    for (const attempt of [1, 2]) {
      try {
        if (groups.length) kafka('kafka-consumer-groups.sh', '--delete', ...groups.flatMap((g) => ['--group', g]))
        break
      } catch {
        // a group still has a member leaving, or never existed: try once more
        if (attempt === 1) await new Promise((r) => setTimeout(r, 3000))
      }
    }
  },
})
export { expect }

// Locators for what has no accessible name.
export const topBar = (page: Page) => page.getByRole('banner')
export const nodeOf = (page: Page, id: string) => page.getByTestId(`node-${id}`)
export const runtimeOf = (page: Page, id: string) => page.getByTestId(`runtime-${id}`)
export const tail = (page: Page) => page.getByTestId('tail')
export const paletteItem = (page: Page, type: string) => page.locator(`.palette-item.${type}`)
// field is the Inspector's control right after a label: the labels are not tied to their controls.
export const field = (page: Page, label: string) => page.locator('.inspector').locator(`label:text-is("${label}") + *`)

// connect wires a node's output to another node's input, handle to handle.
export async function connect(page: Page, from: string, to: string) {
  await nodeOf(page, from).locator('.react-flow__handle.source').dragTo(nodeOf(page, to).locator('.react-flow__handle.target'))
}

// msgs reads the count from a runtime line ("12 msgs · 3.0/s"), or -1 without one.
export async function msgs(line: Locator): Promise<number> {
  const m = /(\d+) msgs/.exec((await line.textContent()) ?? '')
  return m ? Number(m[1]) : -1
}
```

- [ ] **Step 3: The editor tests**

Create `studio/ui/e2e/editor.spec.ts`:

```ts
// The editor: flows built and run through the UI.
import { execFileSync } from 'node:child_process'
import {
  chain,
  connect,
  consumer,
  expect,
  field,
  manual,
  msgs,
  node,
  nodeOf,
  paletteItem,
  runtimeOf,
  tail,
  test,
  topBar,
  topic,
} from './studio'

test('build, run and stop a flow in the editor', async ({ page, studio }) => {
  const name = studio.unique('editor')
  await page.goto('/')
  page.once('dialog', (d) => d.accept(name))
  await page.getByRole('button', { name: 'New' }).click()
  await expect(topBar(page).getByRole('textbox')).toHaveValue(name)

  // The canvas zooms in on the first node it gets and centres it, so the other
  // two go to the free corners; fit view then shows all three.
  const pane = page.locator('.react-flow__pane')
  await paletteItem(page, 'producer').dragTo(pane, { targetPosition: { x: 150, y: 100 } })
  await expect(nodeOf(page, 'producer-1')).toBeVisible()
  await paletteItem(page, 'topic').dragTo(pane, { targetPosition: { x: 40, y: 40 } })
  await paletteItem(page, 'consumer').dragTo(pane, { targetPosition: { x: 80, y: 480 } })
  await page.getByRole('button', { name: 'fit view' }).click()

  await nodeOf(page, 'producer-1').click()
  await field(page, 'Source').selectOption('timer')
  await field(page, 'Interval (ms)').fill('200')
  await nodeOf(page, 'topic-1').click()
  await field(page, 'Name').fill(name)
  await nodeOf(page, 'consumer-1').click()
  await field(page, 'Group id').fill(name)
  await connect(page, 'producer-1', 'topic-1')
  await connect(page, 'topic-1', 'consumer-1')
  await expect(page.locator('.react-flow__edge')).toHaveCount(2)

  await page.getByRole('button', { name: 'Save' }).click()
  await expect(page.getByRole('button', { name: 'Saved' })).toBeVisible()
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page).getByText('running', { exact: true })).toBeVisible({ timeout: 15_000 })

  const line = runtimeOf(page, 'consumer-1')
  await expect.poll(() => msgs(line), { timeout: 20_000 }).toBeGreaterThan(0)
  const first = await msgs(line)
  await expect.poll(() => msgs(line), { timeout: 10_000 }).toBeGreaterThan(first)

  await nodeOf(page, 'consumer-1').click()
  await expect(tail(page).getByRole('listitem').first()).toBeVisible({ timeout: 10_000 })

  await page.getByRole('button', { name: 'Stop' }).click()
  await expect(topBar(page).getByText('stopped', { exact: true })).toBeVisible({ timeout: 15_000 })
  const id = await studio.idOf(name)
  expect(execFileSync('docker', ['ps', '-q', '-f', `label=studio.flow=${id}`], { encoding: 'utf8' }).trim()).toBe('')
})

test('a wire the edge table refuses is not drawn', async ({ page, studio }) => {
  const name = studio.unique('wire')
  await studio.create({
    name,
    nodes: [node('producer-1', 'producer', 0, manual), node('consumer-1', 'consumer', 300, consumer(name))],
    edges: [],
  })
  await studio.open(page, name)
  await connect(page, 'producer-1', 'consumer-1')
  await expect(page.locator('.react-flow__edge')).toHaveCount(0)
})

test('a deploy refused for a node shows in the top bar', async ({ page, studio }) => {
  const name = studio.unique('refused')
  const flow = chain(name, manual, topic(name), consumer(name))
  flow.edges = flow.edges.filter((e) => e.target !== 'consumer-1') // the consumer reads nothing
  await studio.create(flow)
  await studio.open(page, name)
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: consumer-1: a consumer needs exactly one edge from a topic')
  await expect(nodeOf(page, 'consumer-1')).toBeVisible()
})

test('unsaved changes ask before another flow opens', async ({ page, studio }) => {
  const a = studio.unique('edited')
  const b = studio.unique('other')
  for (const name of [a, b]) await studio.create(chain(name, manual, topic(name), consumer(name)))
  await studio.open(page, a)
  const title = topBar(page).getByRole('textbox')
  await title.fill(`${a}-renamed`)
  await expect(page.getByRole('button', { name: 'Save' })).toBeEnabled()

  page.once('dialog', (d) => d.dismiss())
  await page.getByRole('listitem').filter({ hasText: b }).getByText(b).click()
  await expect(title).toHaveValue(`${a}-renamed`)

  page.once('dialog', (d) => d.accept())
  await page.getByRole('listitem').filter({ hasText: b }).getByText(b).click()
  await expect(title).toHaveValue(b)
})

test('a flow deployed elsewhere shows as running in the list', async ({ page, studio }) => {
  const name = studio.unique('elsewhere')
  const id = await studio.create(chain(name, manual, topic(name), consumer(name)))
  await page.goto('/')
  const item = page.getByRole('listitem').filter({ hasText: name })
  await expect(item).toContainText('(stopped)')
  await studio.deploy(id) // another tab, or curl
  await expect(item).toContainText('(running)', { timeout: 7_000 })
})
```

- [ ] **Step 4: `make verify-ui`, `make test`'s type-check, ignored test output**

In `Makefile`, replace

```make
.PHONY: help up down ps logs topics groups nodes produce scale verify verify-studio test
```

with

```make
.PHONY: help up down ps logs topics groups nodes produce scale verify verify-studio verify-ui test
```

In `Makefile`, replace

```make
# Waits until both groups have committed past the record (so it can no longer be
```

with

```make
verify-ui: up ## Check the studio's UI in a browser (Playwright, Chromium): editor, node types, live view; installs Chromium once
	@cd studio/ui && { [ -d node_modules ] || npm ci; } && npx playwright install chromium && \
		STUDIO_URL=$(STUDIO_URL) npx playwright test && echo "UI OK"

# Waits until both groups have committed past the record (so it can no longer be
```

In `Makefile`, replace

```make
test: ## Static checks and unit tests, no running stack needed: go vet, gofmt, go test, UI build (tsc), compose config
```

with

```make
test: ## Static checks and unit tests, no running stack needed: go vet, gofmt, go test, UI build (tsc), UI tests type-check, compose config
```

In `Makefile`, replace

```make
	cd studio/ui && { [ -d node_modules ] || npm ci; } && npm run build
```

with

```make
	cd studio/ui && { [ -d node_modules ] || npm ci; } && npm run build && npx tsc -p e2e
```

In `.gitignore`, replace

```gitignore
!/studio/ui/dist/.gitkeep
```

with

```gitignore
!/studio/ui/dist/.gitkeep
/studio/ui/test-results/
/studio/ui/playwright-report/
```

Recipe lines start with a real tab: `grep -n '^ ' Makefile` prints nothing.

- [ ] **Step 5: Run the tests to see them fail**

Run: `make test`, then `make down && make verify-ui`.
Expected: `make test` passes; `tsc -p e2e` type-checks the tests. `make verify-ui` reports `3 failed, 2 passed`. The three that use `nodeOf` fail waiting for `getByTestId('node-producer-1')` (or `node-consumer-1`): "build, run and stop", "a wire the edge table refuses" and "a deploy refused for a node". "Unsaved changes" and "a flow deployed elsewhere" pass.

- [ ] **Step 6: The `data-testid`s**

In `studio/ui/src/nodes/StudioNodes.tsx`, replace

```tsx
    <div className={`node ${type}${selected ? ' selected' : ''}`} title={rt?.lastError || undefined}>
```

with

```tsx
    <div data-testid={`node-${id}`} className={`node ${type}${selected ? ' selected' : ''}`} title={rt?.lastError || undefined}>
```

In `studio/ui/src/nodes/StudioNodes.tsx`, replace

```tsx
      {line && <div className="node-runtime">{line}</div>}
```

with

```tsx
      {line && (
        <div data-testid={`runtime-${id}`} className="node-runtime">
          {line}
        </div>
      )}
```

In `studio/ui/src/TailDrawer.tsx`, replace

```tsx
    <section className="drawer" ref={box} onScroll={onScroll}>
```

with

```tsx
    <section data-testid="tail" className="drawer" ref={box} onScroll={onScroll}>
```

- [ ] **Step 7: Run the tests to see them pass**

Run: `make test && make verify-ui`
Expected: `5 passed`, then `UI OK`. Then check that nothing was left behind:

```sh
curl -sS localhost:8082/api/flows | grep -c studio-ui            # 0
docker ps -aq -f label=studio.flow | wc -l                       # 0
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
#   no studio-ui-… topic; orders and events still there
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --list | grep -c studio-ui   # 0
ls flows                                                         # 0a1b2c3d.json only
```

Then run `make down`. `git status --short` shows only this task's files: `studio/ui/test-results/` is ignored.

- [ ] **Step 8: Commit**

```bash
git add studio/ui/package.json studio/ui/package-lock.json studio/ui/playwright.config.ts studio/ui/e2e studio/ui/src/nodes/StudioNodes.tsx studio/ui/src/TailDrawer.tsx Makefile .gitignore
git commit -m "ui tests: Playwright harness, data-testids and the editor tests; make verify-ui" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Node types and the tail drawer

**Files:**
- Create: `studio/ui/e2e/nodes.spec.ts`

**Interfaces:**
- Consumes: Task 1's `e2e/studio.ts` exports (`chain`, `consumer`, `edge`, `expect`, `node`, `nodeOf`, `runtimeOf`, `tail`, `test`, `timer`, `topBar`, `topic`, and the `studio` fixture).
- Produces: nothing new.

These tests pin behaviour the app already has (the M5 Transform, the M4 instances), so they pass on their first run. Each assertion names a value the UI must show, and would fail if it showed another.

- [ ] **Step 1: The tests**

Create `studio/ui/e2e/nodes.spec.ts`:

```ts
// Node types and the tail drawer, on flows created through the API.
import { chain, consumer, edge, expect, node, nodeOf, runtimeOf, tail, test, timer, topBar, topic } from './studio'

// The producer's records: the first has qty and price, the second a null qty,
// which the transform cannot multiply.
const twoOrders = {
  source: 'manual',
  key: '',
  value: '{"id": {{.Seq}}, "qty": {{if eq .Seq 1}}2{{else}}null{{end}}, "price": 3}',
}

function withTransform(name: string, expr: string) {
  const flow = chain(name, twoOrders, topic(`${name}-in`), consumer(`${name}-in`))
  flow.nodes.push(
    node('transform-1', 'transform', 750, { expr }),
    node('topic-2', 'topic', 1000, topic(`${name}-out`)),
    node('consumer-2', 'consumer', 1250, consumer(`${name}-out`)),
  )
  flow.edges.push(edge('consumer-1', 'transform-1'), edge('transform-1', 'topic-2'), edge('topic-2', 'consumer-2'))
  return flow
}

test('a transform shows its own counts and last error', async ({ page, studio }) => {
  const name = studio.unique('transform')
  await studio.create(withTransform(name, '{id: msg.id, total: msg.qty * msg.price}'))
  await studio.open(page, name)
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page).getByText('running', { exact: true })).toBeVisible({ timeout: 15_000 })

  await nodeOf(page, 'producer-1').click()
  for (const offset of [0, 1]) {
    await tail(page).getByRole('button', { name: 'Send' }).click()
    await expect(tail(page)).toContainText(`at offset ${offset}`)
  }
  await expect(runtimeOf(page, 'transform-1')).toContainText('2 msgs', { timeout: 15_000 })
  await expect(runtimeOf(page, 'transform-1')).toContainText('1 errors')
  await expect(nodeOf(page, 'transform-1')).toHaveAttribute('title', /invalid operation/)

  const bad = studio.unique('transform-bad')
  await studio.create(withTransform(bad, 'msg.'))
  await studio.open(page, bad)
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: transform-1: expr:')
})

test('a consumer with instances shows each one and tails the one picked', async ({ page, studio }) => {
  const name = studio.unique('instances')
  const id = await studio.create(chain(name, timer(100), topic(name, 3), consumer(name, { instances: 3 })))
  await studio.deploy(id)
  await studio.open(page, name)

  await expect(nodeOf(page, 'consumer-1')).toContainText('3/3 running', { timeout: 15_000 })
  const line = runtimeOf(page, 'consumer-1')
  await expect(line).toHaveText(/#1 p\d.*#2 p\d.*#3 p\d/, { timeout: 45_000 })
  const partition = /#2 p(\d)/.exec((await line.textContent()) ?? '')![1]

  await nodeOf(page, 'consumer-1').click()
  const picker = tail(page).getByRole('combobox')
  await expect(picker.getByRole('option')).toHaveText(['instance 1', 'instance 2', 'instance 3'])
  await picker.selectOption('2')
  const records = tail(page).getByRole('listitem')
  await expect(records.first()).toBeVisible({ timeout: 10_000 })
  for (const text of await records.allTextContents()) expect(text.startsWith(`p${partition}@`)).toBe(true)
})
```

- [ ] **Step 2: Run them**

Run: `cd studio/ui && npx tsc -p e2e`, then `make up && cd studio/ui && npx playwright test e2e/nodes.spec.ts`
Expected: `2 passed`. Run the same cleanup check as Task 1 Step 7, then `make down`.

- [ ] **Step 3: Commit**

```bash
git add studio/ui/e2e/nodes.spec.ts
git commit -m "ui tests: Transform counts and last error, instances and the instance picker" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The live view; `make verify` runs the suite; docs

**Files:**
- Create: `studio/ui/e2e/live.spec.ts`
- Modify: `Makefile`, `AGENTS.md`, `README.md`, `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`

**Interfaces:**
- Consumes: Task 1's `e2e/studio.ts`, including the fixture's `stopStudio`, `startStudio`, `pause`, `resume` and `remove`.
- Produces: nothing new.

- [ ] **Step 1: The tests**

Create `studio/ui/e2e/live.spec.ts`:

```ts
// The live view (spec M6): numbers that stop being live say so, a deleted flow
// closes, a silent node warns, and the tail follows only at the bottom.
import { chain, consumer, expect, manual, msgs, nodeOf, runtimeOf, tail, test, timer, topBar, topic } from './studio'

test('a studio restart pauses the numbers, then they come back', async ({ page, studio }) => {
  const name = studio.unique('restart')
  const id = await studio.create(chain(name, timer(200), topic(name), consumer(name)))
  await studio.deploy(id)
  await studio.open(page, name)
  const line = runtimeOf(page, 'consumer-1')
  await expect.poll(() => msgs(line), { timeout: 20_000 }).toBeGreaterThan(0)

  studio.stopStudio()
  await expect(topBar(page)).toContainText('live numbers paused:', { timeout: 3_000 })
  await expect(page.getByRole('main')).toHaveClass(/paused/)

  await studio.startStudio()
  await expect(topBar(page)).not.toContainText('live numbers paused', { timeout: 15_000 })
  const before = await msgs(line)
  await expect.poll(() => msgs(line), { timeout: 10_000 }).toBeGreaterThan(before)
})

test('a flow deleted elsewhere closes, saying so', async ({ page, studio }) => {
  const name = studio.unique('deleted')
  const id = await studio.create(chain(name, manual, topic(name), consumer(name)))
  await studio.open(page, name)
  await studio.remove(id)
  await expect(topBar(page)).toContainText('the open flow was deleted', { timeout: 3_000 })
  await expect(page.getByRole('main')).toContainText('Open a flow on the left or click New.')
  await expect(page.getByRole('listitem').filter({ hasText: name })).toHaveCount(0)
})

test('a node that stops answering shows why', async ({ page, studio }) => {
  const name = studio.unique('silent')
  const id = await studio.create(chain(name, manual, topic(name), consumer(name)))
  await studio.deploy(id)
  const deployed = Date.now()
  await studio.open(page, name)
  await expect(runtimeOf(page, 'consumer-1')).toContainText('msgs', { timeout: 10_000 })
  await page.waitForTimeout(Math.max(0, 6_000 - (Date.now() - deployed))) // past the 5 s start-up grace

  studio.pause(`studio-${id}-consumer-1`)
  await expect(runtimeOf(page, 'consumer-1')).toContainText('stats:', { timeout: 3_000 })
  await expect(runtimeOf(page, 'producer-1')).toContainText('msgs')
  studio.resume(`studio-${id}-consumer-1`)
  await expect(runtimeOf(page, 'consumer-1')).not.toContainText('stats:', { timeout: 5_000 })
})

test('the tail follows new records only while scrolled to the bottom', async ({ page, studio }) => {
  const name = studio.unique('scroll')
  const id = await studio.create(chain(name, timer(100), topic(name), consumer(name)))
  await studio.deploy(id)
  await studio.open(page, name)
  await nodeOf(page, 'consumer-1').click()
  const drawer = tail(page)
  const records = drawer.getByRole('listitem')
  await expect.poll(() => records.count(), { timeout: 20_000 }).toBeGreaterThan(20) // more than the drawer shows

  await drawer.evaluate((el) => el.scrollTo({ top: 0 }))
  const first = await records.first().textContent()
  const count = await records.count()
  await expect.poll(() => records.count(), { timeout: 5_000 }).toBeGreaterThan(count) // new records arrived
  expect(await records.first().textContent()).toBe(first)
  expect(await drawer.evaluate((el) => el.scrollTop)).toBeLessThan(10)

  await drawer.evaluate((el) => el.scrollTo({ top: el.scrollHeight }))
  const more = await records.count()
  await expect.poll(() => records.count(), { timeout: 5_000 }).toBeGreaterThan(more)
  await expect
    .poll(() => drawer.evaluate((el) => el.scrollHeight - el.scrollTop - el.clientHeight), { timeout: 3_000 })
    .toBeLessThan(8)
})
```

- [ ] **Step 2: Run them**

Run: `make up && cd studio/ui && npx playwright test e2e/live.spec.ts`
Expected: `4 passed`. The restart test takes about 10 s and the node-warning test about 10 s.

- [ ] **Step 3: `make verify` runs the suite; docs**

In `Makefile`, replace

```make
verify: up verify-studio ## End-to-end check: studio API, then one record seen exactly once per consumer group
```

with

```make
verify: up verify-studio verify-ui ## End-to-end check: studio API, studio UI, then one record seen exactly once per consumer group
```

In `AGENTS.md`, replace

```markdown
make test                  # go vet, gofmt, go test, UI build (tsc), compose config
make down && make verify   # always: ends with STUDIO OK and VERIFY OK
```

with

```markdown
make test                  # go vet, gofmt, go test, UI build (tsc), UI tests type-check, compose config
make down && make verify   # always: ends with STUDIO OK, UI OK and VERIFY OK (the first run downloads Chromium)
```

In `AGENTS.md`, replace

```markdown
- Studio node containers are not compose services:
```

with

```markdown
- The UI tests (`studio/ui/e2e/`, Playwright, `make verify-ui`) find elements by role and text, and by `data-testid` where there is none (`node-<id>`, `runtime-<id>`, `tail`). A change to the UI's visible text, roles or those ids runs `make verify-ui`; the top-bar messages they check are quoted in the Studio spec, so rewording one goes through the spec. Each test removes the flows, topics and groups it made, by exact name.
- Studio node containers are not compose services:
```

In `README.md`, replace

```markdown
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the running `studio` container. Go changes need `make up` (it rebuilds the image).
```

with

```markdown
- UI development: `cd studio/ui && npm install && npm run dev` serves http://localhost:5173 and proxies `/api` to the running `studio` container. Go changes need `make up` (it rebuilds the image).
- UI tests: `make verify-ui` drives the studio's page in Chromium (Playwright) against the running stack: building a flow in the editor, the node types, and the live view through a studio restart. `make verify` runs it too. The first run downloads Chromium (about 150 MB); a failed test leaves a trace and a screenshot in `studio/ui/test-results/` (`npx playwright show-trace <file>`).
```

In `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`, replace

```markdown
| `tail` | the tail drawer's record list | `TailDrawer.tsx` |
```

with

```markdown
| `tail` | the tail drawer: its records, its instance picker and Send, and the box that scrolls | `TailDrawer.tsx` |
```

In `docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md`, replace

```markdown
3. Chromium's `EventSource` reconnects by itself after a dropped connection,
   which is what the UI's "connection lost, reconnecting" path relies on.
```

with

```markdown
3. Chromium's `EventSource` reconnects by itself after a dropped connection,
   which is what the UI's "connection lost, reconnecting" path relies on.

## 9. Built as decided in its plan

- `dragTo` drives both the palette's HTML5 drop and React Flow's handle-to-handle
  connection in Chromium; no `page.mouse` fallback was needed (checked live).
- A new flow's canvas zooms in on, and centres, the first node it gets
  (React Flow's `fitView` on an empty canvas). The editor test therefore drops
  the producer first, the topic and consumer into free corners away from the
  controls, then presses React Flow's own "fit view" control before clicking.
- The Inspector's labels are not tied to their controls, so a field is the
  control right after its label text (`label:text-is("Group id") + *`), which
  needs no UI change.
- `data-testid="tail"` marks the whole drawer, not only its list: the instance
  picker, Send and the scrolling box live there too.
- The teardown reads each of the test's flows back through `/api` and deletes
  exactly the topics and groups those files name, then retries the group delete
  once after 3 s (a consumer may still be leaving).
- `e2e/tsconfig.json` uses Node's types from `@types/node`, which `npm ci`
  already installs (vite brings it in); no new dependency.
```

- [ ] **Step 4: Check the whole thing**

Run: `make test && make down && make verify`
Expected: `STUDIO OK (...)`, then `11 passed`, `UI OK` and `VERIFY OK`. Then:

```sh
curl -sS -o /dev/null -w '%{http_code}\n' localhost:8082/api/health   # 200: the restart test left the studio running
```

Run the cleanup check from Task 1 Step 7.

- [ ] **Step 5: Once more, for timing**

Run: `make verify-ui`
Expected: `11 passed` again. Then `make down`. `docker ps -aq -f label=studio.flow` and `git status --short flows` print nothing.

- [ ] **Step 6: Commit**

```bash
git add studio/ui/e2e/live.spec.ts Makefile AGENTS.md README.md docs/superpowers/specs/2026-10-07-studio-ui-tests-design.md
git commit -m "ui tests: the live view (studio restart, deleted flow, node warning, tail scroll); make verify runs the UI suite; docs" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
