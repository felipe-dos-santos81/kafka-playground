// The UI tests' fixture and helpers. Flows are set up and removed through the
// studio's API; tests that stop the studio or pause a node call docker on the
// host. Every name a test makes is unique (studio-ui-<label>-<time>), and the
// fixture removes what the test made (its flows by id or name; the topics and
// groups those flows name, if studio-ui-…), even when the test fails.
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
  // Its own timeout: a test-scoped fixture's teardown would otherwise share the test's budget.
  studio: [async ({}, use) => {
    const names = new Set<string>()
    const ids = new Set<string>()
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
        const id = (await api<{ id: string }>('POST', '/api/flows', flow)).id
        ids.add(id)
        return id
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
        ids.add(f.id)
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
    // Every step runs whatever an earlier one did, and nothing is rethrown.
    for (const c of paused) {
      try {
        docker('kill', '-s', 'CONT', c)
      } catch {
        // gone already
      }
    }
    try {
      if (stopped) await studio.startStudio()
    } catch {
      // the flows below cannot be reached; the topics and groups still can
    }
    const topics: string[] = []
    const groups: string[] = []
    let listed: { id: string; name: string }[] = []
    try {
      listed = await api('GET', '/api/flows')
    } catch {
      // only the ids recorded so far
    }
    const doomed = new Set([...ids, ...listed.filter((f) => names.has(f.name)).map((f) => f.id)])
    for (const id of doomed) {
      try {
        const file = await api<Flow>('GET', `/api/flows/${id}`)
        for (const n of file.nodes) {
          if (n.type === 'topic' && n.data.name) topics.push(String(n.data.name))
          if (n.type === 'consumer' && n.data.group) groups.push(String(n.data.group))
        }
      } catch {
        // already gone, or unreadable: still try to delete it
      }
      try {
        await studio.remove(id) // stops its containers first
      } catch {
        // already gone
      }
    }
    // Only names this suite makes: a test that wires a shared topic such as orders never deletes it.
    const ours = (xs: string[]) => [...new Set(xs.filter((x) => x.startsWith('studio-ui-')))]
    const ourTopics = ours(topics)
    const ourGroups = ours(groups)
    try {
      if (ourTopics.length) kafka('kafka-topics.sh', '--delete', '--topic', ourTopics.join('|'))
    } catch {
      // a topic the test never deployed does not exist
    }
    for (const attempt of [1, 2]) {
      try {
        if (ourGroups.length) kafka('kafka-consumer-groups.sh', '--delete', ...ourGroups.flatMap((g) => ['--group', g]))
        break
      } catch {
        // a group still has a member leaving, or never existed: try once more
        if (attempt === 1) await new Promise((r) => setTimeout(r, 3000))
      }
    }
  }, { timeout: 60_000 }],
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
