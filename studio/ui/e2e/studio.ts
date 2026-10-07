// The UI tests' fixture. Tests that stop the studio or pause a node call docker on
// the host. Every name a test makes is unique (studio-ui-<label>-<time>), and the
// fixture removes what the test made, even when the test fails: its flows (by id,
// or by name for one built in the editor), then the topics and consumer groups
// they named, by literal name and only if they are studio-ui-….
import { test as base, expect, type Page } from '@playwright/test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { STUDIO_URL, api, deleteFlow, flowIdOf, topicsAndGroups, type Flow } from './flows'
import { flowItem, topBar } from './locators'

export * from './flows'
export * from './locators'
export { expect }

const COMPOSE_FILE = fileURLToPath(new URL('../../../docker-compose.yml', import.meta.url))
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP ?? 'localhost:19092' // inside the kafka container

const docker = (...args: string[]) => execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
const compose = (...args: string[]) => docker('compose', '-f', COMPOSE_FILE, ...args)
const kafka = (tool: string, ...args: string[]) =>
  compose('exec', '-T', 'kafka', `/opt/kafka/bin/${tool}`, '--bootstrap-server', KAFKA_BOOTSTRAP, ...args)
// literal makes a name match itself only in kafka-topics.sh's --topic, which is a regex.
const literal = (name: string) => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

// A flow's containers, by the labels the studio puts on them (studio.flow, studio.node).
function containers(flowId: string, node?: string): string[] {
  const filters = ['-f', `label=studio.flow=${flowId}`, ...(node ? ['-f', `label=studio.node=${node}`] : [])]
  return docker('ps', '-q', ...filters).split('\n').filter(Boolean)
}

async function healthy() {
  await expect
    .poll(() => fetch(`${STUDIO_URL}/api/health`).then((r) => r.status, () => 0), { timeout: 45_000 })
    .toBe(200)
}

// Runs one teardown step; a failing step never stops the ones after it.
async function tolerate(step: () => unknown) {
  try {
    await step()
  } catch {
    // best effort: the next step may still clean up
  }
}

type Studio = {
  unique(label: string): string // a name to use for a flow, a topic or a group
  create(flow: Flow): Promise<string> // the new flow's id
  idOf(name: string): Promise<string> // a flow built in the editor
  open(page: Page, name: string): Promise<void>
  containers(flowId: string): string[]
  stopStudio(): void
  startStudio(): Promise<void>
  pause(flowId: string, node: string): void // SIGSTOP the node's container
  resume(flowId: string, node: string): void
}

export const test = base.extend<{ studio: Studio }>({
  // Its own timeout: a test-scoped fixture's teardown would otherwise share the
  // test's budget, and a restart alone may wait 45 s for the studio.
  studio: [
    async ({}, use) => {
      const names = new Set<string>() // flows, topics and groups the test named
      const ids = new Set<string>() // flows the test created or looked up
      const topics = new Set<string>()
      const groups = new Set<string>()
      const paused = new Set<string>() // container ids
      let stopped = false
      const record = (flow: Flow) => {
        const named = topicsAndGroups(flow)
        named.topics.forEach((t) => topics.add(t))
        named.groups.forEach((g) => groups.add(g))
      }
      const studio: Studio = {
        unique(label) {
          const name = `studio-ui-${label}-${Date.now()}`
          names.add(name)
          return name
        },
        async create(flow) {
          names.add(flow.name)
          record(flow) // now, so a flow the test deletes itself still has its topics removed
          const id = (await api<{ id: string }>('POST', '/api/flows', flow)).id
          ids.add(id)
          return id
        },
        async idOf(name) {
          const id = await flowIdOf(name)
          ids.add(id)
          return id
        },
        async open(page, name) {
          await page.goto('/')
          await flowItem(page, name).click()
          await expect(topBar(page).getByRole('textbox')).toHaveValue(name)
        },
        containers,
        stopStudio() {
          stopped = true
          compose('stop', 'studio')
        },
        async startStudio() {
          compose('start', 'studio')
          await healthy()
          stopped = false
        },
        pause(flowId, node) {
          for (const c of containers(flowId, node)) {
            paused.add(c)
            docker('kill', '-s', 'STOP', c)
          }
        },
        resume(flowId, node) {
          for (const c of containers(flowId, node)) {
            docker('kill', '-s', 'CONT', c)
            paused.delete(c)
          }
        },
      }
      await use(studio)

      // Teardown, even after a failure: resume, restart, then remove what the test made.
      for (const c of paused) await tolerate(() => docker('kill', '-s', 'CONT', c))
      if (stopped) await tolerate(() => studio.startStudio())
      await tolerate(async () => {
        for (const f of await api<{ id: string; name: string }[]>('GET', '/api/flows')) {
          if (names.has(f.name)) ids.add(f.id) // built in the editor, never looked up
        }
      })
      for (const id of ids) {
        await tolerate(async () => record(await api<Flow>('GET', `/api/flows/${id}`)))
        await tolerate(() => deleteFlow(id))
      }
      // Only names this suite makes: a test that wires a shared topic such as orders never deletes it.
      const suiteMade = (xs: Set<string>) => [...xs].filter((x) => x.startsWith('studio-ui-'))
      const ownTopics = suiteMade(topics)
      const ownGroups = suiteMade(groups)
      if (ownTopics.length) {
        await tolerate(() => kafka('kafka-topics.sh', '--delete', '--topic', ownTopics.map(literal).join('|')))
      }
      if (ownGroups.length) {
        const deleteGroups = () => kafka('kafka-consumer-groups.sh', '--delete', ...ownGroups.flatMap((g) => ['--group', g]))
        await tolerate(async () => {
          try {
            deleteGroups()
          } catch {
            // a member may still be leaving: once more, a little later
            await new Promise((r) => setTimeout(r, 3000))
            deleteGroups()
          }
        })
      }
    },
    { timeout: 120_000 },
  ],
})
