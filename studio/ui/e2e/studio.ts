// The UI tests' fixtures. Tests that stop the studio or pause a node call docker
// on the host. Every name a test makes is unique (studio-ui-<label>-<time>). Each
// test's fixture removes its flows when it ends, even after a failure (by id, or
// by name for one built in the editor); the topics and consumer groups those
// flows named are deleted once, after the last test, by literal name and only if
// they are studio-ui-….
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

type KafkaCleanup = { topics: Set<string>; groups: Set<string> }

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

export const test = base.extend<{ studio: Studio }, { kafkaCleanup: KafkaCleanup }>({
  // One worker runs every test, so this deletes the run's topics and groups in two
  // Kafka tool runs instead of two per test (each one starts a JVM in the broker).
  kafkaCleanup: [
    async ({}, use) => {
      const cleanup: KafkaCleanup = { topics: new Set(), groups: new Set() }
      await use(cleanup)
      // Only names this suite makes: a test that wires a shared topic such as orders never deletes it.
      const suiteMade = (xs: Set<string>) => [...xs].filter((x) => x.startsWith('studio-ui-'))
      const topics = suiteMade(cleanup.topics)
      const groups = suiteMade(cleanup.groups)
      if (topics.length) await tolerate(() => kafka('kafka-topics.sh', '--delete', '--topic', topics.map(literal).join('|')))
      if (groups.length) {
        const deleteGroups = () => kafka('kafka-consumer-groups.sh', '--delete', ...groups.flatMap((g) => ['--group', g]))
        await tolerate(async () => {
          try {
            deleteGroups()
          } catch {
            // the last test's consumers may still be leaving: once more, a little later
            await new Promise((r) => setTimeout(r, 3000))
            deleteGroups()
          }
        })
      }
    },
    { scope: 'worker', timeout: 60_000 },
  ],
  // Its own timeout: a test-scoped fixture's teardown would otherwise share the
  // test's budget, and a restart alone may wait 45 s for the studio.
  studio: [
    async ({ kafkaCleanup }, use) => {
      const names = new Set<string>() // flows, topics and groups the test named
      const ids = new Set<string>() // flows the test created or looked up
      let stopped = false
      // A flow's topics and groups go to the run's cleanup as soon as it exists, so
      // a test that deletes its own flow still has them removed.
      const record = (flow: Flow) => {
        const named = topicsAndGroups(flow)
        named.topics.forEach((t) => kafkaCleanup.topics.add(t))
        named.groups.forEach((g) => kafkaCleanup.groups.add(g))
      }
      const studio: Studio = {
        unique(label) {
          const name = `studio-ui-${label}-${Date.now()}`
          names.add(name)
          return name
        },
        async create(flow) {
          names.add(flow.name)
          record(flow)
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
          for (const c of containers(flowId, node)) docker('kill', '-s', 'STOP', c)
        },
        resume(flowId, node) {
          for (const c of containers(flowId, node)) docker('kill', '-s', 'CONT', c)
        },
      }
      await use(studio)

      // Teardown, even after a failure: restart the studio, resume the test's
      // containers (a paused one would make its removal wait), remove its flows.
      if (stopped) await tolerate(() => studio.startStudio())
      await tolerate(async () => {
        for (const f of await api<{ id: string; name: string }[]>('GET', '/api/flows')) {
          if (names.has(f.name)) ids.add(f.id) // built in the editor, never looked up
        }
      })
      for (const id of ids) {
        await tolerate(() => containers(id).forEach((c) => docker('kill', '-s', 'CONT', c)))
        await tolerate(async () => record(await api<Flow>('GET', `/api/flows/${id}`)))
        await tolerate(() => deleteFlow(id))
      }
    },
    { timeout: 120_000 },
  ],
})
