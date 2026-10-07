// The UI tests' fixtures. Tests that stop the studio or pause a node call docker
// on the host. Every name a test makes starts studio-ui- (studio-ui-<label>-<time>),
// a namespace the suite owns: after each test, even a failed one, every flow so
// named is removed; after the last test, every topic and consumer group so named.
// That also clears what an earlier, interrupted run left. Nothing else is touched.
import { test as base, expect, type Page } from '@playwright/test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { api, deleteFlow, type Flow } from './flows'
import { flowItem, topBar } from './locators'

export * from './flows'
export * from './locators'
export { expect }

const PREFIX = 'studio-ui-'
const COMPOSE_FILE = fileURLToPath(new URL('../../../docker-compose.yml', import.meta.url))
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP ?? 'localhost:19092' // inside the kafka container

const docker = (...args: string[]) => execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
const compose = (...args: string[]) => docker('compose', '-f', COMPOSE_FILE, ...args)
const kafka = (tool: string, ...args: string[]) =>
  compose('exec', '-T', 'kafka', `/opt/kafka/bin/${tool}`, '--bootstrap-server', KAFKA_BOOTSTRAP, ...args)
const ours = (listing: string) => listing.split('\n').filter((name) => name.startsWith(PREFIX))
// literal makes a name match only itself in kafka-topics.sh's --topic, which takes a regex.
const literal = (name: string) => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

// A flow's containers, by the labels the studio puts on them (studio.flow, studio.node).
function containers(flowId: string, node?: string): string[] {
  const filters = ['-f', `label=studio.flow=${flowId}`, ...(node ? ['-f', `label=studio.node=${node}`] : [])]
  return docker('ps', '-q', ...filters).split('\n').filter(Boolean)
}

async function healthy() {
  await expect.poll(() => api('GET', '/api/health').then(() => true, () => false), { timeout: 45_000 }).toBe(true)
}

// Runs one cleanup step; a failing step never stops the ones after it.
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
  open(page: Page, name: string): Promise<void>
  containers(flowId: string): string[]
  stopStudio(): void
  startStudio(): Promise<void>
  pause(flowId: string, node: string): void // SIGSTOP the node's container
  resume(flowId: string, node: string): void
}

export const test = base.extend<{ studio: Studio }, { kafkaCleanup: void }>({
  // After the last test: one listing and one delete per Kafka tool (each starts a
  // JVM in the broker), rather than a delete per test.
  kafkaCleanup: [
    async ({}, use) => {
      await use()
      await tolerate(() => {
        const topics = ours(kafka('kafka-topics.sh', '--list'))
        if (topics.length) kafka('kafka-topics.sh', '--delete', '--topic', topics.map(literal).join('|'))
      })
      await tolerate(() => {
        const groups = ours(kafka('kafka-consumer-groups.sh', '--list'))
        if (groups.length) kafka('kafka-consumer-groups.sh', '--delete', ...groups.flatMap((g) => ['--group', g]))
      })
    },
    { scope: 'worker', auto: true, timeout: 60_000 },
  ],
  // Its own timeout: a test-scoped fixture's teardown would otherwise share the
  // test's budget, and a restart alone may wait 45 s for the studio.
  studio: [
    async ({}, use) => {
      let stopped = false
      const studio: Studio = {
        unique: (label) => `${PREFIX}${label}-${Date.now()}`,
        create: async (flow) => (await api<{ id: string }>('POST', '/api/flows', flow)).id,
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

      // Even after a failure: restart the studio, then remove every studio-ui flow,
      // resuming its containers first (a paused one would make its removal wait).
      if (stopped) await tolerate(() => studio.startStudio())
      await tolerate(async () => {
        for (const f of await api<{ id: string; name: string }[]>('GET', '/api/flows')) {
          if (!f.name.startsWith(PREFIX)) continue
          await tolerate(() => containers(f.id).forEach((c) => docker('kill', '-s', 'CONT', c)))
          await tolerate(() => deleteFlow(f.id))
        }
      })
    },
    { timeout: 120_000 },
  ],
})
