// The editor: flows built and run through the UI.
import {
  connect,
  consumer,
  deployFlow,
  expect,
  field,
  flowItem,
  manual,
  messageCount,
  node,
  nodeOf,
  paletteItem,
  runtimeOf,
  simple,
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
  await expect.poll(() => messageCount(line), { timeout: 20_000 }).toBeGreaterThan(0)
  const firstCount = (await messageCount(line)) ?? 0
  await expect.poll(() => messageCount(line), { timeout: 10_000 }).toBeGreaterThan(firstCount)

  await nodeOf(page, 'consumer-1').click()
  await expect(tail(page).getByRole('listitem').first()).toBeVisible({ timeout: 10_000 })

  await page.getByRole('button', { name: 'Stop' }).click()
  await expect(topBar(page).getByText('stopped', { exact: true })).toBeVisible({ timeout: 15_000 })
  expect(studio.containers(await studio.idOf(name))).toEqual([])
})

test('a wire the edge table refuses is not drawn', async ({ page, studio }) => {
  const name = studio.unique('wire')
  await studio.create({
    name,
    nodes: [
      node('producer-1', 'producer', 0, manual),
      node('topic-1', 'topic', 250, topic(name)),
      node('consumer-1', 'consumer', 500, consumer(name)),
    ],
    edges: [],
  })
  await studio.open(page, name)
  await connect(page, 'producer-1', 'consumer-1') // producer → consumer: not in the edge table
  await expect(page.locator('.react-flow__edge')).toHaveCount(0)
  await connect(page, 'producer-1', 'topic-1') // the same gesture on an allowed pair draws an edge
  await expect(page.locator('.react-flow__edge')).toHaveCount(1)
})

test('a deploy refused for a node shows in the top bar', async ({ page, studio }) => {
  const name = studio.unique('refused')
  const flow = simple(name)
  flow.edges = flow.edges.filter((e) => e.target !== 'consumer-1') // the consumer reads nothing
  await studio.create(flow)
  await studio.open(page, name)
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: consumer-1: a consumer needs exactly one edge from a topic')
  await expect(nodeOf(page, 'consumer-1')).toBeVisible()
})

test('unsaved changes ask before another flow opens', async ({ page, studio }) => {
  const edited = studio.unique('edited')
  const other = studio.unique('other')
  for (const name of [edited, other]) await studio.create(simple(name))
  await studio.open(page, edited)
  const title = topBar(page).getByRole('textbox')
  await title.fill(`${edited}-renamed`)
  await expect(page.getByRole('button', { name: 'Save' })).toBeEnabled()

  page.once('dialog', (d) => d.dismiss())
  await flowItem(page, other).click()
  await expect(title).toHaveValue(`${edited}-renamed`)

  page.once('dialog', (d) => d.accept())
  await flowItem(page, other).click()
  await expect(title).toHaveValue(other)
})

test('a flow deployed elsewhere shows as running in the list', async ({ page, studio }) => {
  const name = studio.unique('elsewhere')
  const id = await studio.create(simple(name))
  await page.goto('/')
  const item = page.getByRole('listitem').filter({ hasText: name })
  await expect(item).toContainText('(stopped)')
  await deployFlow(id) // another tab, or curl
  await expect(item).toContainText('(running)', { timeout: 6_000 }) // the list re-reads every 5 s
})
