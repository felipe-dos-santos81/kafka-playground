// The editor: flows built and run through the UI.
import {
  chain,
  connect,
  consumer,
  deployFlow,
  edgeOf,
  expectGrowing,
  expect,
  field,
  flowIdOf,
  flowItem,
  flowRow,
  manual,
  messageCount,
  node,
  nodeOf,
  paletteItem,
  ruleOf,
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

  const pane = page.locator('.react-flow__pane')
  await paletteItem(page, 'producer').dragTo(pane, { targetPosition: { x: 60, y: 60 } })
  await paletteItem(page, 'topic').dragTo(pane, { targetPosition: { x: 60, y: 200 } })
  await paletteItem(page, 'consumer').dragTo(pane, { targetPosition: { x: 60, y: 340 } })

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
  await expectGrowing(line)

  await nodeOf(page, 'consumer-1').click()
  await expect(tail(page).getByRole('listitem').first()).toBeVisible({ timeout: 10_000 })

  await page.getByRole('button', { name: 'Stop' }).click()
  await expect(topBar(page).getByText('stopped', { exact: true })).toBeVisible({ timeout: 15_000 })
  expect(studio.containers(await flowIdOf(name))).toEqual([])
})

test('a router built in the editor: wiring adds rules, Deploy wants their conditions', async ({ page, studio }) => {
  const name = studio.unique('router-editor')
  const flow = chain(name, manual, topic(`${name}-in`), consumer(`${name}-in`))
  flow.nodes.forEach((n, i) => (n.position = { x: 60, y: 20 + 80 * i })) // producer, topic, consumer at y 20, 100, 180
  flow.nodes.push(node('topic-2', 'topic', 60, topic(`${name}-big`), 360), node('topic-3', 'topic', 60, topic(`${name}-other`), 440))
  // One column, unfitted: the long names widen the nodes, and the router goes in the gap at y 270.
  flow.viewport = { x: 0, y: 0, zoom: 1 }
  await studio.create(flow)
  await studio.open(page, name)

  await paletteItem(page, 'router').dragTo(page.locator('.react-flow__pane'), { targetPosition: { x: 60, y: 270 } })
  await connect(page, 'consumer-1', 'router-1')
  await connect(page, 'router-1', 'topic-2')
  await connect(page, 'router-1', 'topic-3')
  await nodeOf(page, 'router-1').click()
  await expect(ruleOf(page, 1).getByLabel('Topic')).toHaveValue('topic-2')
  await expect(ruleOf(page, 2).getByLabel('Topic')).toHaveValue('topic-3')
  await expect(nodeOf(page, 'router-1')).toContainText('2 rules · no default')
  await expect(edgeOf(page, 'router-1', 'topic-3')).toContainText('#2')

  // An edge deleted and drawn again finds its rule kept, not a second one.
  await edgeOf(page, 'router-1', 'topic-3').locator('.react-flow__edge-text').dispatchEvent('click') // a node covers it
  await page.keyboard.press('Backspace')
  await expect(edgeOf(page, 'router-1', 'topic-3')).toHaveCount(0)
  await nodeOf(page, 'router-1').click()
  await expect(ruleOf(page, 2)).toContainText('Not wired')
  await connect(page, 'router-1', 'topic-3')
  await expect(ruleOf(page, 2)).not.toContainText('Not wired')
  await expect(page.locator('.inspector fieldset.rule')).toHaveCount(2)

  await page.getByRole('button', { name: 'Save' }).click()
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: router-1: rule 1: when is required')

  await ruleOf(page, 1).getByLabel('When').fill('msg.total > 100')
  await ruleOf(page, 2).getByRole('button', { name: 'Remove' }).click()
  await field(page, 'Default').selectOption('topic-3')
  await expect(nodeOf(page, 'router-1')).toContainText(`1 rule · default ${name}-other`)
  await expect(edgeOf(page, 'router-1', 'topic-2')).toContainText('#1')
  await expect(edgeOf(page, 'router-1', 'topic-3')).toContainText('default')
  await page.getByRole('button', { name: 'Save' }).click()
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page).getByText('running', { exact: true })).toBeVisible({ timeout: 15_000 })
})

test('a wire the edge table refuses is not drawn', async ({ page, studio }) => {
  const name = studio.unique('wire')
  await studio.create({ ...simple(name), edges: [] })
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
  const item = flowRow(page, name)
  await expect(item).toContainText('(stopped)')
  await deployFlow(id) // another tab, or curl
  await expect(item).toContainText('(running)', { timeout: 6_000 }) // the list re-reads every 5 s
})
