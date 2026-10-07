// Node types and the tail drawer, on flows created through the API.
import { chain, consumer, deployFlow, edge, expect, node, nodeOf, runtimeOf, simple, tail, test, timer, topBar, topic } from './studio'

// The producer's records: the first has qty and price, the second neither, so
// the transform cannot multiply them.
const twoOrders = {
  source: 'manual',
  key: '',
  value: '{{if eq .Seq 1}}{"id": 1, "qty": 2, "price": 3}{{else}}{"id": {{.Seq}}}{{end}}',
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
  const transform = runtimeOf(page, 'transform-1')
  await expect(transform).toContainText(/(?<!\d)2 msgs/, { timeout: 15_000 })
  await expect(transform).toContainText(/(?<!\d)1 errors/)
  await expect(nodeOf(page, 'transform-1')).toHaveAttribute('title', /invalid operation/)

  const bad = studio.unique('transform-bad')
  await studio.create(withTransform(bad, 'msg.'))
  await studio.open(page, bad)
  await page.getByRole('button', { name: 'Deploy' }).click()
  await expect(topBar(page)).toContainText('422: transform-1: expr:')
})

test('a consumer with instances shows each one and tails the one picked', async ({ page, studio }) => {
  test.setTimeout(120_000) // three containers start, then the group rebalances
  const name = studio.unique('instances')
  const id = await studio.create(chain(name, timer(100), topic(name, 3), consumer(name, { instances: 3 })))
  await deployFlow(id)
  await studio.open(page, name)

  await expect(nodeOf(page, 'consumer-1')).toContainText('3/3 running', { timeout: 15_000 })
  const line = runtimeOf(page, 'consumer-1')
  let partitionOf: Record<string, string> = {} // instance → the partition it holds
  await expect(async () => {
    const text = (await line.textContent()) ?? ''
    partitionOf = Object.fromEntries([...text.matchAll(/#(\d) p([\d,]+)/g)].map((m) => [m[1], m[2]]))
    expect(Object.keys(partitionOf).sort()).toEqual(['1', '2', '3'])
    expect(Object.values(partitionOf).sort()).toEqual(['0', '1', '2']) // one partition each
  }).toPass({ timeout: 45_000 })

  await nodeOf(page, 'consumer-1').click()
  const picker = tail(page).getByRole('combobox')
  await expect(picker.getByRole('option')).toHaveText(['instance 1', 'instance 2', 'instance 3'])
  await picker.selectOption('2')
  const records = tail(page).getByRole('listitem')
  await expect(records.first()).toBeVisible({ timeout: 10_000 })
  for (const text of await records.allTextContents()) expect(text.startsWith(`p${partitionOf['2']}@`)).toBe(true)
})

test("a consumer's group rewinds from the Inspector once the flow is stopped", async ({ page, studio }) => {
  const name = studio.unique('rewind')
  await deployFlow(await studio.create(simple(name))) // a deploy creates the topic
  await studio.open(page, name)
  await nodeOf(page, 'consumer-1').click()
  const inspector = page.locator('.inspector')
  const earliest = page.getByRole('group', { name: 'Rewind group' }).getByRole('button', { name: 'to earliest' })
  await expect(earliest).toBeDisabled()
  await expect(inspector).toContainText('Stop the flow to rewind its group.')

  await page.getByRole('button', { name: 'Stop' }).click()
  await expect(topBar(page).getByText('stopped', { exact: true })).toBeVisible({ timeout: 15_000 })
  await earliest.click()
  await expect(inspector).toContainText(`${name} on ${name}: 1 partition rewound to earliest`)
})
