// The live view (spec M6): numbers that stop being live say so, a deleted flow
// closes, a silent node warns, and the tail follows only at the bottom.
import { deleteFlow, deployFlow, expect, flowItem, messageCount, nodeOf, runtimeOf, simple, tail, test, timer, topBar } from './studio'

test('a studio restart pauses the numbers, then they come back', async ({ page, studio }) => {
  const name = studio.unique('restart')
  await deployFlow(await studio.create(simple(name, timer(200))))
  await studio.open(page, name)
  const line = runtimeOf(page, 'consumer-1')
  await expect.poll(() => messageCount(line), { timeout: 20_000 }).toBeGreaterThan(0)

  studio.stopStudio()
  await expect(topBar(page)).toContainText('live numbers paused:', { timeout: 3_000 })
  await expect(page.getByRole('main')).toHaveClass(/paused/)

  await studio.startStudio()
  await expect(topBar(page)).not.toContainText('live numbers paused', { timeout: 15_000 })
  const countBefore = await messageCount(line)
  expect(countBefore).not.toBeNull()
  await expect.poll(() => messageCount(line), { timeout: 10_000 }).toBeGreaterThan(countBefore ?? 0)
})

test('a flow deleted elsewhere closes, saying so', async ({ page, studio }) => {
  const name = studio.unique('deleted')
  const id = await studio.create(simple(name))
  await studio.open(page, name)
  await deleteFlow(id)
  await expect(topBar(page)).toContainText('the open flow was deleted', { timeout: 3_000 })
  await expect(page.getByRole('main')).toContainText('Open a flow on the left or click New.')
  await expect(flowItem(page, name)).toHaveCount(0)
})

test('a node that stops answering shows why', async ({ page, studio }) => {
  const name = studio.unique('silent')
  const id = await studio.create(simple(name))
  await deployFlow(id)
  const deployed = Date.now()
  await studio.open(page, name)
  await expect(runtimeOf(page, 'consumer-1')).toContainText('msgs', { timeout: 10_000 })
  await page.waitForTimeout(Math.max(0, 6_000 - (Date.now() - deployed))) // past the 5 s start-up grace

  studio.pause(id, 'consumer-1')
  await expect(runtimeOf(page, 'consumer-1')).toContainText('stats:', { timeout: 3_000 })
  await expect(runtimeOf(page, 'producer-1')).toContainText('msgs')
  studio.resume(id, 'consumer-1')
  await expect(runtimeOf(page, 'consumer-1')).not.toContainText('stats:', { timeout: 5_000 })
})

test('the tail follows new records only while scrolled to the bottom', async ({ page, studio }) => {
  const name = studio.unique('scroll')
  await deployFlow(await studio.create(simple(name, timer(250))))
  await studio.open(page, name)
  await nodeOf(page, 'consumer-1').click()
  const drawer = tail(page)
  const records = drawer.getByRole('listitem')
  await expect.poll(() => records.count(), { timeout: 20_000 }).toBeGreaterThan(20) // more than the drawer shows

  await drawer.evaluate((el) => el.scrollTo({ top: 0 }))
  const firstRecord = await records.first().textContent()
  const countAtTop = await records.count()
  await expect.poll(() => records.count(), { timeout: 5_000 }).toBeGreaterThan(countAtTop) // new records arrived
  expect(await records.first().textContent()).toBe(firstRecord)
  expect(await drawer.evaluate((el) => el.scrollTop)).toBeLessThan(10)

  await drawer.evaluate((el) => el.scrollTo({ top: el.scrollHeight }))
  const countAtBottom = await records.count()
  await expect.poll(() => records.count(), { timeout: 5_000 }).toBeGreaterThan(countAtBottom)
  await expect(records.last()).toBeInViewport() // the newest record came into view
})
