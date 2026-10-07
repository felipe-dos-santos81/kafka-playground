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
  expect(before).toBeGreaterThanOrEqual(0)
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
  const id = await studio.create(chain(name, timer(250), topic(name), consumer(name)))
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
