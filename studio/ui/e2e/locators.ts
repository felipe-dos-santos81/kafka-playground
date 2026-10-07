// Where things are on the studio's page: accessible names first, a data-testid
// where the UI has none (node-<id>, runtime-<id>, tail).
import type { Locator, Page } from '@playwright/test'

export const topBar = (page: Page) => page.getByRole('banner')
export const flowItem = (page: Page, name: string) => page.getByRole('listitem').filter({ hasText: name }).getByText(name)
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

// messageCount reads the record count from a runtime line ("12 msgs · 3.0/s"), or
// null while the line shows none.
export async function messageCount(line: Locator): Promise<number | null> {
  const m = /(\d+) msgs/.exec((await line.textContent()) ?? '')
  return m ? Number(m[1]) : null
}
