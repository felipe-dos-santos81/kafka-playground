// Browser tests of the studio's UI against the running stack (`make verify-ui`).
// One worker: a test that stops the studio or pauses a node container must not
// share them with another test.
import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: 'e2e',
  workers: 1,
  timeout: 60_000,
  reporter: 'list',
  outputDir: 'test-results',
  use: {
    baseURL: process.env.STUDIO_URL ?? 'http://localhost:8082',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
