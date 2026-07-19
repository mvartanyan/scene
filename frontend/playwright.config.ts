import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.BASE_URL ?? 'http://host.docker.internal:8000';
const chromiumChannel = process.env.PLAYWRIGHT_CHROMIUM_CHANNEL;
const webServerCommand = process.env.PW_WEB_SERVER_COMMAND;
const reuseExistingServer = process.env.PW_REUSE_EXISTING_SERVER === 'true';

export default defineConfig({
  testDir: './tests',
  timeout: 60 * 1000,
  expect: {
    timeout: 5000,
  },
  use: {
    baseURL,
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        ...(chromiumChannel ? { channel: chromiumChannel } : {}),
      },
    },
  ],
  reporter: [['list'], ['html', { outputFolder: 'playwright-report', open: 'never' }]],
  ...(webServerCommand
    ? {
        webServer: {
          command: webServerCommand,
          url: baseURL,
          reuseExistingServer,
          timeout: 120 * 1000,
        },
      }
    : {}),
});
