import { test, expect } from '@playwright/test';

const apiBase =
  process.env.API_BASE_URL ?? 'http://host.docker.internal:8000/api';

test.describe('Runs dashboard', () => {
  test('opens run detail overlay from log', async ({ page, request }) => {
    let runsResponse;
    try {
      runsResponse = await request.get(`${apiBase}/runs`);
    } catch (error) {
      test.skip(true, 'Runs API not reachable');
      return;
    }

    test.skip(!runsResponse.ok(), 'Runs API not reachable');

    const runs = await runsResponse.json();
    test.skip(runs.length === 0, 'No runs available in fixture data');

    await page.goto('/runs');

    const entries = page.locator('#run-log .run-log-entry');
    const firstRunButton = entries.nth(0).locator('.run-select-btn');
    await firstRunButton.click();

    const modal = page.locator('#runDetailModal');
    await expect(modal).toBeVisible();

    const title = page.locator('#run-detail-modal-content h5.modal-title');
    await expect(title).not.toHaveText('Run Detail');

    await expect(page.locator('#run-detail-modal-content table')).toBeVisible();
  });
});
