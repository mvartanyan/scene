import { test, expect } from '@playwright/test';

const apiBase =
  process.env.API_BASE_URL ?? 'http://host.docker.internal:8000/api';

test.describe('Projects dashboard', () => {
  test('shows API-created project in list', async ({ page, request }) => {
    const timestamp = Date.now();
    const name = `Playwright Project ${timestamp}`;
    const slug = `pw-${timestamp}`;

    const response = await request.post(`${apiBase}/projects`, {
      data: { name, slug },
    });
    expect(response.ok()).toBeTruthy();
    const project = await response.json();

    await page.goto('/projects');
    await expect(page.locator('.list-group-item', { hasText: name })).toBeVisible();

    await request.delete(`${apiBase}/projects/${project.id}`);
  });

  test('renders run dashboard filters', async ({ page }) => {
    await page.goto('/runs');
    await expect(
      page.getByText('Filter Runs', { exact: false })
    ).toBeVisible();
  });
});
