import { expect, test, type APIRequestContext, type Locator, type Page } from '@playwright/test';

const apiBase =
  process.env.API_BASE_URL ?? 'http://host.docker.internal:8000/api';

type SeededConfiguration = {
  projectId: string;
};

async function seedProjectConfiguration(
  request: APIRequestContext
): Promise<SeededConfiguration> {
  const timestamp = Date.now();
  let projectId: string | undefined;
  try {
    const projectResponse = await request.post(`${apiBase}/projects`, {
      data: {
        name: `Configuration Project ${timestamp}`,
        slug: `config-${timestamp}`,
        description: 'Project seeded by configuration form regression tests',
      },
    });
    expect(projectResponse.ok()).toBeTruthy();
    const project = await projectResponse.json();
    projectId = project.id;

    const pageResponse = await request.post(`${apiBase}/pages`, {
      data: {
        project_id: project.id,
        name: 'Homepage',
        url: 'https://example.com/',
        reference_url: 'https://www.example.com/',
        basic_auth_username: 'scene',
        basic_auth_password: 'secret',
        preparatory_actions: [
          { type: 'wait_for_selector', selector: 'body', timeout_ms: 1000 },
        ],
        preparatory_js: 'window.__sceneReady = true;',
      },
    });
    expect(pageResponse.ok()).toBeTruthy();
    const seededPage = await pageResponse.json();

    const taskResponse = await request.post(`${apiBase}/tasks`, {
      data: {
        project_id: project.id,
        page_id: seededPage.id,
        name: 'Homepage screenshot',
        browsers: ['chromium'],
        viewports: [{ width: 1280, height: 720 }],
        task_js: 'window.__sceneTask = true;',
        task_actions: [{ type: 'wait', wait_ms: 50 }],
      },
    });
    expect(taskResponse.ok()).toBeTruthy();
    const task = await taskResponse.json();

    const batchResponse = await request.post(`${apiBase}/batches`, {
      data: {
        project_id: project.id,
        name: 'Default batch',
        description: 'Configuration regression batch',
        task_ids: [task.id],
        jira_issue: 'SCENE-5',
        run_diff_threshold: 2.5,
        execution_diff_threshold: 1.5,
      },
    });
    expect(batchResponse.ok()).toBeTruthy();

    return { projectId: project.id };
  } catch (error) {
    if (projectId) {
      await request.delete(`${apiBase}/projects/${projectId}`);
    }
    throw error;
  }
}

async function assertNoHorizontalOverflow(page: Page): Promise<void> {
  const overflow = await page.evaluate(() => ({
    viewportWidth: window.innerWidth,
    documentWidth: document.documentElement.scrollWidth,
    bodyWidth: document.body.scrollWidth,
  }));
  expect(overflow.documentWidth).toBeLessThanOrEqual(overflow.viewportWidth + 1);
  expect(overflow.bodyWidth).toBeLessThanOrEqual(overflow.viewportWidth + 1);
}

async function expectNamedField(form: Locator, name: string): Promise<void> {
  await expect(form.locator(`[name="${name}"]`)).toBeVisible();
}

async function openGlobalConfiguration(page: Page): Promise<void> {
  await page.goto('/projects');
  const configLink = page.locator('a[aria-label="Configuration"]');
  if (!(await configLink.isVisible())) {
    await page.locator('.navbar-toggler').click();
  }
  await configLink.click();
  await expect(page.locator('#configModal')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Configuration' })).toBeVisible();
}

async function assertProjectConfigurationForms(page: Page, projectId: string): Promise<void> {
  await page.goto(`/projects?project_id=${projectId}`);

  const pageForm = page.getByTestId('page-configuration-form');
  await expectNamedField(pageForm, 'name');
  await expectNamedField(pageForm, 'url');
  await expectNamedField(pageForm, 'reference_url');
  await expectNamedField(pageForm, 'basic_auth_username');
  await expectNamedField(pageForm, 'basic_auth_password');
  await expectNamedField(pageForm, 'preparatory_actions');
  await expectNamedField(pageForm, 'preparatory_js');
  await assertNoHorizontalOverflow(page);

  await page.getByRole('tab', { name: /Tasks/ }).click();
  const taskForm = page.getByTestId('task-configuration-form');
  await expectNamedField(taskForm, 'name');
  await expectNamedField(taskForm, 'page_id');
  await expect(taskForm.getByText('Browsers')).toBeVisible();
  await expect(taskForm.getByLabel('Chromium')).toBeVisible();
  await expect(taskForm.getByText('Viewports')).toBeVisible();
  await expect(taskForm.getByLabel('1280 × 720')).toBeVisible();
  await expectNamedField(taskForm, 'task_js');
  await expectNamedField(taskForm, 'task_actions');
  await assertNoHorizontalOverflow(page);

  await page.getByRole('tab', { name: /Batches/ }).click();
  const batchForm = page.getByTestId('batch-configuration-form');
  await expectNamedField(batchForm, 'name');
  await expectNamedField(batchForm, 'description');
  await expect(batchForm.getByLabel('Homepage screenshot')).toBeVisible();
  await expectNamedField(batchForm, 'jira_issue');
  await expectNamedField(batchForm, 'run_diff_threshold');
  await expectNamedField(batchForm, 'execution_diff_threshold');
  await assertNoHorizontalOverflow(page);
}

test.describe('Configuration forms', () => {
  test('global configuration modal exposes aligned responsive controls', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 900 });
    await openGlobalConfiguration(page);

    await expect(page.getByTestId('global-config-browsers')).toBeVisible();
    await expect(page.getByTestId('global-config-viewports')).toBeVisible();
    await expect(page.getByTestId('global-config-run-timeout')).toBeVisible();
    await expect(page.getByTestId('global-config-capture-delay')).toBeVisible();
    await expect(page.getByTestId('global-config-diff-tolerance')).toBeVisible();
    await expect(page.getByTestId('global-config-run-concurrency')).toBeVisible();
    await expect(page.getByTestId('global-config-host-url')).toBeVisible();
    await expect(page.getByTestId('global-config-timezone')).toBeVisible();

    await expect(page.locator('[name="new_browser"]')).toBeVisible();
    await expect(page.locator('[name="run_timeout_seconds"]')).toBeVisible();
    await expect(page.locator('[name="capture_post_wait_ms"]')).toBeVisible();
    await expect(page.locator('[name="diff_pixel_tolerance"]')).toBeVisible();
    await expect(page.locator('[name="max_concurrent_executions"]')).toBeVisible();
    await expect(page.locator('[name="scene_host_url"]')).toBeVisible();
    await expect(page.getByLabel('Coordinated Universal Time (UTC)')).toBeVisible();
    await expect(page.getByLabel('Local browser timezone')).toBeVisible();

    const uniqueWidth = 390 + (Date.now() % 1000);
    const uniqueHeight = 844 + (Date.now() % 1000);
    const uniqueLabel = `${uniqueWidth} × ${uniqueHeight}`;
    const viewportSection = page.getByTestId('global-config-viewports');
    try {
      await viewportSection.locator('[name="width"]').fill(String(uniqueWidth));
      await viewportSection.locator('[name="height"]').fill(String(uniqueHeight));
      await viewportSection.getByRole('button', { name: 'Add Viewport' }).click();
      await expect(page.getByRole('alert')).toContainText('Viewport added.');
      await expect(page.getByTestId('global-config-viewports').getByText(uniqueLabel)).toBeVisible();
      await assertNoHorizontalOverflow(page);
    } finally {
      const createdViewport = page
        .getByTestId('global-config-viewports')
        .locator('.list-group-item')
        .filter({ hasText: uniqueLabel });
      if (await createdViewport.isVisible()) {
        await createdViewport.getByRole('button', { name: 'Remove' }).click();
        await expect(page.getByRole('alert')).toContainText('Viewport removed.');
      }
    }
  });

  test('project configuration forms preserve page task and batch fields', async ({
    page,
    request,
  }) => {
    const seeded = await seedProjectConfiguration(request);
    try {
      await page.setViewportSize({ width: 1280, height: 900 });
      await assertProjectConfigurationForms(page, seeded.projectId);

      await page.setViewportSize({ width: 390, height: 900 });
      await assertProjectConfigurationForms(page, seeded.projectId);
    } finally {
      await request.delete(`${apiBase}/projects/${seeded.projectId}`);
    }
  });
});
