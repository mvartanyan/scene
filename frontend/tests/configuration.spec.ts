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
        spm_ticket: 'SCENE-5',
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
  await expectNamedField(batchForm, 'spm_ticket');
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

  test('large project tabs and run scopes stay bounded and interactive', async ({
    page,
    request,
  }) => {
    test.setTimeout(120_000);
    const timestamp = Date.now();
    const itemCount = 205;
    const pageNames = Array.from({ length: itemCount }, (_, index) => `Large page ${index}`);
    const taskNames = Array.from({ length: itemCount }, (_, index) => `Large task ${index}`);
    const setupResponse = await request.post(`${apiBase}/agent/setup`, {
      data: {
        project: {
          name: `Large UI Project ${timestamp}`,
          slug: `large-ui-${timestamp}`,
          description: 'Synthetic SCENE-17 browser fixture',
        },
        pages: pageNames.map((name, index) => ({
          name,
          url: `https://example.com/large/${index}`,
        })),
        tasks: taskNames.map((name, index) => ({
          name,
          page_name: pageNames[index],
          browsers: ['chromium', 'firefox'],
          viewports: [
            { width: 1280, height: 720 },
            { width: 390, height: 844 },
          ],
        })),
        batches: [
          {
            name: 'Large browser batch',
            task_names: taskNames,
            spm_ticket: 'SCENE-17',
          },
        ],
      },
    });
    expect(setupResponse.ok()).toBeTruthy();
    const setup = await setupResponse.json();
    const projectId = setup.project.id;

    try {
      await page.goto(`/projects?project_id=${projectId}`);
      await expect(page.getByTestId('page-list-item')).toHaveCount(25);
      await expect(page.getByTestId('pages-pagination')).toContainText('1-25 of 205');
      await expect(page.getByTestId('task-list-item')).toHaveCount(0);

      await page.getByRole('tab', { name: /Tasks/ }).click();
      await expect(page.getByTestId('task-list-item')).toHaveCount(25);
      await expect(page.getByTestId('tasks-pagination')).toContainText('1-25 of 205');
      await expect(page.getByTestId('page-list-item')).toHaveCount(0);

      await page.getByRole('tab', { name: /Batches/ }).click();
      await expect(page.getByTestId('batch-list-item')).toHaveCount(1);
      await expect(page.getByTestId('batch-configuration-form').locator('[name="task_ids"]')).toHaveCount(itemCount);

      await page.goto('/runs');
      const launchForm = page.locator('#run-launch-form');
      await launchForm.locator('[data-role="launch-project"]').selectOption(projectId);
      await expect(page.getByTestId('launch-estimate')).toContainText('820 executions');
      await expect(launchForm.locator('[data-role="launch-large-warning"]')).toBeVisible();

      await launchForm.locator('label[for="task-scope-smoke"]').click();
      await expect(page.getByTestId('launch-estimate')).toContainText('4 executions');
      await expect(page.getByTestId('launch-estimate')).toContainText('(1 task)');
      await expect(launchForm.locator('[data-role="launch-large-warning"]')).toBeHidden();

      await launchForm.locator('label[for="task-scope-selected"]').click();
      await expect(page.getByTestId('launch-task-options')).toBeVisible();
      await expect(launchForm.locator('[data-role="launch-submit"]')).toBeDisabled();
      const taskCheckboxes = launchForm.locator('[data-role="launch-task-checkbox"]');
      await taskCheckboxes.nth(0).check();
      await taskCheckboxes.nth(1).check();
      await expect(page.getByTestId('launch-estimate')).toContainText('8 executions');
      await expect(page.getByTestId('launch-estimate')).toContainText('(2 tasks)');
      await expect(launchForm.locator('[data-role="launch-submit"]')).toBeEnabled();
    } finally {
      await request.delete(`${apiBase}/projects/${projectId}`);
    }
  });
});
