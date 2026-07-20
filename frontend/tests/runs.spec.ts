import { expect, test, type APIRequestContext, type Page } from '@playwright/test';

const apiBase =
  process.env.API_BASE_URL ?? 'http://host.docker.internal:8000/api';

declare global {
  interface Window {
    __sceneRunsDashboardController?: {
      debug: () => any;
    };
    __sceneRunDashboardDebug?: any;
  }
}

type SeededRun = {
  projectId: string;
  runId: string;
};

async function seedRun(request: APIRequestContext): Promise<SeededRun> {
  const timestamp = Date.now();
  let projectId = '';

  const projectResponse = await request.post(`${apiBase}/projects`, {
    data: {
      name: `Runs Project ${timestamp}`,
      slug: `runs-${timestamp}`,
      description: 'Project seeded by runs dashboard regression tests',
    },
  });
  expect(projectResponse.ok()).toBeTruthy();
  const project = await projectResponse.json();
  projectId = project.id;

  try {
    const pageResponse = await request.post(`${apiBase}/pages`, {
      data: {
        project_id: project.id,
        name: 'Static fixture',
        url: 'https://example.com/',
      },
    });
    expect(pageResponse.ok()).toBeTruthy();
    const seededPage = await pageResponse.json();

    const taskResponse = await request.post(`${apiBase}/tasks`, {
      data: {
        project_id: project.id,
        page_id: seededPage.id,
        name: 'Modal fixture task',
        browsers: ['chromium'],
        viewports: [{ width: 800, height: 600 }],
      },
    });
    expect(taskResponse.ok()).toBeTruthy();
    const task = await taskResponse.json();

    const batchResponse = await request.post(`${apiBase}/batches`, {
      data: {
        project_id: project.id,
        name: 'Modal fixture batch',
        task_ids: [task.id],
      },
    });
    expect(batchResponse.ok()).toBeTruthy();
    const batch = await batchResponse.json();

    const runResponse = await request.post(`${apiBase}/runs`, {
      data: {
        project_id: project.id,
        batch_id: batch.id,
        purpose: 'baseline_recording',
        requested_by: 'playwright',
        timeout_seconds: 10,
      },
    });
    expect(runResponse.ok()).toBeTruthy();
    const run = await runResponse.json();
    return { projectId, runId: run.id };
  } catch (error) {
    await request.delete(`${apiBase}/projects/${projectId}`);
    throw error;
  }
}

async function waitForRunExecution(
  request: APIRequestContext,
  runId: string
): Promise<string> {
  const deadline = Date.now() + 5000;
  let firstExecutionId = '';
  while (Date.now() < deadline) {
    const response = await request.get(`${apiBase}/runs/${runId}/executions`);
    expect(response.ok()).toBeTruthy();
    const executions = await response.json();
    if (executions.length > 0) {
      firstExecutionId = executions[0].id;
      if (!['queued', 'executing'].includes(executions[0].status)) {
        return firstExecutionId;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error(
    firstExecutionId
      ? `Run ${runId} execution ${firstExecutionId} did not become terminal`
      : `Run ${runId} did not create an execution`
  );
}

async function openSeededRunModal(page: Page, runId: string): Promise<void> {
  await page.goto('/runs');
  const runButton = page.locator(`.run-log-entry[data-run-id="${runId}"] .run-select-btn`);
  await expect(runButton).toBeVisible();
  await runButton.click();
  await expect(page.locator('#runDetailModal')).toBeVisible();
  await expect(page.locator('#run-detail-shell-inner')).toHaveAttribute('data-run-id', runId);
  await expect(page.locator('#run-detail-shell table')).toBeVisible();
}

test.describe('Runs dashboard', () => {
  test('keeps the run modal stable while the run log refreshes', async ({
    page,
    request,
  }) => {
    const seeded = await seedRun(request);
    try {
      await waitForRunExecution(request, seeded.runId);
      await openSeededRunModal(page, seeded.runId);

      await page.evaluate(() => window.__sceneRunsDashboardController?.debug());
      const before = await page.evaluate(() => window.__sceneRunDashboardDebug);

      await page.waitForTimeout(11000);

      await expect(page.locator('#runDetailModal')).toBeVisible();
      await expect(page.locator('#run-detail-shell-inner')).toHaveAttribute('data-run-id', seeded.runId);
      await expect(page.locator(`.run-log-entry[data-run-id="${seeded.runId}"]`)).toHaveClass(/selected-run/);

      const after = await page.evaluate(() => window.__sceneRunsDashboardController?.debug());
      expect(after.counters.globalListeners).toBe(before.counters.globalListeners);
      expect(after.counters.runModalListeners).toBe(before.counters.runModalListeners);
      expect(after.counters.runLogPollStarts).toBe(1);
      expect(after.counters.detailPollStarts).toBeLessThanOrEqual(1);
    } finally {
      await request.delete(`${apiBase}/projects/${seeded.projectId}`);
    }
  });

  test('keeps execution viewer and log modals isolated from background refresh', async ({
    page,
    request,
  }) => {
    const seeded = await seedRun(request);
    try {
      const executionId = await waitForRunExecution(request, seeded.runId);
      await openSeededRunModal(page, seeded.runId);

      await page.locator(`[data-execution-id="${executionId}"]`).first().click();
      await expect(page.locator('#executionViewerModal')).toBeVisible();
      await expect(page.locator('#run-detail-shell-inner')).toHaveAttribute('data-run-id', seeded.runId);
      await page.waitForTimeout(5500);
      await expect(page.locator('#executionViewerModal')).toBeVisible();
      await expect(page.locator('#run-detail-shell-inner')).toHaveAttribute('data-run-id', seeded.runId);
      await page.locator('#executionViewerModal .btn-close').click();
      await expect(page.locator('#executionViewerModal')).toBeHidden();
      await expect(page.locator('#runDetailModal')).toBeVisible();
      await expect(page.locator('#run-detail-shell-inner')).toHaveAttribute('data-run-id', seeded.runId);

      await page.locator(`[data-role="execution-log-trigger"][data-execution-id="${executionId}"]`).click();
      await expect(page.locator('#executionLogModal')).toBeVisible();
      await expect(page.locator('#execution-log-body')).toBeVisible();
      await page.waitForTimeout(5500);
      await expect(page.locator('#executionLogModal')).toBeVisible();
      await expect(page.locator('#run-detail-shell-inner')).toHaveAttribute('data-run-id', seeded.runId);

      const debug = await page.evaluate(() => window.__sceneRunsDashboardController?.debug());
      expect(debug.counters.executionLogPollStarts).toBe(1);
      expect(debug.state.logPolling).toBeTruthy();
    } finally {
      await request.delete(`${apiBase}/projects/${seeded.projectId}`);
    }
  });
});
