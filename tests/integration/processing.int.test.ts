import type { Worker } from 'bullmq';
import request from 'supertest';
import type { Express } from 'express';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { deadLetterJobId, deadLetterQueue } from '../../src/queues/dead-letter.queue.js';
import {
  BillingServiceException,
  type BillingGateway,
} from '../../src/services/billing.mock.service.js';
import {
  taskResultSchema,
  type TaskJobData,
  type TaskResult,
  type TaskType,
} from '../../src/types/task.types.js';
import { createTaskWorker, type TaskWorkerOptions } from '../../src/workers/task.worker.js';
import { KEY_A1, TENANT_A } from './constants.js';
import {
  buildApp,
  closeConnections,
  documentPayload,
  invoicePayload,
  metricValue,
  notificationPayload,
  resetState,
  waitFor,
  waitForJobState,
} from './helpers.js';

const paidInvoice: BillingGateway['processInvoice'] = async () => ({
  taskType: 'PROCESS_INVOICE',
  invoiceId: '7d0f8c63-0c2a-4c55-9f0e-2a1d9a7d4c11',
  status: 'PAID',
  pdfUrl: 'https://billing.example.test/invoices/7d0f8c63.pdf',
  processedAt: new Date().toISOString(),
});

describe('task processing against a real Redis', () => {
  let app: Express;
  let worker: Worker<TaskJobData, TaskResult, TaskType> | undefined;

  const startWorker = (options: TaskWorkerOptions): void => {
    worker = createTaskWorker(options);
  };
  const submit = async (body: unknown): Promise<string> => {
    const response = await request(app)
      .post('/api/v1/tasks/process')
      .set('X-API-Key', KEY_A1)
      .send(body as object);
    expect(response.status).toBe(202);
    return response.body.taskId as string;
  };
  const status = async (taskId: string) =>
    (await request(app).get(`/api/v1/tasks/${taskId}`).set('X-API-Key', KEY_A1)).body as Record<
      string,
      unknown
    >;

  beforeAll(async () => {
    app = await buildApp();
  });

  beforeEach(resetState);

  afterEach(async () => {
    await worker?.close();
    worker = undefined;
  });

  afterAll(closeConnections);

  it('completes all three task types with schema-valid results', async () => {
    const billing = vi.fn(paidInvoice);
    startWorker({ billing: { processInvoice: billing } });

    const taskIds = await Promise.all(
      [documentPayload, notificationPayload, invoicePayload].map(submit),
    );
    await Promise.all(taskIds.map((taskId) => waitForJobState(taskId, ['completed'])));

    const results = await Promise.all(taskIds.map(status));
    for (const result of results) {
      expect(result).toMatchObject({ state: 'completed', attemptsMade: 1, failedReason: null });
      expect(taskResultSchema.safeParse(result.result).success).toBe(true);
    }
    expect(results.map((result) => (result.result as TaskResult).taskType)).toEqual([
      'PROCESS_DOCUMENT',
      'SEND_NOTIFICATION',
      'PROCESS_INVOICE',
    ]);
    expect(results[0]?.result).toMatchObject({ documentId: 'doc-1', wordCount: 3 });
    expect(billing).toHaveBeenCalledOnce();
    // The worker's "completed" event (which bumps the counter) may land after the state change.
    for (const taskType of ['PROCESS_DOCUMENT', 'SEND_NOTIFICATION', 'PROCESS_INVOICE']) {
      await waitFor(
        async () =>
          (await metricValue('tasks_processed_total', {
            task_type: taskType,
            outcome: 'completed',
          })) === 1,
        `tasks_processed_total{${taskType},completed} to reach 1`,
      );
    }
  });

  it('retries with exponential backoff until the task fails, then copies it to the dead-letter queue', async () => {
    const callTimes: number[] = [];
    startWorker({
      // A high threshold keeps the breaker out of this test.
      circuitBreaker: { failureThreshold: 100 },
      billing: {
        processInvoice: async () => {
          callTimes.push(Date.now());
          throw new BillingServiceException();
        },
      },
    });

    const taskId = await submit(invoicePayload);
    await waitForJobState(taskId, ['failed']);

    expect(await status(taskId)).toMatchObject({
      state: 'failed',
      attemptsMade: 3,
      maxAttempts: 3,
      failedReason: 'Billing service is temporarily unavailable',
    });
    expect(callTimes).toHaveLength(3);
    // TASK_BACKOFF_DELAY_MS=100 with exponential backoff: ~100ms, then ~200ms.
    const [first = 0, second = 0, third = 0] = callTimes;
    expect(second - first).toBeGreaterThanOrEqual(90);
    expect(third - second).toBeGreaterThanOrEqual(190);

    const entry = await waitFor(
      () => deadLetterQueue.getJob(deadLetterJobId(taskId)),
      'dead-letter copy',
    );
    expect(entry.data).toMatchObject({
      originalJobId: taskId,
      taskType: 'PROCESS_INVOICE',
      attemptsMade: 3,
      failedReason: 'Billing service is temporarily unavailable',
      data: { ...invoicePayload, meta: expect.objectContaining({ tenantId: TENANT_A }) },
    });
    await waitFor(
      async () =>
        (await metricValue('tasks_dead_lettered_total', { task_type: 'PROCESS_INVOICE' })) === 1,
      'tasks_dead_lettered_total to reach 1',
    );
  });

  it('opens the circuit after consecutive billing failures and then rejects without calling billing', async () => {
    const billing = vi.fn<BillingGateway['processInvoice']>(async () => {
      throw new BillingServiceException();
    });
    // CIRCUIT_BREAKER_FAILURE_THRESHOLD=2 from the integration environment.
    startWorker({ billing: { processInvoice: billing } });

    const first = await submit(invoicePayload);
    await waitForJobState(first, ['failed']);
    expect(billing).toHaveBeenCalledTimes(2);
    expect((await status(first)).failedReason).toMatch(
      /^Circuit "billing" is open; retry after \d+ms$/u,
    );
    expect(await metricValue('circuit_breaker_state', { circuit: 'billing' })).toBe(2);

    const second = await submit({ ...invoicePayload, customerId: 'customer-2' });
    await waitForJobState(second, ['failed']);
    expect(await status(second)).toMatchObject({ state: 'failed', attemptsMade: 3 });
    expect(billing).toHaveBeenCalledTimes(2);

    // Non-billing work is unaffected by the open circuit.
    const document = await submit(documentPayload);
    expect(await waitForJobState(document, ['completed'])).toBe('completed');
  });
});
