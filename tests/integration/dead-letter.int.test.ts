import type { Worker } from 'bullmq';
import request from 'supertest';
import type { Express } from 'express';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { redis } from '../../src/config/redis.js';
import type { AuditRecord } from '../../src/observability/audit.js';
import {
  addDeadLetter,
  deadLetterJobId,
  deadLetterQueue,
} from '../../src/queues/dead-letter.queue.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { BillingServiceException } from '../../src/services/billing.mock.service.js';
import type { TaskJobData, TaskResult, TaskType } from '../../src/types/task.types.js';
import { createTaskWorker } from '../../src/workers/task.worker.js';
import { ADMIN_PASSWORD, KEY_A1, KEY_A2, KEY_B, TENANT_A } from './constants.js';
import {
  AUDIT_STREAM_KEY,
  adminAuthHeader,
  buildApp,
  closeConnections,
  invoicePayload,
  metricValue,
  resetState,
  waitFor,
  waitForJobState,
} from './helpers.js';

describe('dead-letter queue and admin audit against a real Redis', () => {
  let app: Express;
  let worker: Worker<TaskJobData, TaskResult, TaskType>;
  let billingUp = false;

  const admin = (method: 'get' | 'post' | 'delete', path: string) =>
    request(app)[method](path).set('Authorization', adminAuthHeader);

  /** Drives one invoice through every retry into the DLQ and returns its task id. */
  const deadLetteredInvoice = async (): Promise<string> => {
    billingUp = false;
    const accepted = await request(app)
      .post('/api/v1/tasks/process')
      .set('X-API-Key', KEY_A1)
      .set('X-Request-Id', 'it-dlq-origin')
      .send(invoicePayload);
    expect(accepted.status).toBe(202);
    const taskId = accepted.body.taskId as string;
    await waitForJobState(taskId, ['failed']);
    await waitFor(() => deadLetterQueue.getJob(deadLetterJobId(taskId)), 'dead-letter copy');
    return taskId;
  };

  const auditRecords = async (): Promise<AuditRecord[]> => {
    const response = await admin('get', '/admin/api/audit?limit=50');
    expect(response.status).toBe(200);
    return response.body.items as AuditRecord[];
  };

  beforeAll(async () => {
    app = await buildApp();
  });

  beforeEach(async () => {
    await resetState();
    worker = createTaskWorker({
      circuitBreaker: { failureThreshold: 100 },
      billing: {
        processInvoice: async () => {
          if (!billingUp) {
            throw new BillingServiceException();
          }
          return {
            taskType: 'PROCESS_INVOICE',
            invoiceId: '6a4b3f1e-1d2c-4b5a-8c7d-9e0f1a2b3c4d',
            status: 'PAID',
            pdfUrl: 'https://billing.example.test/invoices/6a4b3f1e.pdf',
            processedAt: new Date().toISOString(),
          };
        },
      },
    });
  });

  afterEach(async () => {
    await worker.close();
  });

  afterAll(closeConnections);

  it('copies the exhausted task with its tenant and request id and counts it', async () => {
    const taskId = await deadLetteredInvoice();

    const entry = await deadLetterQueue.getJob(deadLetterJobId(taskId));
    expect(entry?.data.data.meta).toMatchObject({ tenantId: TENANT_A, requestId: 'it-dlq-origin' });
    await waitFor(
      async () =>
        (await metricValue('tasks_dead_lettered_total', { task_type: 'PROCESS_INVOICE' })) === 1,
      'tasks_dead_lettered_total to reach 1',
    );

    const listed = await admin('get', '/admin/api/dead-letter');
    expect(listed.status).toBe(200);
    expect(listed.body).toMatchObject({
      total: 1,
      items: [
        {
          taskId,
          taskType: 'PROCESS_INVOICE',
          attemptsMade: 3,
          tenantId: TENANT_A,
          requestId: 'it-dlq-origin',
        },
      ],
    });
  });

  it('atomically deduplicates concurrent workers, including after an entry is deleted', async () => {
    const taskId = await deadLetteredInvoice();
    const entry = await deadLetterQueue.getJob(deadLetterJobId(taskId));
    if (!entry) throw new Error('Expected dead-letter entry');
    const data = entry.data;
    await entry.remove();
    await redis.hdel(taskQueue.toKey(taskId), 'dtqDlqRouted');

    const clients = Array.from({ length: 8 }, () => redis.duplicate({ lazyConnect: true }));
    try {
      await Promise.all(clients.map((client) => client.connect()));
      const results = await Promise.all(
        Array.from({ length: 64 }, (_, index) =>
          addDeadLetter(data, clients[index % clients.length]),
        ),
      );
      expect(results.filter(Boolean)).toHaveLength(1);
      expect(await deadLetterQueue.getWaitingCount()).toBe(1);
      expect(await deadLetterQueue.getJob(deadLetterJobId(taskId))).toMatchObject({ data });
      expect(await metricValue('tasks_dead_lettered_total', { task_type: 'PROCESS_INVOICE' })).toBe(
        2,
      );

      const removed = await admin('delete', `/admin/api/dead-letter/${taskId}`);
      expect(removed.status).toBe(204);
      expect(await addDeadLetter(data)).toBe(false);
      expect(await deadLetterQueue.getWaitingCount()).toBe(0);
      expect(await metricValue('tasks_dead_lettered_total', { task_type: 'PROCESS_INVOICE' })).toBe(
        2,
      );
      const nextFailure = {
        ...data,
        failedAt: new Date(Date.parse(data.failedAt) + 1_000).toISOString(),
        attemptsMade: data.attemptsMade + 1,
      };
      await redis.hset(
        taskQueue.toKey(taskId),
        'finishedOn',
        String(Date.parse(nextFailure.failedAt)),
        'atm',
        String(nextFailure.attemptsMade),
      );
      expect(await addDeadLetter(data)).toBe(false);
      expect(await addDeadLetter(nextFailure)).toBe(true);
      expect(await metricValue('tasks_dead_lettered_total', { task_type: 'PROCESS_INVOICE' })).toBe(
        3,
      );
    } finally {
      await Promise.all(clients.map((client) => client.quit()));
    }
  });

  it('does not partially route a job when a queue key has the wrong type', async () => {
    const taskId = await deadLetteredInvoice();
    const entry = await deadLetterQueue.getJob(deadLetterJobId(taskId));
    if (!entry) throw new Error('Expected dead-letter entry');
    const data = entry.data;
    await entry.remove();
    await redis.hdel(taskQueue.toKey(taskId), 'dtqDlqRouted');
    await redis.del(deadLetterQueue.toKey('events'));
    await redis.set(deadLetterQueue.toKey('events'), 'invalid');

    await expect(addDeadLetter(data)).rejects.toThrow('Unexpected Redis key type');
    expect(await deadLetterQueue.getJob(deadLetterJobId(taskId))).toBeUndefined();
    expect(await redis.hget(taskQueue.toKey(taskId), 'dtqDlqRouted')).toBeNull();
    expect(await metricValue('tasks_dead_lettered_total', { task_type: 'PROCESS_INVOICE' })).toBe(
      1,
    );
  });

  it('rejects a corrupt counter before writing the job or its deduplication marker', async () => {
    const taskId = await deadLetteredInvoice();
    const entry = await deadLetterQueue.getJob(deadLetterJobId(taskId));
    if (!entry) throw new Error('Expected dead-letter entry');
    const data = entry.data;
    await entry.remove();
    await redis.hdel(taskQueue.toKey(taskId), 'dtqDlqRouted');
    const keys = await redis.keys(deadLetterQueue.toKey('routed-metrics-*'));
    expect(keys).toHaveLength(1);
    const key = keys[0];
    await redis.hset(key, 'PROCESS_INVOICE', '00');

    await expect(addDeadLetter(data)).rejects.toThrow('Invalid dead-letter counter');
    expect(await deadLetterQueue.getJob(deadLetterJobId(taskId))).toBeUndefined();
    expect(await redis.hget(taskQueue.toKey(taskId), 'dtqDlqRouted')).toBeNull();
    expect(await redis.hget(key, 'PROCESS_INVOICE')).toBe('00');
  });

  it('replays into a new job owned by the original tenant and removes the dead-letter entry', async () => {
    const taskId = await deadLetteredInvoice();
    billingUp = true;

    const replay = await admin('post', `/admin/api/dead-letter/${taskId}/replay`).set(
      'X-Request-Id',
      'it-replay-1',
    );
    expect(replay.status).toBe(202);
    expect(replay.body.replayOf).toBe(taskId);
    const newTaskId = replay.body.taskId as string;
    expect(newTaskId).not.toBe(taskId);

    expect(await deadLetterQueue.getJob(deadLetterJobId(taskId))).toBeUndefined();
    await waitForJobState(newTaskId, ['completed']);

    // The original client (and its rotated key) can follow the replay; other tenants cannot.
    const owner = await request(app).get(`/api/v1/tasks/${newTaskId}`).set('X-API-Key', KEY_A2);
    expect(owner.status).toBe(200);
    expect(owner.body).toMatchObject({
      state: 'completed',
      result: { taskType: 'PROCESS_INVOICE', status: 'PAID' },
    });
    expect(
      (await request(app).get(`/api/v1/tasks/${newTaskId}`).set('X-API-Key', KEY_B)).status,
    ).toBe(404);

    // Replaying again is a 404 because the entry is gone.
    expect((await admin('post', `/admin/api/dead-letter/${taskId}/replay`)).status).toBe(404);

    const records = await auditRecords();
    expect(
      records.find(
        (record) => record.action === 'dead_letter.replay' && record.outcome === 'success',
      ),
    ).toMatchObject({
      actor: 'operator',
      requestId: 'it-replay-1',
      tenantId: TENANT_A,
      taskId,
      newTaskId,
      statusCode: 202,
      timestamp: expect.any(String),
    });
    expect(
      records.some(
        (record) => record.action === 'dead_letter.replay' && record.outcome === 'not_found',
      ),
    ).toBe(true);
  });

  it('deletes a dead-letter entry, audits it, and returns 404 afterwards', async () => {
    const taskId = await deadLetteredInvoice();

    const removed = await admin('delete', `/admin/api/dead-letter/${taskId}`).set(
      'X-Request-Id',
      'it-delete-1',
    );
    expect(removed.status).toBe(204);
    expect(await deadLetterQueue.getJob(deadLetterJobId(taskId))).toBeUndefined();
    expect((await admin('delete', `/admin/api/dead-letter/${taskId}`)).status).toBe(404);

    const records = await auditRecords();
    expect(
      records.find(
        (record) => record.action === 'dead_letter.delete' && record.outcome === 'success',
      ),
    ).toMatchObject({
      actor: 'operator',
      requestId: 'it-delete-1',
      tenantId: TENANT_A,
      taskId,
      statusCode: 204,
    });
  });

  it('audits rejected admin access without persisting credentials', async () => {
    const wrongPassword = `Basic ${Buffer.from('operator:wrong-password').toString('base64')}`;
    const denied = await request(app)
      .get('/admin/api/dead-letter')
      .set('Authorization', wrongPassword);
    expect(denied.status).toBe(401);

    const records = await waitFor(async () => {
      const items = await auditRecords();
      return items.some((record) => record.action === 'admin.auth_failed') ? items : undefined;
    }, 'admin.auth_failed audit record');
    expect(records.find((record) => record.action === 'admin.auth_failed')).toMatchObject({
      outcome: 'denied',
      actor: null,
      statusCode: 401,
      path: '/admin/api/dead-letter',
    });

    const persisted = JSON.stringify(await redis.xrange(AUDIT_STREAM_KEY, '-', '+'));
    for (const secret of [ADMIN_PASSWORD, 'wrong-password', KEY_A1, KEY_A2, KEY_B, 'Basic ']) {
      expect(persisted).not.toContain(secret);
    }
  });
});
