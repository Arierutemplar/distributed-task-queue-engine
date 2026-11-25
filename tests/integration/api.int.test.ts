import request from 'supertest';
import type { Express } from 'express';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { tenantIdForApiKey } from '../../src/middleware/http.middleware.js';
import { createApp } from '../../src/app.js';
import { config } from '../../src/config/env.js';
import { createTaskWorker } from '../../src/workers/task.worker.js';
import { taskPayloadSchema } from '../../src/types/task.types.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { KEY_A1, KEY_A2, KEY_B, TENANT_A } from './constants.js';
import {
  buildApp,
  closeConnections,
  documentPayload,
  invoicePayload,
  metricValue,
  resetState,
  waitForJobState,
} from './helpers.js';

// No worker runs in this file: accepted tasks stay "waiting" in Redis.
describe('task API against a real Redis', () => {
  let app: Express;

  const submit = (
    apiKey: string | undefined,
    body: unknown,
    headers: Record<string, string> = {},
  ) => {
    const call = request(app).post('/api/v1/tasks/process').set(headers);
    return (apiKey ? call.set('X-API-Key', apiKey) : call).send(body as object);
  };
  const status = (apiKey: string, taskId: string) =>
    request(app).get(`/api/v1/tasks/${taskId}`).set('X-API-Key', apiKey);

  beforeAll(async () => {
    app = await buildApp();
  });

  beforeEach(resetState);

  afterAll(closeConnections);

  describe('authentication', () => {
    it('rejects missing and unknown API keys without enqueuing anything', async () => {
      const missing = await submit(undefined, documentPayload);
      const unknown = await submit('unknown-key-0123456789abcdef0123456789', documentPayload);

      expect(missing.status).toBe(401);
      expect(unknown.status).toBe(401);
      expect(unknown.body).toEqual({
        error: 'Missing or invalid API key',
        requestId: unknown.headers['x-request-id'],
      });
      expect(await taskQueue.count()).toBe(0);
    });
  });

  describe('tenant isolation', () => {
    it('atomically enforces tenant inflight quota across concurrent requests', async () => {
      const limited = await createApp({
        config: { ...config, TENANT_MAX_INFLIGHT: 2 },
      });
      const replica = await createApp({ config: { ...config, TENANT_MAX_INFLIGHT: 2 } });
      const send = (key: string, appInstance = limited) =>
        request(appInstance)
          .post('/api/v1/tasks/process')
          .set('X-API-Key', key)
          .send(documentPayload);
      const results = await Promise.all(
        Array.from({ length: 10 }, (_, index) => send(KEY_A1, index % 2 ? replica : limited)),
      );
      expect(results.filter((result) => result.status === 202)).toHaveLength(2);
      const rejected = results.filter((result) => result.status === 429);
      expect(rejected).toHaveLength(8);
      expect(rejected[0]?.headers['retry-after']).toBe('1');
      expect((await send(KEY_B)).status).toBe(202);
      expect(await metricValue('tenant_tasks_inflight', { tenant: TENANT_A })).toBe(2);
      expect(await metricValue('task_backpressure_rejections_total', { reason: 'tenant' })).toBe(8);
    });

    it('enforces queue depth while still permitting idempotent replays', async () => {
      const limited = await createApp({ config: { ...config, QUEUE_MAX_WAITING: 1 } });
      const send = (key: string, idempotencyKey?: string) => {
        const call = request(limited).post('/api/v1/tasks/process').set('X-API-Key', key);
        if (idempotencyKey) call.set('Idempotency-Key', idempotencyKey);
        return call.send(documentPayload);
      };
      expect((await send(KEY_A1, 'same')).status).toBe(202);
      const rejected = await send(KEY_B);
      expect(rejected.status).toBe(429);
      expect(rejected.headers['retry-after']).toBe('1');
      const replay = await send(KEY_A2, 'same');
      expect(replay.status).toBe(202);
      expect(replay.headers['idempotent-replayed']).toBe('true');
      expect(await metricValue('task_backpressure_rejections_total', { reason: 'queue' })).toBe(1);
    });

    it('does not overshoot the queue limit under concurrent submissions', async () => {
      const limited = await createApp({ config: { ...config, QUEUE_MAX_WAITING: 1 } });
      const results = await Promise.all(
        Array.from({ length: 10 }, () =>
          request(limited)
            .post('/api/v1/tasks/process')
            .set('X-API-Key', KEY_A1)
            .send(documentPayload),
        ),
      );
      expect(results.filter((result) => result.status === 202)).toHaveLength(1);
      expect(results.filter((result) => result.status === 429)).toHaveLength(9);
      expect(await taskQueue.getWaitingCount()).toBe(1);
    });

    it('processes another tenant after one tenant reaches its inflight limit', async () => {
      const limited = await createApp({ config: { ...config, TENANT_MAX_INFLIGHT: 2 } });
      const send = (key: string) =>
        request(limited).post('/api/v1/tasks/process').set('X-API-Key', key).send(documentPayload);
      const first = await send(KEY_A1);
      const second = await send(KEY_A1);
      expect((await send(KEY_A1)).status).toBe(429);
      const other = await send(KEY_B);
      expect(other.status).toBe(202);
      const worker = createTaskWorker({ concurrency: 1 });
      try {
        await Promise.all(
          [first, second, other].map((result) =>
            waitForJobState(result.body.taskId as string, ['completed']),
          ),
        );
        expect((await send(KEY_A1)).status).toBe(202);
      } finally {
        await worker.close();
      }
    });

    it('counts delayed retries against the owner until they are removed', async () => {
      const delayed = await taskQueue.add(
        'PROCESS_DOCUMENT',
        {
          ...taskPayloadSchema.parse(documentPayload),
          meta: {
            tenantId: TENANT_A,
            requestId: 'delayed-test',
            payloadHash: 'test',
            acceptedAt: new Date().toISOString(),
          },
        },
        { delay: 60_000 },
      );
      const limited = await createApp({ config: { ...config, TENANT_MAX_INFLIGHT: 1 } });
      const send = () =>
        request(limited)
          .post('/api/v1/tasks/process')
          .set('X-API-Key', KEY_A1)
          .send(documentPayload);
      expect((await send()).status).toBe(429);
      expect(await metricValue('tenant_tasks_inflight', { tenant: TENANT_A })).toBe(1);
      await delayed.remove();
      expect((await send()).status).toBe(202);
    });

    it('counts an active task against its tenant quota while another tenant can enqueue', async () => {
      const limited = await createApp({ config: { ...config, TENANT_MAX_INFLIGHT: 1 } });
      let release: (() => void) | undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const worker = createTaskWorker({
        concurrency: 1,
        billing: {
          processInvoice: async () => {
            await gate;
            return {
              taskType: 'PROCESS_INVOICE',
              invoiceId: '7d0f8c63-0c2a-4c55-9f0e-2a1d9a7d4c11',
              status: 'PAID',
              pdfUrl: 'https://billing.example.test/invoices/1.pdf',
              processedAt: new Date().toISOString(),
            };
          },
        },
      });
      const send = (key: string, body: object) =>
        request(limited).post('/api/v1/tasks/process').set('X-API-Key', key).send(body);
      try {
        const first = await send(KEY_A1, invoicePayload);
        expect(first.status).toBe(202);
        await waitForJobState(first.body.taskId as string, ['active']);
        expect((await send(KEY_A2, documentPayload)).status).toBe(429);
        const other = await send(KEY_B, documentPayload);
        expect(other.status).toBe(202);
        release?.();
        await waitForJobState(other.body.taskId as string, ['completed']);
        expect((await send(KEY_A1, documentPayload)).status).toBe(202);
      } finally {
        release?.();
        await worker.close();
      }
    });

    it("hides one tenant's tasks from another, while rotated keys keep access", async () => {
      const accepted = await submit(KEY_A1, documentPayload);
      expect(accepted.status).toBe(202);
      expect(accepted.headers.location).toBe(`/api/v1/tasks/${accepted.body.taskId}`);
      const { taskId } = accepted.body as { taskId: string };

      const owner = await status(KEY_A1, taskId);
      expect(owner.status).toBe(200);
      expect(owner.body).toMatchObject({
        taskId,
        taskType: 'PROCESS_DOCUMENT',
        state: 'waiting',
        maxAttempts: 3,
      });

      // A second key mapped to the same tenant (key rotation) sees the same task.
      expect((await status(KEY_A2, taskId)).status).toBe(200);
      // Another tenant gets the same 404 as for a task that does not exist.
      const foreign = await status(KEY_B, taskId);
      expect(foreign.status).toBe(404);
      expect(foreign.body).toEqual((await status(KEY_B, 'does-not-exist')).body);

      const stored = await taskQueue.getJob(taskId);
      expect(stored?.data.meta?.tenantId).toBe(TENANT_A);
    });
  });

  describe('idempotency', () => {
    it('replays a legacy payload when version 1 is explicitly supplied', async () => {
      const headers = { 'Idempotency-Key': 'legacy-version' };
      const first = await submit(KEY_A1, documentPayload, headers);
      const replay = await submit(KEY_A2, { ...documentPayload, schemaVersion: 1 }, headers);
      expect(first.status).toBe(202);
      expect(replay.status).toBe(202);
      expect(replay.headers['idempotent-replayed']).toBe('true');
      expect(replay.body).toEqual(first.body);
      expect(await taskQueue.count()).toBe(1);
    });

    it('replays the same task for a repeated key and payload, and rejects a different payload with 409', async () => {
      const headers = { 'Idempotency-Key': 'order-1001' };
      const first = await submit(KEY_A1, invoicePayload, headers);
      const repeat = await submit(KEY_A2, invoicePayload, headers);

      expect(first.status).toBe(202);
      expect(first.headers['idempotent-replayed']).toBeUndefined();
      expect(repeat.status).toBe(202);
      expect(repeat.headers['idempotent-replayed']).toBe('true');
      expect(repeat.body).toEqual(first.body);

      const conflict = await submit(KEY_A1, { ...invoicePayload, amount: 99 }, headers);
      expect(conflict.status).toBe(409);
      expect(conflict.body.error).toBe('Idempotency-Key was already used with a different payload');

      // Idempotency keys are namespaced per tenant.
      const otherTenant = await submit(KEY_B, invoicePayload, headers);
      expect(otherTenant.status).toBe(202);
      expect(otherTenant.body.taskId).not.toBe(first.body.taskId);

      expect(await taskQueue.count()).toBe(2);
      expect(
        await metricValue('tasks_enqueued_total', {
          task_type: 'PROCESS_INVOICE',
          replayed: 'true',
        }),
      ).toBe(1);
      expect(
        await metricValue('tasks_enqueued_total', {
          task_type: 'PROCESS_INVOICE',
          replayed: 'false',
        }),
      ).toBe(2);
    });
  });

  describe('validation', () => {
    it('accepts omitted or version 1 payloads, and rejects unsupported versions', async () => {
      expect((await submit(KEY_A1, documentPayload)).status).toBe(202);
      expect((await submit(KEY_A1, { ...documentPayload, schemaVersion: 1 })).status).toBe(202);
      const invalid = await submit(KEY_A1, { ...documentPayload, schemaVersion: 2 });
      expect(invalid.status).toBe(400);
      expect(invalid.body.issues).toEqual(
        expect.arrayContaining([expect.objectContaining({ path: 'schemaVersion' })]),
      );
      expect(await taskQueue.count()).toBe(2);
    });

    it('returns 400 for invalid payloads, idempotency keys and JSON, without enqueuing', async () => {
      const invalidPayload = await submit(KEY_A1, {
        type: 'SEND_NOTIFICATION',
        recipient: 'not-an-email',
      });
      expect(invalidPayload.status).toBe(400);
      expect(invalidPayload.body.error).toBe('Invalid task payload');
      expect(invalidPayload.body.issues).toEqual(
        expect.arrayContaining([expect.objectContaining({ path: 'recipient' })]),
      );

      const unknownType = await submit(KEY_A1, { type: 'DELETE_EVERYTHING' });
      expect(unknownType.status).toBe(400);

      const badKey = await submit(KEY_A1, documentPayload, { 'Idempotency-Key': 'has spaces' });
      expect(badKey.status).toBe(400);
      expect(badKey.body.error).toBe('Invalid Idempotency-Key header');

      const badJson = await request(app)
        .post('/api/v1/tasks/process')
        .set('X-API-Key', KEY_A1)
        .set('Content-Type', 'application/json')
        .send('{"type":');
      expect(badJson.status).toBe(400);
      expect(badJson.body.error).toBe('Invalid JSON request body');

      expect((await status(KEY_A1, 'bad:id')).status).toBe(400);
      expect(await taskQueue.count()).toBe(0);
    });
  });

  describe('request correlation', () => {
    it('propagates X-Request-Id into the stored job metadata', async () => {
      const accepted = await submit(KEY_B, documentPayload, { 'X-Request-Id': 'it-request-42' });
      expect(accepted.headers['x-request-id']).toBe('it-request-42');

      const job = await taskQueue.getJob(accepted.body.taskId as string);
      expect(job?.data.meta).toMatchObject({
        requestId: 'it-request-42',
        tenantId: tenantIdForApiKey(KEY_B),
        payloadHash: expect.stringMatching(/^[0-9a-f]{64}$/u),
      });
    });
  });
});
