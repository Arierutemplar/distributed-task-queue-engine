import request from 'supertest';
import type { Express } from 'express';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { tenantIdForApiKey } from '../../src/middleware/http.middleware.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { KEY_A1, KEY_A2, KEY_B, TENANT_A } from './constants.js';
import {
  buildApp,
  closeConnections,
  documentPayload,
  invoicePayload,
  metricValue,
  resetState,
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
