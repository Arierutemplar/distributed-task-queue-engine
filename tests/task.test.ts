import type { Express } from 'express';
import request from 'supertest';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  getJob: vi.fn(),
  deadLetterGetJob: vi.fn(),
}));

vi.mock('../src/config/redis.js', () => ({ redis: {}, pingRedis: vi.fn() }));

vi.mock('../src/queues/task.queue.js', () => ({
  TASK_QUEUE_NAME: 'tasks',
  TASK_MAX_ATTEMPTS: 5,
  taskQueue: {
    name: 'tasks',
    add: mocks.add,
    getJob: mocks.getJob,
    getJobCounts: vi.fn(async () => ({})),
  },
}));

vi.mock('../src/queues/dead-letter.queue.js', () => ({
  DEAD_LETTER_QUEUE_NAME: 'tasks-dead-letter',
  DEAD_LETTER_JOB_NAME: 'dead-letter',
  deadLetterJobId: (id: string) => `dlq-${id}`,
  deadLetterQueue: {
    name: 'tasks-dead-letter',
    getJob: mocks.deadLetterGetJob,
    getJobCounts: vi.fn(async () => ({})),
  },
}));

vi.mock('../src/routes/board.routes.js', async () => {
  const { Router } = await import('express');
  return { boardRouter: Router() };
});

import { createApp } from '../src/app.js';
import { loadConfig } from '../src/config/env.js';
import { idempotentJobId, payloadFingerprint } from '../src/services/task.service.js';
import { tenantIdForApiKey } from '../src/middleware/http.middleware.js';
import { PUBLIC_TENANT_ID, type TaskPayload } from '../src/types/task.types.js';

const JOB_ID = 'job-123e4567-e89b-12d3-a456-426614174000';

const documentPayload = {
  type: 'PROCESS_DOCUMENT',
  documentId: 'document-42',
  content: 'A valid document body.',
} as const;

const invoicePayload = {
  type: 'PROCESS_INVOICE' as const,
  customerId: 'customer-42',
  amount: 125.5,
  currency: 'USD',
  taxId: 'TAX-123',
  items: [{ description: 'Consulting services', quantity: 2, unitPrice: 62.75 }],
};

let app: Express;

beforeAll(async () => {
  app = await createApp({ config: loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent' }) });
});

describe('POST /api/v1/tasks/process', () => {
  beforeEach(() => {
    mocks.add.mockResolvedValue({ id: JOB_ID });
    mocks.getJob.mockResolvedValue(undefined);
    mocks.deadLetterGetJob.mockResolvedValue(undefined);
  });

  it('returns 400 when the payload fails Zod validation', async () => {
    const response = await request(app)
      .post('/api/v1/tasks/process')
      .send({ type: 'PROCESS_DOCUMENT', documentId: '', content: '' });

    expect(response.status).toBe(400);
    expect(response.body).toMatchObject({ error: 'Invalid task payload' });
    expect(response.body.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: 'documentId' }),
        expect.objectContaining({ path: 'content' }),
      ]),
    );
    expect(mocks.add).not.toHaveBeenCalled();
  });

  it.each([
    ['PROCESS_DOCUMENT', documentPayload],
    ['PROCESS_INVOICE', invoicePayload],
    [
      'SEND_NOTIFICATION',
      { type: 'SEND_NOTIFICATION', recipient: 'ops@example.com', subject: 'Hi', message: 'Body' },
    ],
  ])(
    'accepts a valid %s task with 202, Location and correlation metadata',
    async (type, payload) => {
      const response = await request(app)
        .post('/api/v1/tasks/process')
        .set('X-Request-Id', 'req-abc-123')
        .send(payload);

      expect(response.status).toBe(202);
      expect(response.body).toMatchObject({ taskId: JOB_ID, taskType: type, status: 'accepted' });
      expect(response.body.acceptedAt).toEqual(expect.any(String));
      expect(response.headers.location).toBe(`/api/v1/tasks/${JOB_ID}`);
      expect(response.headers['x-request-id']).toBe('req-abc-123');
      expect(response.headers['idempotent-replayed']).toBeUndefined();
      expect(mocks.add).toHaveBeenCalledWith(
        type,
        {
          ...payload,
          meta: expect.objectContaining({
            requestId: 'req-abc-123',
            payloadHash: payloadFingerprint(payload as TaskPayload),
          }),
        },
        undefined,
      );
    },
  );

  it('uses the Idempotency-Key as a deterministic job id and replays the original acceptance', async () => {
    const jobId = idempotentJobId(PUBLIC_TENANT_ID, 'order-77');
    mocks.add.mockResolvedValue({ id: jobId });
    mocks.getJob.mockResolvedValue({
      data: {
        ...invoicePayload,
        meta: {
          requestId: 'first-request',
          payloadHash: payloadFingerprint(invoicePayload),
          acceptedAt: '2026-01-01T00:00:00.000Z',
        },
      },
    });

    const response = await request(app)
      .post('/api/v1/tasks/process')
      .set('Idempotency-Key', 'order-77')
      .send(invoicePayload);

    expect(response.status).toBe(202);
    expect(response.headers['idempotent-replayed']).toBe('true');
    expect(response.body).toMatchObject({ taskId: jobId, acceptedAt: '2026-01-01T00:00:00.000Z' });
    expect(mocks.add).toHaveBeenCalledWith('PROCESS_INVOICE', expect.any(Object), { jobId });
    expect(jobId).toMatch(/^idem-[0-9a-f]{64}$/u);
  });

  it('returns 409 when an Idempotency-Key is reused with a different payload', async () => {
    mocks.getJob.mockResolvedValue({
      data: {
        ...invoicePayload,
        meta: { requestId: 'other', payloadHash: 'f'.repeat(64), acceptedAt: 'x' },
      },
    });

    const response = await request(app)
      .post('/api/v1/tasks/process')
      .set('Idempotency-Key', 'order-77')
      .send(invoicePayload);

    expect(response.status).toBe(409);
    expect(response.body.error).toMatch(/different payload/u);
  });

  it('rejects a malformed Idempotency-Key before touching the queue', async () => {
    const response = await request(app)
      .post('/api/v1/tasks/process')
      .set('Idempotency-Key', 'has spaces')
      .send(documentPayload);

    expect(response.status).toBe(400);
    expect(mocks.add).not.toHaveBeenCalled();
  });

  it('returns 400 for invalid JSON and 413 for bodies over 1 MB', async () => {
    const invalid = await request(app)
      .post('/api/v1/tasks/process')
      .set('Content-Type', 'application/json')
      .send('{"type":');
    expect(invalid.status).toBe(400);
    expect(invalid.body.error).toBe('Invalid JSON request body');

    const tooLarge = await request(app)
      .post('/api/v1/tasks/process')
      .send({ ...documentPayload, content: 'x'.repeat(1_100_000) });
    expect(tooLarge.status).toBe(413);
    expect(mocks.add).not.toHaveBeenCalled();
  });

  it('returns 500 with a request id when the queue is unavailable', async () => {
    mocks.add.mockRejectedValue(new Error('Connection is closed'));

    const response = await request(app).post('/api/v1/tasks/process').send(documentPayload);

    expect(response.status).toBe(500);
    expect(response.body).toEqual({
      error: 'Internal server error',
      requestId: expect.any(String),
    });
  });
});

describe('GET /api/v1/tasks/:taskId', () => {
  it('returns the BullMQ state, attempts and result', async () => {
    mocks.getJob.mockResolvedValue({
      name: 'PROCESS_DOCUMENT',
      data: { ...documentPayload, meta: { acceptedAt: '2026-01-01T00:00:00.000Z' } },
      attemptsMade: 1,
      opts: { attempts: 5 },
      timestamp: 1,
      processedOn: Date.parse('2026-01-01T00:00:01.000Z'),
      finishedOn: Date.parse('2026-01-01T00:00:02.000Z'),
      returnvalue: { taskType: 'PROCESS_DOCUMENT' },
      failedReason: undefined,
      getState: vi.fn(async () => 'completed'),
    });

    const response = await request(app).get('/api/v1/tasks/42');

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({
      taskId: '42',
      state: 'completed',
      attemptsMade: 1,
      maxAttempts: 5,
      acceptedAt: '2026-01-01T00:00:00.000Z',
      finishedAt: '2026-01-01T00:00:02.000Z',
      failedReason: null,
    });
  });

  it('falls back to the dead-letter entry after the failed set was trimmed', async () => {
    mocks.getJob.mockResolvedValue(undefined);
    mocks.deadLetterGetJob.mockResolvedValue({
      data: {
        originalJobId: '7',
        taskType: 'PROCESS_INVOICE',
        data: invoicePayload,
        failedReason: 'Billing service is temporarily unavailable',
        attemptsMade: 5,
        failedAt: '2026-01-02T00:00:00.000Z',
      },
    });

    const response = await request(app).get('/api/v1/tasks/7');

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({ state: 'dead-lettered', attemptsMade: 5 });
    expect(mocks.deadLetterGetJob).toHaveBeenCalledWith('dlq-7');
  });

  it('returns 404 for unknown tasks and 400 for malformed ids', async () => {
    mocks.getJob.mockResolvedValue(undefined);
    mocks.deadLetterGetJob.mockResolvedValue(undefined);

    expect((await request(app).get('/api/v1/tasks/missing')).status).toBe(404);
    expect((await request(app).get('/api/v1/tasks/bad%3Aid')).status).toBe(400);
  });
});

describe('API key authentication and tenant isolation', () => {
  const KEY_A = 'tenant-a-key-0123456789abcdef0123456789';
  const KEY_B = 'tenant-b-key-0123456789abcdef0123456789';
  let securedApp: Express;

  beforeAll(async () => {
    securedApp = await createApp({
      config: loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent', API_KEYS: `${KEY_A}, ${KEY_B}` }),
    });
  });

  beforeEach(() => {
    mocks.add.mockResolvedValue({ id: JOB_ID });
    mocks.getJob.mockResolvedValue(undefined);
    mocks.deadLetterGetJob.mockResolvedValue(undefined);
  });

  it('rejects missing or unknown keys before touching the queue', async () => {
    const missing = await request(securedApp).post('/api/v1/tasks/process').send(documentPayload);
    const wrong = await request(securedApp)
      .post('/api/v1/tasks/process')
      .set('X-API-Key', 'not-a-configured-key-0123456789abcdef')
      .send(documentPayload);

    expect(missing.status).toBe(401);
    expect(wrong.status).toBe(401);
    expect(wrong.body).toEqual({
      error: 'Missing or invalid API key',
      requestId: expect.any(String),
    });
    expect(mocks.add).not.toHaveBeenCalled();
  });

  it('stamps the tenant on the job and namespaces idempotency keys per tenant', async () => {
    const tenantA = tenantIdForApiKey(KEY_A);
    const response = await request(securedApp)
      .post('/api/v1/tasks/process')
      .set('X-API-Key', KEY_A)
      .set('Idempotency-Key', 'order-77')
      .send(documentPayload);

    expect(response.status).toBe(202);
    expect(mocks.add).toHaveBeenCalledWith(
      'PROCESS_DOCUMENT',
      expect.objectContaining({ meta: expect.objectContaining({ tenantId: tenantA }) }),
      { jobId: idempotentJobId(tenantA, 'order-77') },
    );
    expect(idempotentJobId(tenantA, 'order-77')).not.toBe(
      idempotentJobId(tenantIdForApiKey(KEY_B), 'order-77'),
    );
  });

  it("returns 404 for another tenant's task, including its dead-letter entry", async () => {
    const meta = { tenantId: tenantIdForApiKey(KEY_A), acceptedAt: '2026-01-01T00:00:00.000Z' };
    mocks.getJob.mockResolvedValue({
      name: 'PROCESS_DOCUMENT',
      data: { ...documentPayload, meta },
      attemptsMade: 1,
      opts: { attempts: 5 },
      timestamp: 1,
      getState: vi.fn(async () => 'completed'),
    });

    expect((await request(securedApp).get('/api/v1/tasks/42').set('X-API-Key', KEY_A)).status).toBe(
      200,
    );
    expect((await request(securedApp).get('/api/v1/tasks/42').set('X-API-Key', KEY_B)).status).toBe(
      404,
    );

    mocks.getJob.mockResolvedValue(undefined);
    mocks.deadLetterGetJob.mockResolvedValue({
      data: {
        originalJobId: '7',
        taskType: 'PROCESS_DOCUMENT',
        data: { ...documentPayload, meta },
        failedReason: 'x',
        attemptsMade: 5,
        failedAt: '2026-01-02T00:00:00.000Z',
      },
    });
    expect((await request(securedApp).get('/api/v1/tasks/7').set('X-API-Key', KEY_B)).status).toBe(
      404,
    );
  });

  it('lets several keys share a stable tenant so rotation keeps access to existing tasks', async () => {
    const OLD_KEY = 'old-key-0123456789abcdef0123456789abcdef';
    const NEW_KEY = 'new-key-0123456789abcdef0123456789abcdef';
    const rotatingApp = await createApp({
      config: loadConfig({
        NODE_ENV: 'test',
        LOG_LEVEL: 'silent',
        API_KEYS: `acme:${OLD_KEY},acme:${NEW_KEY},${KEY_B}`,
      }),
    });

    await request(rotatingApp)
      .post('/api/v1/tasks/process')
      .set('X-API-Key', OLD_KEY)
      .send(documentPayload);
    expect(mocks.add).toHaveBeenLastCalledWith(
      'PROCESS_DOCUMENT',
      expect.objectContaining({ meta: expect.objectContaining({ tenantId: 'acme' }) }),
      undefined,
    );

    mocks.getJob.mockResolvedValue({
      name: 'PROCESS_DOCUMENT',
      data: {
        ...documentPayload,
        meta: { tenantId: 'acme', acceptedAt: '2026-01-01T00:00:00.000Z' },
      },
      attemptsMade: 0,
      opts: { attempts: 5 },
      timestamp: 1,
      getState: vi.fn(async () => 'waiting'),
    });
    expect(
      (await request(rotatingApp).get('/api/v1/tasks/1').set('X-API-Key', NEW_KEY)).status,
    ).toBe(200);
    expect((await request(rotatingApp).get('/api/v1/tasks/1').set('X-API-Key', KEY_B)).status).toBe(
      404,
    );
  });

  it('enforces the optional per-tenant quota independently for each tenant', async () => {
    const quotaApp = await createApp({
      config: loadConfig({
        NODE_ENV: 'test',
        LOG_LEVEL: 'silent',
        API_KEYS: `${KEY_A},${KEY_B}`,
        RATE_LIMIT_TENANT_MAX: '1',
      }),
    });
    const send = (key: string) =>
      request(quotaApp).post('/api/v1/tasks/process').set('X-API-Key', key).send(documentPayload);

    expect((await send(KEY_A)).status).toBe(202);
    const limited = await send(KEY_A);
    expect(limited.status).toBe(429);
    expect(limited.body.error).toBe('Tenant request quota exceeded');
    expect((await send(KEY_B)).status).toBe(202);
  });
});
