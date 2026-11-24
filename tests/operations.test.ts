import type { Express } from 'express';
import request from 'supertest';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  deadLetterGetJob: vi.fn(),
  deadLetterAdd: vi.fn(),
  getWaiting: vi.fn(),
  getWaitingCount: vi.fn(),
}));

vi.mock('../src/config/redis.js', () => ({ redis: {}, pingRedis: vi.fn() }));

vi.mock('../src/queues/task.queue.js', () => ({
  TASK_QUEUE_NAME: 'tasks',
  TASK_MAX_ATTEMPTS: 5,
  taskQueue: {
    name: 'tasks',
    add: mocks.add,
    getJob: vi.fn(),
    getJobCounts: vi.fn(async () => ({ waiting: 3, failed: 1 })),
  },
}));

vi.mock('../src/queues/dead-letter.queue.js', () => ({
  DEAD_LETTER_QUEUE_NAME: 'tasks-dead-letter',
  DEAD_LETTER_JOB_NAME: 'dead-letter',
  deadLetterJobId: (id: string) => `dlq-${id}`,
  deadLetterQueue: {
    name: 'tasks-dead-letter',
    add: mocks.deadLetterAdd,
    getJob: mocks.deadLetterGetJob,
    getWaiting: mocks.getWaiting,
    getWaitingCount: mocks.getWaitingCount,
    getJobCounts: vi.fn(async () => ({ waiting: 2 })),
  },
}));

vi.mock('../src/routes/board.routes.js', async () => {
  const { Router } = await import('express');
  const router = Router();
  router.get('/', (_request, response) => {
    response.send('bull-board');
  });
  return { boardRouter: router };
});

import { createApp } from '../src/app.js';
import { loadConfig } from '../src/config/env.js';
import type { AuditLog } from '../src/observability/audit.js';
import { routeToDeadLetter } from '../src/workers/task.worker.js';
import type { TaskJob } from '../src/workers/task.processor.js';

const ADMIN_PASSWORD = 'correct-horse-battery';
const basic = (user: string, password: string) =>
  `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`;

const auditLog = {
  record: vi.fn<AuditLog['record']>(async () => undefined),
  list: vi.fn<AuditLog['list']>(async () => []),
};
const auditActions = () => auditLog.record.mock.calls.map(([event]) => event.action);

const buildApp = (env: Record<string, string> = {}, readinessFails = false): Promise<Express> =>
  createApp({
    config: loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent', ADMIN_PASSWORD, ...env }),
    auditLog,
    readinessChecks: [
      {
        name: 'redis',
        check: () => {
          if (readinessFails) {
            throw new Error('Redis ping timed out');
          }
        },
      },
    ],
  });

const invoice = {
  type: 'PROCESS_INVOICE',
  customerId: 'customer-1',
  amount: 10,
  currency: 'USD',
  taxId: 'TAX-1',
  items: [{ description: 'Item', quantity: 1, unitPrice: 10 }],
};

beforeEach(() => {
  mocks.add.mockResolvedValue({ id: 'replayed-1' });
});

describe('health and metrics', () => {
  it('reports liveness independently of dependencies', async () => {
    const response = await request(await buildApp({}, true)).get('/health/live');
    expect(response.status).toBe(200);
    expect(response.body.status).toBe('ok');
  });

  it('returns 503 from readiness when a dependency check fails', async () => {
    const ready = await request(await buildApp()).get('/health/ready');
    expect(ready.status).toBe(200);
    expect(ready.body).toEqual({ status: 'ready', checks: { redis: { status: 'up' } } });

    const notReady = await request(await buildApp({}, true)).get('/health/ready');
    expect(notReady.status).toBe(503);
    expect(notReady.body.checks.redis).toEqual({ status: 'down', error: 'Redis ping timed out' });
  });

  it('exposes Prometheus metrics including queue depth', async () => {
    const response = await request(await buildApp()).get('/metrics');
    expect(response.status).toBe(200);
    expect(response.text).toContain('queue_jobs{queue="tasks",state="waiting"');
    expect(response.text).toContain('queue_jobs{queue="tasks-dead-letter",state="waiting"');
  });

  it('only mounts health and metrics in the worker role', async () => {
    const app = await buildApp({ PROCESS_ROLE: 'worker' });
    expect((await request(app).post('/api/v1/tasks/process').send(invoice)).status).toBe(404);
    expect((await request(app).get('/health/live')).status).toBe(200);
  });
});

describe('admin surface', () => {
  it('is disabled when ADMIN_PASSWORD is not configured', async () => {
    const app = await createApp({ config: loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent' }) });
    const response = await request(app).get('/admin/queues');
    expect(response.status).toBe(503);
  });

  it('requires valid Basic credentials', async () => {
    const app = await buildApp();

    const anonymous = await request(app).get('/admin/queues');
    expect(anonymous.status).toBe(401);
    expect(anonymous.headers['www-authenticate']).toContain('Basic');

    const wrong = await request(app)
      .get('/admin/queues')
      .set('Authorization', basic('admin', 'nope'));
    expect(wrong.status).toBe(401);

    const ok = await request(app)
      .get('/admin/queues')
      .set('Authorization', basic('admin', ADMIN_PASSWORD));
    expect(ok.status).toBe(200);
    expect(ok.text).toBe('bull-board');
  });

  it('locks out a client after repeated failed logins without counting successful requests', async () => {
    const app = await buildApp({ ADMIN_AUTH_MAX_FAILURES: '2' });
    const login = (password: string) =>
      request(app).get('/admin/queues').set('Authorization', basic('admin', password));

    for (let i = 0; i < 3; i += 1) {
      expect((await login(ADMIN_PASSWORD)).status).toBe(200);
    }
    expect((await login('wrong-1')).status).toBe(401);
    expect((await login('wrong-2')).status).toBe(401);

    const locked = await login(ADMIN_PASSWORD);
    expect(locked.status).toBe(429);
    expect(locked.body.error).toBe('Too many failed admin login attempts');
  });

  it('lists and replays dead-letter entries', async () => {
    const app = await buildApp();
    const remove = vi.fn();
    const entry = {
      data: {
        originalJobId: '9',
        taskType: 'PROCESS_INVOICE',
        data: {
          ...invoice,
          meta: {
            requestId: 'original',
            tenantId: 'key-original-tenant',
            payloadHash: 'h',
            acceptedAt: 'a',
          },
        },
        failedReason: 'Billing service is temporarily unavailable',
        attemptsMade: 5,
        failedAt: '2026-01-01T00:00:00.000Z',
      },
      remove,
    };
    mocks.getWaitingCount.mockResolvedValue(1);
    mocks.getWaiting.mockResolvedValue([entry]);
    mocks.deadLetterGetJob.mockResolvedValue(entry);

    const list = await request(app)
      .get('/admin/api/dead-letter?limit=10')
      .set('Authorization', basic('admin', ADMIN_PASSWORD));
    expect(list.status).toBe(200);
    expect(list.body).toMatchObject({
      total: 1,
      items: [{ taskId: '9', requestId: 'original', tenantId: 'key-original-tenant' }],
    });
    expect(mocks.getWaiting).toHaveBeenCalledWith(0, 9);

    const replay = await request(app)
      .post('/admin/api/dead-letter/9/replay')
      .set('Authorization', basic('admin', ADMIN_PASSWORD));
    expect(replay.status).toBe(202);
    expect(replay.body).toMatchObject({ taskId: 'replayed-1', replayOf: '9', status: 'accepted' });
    expect(mocks.add).toHaveBeenCalledWith(
      'PROCESS_INVOICE',
      {
        ...invoice,
        meta: expect.objectContaining({ replayOf: '9', tenantId: 'key-original-tenant' }),
      },
      undefined,
    );
    expect(mocks.deadLetterGetJob).toHaveBeenCalledWith('dlq-9');
    expect(remove).toHaveBeenCalledOnce();

    expect(auditLog.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: 'dead_letter.replay',
        outcome: 'success',
        actor: 'admin',
        requestId: replay.headers['x-request-id'],
        tenantId: 'key-original-tenant',
        taskId: '9',
        newTaskId: 'replayed-1',
        statusCode: 202,
      }),
    );
  });

  it('audits dead-letter deletions, including entries that do not exist', async () => {
    const app = await buildApp();
    const remove = vi.fn();
    mocks.deadLetterGetJob.mockResolvedValueOnce({
      data: { originalJobId: '5', data: { ...invoice, meta: { tenantId: 'acme' } } },
      remove,
    });

    const deleted = await request(app)
      .delete('/admin/api/dead-letter/5')
      .set('Authorization', basic('admin', ADMIN_PASSWORD));
    expect(deleted.status).toBe(204);
    expect(remove).toHaveBeenCalledOnce();

    mocks.deadLetterGetJob.mockResolvedValueOnce(undefined);
    const missing = await request(app)
      .delete('/admin/api/dead-letter/6')
      .set('Authorization', basic('admin', ADMIN_PASSWORD));
    expect(missing.status).toBe(404);

    const events = auditLog.record.mock.calls.map(([event]) => event);
    expect(events).toEqual([
      expect.objectContaining({
        action: 'dead_letter.delete',
        outcome: 'success',
        actor: 'admin',
        taskId: '5',
        tenantId: 'acme',
      }),
      expect.objectContaining({
        action: 'dead_letter.delete',
        outcome: 'not_found',
        actor: 'admin',
        taskId: '6',
      }),
    ]);
  });

  it('audits rejected credentials and lockouts without recording any secret', async () => {
    const app = await buildApp({ ADMIN_AUTH_MAX_FAILURES: '1' });
    await request(app)
      .get('/admin/queues')
      .set('Authorization', basic('admin', 'wrong-password-1'));
    await request(app)
      .get('/admin/queues')
      .set('Authorization', basic('admin', 'wrong-password-2'));

    expect(auditActions()).toEqual(['admin.auth_failed', 'admin.auth_locked']);
    const serialized = JSON.stringify(auditLog.record.mock.calls);
    expect(serialized).not.toContain('wrong-password');
    expect(serialized).not.toContain(ADMIN_PASSWORD);
    expect(auditLog.record.mock.calls[0]?.[0]).toMatchObject({
      actor: null,
      outcome: 'denied',
      statusCode: 401,
    });
  });

  it('serves the audit trail to authenticated operators only', async () => {
    const app = await buildApp();
    auditLog.list.mockResolvedValueOnce([
      {
        timestamp: '2026-01-01T00:00:00.000Z',
        action: 'dead_letter.delete',
        outcome: 'success',
        actor: 'admin',
        requestId: 'r',
        method: 'DELETE',
        path: '/admin/api/dead-letter/1',
      },
    ]);

    expect((await request(app).get('/admin/api/audit')).status).toBe(401);
    const response = await request(app)
      .get('/admin/api/audit?limit=5')
      .set('Authorization', basic('admin', ADMIN_PASSWORD));
    expect(response.status).toBe(200);
    expect(response.body.items).toHaveLength(1);
    expect(auditLog.list).toHaveBeenCalledWith(5);
    expect(auditActions()).toContain('admin.audit_read');
  });
});

describe('rate limiting', () => {
  it('returns 429 once the per-window budget is spent', async () => {
    const app = await buildApp({ RATE_LIMIT_MAX: '2' });
    const send = () => request(app).post('/api/v1/tasks/process').send(invoice);

    expect((await send()).status).toBe(202);
    expect((await send()).status).toBe(202);
    const limited = await send();
    expect(limited.status).toBe(429);
    expect(limited.body.error).toBe('Too many requests');
  });
});

describe('routeToDeadLetter', () => {
  const job = (state: string) =>
    ({
      id: '42',
      name: 'PROCESS_INVOICE',
      data: invoice,
      attemptsMade: 5,
      failedReason: 'Billing service is temporarily unavailable',
      finishedOn: Date.parse('2026-01-01T00:00:00.000Z'),
      getState: vi.fn(async () => state),
    }) as unknown as TaskJob;

  it('copies terminally failed jobs with a deterministic id', async () => {
    await expect(routeToDeadLetter(job('failed'), new Error('x'))).resolves.toBe(true);
    expect(mocks.deadLetterAdd).toHaveBeenCalledWith(
      'dead-letter',
      expect.objectContaining({
        originalJobId: '42',
        attemptsMade: 5,
        failedAt: '2026-01-01T00:00:00.000Z',
      }),
      { jobId: 'dlq-42' },
    );
  });

  it('ignores attempts that BullMQ will retry', async () => {
    await expect(routeToDeadLetter(job('delayed'), new Error('x'))).resolves.toBe(false);
    expect(mocks.deadLetterAdd).not.toHaveBeenCalled();
  });
});
