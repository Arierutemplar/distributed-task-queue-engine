import type { Express } from 'express';
import request from 'supertest';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../../src/app.js';
import { config } from '../../src/config/env.js';
import { tenantIdForApiKey } from '../../src/middleware/http.middleware.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { createTaskWorker } from '../../src/workers/task.worker.js';
import { KEY_A1, KEY_B } from './constants.js';
import {
  adminAuthHeader,
  closeConnections,
  documentPayload,
  resetState,
  waitForJobState,
} from './helpers.js';

beforeEach(resetState);
afterAll(closeConnections);

describe('admission after Bull Board mutations', () => {
  const appWithQuota = (): Promise<Express> =>
    createApp({ config: { ...config, TENANT_MAX_INFLIGHT: 1 } });
  const submit = (app: Express, key: string) =>
    request(app).post('/api/v1/tasks/process').set('X-API-Key', key).send(documentPayload);
  const board = (app: Express, method: 'put' | 'patch', path: string) =>
    request(app)
      [method](`/admin/queues/api/queues/tasks${path}`)
      .set('Authorization', adminAuthHeader);

  it('reflects a changed tenant, single-job removal, and queue drain', async () => {
    const app = await appWithQuota();
    const first = await submit(app, KEY_A1);
    expect(first.status).toBe(202);
    expect((await submit(app, KEY_A1)).status).toBe(429);
    const job = await taskQueue.getJob(first.body.taskId as string);
    if (!job) throw new Error('Expected waiting job');

    const updated = await board(app, 'patch', `/${job.id}/update-data`).send({
      jobData: { ...job.data, meta: { ...job.data.meta, tenantId: tenantIdForApiKey(KEY_B) } },
    });
    expect(updated.status).toBe(200);
    expect((await submit(app, KEY_B)).status).toBe(429);
    expect((await submit(app, KEY_A1)).status).toBe(202);
    expect((await board(app, 'put', `/${job.id}/clean`)).status).toBe(204);
    expect((await submit(app, KEY_B)).status).toBe(202);

    expect((await board(app, 'put', '/empty')).status).toBe(200);
    expect((await submit(app, KEY_A1)).status).toBe(202);
    expect((await submit(app, KEY_B)).status).toBe(202);
  });

  it('counts a completed job that Bull Board retries as inflight again', async () => {
    const app = await appWithQuota();
    const worker = createTaskWorker({ concurrency: 1 });
    let taskId: string;
    try {
      const accepted = await submit(app, KEY_A1);
      expect(accepted.status).toBe(202);
      taskId = accepted.body.taskId as string;
      await waitForJobState(taskId, ['completed']);
    } finally {
      await worker.close();
    }

    expect((await board(app, 'put', `/${taskId}/retry`)).status).toBe(204);
    const rejected = await submit(app, KEY_A1);
    expect(rejected.status).toBe(429);
    expect(rejected.headers['retry-after']).toBe('1');
    expect((await board(app, 'put', `/${taskId}/clean`)).status).toBe(204);
    expect((await submit(app, KEY_A1)).status).toBe(202);
  });
});
