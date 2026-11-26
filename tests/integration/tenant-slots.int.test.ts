import { setTimeout as sleep } from 'node:timers/promises';
import request from 'supertest';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../../src/app.js';
import { config } from '../../src/config/env.js';
import { readTenantInflight } from '../../src/queues/admission.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import {
  acquireTenantSlot,
  readTenantActiveSlots,
  releaseTenantSlot,
} from '../../src/queues/tenant-slots.js';
import type { BillingGateway } from '../../src/services/billing.mock.service.js';
import { createTaskWorker } from '../../src/workers/task.worker.js';
import { KEY_A1, KEY_B, TENANT_A } from './constants.js';
import {
  closeConnections,
  documentPayload,
  invoicePayload,
  metricValue,
  resetState,
  waitFor,
  waitForJobState,
} from './helpers.js';

beforeEach(resetState);
afterAll(closeConnections);

describe('distributed tenant worker slots', () => {
  it('expires abandoned leases and does not let an old owner release a replacement', async () => {
    expect(await acquireTenantSlot(TENANT_A, 'job', 'old', 1, 150)).toBe(true);
    expect(await acquireTenantSlot(TENANT_A, 'another', 'other', 1, 150)).toBe(false);
    await sleep(200);
    expect(await acquireTenantSlot(TENANT_A, 'job', 'new', 1, 2_000)).toBe(true);
    await releaseTenantSlot(TENANT_A, 'job', 'old');
    expect((await readTenantActiveSlots()).get(TENANT_A)).toBe(1);
    await releaseTenantSlot(TENANT_A, 'job', 'new');
    expect((await readTenantActiveSlots()).has(TENANT_A)).toBe(false);
  });

  it('keeps worker leases when Bull Board obliterates queue keys', async () => {
    await taskQueue.add('PROCESS_DOCUMENT', { ...documentPayload, type: 'PROCESS_DOCUMENT' });
    expect(await acquireTenantSlot(TENANT_A, 'job', 'owner', 1, 2_000)).toBe(true);
    await taskQueue.pause();
    await taskQueue.obliterate({ force: true });
    expect(await acquireTenantSlot(TENANT_A, 'other', 'owner', 1, 2_000)).toBe(false);
    await releaseTenantSlot(TENANT_A, 'job', 'owner');
    expect((await readTenantActiveSlots()).size).toBe(0);
  });

  it('defers a large backlog without consuming retries and serves another tenant', async () => {
    const app = await createApp({ config: { ...config, TENANT_MAX_INFLIGHT: 100 } });
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = 0;
    const billing: BillingGateway = {
      processInvoice: async () => {
        started += 1;
        await gate;
        return {
          taskType: 'PROCESS_INVOICE',
          invoiceId: '7d0f8c63-0c2a-4c55-9f0e-2a1d9a7d4c11',
          status: 'PAID',
          pdfUrl: 'https://billing.example.test/invoices/1.pdf',
          processedAt: new Date().toISOString(),
        };
      },
    };
    const workers = Array.from({ length: 2 }, () =>
      createTaskWorker({ concurrency: 2, activeSlots: 1, timeoutMs: 50_000, billing }),
    );
    const send = (key: string, payload: object) =>
      request(app).post('/api/v1/tasks/process').set('X-API-Key', key).send(payload);
    try {
      const first = await send(KEY_A1, invoicePayload);
      expect(first.status).toBe(202);
      await waitForJobState(first.body.taskId as string, ['active']);
      const jobs = await taskQueue.addBulk(
        Array.from({ length: 100 }, (_, index) => ({
          name: 'PROCESS_INVOICE' as const,
          data: {
            ...invoicePayload,
            type: 'PROCESS_INVOICE' as const,
            meta: {
              tenantId: TENANT_A,
              requestId: `noisy-${index}`,
              payloadHash: 'load',
              acceptedAt: new Date().toISOString(),
            },
          },
          opts: { removeOnComplete: false },
        })),
      );
      await waitFor(
        async () =>
          ((await metricValue('tenant_fair_share_deferrals_total', { tenant: TENANT_A })) ?? 0) > 0,
        'fair-share deferral',
      );
      const rejected = await send(KEY_A1, invoicePayload);
      expect(rejected.status).toBe(429);
      expect(rejected.headers['retry-after']).toBe('1');
      for (let index = 0; index < 50; index += 1) {
        const start = Date.now();
        const normal = await send(KEY_B, documentPayload);
        expect(normal.status).toBe(202);
        await waitForJobState(normal.body.taskId as string, ['completed']);
        expect(Date.now() - start).toBeLessThan(10_000);
        await sleep(100);
      }
      expect((await readTenantActiveSlots()).get(TENANT_A)).toBe(1);
      expect(await metricValue('tenant_active_slots', { tenant: TENANT_A })).toBe(1);
      expect(started).toBe(1);
      const deferred = await taskQueue.getJobs(['delayed']);
      expect(deferred.some((job) => job.attemptsMade === 0)).toBe(true);
      release?.();
      await waitFor(
        async () => (await taskQueue.getCompletedCount()) === 151,
        'backlog to drain',
        40_000,
      );
      expect(jobs).toHaveLength(100);
      expect((await readTenantActiveSlots()).size).toBe(0);
      expect((await readTenantInflight()).size).toBe(0);
    } finally {
      release?.();
      await Promise.all(workers.map((worker) => worker.close()));
    }
  }, 60_000);
});
